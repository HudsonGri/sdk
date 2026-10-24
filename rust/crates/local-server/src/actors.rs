use super::*;
use prost::Message;
use wire::actors_service_server::ActorsService;
#[cfg(test)]
#[path = "admission_tests.rs"]
mod admission_tests;

impl Actors {
    async fn remember_actor(deployment: &Deployment, id: String, snapshot: Snapshot) {
        let mut actors = deployment.actors.lock().await;
        // RPC completion order is not placement order. Late replies from an old
        // owner cannot regress the fence used by inspection/checkpoint requests.
        if actors
            .get(&id)
            .is_none_or(|current| current.placement.epoch <= snapshot.placement.epoch)
        {
            actors.insert(id, snapshot);
        }
    }
    async fn deployment(
        &self,
        account: Vec<u8>,
        namespace: Option<String>,
    ) -> Result<DeploymentLease, Status> {
        let scope = Scope::new(
            account,
            namespace.ok_or_else(|| invalid("explicit namespace required"))?,
        )?;
        let catalog = self.catalog.lock().await;
        if catalog.closing {
            return Err(Status::unavailable("server admission closed"));
        }
        let deployment = catalog
            .namespaces
            .get(&scope)
            .cloned()
            .ok_or_else(|| Status::not_found("namespace"))?;
        let active = if matches!(deployment.runtime, Instance::Workflow(_)) {
            Some(self.request_admissions.clone().read_owned().await)
        } else {
            None
        };
        Ok(DeploymentLease {
            deployment,
            _active: active,
        })
    }
    async fn deploy(
        &self,
        request: wire::DeploymentRequest,
        worker: bool,
    ) -> Result<wire::DeploymentResponse, Status> {
        acyclic_actors::validate_deployment(&request).map_err(invalid)?;
        if !request.subscriptions.is_empty()
            || !request.bindings.is_empty()
            || !request.jurisdiction.is_empty()
        {
            return Err(Status::unimplemented(
                "local artifacts have explicit compiled resource bindings and package subscriptions",
            ));
        }
        if worker != request.worker_limits.is_some() {
            return Err(invalid("deployment kind disagrees with limits"));
        }
        let scope = Scope::new(request.account_id.clone(), request.name.clone())?;
        let digest: [u8; 32] = request.code_sha256.as_slice().try_into().map_err(invalid)?;
        let mut catalog = self.catalog.lock().await;
        if catalog.closing {
            return Err(Status::unavailable("server admission closed"));
        }
        let method = if worker {
            "deploy_worker"
        } else {
            "deploy_namespace"
        };
        let bytes = request.encode_to_vec();
        if let Some(response) = catalog.replay(&scope, method, &request.idempotency_key, &bytes)? {
            return Ok(response);
        }
        let revision = if worker {
            if !self.registry.workers.contains_key(&digest) {
                return Err(Status::not_found("compiled Worker artifact"));
            }
            if let Some(current) = catalog.workers.get(&scope) {
                if current != &request {
                    return Err(Status::already_exists("Worker deployment differs"));
                }
            } else {
                if request.expected_revision != 0 {
                    return Err(failure("missing deployment revision"));
                }
                catalog.workers.insert(scope.clone(), request.clone());
            }
            1
        } else if let Some(current) = catalog.namespaces.get(&scope) {
            if current.request != request {
                return Err(Status::already_exists("namespace deployment differs"));
            }
            current.revision
        } else {
            if request.expected_revision != 0 {
                return Err(failure("missing deployment revision"));
            }
            let code = self
                .registry
                .packages
                .get(&digest)
                .ok_or_else(|| Status::not_found("compiled Actors artifact"))?;
            let runtime = code.deploy(scope.clone()).await?;
            catalog.namespaces.insert(
                scope.clone(),
                Arc::new(Deployment {
                    request: request.clone(),
                    revision: 1,
                    runtime,
                    actors: Arc::new(Mutex::new(BTreeMap::new())),
                }),
            );
            1
        };
        let response = wire::DeploymentResponse {
            deployment: Some(deployment_observation(&request, revision)),
        };
        catalog.remember(scope, method, request.idempotency_key, bytes, &response);
        Ok(response)
    }
    async fn observe(
        &self,
        account: Vec<u8>,
        namespace: Option<String>,
        id: String,
    ) -> Result<wire::ActorObservation, Status> {
        acyclic_actors::addressing::validate_actor_id(id.as_bytes()).map_err(invalid)?;
        let deployment = self.deployment(account, namespace).await?;
        let snapshots = deployment.actors.lock().await;
        let snapshot = snapshots
            .get(&id)
            .ok_or_else(|| Status::not_found("actor"))?;
        Ok(wire::ActorObservation {
            actor_id: id,
            code_sha256: deployment.request.code_sha256.clone(),
            home_region: "localhost".into(),
            state: wire::ActorState::Active as i32,
            subscriptions: vec![],
            checkpoint_unix_millis: None,
            checkpoint_epoch: snapshot.placement.epoch,
            configuration_revision: deployment.revision,
            namespace: Some(deployment.request.name.clone()),
            account_id: deployment.request.account_id.clone(),
            observation_stream_path: None,
        })
    }
}
fn deployment_observation(
    request: &wire::DeploymentRequest,
    revision: u64,
) -> wire::DeploymentObservation {
    wire::DeploymentObservation {
        name: request.name.clone(),
        code_sha256: request.code_sha256.clone(),
        revision,
        jurisdiction: request.jurisdiction.clone(),
        account_id: request.account_id.clone(),
    }
}

#[tonic::async_trait]
impl ActorsService for Actors {
    async fn deploy_namespace(
        &self,
        request: Request<wire::DeploymentRequest>,
    ) -> Result<Response<wire::DeploymentResponse>, Status> {
        self.deploy(request.into_inner(), false)
            .await
            .map(Response::new)
    }
    async fn deploy_worker(
        &self,
        request: Request<wire::DeploymentRequest>,
    ) -> Result<Response<wire::DeploymentResponse>, Status> {
        self.deploy(request.into_inner(), true)
            .await
            .map(Response::new)
    }
    async fn invoke_actor(
        &self,
        request: Request<wire::InvokeActorRequest>,
    ) -> Result<Response<wire::InvokeActorResponse>, Status> {
        let request = request.into_inner();
        let _admitted = acyclic_actors::domain::InvokeActorRequest::try_from(request.clone())
            .map_err(invalid)?;
        let deployment = self
            .deployment(request.account_id.clone(), request.namespace.clone())
            .await?;
        let Instance::Workflow(runtime) = &deployment.runtime else {
            let Instance::Fetch(fetch) = &deployment.runtime else {
                return Err(failure("missing fetch hook"));
            };
            return fetch.fetch(request).await.map(Response::new);
        };
        let call: Call = serde_json::from_slice(&request.body).map_err(invalid)?;
        if call.placement().actor.to_string() != request.actor_id {
            return Err(invalid("invocation and placement actor disagree"));
        }
        let snapshot = call.apply(runtime.as_ref()).await.map_err(runtime_error)?;
        Self::remember_actor(&deployment, request.actor_id, snapshot.clone()).await;
        Ok(Response::new(wire::InvokeActorResponse {
            status: 200,
            body: serde_json::to_vec(&snapshot).map_err(failure)?,
            headers: vec![],
        }))
    }
    async fn inspect_actor(
        &self,
        request: Request<wire::InspectActorRequest>,
    ) -> Result<Response<wire::InspectActorResponse>, Status> {
        let r = request.into_inner();
        let actor = self.observe(r.account_id, r.namespace, r.actor_id).await?;
        Ok(Response::new(wire::InspectActorResponse {
            actor: Some(actor),
        }))
    }
    async fn checkpoint_actor(
        &self,
        request: Request<wire::CheckpointActorRequest>,
    ) -> Result<Response<wire::CheckpointActorResponse>, Status> {
        let r = request.into_inner();
        let _admitted =
            acyclic_actors::domain::CheckpointActorRequest::try_from(r.clone()).map_err(invalid)?;
        let d = self
            .deployment(r.account_id.clone(), r.namespace.clone())
            .await?;
        let placement = d
            .actors
            .lock()
            .await
            .get(&r.actor_id)
            .ok_or_else(|| Status::not_found("actor"))?
            .placement
            .clone();
        let Instance::Workflow(runtime) = &d.runtime else {
            return Err(Status::unimplemented("fetch guest owns its checkpoints"));
        };
        let snapshot = runtime.recover(placement).await.map_err(runtime_error)?;
        Self::remember_actor(&d, r.actor_id.clone(), snapshot).await;
        Ok(Response::new(wire::CheckpointActorResponse {
            actor: Some(self.observe(r.account_id, r.namespace, r.actor_id).await?),
        }))
    }
    async fn update_namespace(
        &self,
        request: Request<wire::UpdateNamespaceRequest>,
    ) -> Result<Response<wire::UpdateNamespaceResponse>, Status> {
        let r = request.into_inner();
        acyclic_actors::validate_update(&r).map_err(invalid)?;
        let scope = Scope::new(r.account_id.clone(), r.namespace_id.clone())?;
        let mut catalog = self.catalog.lock().await;
        if catalog.closing {
            return Err(Status::unavailable("server admission closed"));
        }
        let bytes = r.encode_to_vec();
        if let Some(response) =
            catalog.replay(&scope, "update_namespace", &r.idempotency_key, &bytes)?
        {
            return Ok(Response::new(response));
        }
        let d = catalog
            .namespaces
            .get_mut(&scope)
            .ok_or_else(|| Status::not_found("namespace"))?;
        if d.revision != r.expected_configuration_revision {
            return Err(failure("namespace revision conflict"));
        }
        // Replacing a live deterministic handler would invalidate its checkpoint.
        // Upgrade needs the package's migration contract, never an implicit reset.
        if d.request.code_sha256 != r.code_sha256
            || d.request.bindings != r.bindings
            || d.request.limits != r.limits
        {
            return Err(failure(
                "live package resource or code replacement requires migration",
            ));
        }
        let revision = d
            .revision
            .checked_add(1)
            .ok_or_else(|| failure("revision exhausted"))?;
        let next = Arc::new(Deployment {
            request: d.request.clone(),
            revision,
            runtime: d.runtime.clone(),
            actors: d.actors.clone(),
        });
        let observation = deployment_observation(&next.request, revision);
        *d = next;
        let response = wire::UpdateNamespaceResponse {
            deployment: Some(observation),
        };
        catalog.remember(
            scope,
            "update_namespace",
            r.idempotency_key,
            bytes,
            &response,
        );
        Ok(Response::new(response))
    }
    async fn invoke_worker(
        &self,
        request: Request<wire::InvokeWorkerRequest>,
    ) -> Result<Response<wire::InvokeActorResponse>, Status> {
        let request = request.into_inner();
        let _admitted = acyclic_actors::domain::InvokeWorkerRequest::try_from(request.clone())
            .map_err(invalid)?;
        let scope = Scope::new(request.account_id.clone(), request.worker_name.clone())?;
        let catalog = self.catalog.lock().await;
        if catalog.closing {
            return Err(Status::unavailable("server admission closed"));
        }
        let digest: [u8; 32] = catalog
            .workers
            .get(&scope)
            .ok_or_else(|| Status::not_found("Worker"))?
            .code_sha256
            .as_slice()
            .try_into()
            .map_err(invalid)?;
        // Workers have no separate application shutdown hook. Keep their
        // admitted request alive in the same drain as workflow RPCs, before
        // releasing the catalog fence or retiring their effect transports.
        let _active = self.request_admissions.clone().read_owned().await;
        drop(catalog);
        self.registry
            .workers
            .get(&digest)
            .ok_or_else(|| Status::not_found("Worker artifact"))?
            .fetch(scope, request)
            .await
            .map(Response::new)
    }
    async fn delete_worker(
        &self,
        request: Request<wire::DeleteWorkerRequest>,
    ) -> Result<Response<wire::DeleteWorkerResponse>, Status> {
        let r = request.into_inner();
        let _admitted =
            acyclic_actors::domain::DeleteWorkerRequest::try_from(r.clone()).map_err(invalid)?;
        let scope = Scope::new(r.account_id.clone(), r.worker_name.clone())?;
        let bytes = r.encode_to_vec();
        let mut catalog = self.catalog.lock().await;
        if catalog.closing {
            return Err(Status::unavailable("server admission closed"));
        }
        if let Some(response) =
            catalog.replay(&scope, "delete_worker", &r.idempotency_key, &bytes)?
        {
            return Ok(Response::new(response));
        }
        catalog.workers.remove(&scope);
        let response = wire::DeleteWorkerResponse {};
        catalog.remember(scope, "delete_worker", r.idempotency_key, bytes, &response);
        Ok(Response::new(response))
    }
    async fn delete_all(
        &self,
        request: Request<wire::DeleteAllRequest>,
    ) -> Result<Response<wire::DeleteAllResponse>, Status> {
        let r = request.into_inner();
        let _admitted =
            acyclic_actors::domain::DeleteAllRequest::try_from(r.clone()).map_err(invalid)?;
        // Checkpoints are runtime-owned. Never report success after deleting
        // just a transport cache while leaving a live journal/effect owner.
        let _d = self.deployment(r.account_id, r.namespace).await?;
        Err(Status::unimplemented(
            "package runtime does not expose deleteAll",
        ))
    }
    async fn add_subscription(
        &self,
        _: Request<wire::AddSubscriptionRequest>,
    ) -> Result<Response<wire::AddSubscriptionResponse>, Status> {
        Err(Status::unimplemented(
            "subscriptions are owned by the installed package manifest",
        ))
    }
    async fn remove_subscription(
        &self,
        _: Request<wire::RemoveSubscriptionRequest>,
    ) -> Result<Response<wire::RemoveSubscriptionResponse>, Status> {
        Err(Status::unimplemented(
            "subscriptions are owned by the installed package manifest",
        ))
    }
    async fn resume_subscription(
        &self,
        _: Request<wire::ResumeSubscriptionRequest>,
    ) -> Result<Response<wire::ResumeSubscriptionResponse>, Status> {
        Err(Status::unimplemented(
            "subscriptions are owned by the installed package manifest",
        ))
    }
}

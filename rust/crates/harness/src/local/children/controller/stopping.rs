use super::*;

// Controls may fail after accepting a terminal intent. Keep the original effect
// future alive until its cancellation and final publications have drained.
pub(super) async fn drain(
    effect: impl Future<Output = Result<()>>,
    controls: impl Future<Output = Result<std::convert::Infallible>>,
    cancellation: &acyclic_fs::CancellationToken,
) -> Result<()> {
    futures::pin_mut!(effect, controls);
    match futures::future::select(controls, effect).await {
        futures::future::Either::Right((result, _)) => result,
        futures::future::Either::Left((result, effect)) => {
            let error = match result {
                Err(error) => error,
                Ok(never) => match never {},
            };
            cancellation.cancel();
            #[cfg(acyclic_harness_loom)]
            if crate::task_volume::model_checking::enabled(
                crate::task_volume::model_checking::Fault::DropActiveChild,
            ) {
                return Err(error);
            }
            effect.await?;
            Err(error)
        }
    }
}

#[cfg(all(test, acyclic_harness_loom, acyclic_fs_loom))]
mod models;

pub(in crate::local::children) fn requested(state: ChildState) -> bool {
    matches!(
        state,
        ChildState::Interrupted | ChildState::Cancelled | ChildState::Archived
    )
}

// Parent cancellation is another file write. Retry only after the existing
// child follow supplies the competing accepted cut; never resample or poll.
pub(in crate::local::children) async fn cancel(
    files: &Files,
    reference: &Component,
    observation: &mut crate::task_volume::Observation,
    values: &mut BTreeMap<FileKey, FileValue>,
    generation: &mut Generation,
) -> Result<()> {
    let control = FileValue::new(
        FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Control,
        },
        Control::Cancel.bytes().to_vec(),
    );
    loop {
        if matches!(
            desired(reference, values)?,
            ChildState::Cancelled | ChildState::Archived
        ) {
            return Ok(());
        }
        match files.write(*generation, &control).await {
            Ok(next) => {
                *generation = next;
                values.insert(control.key.clone(), control.clone());
                return Ok(());
            }
            Err(crate::task_volume::Error::Conflict(_)) => {
                let change = observation
                    .changes
                    .next()
                    .await
                    .ok_or_else(|| Error::Conflict("inherited control follow ended".into()))?
                    .map_err(storage)?;
                advance_fold(values, generation, change);
            }
            Err(error) => return Err(storage(error)),
        }
    }
}

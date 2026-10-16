use super::*;
use std::sync::Arc;

pub(super) async fn files(context: &ToolContext) -> Result<Arc<TaskFiles<dyn StreamProvider>>> {
    let root = context
        .task()
        .harness
        .task_files
        .as_ref()
        .ok_or_else(|| Error::Unsupported("agent files are absent".into()))?;
    #[cfg(feature = "filesystem")]
    if let Some(child) = root
        .for_created_task(context.task(), context.effective_scope())
        .await?
    {
        return Ok(Arc::new(child));
    }
    #[cfg(not(feature = "filesystem"))]
    if context.task().scope().created_access.is_some() {
        return Err(Error::Unsupported(
            "created agent files require filesystem authority".into(),
        ));
    }
    Ok(root.clone())
}

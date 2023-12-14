use std::{sync::Arc, time::Duration};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::{
    CustomResource,
    runtime::{Controller, watcher::Config, controller::Action},
    Api,
    client, core::ObjectMeta
};
use kube_runtime::reflector::ObjectRef;
use serde::{Deserialize, Serialize};
use garde::Validate;
use schemars::JsonSchema;
use tracing::{warn, info};
//use tokio::sync::mpsc;
use futures::channel::mpsc;
use futures::stream::StreamExt;

use crate::{
    resource::resource::{ReconcileError, ResourceClient, ReconcileAction, reconcile_action, add_finalizer, del_finalizer}, 
    interface::interface::{InterfaceSpec, Interface}
};


#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, Validate, JsonSchema)]
#[kube(group = "virt.dev", version = "v1", kind = "Instance", namespaced)]
#[kube(status = "InstanceStatus")]
#[kube(printcolumn = r#"{"name":"Ip", "jsonPath": ".spec.metadata.team", "type": "string"}"#)]
#[serde(rename_all = "camelCase")]
pub struct InstanceSpec {
    #[garde(skip)]
    pub memory: String,
    #[garde(skip)]
    pub image: String,
    #[garde(skip)]
    pub vcpu: i32,
    #[garde(skip)]
    pub interfaces: Vec<InterfaceSpec>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InstanceStatus {
    state: String,
}

impl Instance{
    pub fn controller(client: client::Client, update_rx: mpsc::Receiver<ObjectRef<Instance>>) -> Controller<Instance> {
        let api = Api::<Instance>::default_namespaced(client.clone());
        Controller::new(api, Config::default())
        .reconcile_on(update_rx.map(|x| (x)))
        .watches(
            Api::<Instance>::all(client),
            Config::default(),
            |object| {
                let object_list = Vec::new();
                object_list.into_iter()
            }
        )
    }

    pub async fn reconcile(g: Arc<Instance>, ctx: Arc<ResourceClient<Instance>>) ->  Result<Action, ReconcileError> {        
        info!("reconciling instance: {:?}", g.metadata.name);
        ctx.setup_finalizer(g, ctx.clone(), Instance::apply, Instance::cleanup).await
    }

    pub async fn cleanup(g: Arc<Instance>, ctx: Arc<ResourceClient<Instance>>) ->  Result<Action, ReconcileError> {      
        info!("cleaning up Instance: {:?}", g.metadata.name);
        if let Err(e) = ctx.lxd_client.as_ref().unwrap().delete(g.metadata.name.clone().unwrap()).await{
            warn!("failed to delete instance: {:?}", e);
            return Err(ReconcileError(e));
        }
        return Ok(Action::requeue(std::time::Duration::from_secs(5 * 60)))
    }

    pub async fn apply(g: Arc<Instance>, ctx: Arc<ResourceClient<Instance>>) ->  Result<Action, ReconcileError> {
        info!("reconciling instance: {:?}", g.metadata.name);
        let instance = match ctx.get::<Instance>(&g.metadata).await?{
            Some(instance) => {
                instance
            },
            None => {
                warn!("instance not found, probably deleted");
                return Ok(Action::requeue(std::time::Duration::from_secs(5 * 60)))
            }
        };
        match reconcile_action(&instance) {
            ReconcileAction::Create => {
                add_finalizer(ctx.api.clone(), &instance.metadata.name.as_ref().unwrap().clone()).await?;
            }
            ReconcileAction::Delete => {
                warn!("instance not found, probably deleted");
                if let Err(e) = ctx.lxd_client.as_ref().unwrap().delete(g.metadata.name.clone().unwrap()).await{
                    warn!("failed to delete instance: {:?}", e);
                    return Err(ReconcileError(e));
                }
                del_finalizer(ctx.api.clone(), &instance.metadata.name.as_ref().unwrap().clone()).await?;
                return Ok(Action::requeue(std::time::Duration::from_secs(5 * 60)))
            }
            ReconcileAction::NoOp => {
            }
        }
        let instance_state = match ctx.lxd_client.as_ref().unwrap().status(instance.metadata.name.clone().unwrap()).await{
            Ok(instance_state) => {
                instance_state
            },
            Err(e) => {
                warn!("failed to get instance state: {:?}", e);
                return Err(ReconcileError(e));
            }
        };
        if let Some(instance_state) = instance_state{
            info!("instance state: {:?}", instance_state);
            if instance_state.status == "Running"{
                info!("instance is running");
                return Ok(Action::requeue(std::time::Duration::from_secs(5 * 60)))
            }
        } else {
            info!("instance not found");
            if let Err(e) = ctx.lxd_client.as_ref().unwrap().create(instance.clone()).await{
                warn!("failed to create instance: {:?}", e);
                return Err(ReconcileError(e));
            
            }
        }
        
        /*
        for interface_spec in &instance.spec.interfaces{
            let mut metadata = ObjectMeta{
                name: Some(format!("{}-{}",instance.metadata.name.as_ref().unwrap().clone(), interface_spec.name.clone())),
                namespace: Some(instance.metadata.namespace.clone().unwrap()),
                ..Default::default()
            };
            match ctx.get::<Interface>(&metadata).await?{
                Some(_) => {
                    info!("interface found: {:?}", metadata);
                },
                None => {
                    info!("interface not found, creating");
                    metadata.owner_references = Some(vec![
                        OwnerReference{
                            api_version: "virt.dev/v1".to_string(),
                            block_owner_deletion: Some(true),
                            controller: Some(true),
                            kind: "Instance".to_string(),
                            name: instance.metadata.name.clone().unwrap(),
                            uid: instance.metadata.uid.clone().unwrap(),
                        }
                    ]);
                    let interface = Interface{
                        metadata: metadata.clone(),
                        spec: interface_spec.clone(),
                        status: None,
                    };
                    match ctx.create(&interface).await?{
                        Some(_) => {
                            info!("interface created: {:?}", metadata);
                        },
                        None => {
                            info!("interface not created: {:?}", metadata);
                        }
                    }
                    
                    
                }
            }
        }
        */
        Ok(Action::requeue(std::time::Duration::from_secs(5 * 60)))
    }
    pub fn error_policy(_g: Arc<Instance>, error: &ReconcileError, _ctx: Arc<ResourceClient<Instance>>) -> Action {
        warn!("reconcile failed: {:?}", error);
        Action::requeue(Duration::from_secs(5 * 60))
    }
}
use std::{sync::Arc, time::Duration};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::{
    CustomResource,
    runtime::{Controller, watcher::Config, controller::Action},
    Api,
    client, core::ObjectMeta
};
use serde::{Deserialize, Serialize};
use garde::Validate;
use schemars::JsonSchema;
use tracing::{warn, info};

use crate::{resource::resource::{ReconcileError, ResourceClient}, interface::interface::{InterfaceSpec, Interface}};


#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, Validate, JsonSchema)]
#[kube(group = "virt.dev", version = "v1", kind = "Instance", namespaced)]
#[kube(status = "InstanceStatus")]
#[kube(printcolumn = r#"{"name":"Ip", "jsonPath": ".spec.metadata.team", "type": "string"}"#)]
#[serde(rename_all = "camelCase")]
pub struct InstanceSpec {
    #[garde(skip)]
    memory: String,
    #[garde(skip)]
    vcpu: i32,
    #[garde(skip)]
    interfaces: Vec<InterfaceSpec>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InstanceStatus {
    state: String,
}

impl Instance{
    pub fn controller(client: client::Client) -> Controller<Instance> {
        let api = Api::<Instance>::default_namespaced(client.clone());
        Controller::new(api, Config::default())
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
        let instance = match ctx.get::<Instance>(&g.metadata).await?{
            Some(instance) => {
                instance
            },
            None => {
                warn!("instance not found, probably deleted");
                return Ok(Action::await_change());
            }
        };
        for interface_spec in &instance.spec.interfaces{
            let mut metadata = ObjectMeta{
                name: Some(format!("{}_{}",instance.metadata.name.as_ref().unwrap().clone(), interface_spec.name.clone())),
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
        Ok(Action::await_change())
    }
    pub fn error_policy(_g: Arc<Instance>, error: &ReconcileError, _ctx: Arc<ResourceClient<Instance>>) -> Action {
        warn!("reconcile failed: {:?}", error);
        Action::requeue(Duration::from_secs(5 * 60))
    }
}
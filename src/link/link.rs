use std::{sync::Arc, time::Duration, collections::BTreeMap};
use k8s_openapi::{api::core::v1::LocalObjectReference, apimachinery::pkg::apis::meta::v1::OwnerReference};
use kube::{CustomResource, runtime::{Controller, watcher::Config, controller::Action}, Api, client, core::ObjectMeta, Resource};
use serde::{Deserialize, Serialize};
use garde::Validate;
use schemars::JsonSchema;
use tracing::{warn, info};

use crate::{
    resource::resource::{ReconcileError, ResourceClient},
    instance::instance::{InstanceInterface, Instance}, interface::interface::{Interface, InterfaceSpec},
};

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, Validate, JsonSchema)]
#[kube(group = "virt.dev", version = "v1", kind = "Link", namespaced)]
#[kube(status = "LinkStatus")]
#[kube(printcolumn = r#"{"name":"Ip", "jsonPath": ".spec.metadata.team", "type": "string"}"#)]
#[serde(rename_all = "camelCase")]
pub struct LinkSpec {
    #[garde(skip)]
    pub network: String,
    #[garde(skip)]
    pub peer1: Peer,
    #[garde(skip)]
    pub peer2: Peer,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Peer {
    pub instance: String,
    pub interface: PeerInterface,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PeerInterface {
    pub mgmt: Option<bool>,
    pub mtu: u32,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LinkStatus {
    pub peer1: bool,
    pub peer2: bool,
}

impl Link{
    pub fn controller(client: client::Client) -> Controller<Link> {
        let api = Api::<Link>::all(client.clone());
        Controller::new(api, Config::default())
        .watches(
            Api::<Interface>::all(client),
            Config::default(),
            |_object| {
                let object_list = Vec::new();
                object_list.into_iter()
            }
        )
    }
    pub async fn reconcile(g: Arc<Link>, ctx: Arc<ResourceClient<Link>>) ->  Result<Action, ReconcileError> {
        ctx.setup_finalizer(g, ctx.clone(), Link::apply, Link::cleanup).await
    }

    pub async fn apply(g: Arc<Link>, ctx: Arc<ResourceClient<Link>>) ->  Result<Action, ReconcileError> {
        info!("reconciling Link: {:?}", g.metadata.name);
        let mut link = match ctx.get::<Link>(&g.metadata).await?{
            Some(link) => {
                link
            },
            None => {
                warn!("link not found, probably deleted");
                return Ok(Action::await_change());
            }
        };

        if link.status.is_none(){
            link.status = Some(LinkStatus::default());
        }

        let mut update = false;

        if !link.status.as_ref().unwrap().peer1{
            if Link::create_interface(&link.spec.peer1, link.meta().namespace.as_ref().unwrap(), &link.spec.network, ctx.clone()).await?{
                link.status.as_mut().unwrap().peer1 = true;
                if let Some(res) = ctx.update_status(&link).await?{
                    link = res;
                }
                update = true;
            }
        }

        if !link.status.as_ref().unwrap().peer2{
            if Link::create_interface(&link.spec.peer2, link.meta().namespace.as_ref().unwrap(), &link.spec.network, ctx.clone()).await?{
                link.status.as_mut().unwrap().peer2 = true;
                ctx.update_status(&link).await?;
                update = true;
            }
        }

        if update{
            //ctx.update_status(&link).await?;
        }
        
        Ok(Action::requeue(Duration::from_secs(5 * 300)))
    }


    pub async fn cleanup(_g: Arc<Link>, _ctx: Arc<ResourceClient<Link>>) ->  Result<Action, ReconcileError> {      
        return Ok(Action::await_change())
    }

    pub fn error_policy(_g: Arc<Link>, error: &ReconcileError, _ctx: Arc<ResourceClient<Link>>) -> Action {
        warn!("reconcile failed: {:?}", error);
        Action::requeue(Duration::from_secs(1))
    }

    async fn create_interface(peer: &Peer, namespace: &str, network: &str, ctx: Arc<ResourceClient<Link>>) -> Result<bool, ReconcileError>{
        let peer_instance_name = peer.instance.clone();
        let peer_instance = match ctx.get::<Instance>(&ObjectMeta{
            name: Some(peer_instance_name.clone()),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        }).await?{
            Some(instance) => {
                instance
            },
            None => {
                warn!("instance not found, probably deleted");
                return Err(ReconcileError(anyhow::anyhow!("instance not found")));
            }
        };
        let peer_instance_status = if let Some(status) = peer_instance.status.as_ref(){
            status
        } else {
            warn!("instance has no status");
            return Err(ReconcileError(anyhow::anyhow!("instance has no status")));
        };

        let mut interface_created = false;

        if peer_instance_status.ready{
            let instance_interface_list = ctx.list::<Interface>(namespace, Some(BTreeMap::from([(
                "virt.dev/instance".to_string(),
                peer_instance_name.clone(),
            )]))).await?;

            let pci_idx = if let Some(interface_list) = instance_interface_list{
                (interface_list.items.len() + 1) as u32
            } else {
                1 as u32
            };

            let intf_name = format!("enp{}s0", pci_idx + 5);

            let intf_meta = ObjectMeta{
                name: Some(format!("{}-{}", peer_instance_name, intf_name)),
                namespace: Some(namespace.to_string()),
                owner_references: Some(vec![
                    OwnerReference{
                        api_version: "virt.dev/v1".to_string(),
                        kind: "Instance".to_string(),
                        name: peer_instance_name.clone(),
                        uid: peer_instance.meta().uid.clone().unwrap(),
                        ..Default::default()
                    }
                ]),
                labels: Some(BTreeMap::from([
                    (
                        "virt.dev/instance".to_string(),
                        peer_instance_name.clone(),
                    ),
                    (
                        "virt.dev/network".to_string(),
                        network.to_string(),
                    )
                ])),
                ..Default::default()
            };
            let res = ctx.get::<Interface>(&intf_meta).await?;
            if res.is_none(){
                let intf = Interface{
                    metadata: intf_meta,
                    spec: InterfaceSpec{
                        name: intf_name,
                        network: LocalObjectReference { name: Some(network.to_string()) },
                        mtu: peer.interface.mtu,
                        mgmt: peer.interface.mgmt,
                        pci_idx,
                    },
                    status: None,
                };
                ctx.create(&intf).await?;
                interface_created = true;
            }
        } else {
            warn!("instance is not ready");
            return Err(ReconcileError(anyhow::anyhow!("instance is not ready")));
        };

        Ok(interface_created)
    } 
}



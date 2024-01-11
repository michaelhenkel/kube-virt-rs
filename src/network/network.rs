use std::{sync::Arc, time::Duration, collections::{HashMap, BTreeMap}};

use anyhow::anyhow;
use k8s_openapi::{apimachinery::pkg::apis::meta::v1::OwnerReference, api::core::v1::LocalObjectReference};
use kube::{CustomResource, runtime::{Controller, watcher::Config, controller::Action}, Api, client, core::ObjectMeta};
use kube_runtime::reflector::ObjectRef;
use serde::{Deserialize, Serialize};
use garde::Validate;
use schemars::JsonSchema;
use tracing::{warn, info};
use ipnet;
use rtnetlink::{NetworkNamespace, new_connection};
use crate::{
    instance::instance::InstanceInterface,
    resource::resource::{ReconcileError, ResourceClient}, interface::{interface::Interface, self}, flowtable::flowtable
};

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, Validate, JsonSchema)]
#[kube(group = "virt.dev", version = "v1", kind = "Network", namespaced)]
#[kube(status = "NetworkStatus")]
#[kube(printcolumn = r#"{"name":"Ip", "jsonPath": ".spec.metadata.team", "type": "string"}"#)]
#[serde(rename_all = "camelCase")]
pub struct NetworkSpec {
    #[garde(skip)]
    pub subnet: String
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NetworkStatus {
    pub unused: Vec<u32>,
    pub last_ip: u32,
    pub gateway: String,
    pub namespace_created: bool,
    pub interfaces: HashMap<String,InstanceInterface>
}

impl Network{
    pub fn controller(client: client::Client) -> Controller<Network> {
        let api = Api::<Network>::all(client.clone());
        Controller::new(api, Config::default())
        .watches(
            Api::<Interface>::all(client),
            Config::default(),
            |object| {
                let mut object_list = Vec::new();
                if let Some(owner_ref) = object.metadata.owner_references.as_ref(){
                    if owner_ref.len() == 1 && owner_ref[0].kind == "Network"{
                        object_list.push(ObjectRef::new(&owner_ref[0].name).
                        within(&object.metadata.namespace.clone().unwrap()));
                    }
                }
                object_list.into_iter()
            }
        )
    }
    pub async fn reconcile(g: Arc<Network>, ctx: Arc<ResourceClient<Network>>) ->  Result<Action, ReconcileError> {        
        info!("reconciling network: {:?}", g.metadata.name);
        ctx.setup_finalizer(g, ctx.clone(), Network::apply, Network::cleanup).await?; 
        Ok(Action::await_change())
    }
    
    pub async fn cleanup(g: Arc<Network>, _ctx: Arc<ResourceClient<Network>>) ->  Result<Action, ReconcileError> {
        if std::path::Path::new(&format!("/var/run/netns/{}", g.metadata.name.as_ref().unwrap().clone())).exists(){
            match NetworkNamespace::del(g.metadata.name.as_ref().unwrap().clone()).await.map_err(|e| ReconcileError(anyhow!("failed to create namespace: {:?}", e))){
                Ok(_) => {
                    info!("namespace deleted");
                },
                Err(e) => {
                    warn!("failed to delete namespace: {:?}", e);
                }
            }
        }
        Ok(Action::await_change())
    }

    pub async fn apply(g: Arc<Network>, ctx: Arc<ResourceClient<Network>>) ->  Result<Action, ReconcileError> {      
        let mut network = match ctx.get::<Network>(&g.metadata).await?{
            Some(network) => {
                network
            },
            None => {
                warn!("network not found, probably deleted");
                return Ok(Action::requeue(std::time::Duration::from_secs(5 * 300)));
            }
        };

        if network.status.is_none(){
            let ip_net: ipnet::Ipv4Net = network.spec.subnet.parse().unwrap();
            let ip_net_octets = ip_net.network().octets();
            let ip_net_dec = u32::from_be_bytes(ip_net_octets);
            let gateway = ip_net_dec + 1;
            let last_ip = std::net::Ipv4Addr::from(gateway);
            let mut status = NetworkStatus::default();
            status.gateway = last_ip.to_string();
            status.last_ip = gateway;
            status.unused = Vec::new();
            network.status = Some(status);
            ctx.update_status(&network).await?;
        }

        match ctx.get::<flowtable::Flowtable>(&g.metadata).await?{
            Some(_) => {},
            None => {
                let flow_table = flowtable::Flowtable{
                    metadata: ObjectMeta{
                        name: network.metadata.name.clone(),
                        namespace: network.metadata.namespace.clone(),
                        owner_references: Some(vec![
                            OwnerReference{
                                api_version: "virt.dev/v1".to_string(),
                                kind: "Network".to_string(),
                                name: network.metadata.name.as_ref().unwrap().clone(),
                                uid: network.metadata.uid.as_ref().unwrap().clone(),
                                ..Default::default()
                            }
                        ]),
                        ..Default::default()
                    }, 
                    spec: flowtable::FlowtableSpec{
                        flow_table_type: flowtable::FlowTableType::Network,
                    },
                    status: None,
                };
                ctx.create(&flow_table).await?;
            }
        }

        /*
        match ctx.get::<flowtable::Flowtable>(&g.metadata).await?{
            Some(_) => {},
            None => {
                let mut flow_table_meta_data = g.metadata.clone();
                flow_table_meta_data.owner_references = Some(vec![
                    OwnerReference{
                        api_version: "virt.dev/v1".to_string(),
                        kind: "Network".to_string(),
                        name: network.metadata.name.as_ref().unwrap().clone(),
                        uid: network.metadata.uid.as_ref().unwrap().clone(),
                        ..Default::default()
                    }
                ]);
                let flow_table = flowtable::Flowtable{
                    metadata: flow_table_meta_data,
                    spec: flowtable::FlowtableSpec{
                        network: LocalObjectReference{
                            name: network.metadata.name.clone(),
                        },
                    },
                    status: None,
                };
                ctx.create(&flow_table).await?;
            }
        }

        let interface_list: Option<kube::core::ObjectList<Interface>> = ctx.list(
            &network.metadata.namespace.unwrap(),
            Some(BTreeMap::from([(
                "network".to_string(),
                network.metadata.name.as_ref().unwrap().clone()
        )]))).await?;
    
        if let Some(interface_list) = interface_list{
            for interface in interface_list.items{

            }
        }
        */

        /*
        let mut update_status = false;
        if std::path::Path::new(&format!("/var/run/netns/{}", network.metadata.name.as_ref().unwrap().clone())).exists(){
            info!("namespace already exists");
        } else {
            info!("creating namespace");
            NetworkNamespace::add(network.metadata.name.as_ref().unwrap().clone()).await.map_err(|e| ReconcileError(anyhow!("failed to create namespace: {:?}", e)))?;
            network.status.as_mut().unwrap().namespace_created = true;
            update_status = true;
            if let Some(res) = ctx.update_status(&network).await?{
                network = res;
            }
        }
        */

        Ok(Action::requeue(std::time::Duration::from_secs(5 * 300)))
    }
    pub fn error_policy(_g: Arc<Network>, error: &ReconcileError, _ctx: Arc<ResourceClient<Network>>) -> Action {
        warn!("reconcile failed: {:?}", error);
        Action::requeue(Duration::from_secs(1))
    }
}

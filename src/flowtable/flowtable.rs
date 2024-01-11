use std::{sync::Arc, time::Duration, collections::BTreeMap};

use kube::{
    CustomResource,
    runtime::{Controller, watcher::Config, controller::Action},
    Api,
    client, Resource, ResourceExt
};
use kube_runtime::reflector::ObjectRef;
use serde::{Deserialize, Serialize};
use garde::Validate;
use schemars::JsonSchema;
use tracing::warn;
use crate::{resource::resource::{ReconcileError, ResourceClient}, instance::{instance::Instance, self}, interface::interface::Interface, link::link::Link};


#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, Validate, JsonSchema)]
#[kube(group = "virt.dev", version = "v1", kind = "Flowtable", namespaced)]
#[kube(status = "FlowtableStatus")]
#[kube(printcolumn = r#"{"name":"Ip", "jsonPath": ".spec.metadata.team", "type": "string"}"#)]
#[serde(rename_all = "camelCase")]
pub struct FlowtableSpec {
    #[garde(skip)]
    pub flow_table_type: FlowTableType,
}

#[derive(Deserialize, Serialize, Clone, Debug, Validate, JsonSchema)]
pub enum FlowTableType{
    Network,
    Instance,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FlowtableStatus {
    pub flows: Vec<Flow>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Flow{
    pub match_type: MatchType,
    pub next_hop: Vec<NextHop>,
}

impl Default for MatchType{
    fn default() -> Self{
        MatchType::Interface{idx: 0}
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq)]
pub enum MatchType{
    Interface{
        idx: u32
    },
    Cidr{
        prefix: u32,
        len: u8
    },
}

impl Default for FlowTableType{
    fn default() -> Self{
        FlowTableType::Network
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NextHop{
    interface: u32,
    source_mac: [u8;6],
    destination_mac: [u8;6],
}

impl Flowtable{
    pub fn controller(client: client::Client) -> Controller<Flowtable> {
        let api = Api::<Flowtable>::default_namespaced(client.clone());
        Controller::new(api, Config::default())
        .watches(
            Api::<Interface>::all(client),
            Config::default(),
            |object| {
                let mut object_list = Vec::new();
                if let Some(status) = &object.status{
                    if status.state.host_ifidx.is_some() && status.state.host_mac.is_some() && status.state.hwaddr != "" {
                        if let Some(labels) = &object.meta().labels{
                            if let Some(network) = labels.get("virt.dev/network"){
                                object_list.push(ObjectRef::new(network).within(object.meta().namespace.as_ref().unwrap()));
                            }
                            if let Some(instance) = labels.get("virt.dev/instance"){
                                object_list.push(ObjectRef::new(instance).within(object.meta().namespace.as_ref().unwrap()));
                            }
                        }
                    }
                }
                object_list.into_iter()
            }
        )
    }
    pub async fn reconcile(g: Arc<Flowtable>, ctx: Arc<ResourceClient<Flowtable>>) ->  Result<Action, ReconcileError> {
        let mut flow_table = match ctx.get::<Flowtable>(&g.metadata).await?{
            Some(flow_table) => {
                flow_table
            },
            None => {
                warn!("flow_table not found, probably deleted");
                return Ok(Action::requeue(std::time::Duration::from_secs(5 * 300)));
            }
        };

        if flow_table.status.is_none(){
            flow_table.status = Some(FlowtableStatus::default());
        }

        match flow_table.spec.flow_table_type{
            FlowTableType::Instance => {
                let instance = match ctx.get::<Instance>(&g.metadata).await?{
                    Some(instance) => {
                        instance
                    },
                    None => {
                        warn!("instance not found, probably deleted");
                        return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                    }
                };
                if let Some(routes) = &instance.spec.routes{
                    let mut update = false;
                    for route in routes{
                        let destination_interface = match ctx.list::<Interface>(g.namespace().unwrap().as_str(), Some(BTreeMap::from([
                            (
                                "virt.dev/instance".to_string(),
                                route.destination.instance.clone(),
                            ),
                            (
                                "virt.dev/network".to_string(),
                                route.destination.network.clone(),
                            ),
                        ]))).await{
                            Ok(destination_interface) => {
                                if let Some(destination_interface) = destination_interface {
                                    if destination_interface.items.len() == 1 {
                                        destination_interface.items[0].clone()
                                    } else {
                                        warn!("destination_interface not found, probably deleted: {}/{}", route.destination.instance, route.destination.network);
                                        return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                                    }
                                } else {
                                    warn!("destination_interface not found, probably deleted");
                                    return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                                }
                            },
                            Err(e) => {
                                warn!("failed to list destination_interface: {:?}", e);
                                return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                            }
                        };

                        let destination_interface_status = if let Some(destination_interface_status) = destination_interface.status{
                            destination_interface_status
                        } else {
                            warn!("destination_interface status not ready");
                            return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                        };

                        let mut next_hop_interface_list = Vec::new();
                        for next_hop in &route.next_hops{
                            let next_hop_interfaces = match ctx.list::<Interface>(g.namespace().unwrap().as_str(), Some(BTreeMap::from([
                                (
                                    "virt.dev/instance".to_string(),
                                    instance.meta().name.as_ref().unwrap().clone(),
                                ),
                                (
                                    "virt.dev/network".to_string(),
                                    next_hop.network.clone(),
                                ),
                            ]))).await{
                                Ok(next_hop_interface) => {
                                    next_hop_interface
                                },
                                Err(e) => {
                                    warn!("failed to list next_hop_interface: {:?}", e);
                                    return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                                }
                            };
                            if let Some(next_hop_interfaces) = next_hop_interfaces{
                                next_hop_interface_list.extend(next_hop_interfaces.items);
                            }
                        }
                        let mut next_hop_list = Vec::new();
                        if next_hop_interface_list.len() > 0{
                            for next_hop_interface in next_hop_interface_list{
                                if let Some(status) = next_hop_interface.status{
                                    let ifidx = if let Some(ifidx) = status.state.instance_ifidx{
                                        ifidx
                                    } else {
                                        warn!("instance_ifidx not found, probably deleted");
                                        return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                                    };
                                    let dst_mac = if let Some(host_mac) = status.state.host_mac{
                                        mac_address_to_bytes(host_mac.as_str()).unwrap()
                                    } else {
                                        warn!("host_mac not found, probably deleted");
                                        return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                                    };
                                
                                    let src_mac = mac_address_to_bytes(status.state.hwaddr.as_str()).unwrap();
                                    let next_hop = NextHop{
                                        interface: ifidx as u32,
                                        source_mac: src_mac,
                                        destination_mac: dst_mac,
                                    };
                                    next_hop_list.push(next_hop);
                                } else {
                                    warn!("next_hop_interface status not ready");
                                    return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                                }
                            }
                        }

                        let ip: std::net::Ipv4Addr = destination_interface_status.prefix.parse().unwrap();
                        let ip = u32::from_be_bytes(ip.octets());
                        let flow = Flow{
                            match_type: MatchType::Cidr{prefix: ip, len: destination_interface_status.prefix_len},
                            next_hop: next_hop_list,
                        };
                        if !flow_table.status.as_ref().unwrap().flows.contains(&flow){
                            flow_table.status.as_mut().unwrap().flows.push(flow);
                            update = true;
                        }

                    }
                    if update{
                        ctx.update_status(&flow_table).await?;
                    }
                }
            },
            FlowTableType::Network => {
                let link_list = match ctx.list::<Link>(g.namespace().unwrap().as_str(), None).await{
                    Ok(link_list) => {
                        link_list
                    },
                    Err(e) => {
                        warn!("failed to list link: {:?}", e);
                        return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                    }
                };
                let mut new_link_list = Vec::new();
                if let Some(link_list) = link_list{
                    for link in &link_list.items{
                        if link.spec.network == flow_table.meta().name.as_ref().unwrap().clone(){
                            new_link_list.push(link.clone());
                        }
                    }
                }
                for link in &new_link_list{
                    let peer1_interface = match ctx.list::<Interface>(g.namespace().unwrap().as_str(), Some(BTreeMap::from([
                        (
                            "virt.dev/instance".to_string(),
                            link.spec.peer1.instance.clone(),
                        ),
                        (
                            "virt.dev/network".to_string(),
                            link.spec.network.clone(),
                        ),
                    ]))).await{
                        Ok(peer1_interface) => {
                            if let Some(peer1_interface) = peer1_interface{
                                if peer1_interface.items.len() == 1{
                                    peer1_interface.items[0].clone()
                                } else {
                                    warn!("peer1_interface not found, probably deleted: {}/{}", link.spec.peer1.instance, link.spec.network);
                                    return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                                }
                            } else {
                                warn!("peer1_interface not found 2, probably deleted: {}/{}", link.spec.peer1.instance, link.spec.network);
                                return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                            }
                        },
                        Err(e) => {
                            warn!("failed to list next_hop_interface: {:?}", e);
                            return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                        }
                    };
                    let peer2_interface = match ctx.list::<Interface>(g.namespace().unwrap().as_str(), Some(BTreeMap::from([
                        (
                            "virt.dev/instance".to_string(),
                            link.spec.peer2.instance.clone(),
                        ),
                        (
                            "virt.dev/network".to_string(),
                            link.spec.network.clone(),
                        ),
                    ]))).await{
                        Ok(peer2_interface) => {
                            if let Some(peer2_interface) = peer2_interface{
                                if peer2_interface.items.len() == 1{
                                    peer2_interface.items[0].clone()
                                } else {
                                    warn!("peer2_interface not found, probably deleted");
                                    return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                                }
                            } else {
                                warn!("peer2_interface not found, probably deleted");
                                return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                            }
                        },
                        Err(e) => {
                            warn!("failed to list next_hop_interface: {:?}", e);
                            return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                        }
                    };

                    let peer1_interface_status = if let Some(status) = peer1_interface.status{
                        status
                    } else {
                        warn!("peer1_interface status not ready");
                        return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                    };

                    let peer2_interface_status = if let Some(status) = peer2_interface.status{
                        status
                    } else {
                        warn!("peer2_interface status not ready");
                        return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                    };

                    let peer1_interface_host_ifidx = if let Some(host_ifidx) = peer1_interface_status.state.host_ifidx{
                        host_ifidx
                    } else {
                        warn!("peer1_interface host_ifidx not ready");
                        return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                    };

                    let peer2_interface_host_ifidx = if let Some(host_ifidx) = peer2_interface_status.state.host_ifidx{
                        host_ifidx
                    } else {
                        warn!("peer2_interface host_ifidx not ready");
                        return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                    };

                    let peer1_interface_host_mac = if let Some(host_mac) = peer1_interface_status.state.host_mac{
                        mac_address_to_bytes(host_mac.as_str()).unwrap()
                    } else {
                        warn!("peer1_interface host_mac not ready");
                        return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                    };

                    let peer2_interface_host_mac = if let Some(host_mac) = peer2_interface_status.state.host_mac{
                        mac_address_to_bytes(host_mac.as_str()).unwrap()
                    } else {
                        warn!("peer2_interface host_mac not ready");
                        return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                    };

                    let peer1_interface_hwaddr = if peer1_interface_status.state.hwaddr != ""{
                        mac_address_to_bytes(peer1_interface_status.state.hwaddr.as_str()).unwrap()
                    } else {
                        warn!("peer1_interface hwaddr not ready");
                        return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                    };

                    let peer2_interface_hwaddr = if peer2_interface_status.state.hwaddr != ""{
                        mac_address_to_bytes(peer2_interface_status.state.hwaddr.as_str()).unwrap()
                    } else {
                        warn!("peer2_interface hwaddr not ready");
                        return Ok(Action::requeue(std::time::Duration::from_secs(5)));
                    };

                    let peer1_to_peer2_flow = Flow{
                        match_type: MatchType::Interface{idx: peer1_interface_host_ifidx as u32},
                        next_hop: vec![NextHop{
                            interface: peer2_interface_host_ifidx as u32,
                            source_mac: peer2_interface_host_mac,
                            destination_mac: peer2_interface_hwaddr,
                        }],
                    };
                    let mut update = false;
                    if !flow_table.status.as_ref().unwrap().flows.contains(&peer1_to_peer2_flow){
                        flow_table.status.as_mut().unwrap().flows.push(peer1_to_peer2_flow);
                        update = true;
                    }
                    let peer2_to_peer1_flow = Flow{
                        match_type: MatchType::Interface{idx: peer2_interface_host_ifidx as u32},
                        next_hop: vec![NextHop{
                            interface: peer1_interface_host_ifidx as u32,
                            source_mac: peer1_interface_host_mac,
                            destination_mac: peer1_interface_hwaddr,
                        }],
                    };
                    if !flow_table.status.as_ref().unwrap().flows.contains(&peer2_to_peer1_flow){
                        flow_table.status.as_mut().unwrap().flows.push(peer2_to_peer1_flow);
                        update = true;
                    }
                    if update{
                        ctx.update_status(&flow_table).await?;
                    }
                }
            },
        }
        Ok(Action::requeue(Duration::from_secs(5*60)))
    }
    pub fn error_policy(_g: Arc<Flowtable>, error: &ReconcileError, _ctx: Arc<ResourceClient<Flowtable>>) -> Action {
        warn!("reconcile failed: {:?}", error);
        Action::requeue(Duration::from_secs(1))
    }
}

fn mac_address_to_bytes(mac: &str) -> Result<[u8; 6], std::num::ParseIntError> {
    let bytes: Result<Vec<_>, _> = mac.split(':')
        .map(|s| u8::from_str_radix(s, 16))
        .collect();

    bytes.map(|v| [v[0], v[1], v[2], v[3], v[4], v[5]])
}
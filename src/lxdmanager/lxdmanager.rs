use k8s_openapi::api::core::v1::ObjectReference;
use kube_runtime::reflector::ObjectRef;
use tokio::process::Command;
use anyhow::anyhow;
use tracing::{info, warn};
use std::{io::{self, ErrorKind}, collections::HashMap};
use serde::{Deserialize, Serialize};
use futures::channel::mpsc;
//use tokio::sync::mpsc;
use tokio_stream::{self as stream, StreamExt};
use tokio::time::{self, Duration};

use crate::instance::instance::Instance;

pub struct LxdManager{
	pub instance_states: HashMap<String, Option<LxdInstanceState>>,
	pub client: LxdClient,
	pub rx: tokio::sync::mpsc::Receiver<LxdCommand>,
	pub update_tx: mpsc::Sender<ObjectRef<Instance>>
}

#[derive(Clone)]
pub struct LxdClient{
	tx: tokio::sync::mpsc::Sender<LxdCommand>,
}

pub enum LxdCommand{
	Create(Instance),
	Delete(String),
	Status(
		String,
		tokio::sync::oneshot::Sender<Option<LxdInstanceState>>,
	),
}

impl LxdClient{
	pub fn new(tx: tokio::sync::mpsc::Sender<LxdCommand>) -> LxdClient{
		LxdClient{
			tx
		}
	}
	pub async fn create(&self, instance: Instance) -> anyhow::Result<()>{
		info!("lxd client create request");
		self.tx.send(LxdCommand::Create(instance)).await?;
		info!("lxd client create request sent");
		Ok(())
	}
	pub async fn delete(&self, name: String) -> anyhow::Result<()>{
		self.tx.send(LxdCommand::Delete(name)).await?;
		Ok(())
	}
	pub async fn status(&self, name: String) -> anyhow::Result<Option<LxdInstanceState>>{
		info!("lxd client status request");
		let (tx, rx) = tokio::sync::oneshot::channel();
		self.tx.send(LxdCommand::Status(name, tx)).await?;
		info!("lxd client status request sent");
		let instance_state = rx.await?;
		info!("lxd client status request received");
		Ok(instance_state)
	}
}

impl LxdManager{
    pub fn new(update_tx: mpsc::Sender<ObjectRef<Instance>>) -> LxdManager{
		let (tx, rx) = tokio::sync::mpsc::channel(100);
        LxdManager{
			instance_states: HashMap::new(),
			client: LxdClient::new(tx),
			rx,
			update_tx
		}
    }

	pub async fn start(&mut self) -> anyhow::Result<()>{

		/*
		while let Some(cmd) = self.rx.recv().await{
			match cmd {
				LxdCommand::Create(instance) => {
					info!("lxd command create");
					self.instance_create(&instance).await?;
				},
				LxdCommand::Delete(name) => {
					info!("lxd command delete");
					self.instance_delete(&name).await?;
				},
				LxdCommand::Status(name, reply_tx) => {
					info!("lxd command status");
					let instance_state = instance_status(&name).await?;
					if let Err(_e) = reply_tx.send(instance_state){
						return Err(anyhow!(ErrorKind::Other));
					}
				}
			}
		}
		Ok(())
		*/
		
		let sleep = time::sleep(Duration::from_secs(20));
		tokio::pin!(sleep);
		loop {
			tokio::select! {
				Some(cmd) = self.rx.recv() => {
					match cmd {
						LxdCommand::Create(instance) => {
							info!("lxd command create");
							if let Err(e) = self.instance_create(&instance).await{
								warn!("failed to create instance: {:?}", e);
							}
						},
						LxdCommand::Delete(name) => {
							info!("lxd command delete");
							if let Err(e) = self.instance_delete(&name).await{
								warn!("failed to delete instance: {:?}", e);
							}
						},
						LxdCommand::Status(name, reply_tx) => {
							info!("lxd command status");
							let res = instance_status(&name).await;
							match res {
								Ok(instance_state) => {
									if let Err(_e) = reply_tx.send(instance_state){
										warn!("failed to send instance state");
									}
								},
								Err(e) => {
									warn!("failed to get instance state: {:?}", e);
								}
							}
						}
					}
				},
				_ = async {} => {
					tokio::time::sleep(Duration::from_secs(1)).await;
					info!("lxd manager tick");
					let mut existing_instances = HashMap::new();
					let mut new_instances = HashMap::new();
					for (instance_name, instance) in &mut self.instance_states{
						match instance_status(instance_name).await{
							Ok(Some(instance_state)) => {
								match instance{
									Some(_instance) => {
										existing_instances.insert(instance_name.clone(), instance_state);
									},
									None => {
										info!("new instance");
										new_instances.insert(instance_name.clone(), instance_state);
									}
								}
							},
							Ok(None) => {
								info!("instance not found");
								*instance = None;
							},
							Err(e) => {
								warn!("failed to get instance state: {:?}", e);
							}
						}
					}
					for (instance_name, instance_state) in existing_instances{
						if let Some(instance) = self.instance_states.get_mut(&instance_name){
							if instance_state.status != instance.as_ref().unwrap().status 
							//|| instance_state.network.len() != instance.as_ref().unwrap().network.len()
							{
								info!("instance state changed");
								let obj_ref = ObjectRef::new(&format!("{}", instance_name)).within("default");
								if let Err(e) = self.update_tx.clone().try_send(obj_ref){
									warn!("failed to send instance state: {:?}", e);
								}
							}
							*instance = Some(instance_state);
						}
					}
					for (instance_name, instance_state) in new_instances{
						info!("new instance");
						self.instance_states.insert(instance_name.clone(), Some(instance_state));
						let obj_ref = ObjectRef::new(&format!("{}", instance_name)).within("default");
						if let Err(e) = self.update_tx.clone().try_send(obj_ref){
							warn!("failed to send instance state: {:?}", e);
						}
						info!("new instance sent");
					}
				}
			}
			info!("lxd manager tick end");
		}	
	}


    pub async fn instance_create(&mut self, instance: &Instance) -> anyhow::Result<()>{
        let mut cmd = Command::new("lxc");
        cmd.arg("launch").
            arg(instance.spec.image.clone()).
            arg(instance.metadata.name.clone().unwrap()).
            arg("--vm").
            arg("-c").
            arg(format!("limits.cpu={}", instance.spec.vcpu)).
            arg("-c").
            arg(format!("limits.memory={}", instance.spec.memory));
        let res = cmd.output().await;
        match res {
            Ok(res) => {
                if !res.status.success(){
                    let stderr = std::str::from_utf8(&res.stderr).unwrap();
                    return Err(io::Error::new(io::ErrorKind::Other, stderr).into());
                } else {
					info!("instance successfully created");
					self.instance_states.insert(instance.metadata.name.clone().unwrap(), None);
                    return Ok(());
                }
            },
            Err(e) => {
                return Err(anyhow!(e));
            }
        }
    }
    pub async fn instance_delete(&self, name: &str) -> anyhow::Result<()>{
        let mut cmd = Command::new("lxc");
        cmd.arg("delete").
            arg(name).
            arg("--force");
        let res = cmd.output().await;
        match res {
            Ok(res) => {
                if !res.status.success(){
                    let stderr = std::str::from_utf8(&res.stderr).unwrap();
                    return Err(io::Error::new(io::ErrorKind::Other, stderr).into());
                } else {
                    return Ok(());
                }
            },
            Err(e) => {
                return Err(anyhow!(e));
            }
        }
    }
}

pub async fn instance_status(name: &str) -> anyhow::Result<Option<LxdInstanceState>>{
	let mut cmd = Command::new("lxc");
	cmd.arg("query").
		arg(format!("/1.0/virtual-machines/{}/state",name));
	let res = cmd.output().await;
	match res {
		Ok(res) => {
			if !res.status.success(){
				let stderr = std::str::from_utf8(&res.stderr).unwrap();
				if stderr.contains("Error: Instance not found"){
					return Ok(None);
				}
				return Err(io::Error::new(io::ErrorKind::Other, stderr).into());
			} else {
				let stdout = std::str::from_utf8(&res.stdout).unwrap();
				info!("instance state: {}", stdout);
				let lxd_instance_state: LxdInstanceState = serde_json::from_str(stdout)?;
				return Ok(Some(lxd_instance_state));
			}
		},
		Err(e) => {
			return Err(anyhow!(e));
		}
	}
}

#[derive(Deserialize, Debug)]
pub struct LxdInstanceState{
    pub cpu: Cpu,
    pub disk: Disk,
    pub memory: Memory,
    pub network: Option<HashMap<String, Interface>>,
    pub pid: u64,
    pub processes: u64,
    pub status: String,
    pub status_code: u64,
}

#[derive(Deserialize, Debug)]
pub struct Cpu{
    pub usage: u64,
}

#[derive(Deserialize, Debug)]
pub struct Disk{
    pub root: Root,
}

#[derive(Deserialize, Debug)]
pub struct Root{
    pub total: u64,
    pub usage: u64,
}

#[derive(Deserialize, Debug)]
pub struct Memory{
    pub swap_usage: u64,
    pub swap_usage_peak: u64,
    pub total: u64,
    pub usage: u64,
    pub usage_peak: u64,
}

#[derive(Deserialize, Debug)]
pub struct Network(HashMap<String, Interface>);

#[derive(Deserialize, Debug)]
pub struct Interface{
    pub addresses: Vec<Address>,
    pub counters: Counters,
    pub host_name: String,
    pub hwaddr: String,
    pub mtu: u64,
    pub state: String,
    pub r#type: String,
}

#[derive(Deserialize, Debug)]
pub struct Address{
    pub address: String,
    pub family: String,
    pub netmask: String,
    pub scope: String,
}

#[derive(Deserialize, Debug)]
pub struct Counters{
    pub bytes_received: u64,
    pub bytes_sent: u64,
    pub errors_received: u64,
    pub errors_sent: u64,
    pub packets_dropped_inbound: u64,
    pub packets_dropped_outbound: u64,
    pub packets_received: u64,
    pub packets_sent: u64,
}


/*
lxc query /1.0/virtual-machines/router1/state
{
	"cpu": {
		"usage": 66064416000
	},
	"disk": {
		"root": {
			"total": 0,
			"usage": 113745920
		}
	},
	"memory": {
		"swap_usage": 0,
		"swap_usage_peak": 0,
		"total": 15584677888,
		"usage": 527044608,
		"usage_peak": 0
	},
	"network": {
		"enp5s0": {
			"addresses": [
				{
					"address": "10.238.56.113",
					"family": "inet",
					"netmask": "24",
					"scope": "global"
				},
				{
					"address": "fd42:b391:ff85:63b4:216:3eff:fe24:d65c",
					"family": "inet6",
					"netmask": "64",
					"scope": "global"
				},
				{
					"address": "fe80::216:3eff:fe24:d65c",
					"family": "inet6",
					"netmask": "64",
					"scope": "link"
				}
			],
			"counters": {
				"bytes_received": 431238,
				"bytes_sent": 229129,
				"errors_received": 0,
				"errors_sent": 0,
				"packets_dropped_inbound": 0,
				"packets_dropped_outbound": 0,
				"packets_received": 4101,
				"packets_sent": 2433
			},
			"host_name": "tapd8fddb01",
			"hwaddr": "00:16:3e:24:d6:5c",
			"mtu": 1500,
			"state": "up",
			"type": "broadcast"
		},
		"enp6s0": {
			"addresses": [],
			"counters": {
				"bytes_received": 0,
				"bytes_sent": 0,
				"errors_received": 0,
				"errors_sent": 0,
				"packets_dropped_inbound": 0,
				"packets_dropped_outbound": 0,
				"packets_received": 0,
				"packets_sent": 0
			},
			"host_name": "router1_eth4",
			"hwaddr": "50:d8:1d:63:de:e3",
			"mtu": 1500,
			"state": "down",
			"type": "broadcast"
		},
		"enp7s0": {
			"addresses": [],
			"counters": {
				"bytes_received": 0,
				"bytes_sent": 0,
				"errors_received": 0,
				"errors_sent": 0,
				"packets_dropped_inbound": 0,
				"packets_dropped_outbound": 0,
				"packets_received": 0,
				"packets_sent": 0
			},
			"host_name": "router1_eth5",
			"hwaddr": "92:0b:30:28:67:a1",
			"mtu": 1500,
			"state": "down",
			"type": "broadcast"
		},
		"enp8s0": {
			"addresses": [],
			"counters": {
				"bytes_received": 0,
				"bytes_sent": 0,
				"errors_received": 0,
				"errors_sent": 0,
				"packets_dropped_inbound": 0,
				"packets_dropped_outbound": 0,
				"packets_received": 0,
				"packets_sent": 0
			},
			"host_name": "router1_eth3",
			"hwaddr": "ca:10:e8:24:7d:f7",
			"mtu": 1500,
			"state": "down",
			"type": "broadcast"
		},
		"lo": {
			"addresses": [
				{
					"address": "127.0.0.1",
					"family": "inet",
					"netmask": "8",
					"scope": "local"
				},
				{
					"address": "::1",
					"family": "inet6",
					"netmask": "128",
					"scope": "local"
				}
			],
			"counters": {
				"bytes_received": 46960,
				"bytes_sent": 46960,
				"errors_received": 0,
				"errors_sent": 0,
				"packets_dropped_inbound": 0,
				"packets_dropped_outbound": 0,
				"packets_received": 460,
				"packets_sent": 460
			},
			"host_name": "",
			"hwaddr": "",
			"mtu": 65536,
			"state": "up",
			"type": "loopback"
		}
	},
	"pid": 3844503,
	"processes": 13,
	"status": "Running",
	"status_code": 103
}

*/
use kube_runtime::reflector::ObjectRef;
use serde::{Deserialize, Serialize};
use tokio::{process::Command, io::AsyncWriteExt};
use std::{process::{Command as StdCommand, Stdio}, io::Write};
use anyhow::anyhow;
use tracing::{info, warn};
use std::{io::{self}, collections::HashMap};
use futures::channel::mpsc;
use tokio::time::Duration;

use crate::{instance::instance::{self, Instance, InstanceState, InstanceSpec}, network};

pub struct LxdManager{
	pub instance_states: HashMap<String, Option<InstanceState>>,
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
	Define(InstanceConfig),
	Delete(String),
	DeleteInterface(Instance, String),
	Status(
		String,
		tokio::sync::oneshot::Sender<Option<InstanceState>>,
	),
}

impl LxdClient{
	pub fn new(tx: tokio::sync::mpsc::Sender<LxdCommand>) -> LxdClient{
		LxdClient{
			tx
		}
	}
	pub async fn define(&self, instance_config: InstanceConfig) -> anyhow::Result<()>{
		info!("lxd client define request");
		self.tx.send(LxdCommand::Define(instance_config)).await?;
		info!("lxd client define request sent");
		Ok(())
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
	pub async fn delete_interface(&self,instance: Instance, interface_name: String) -> anyhow::Result<()>{
		self.tx.send(LxdCommand::DeleteInterface(instance, interface_name)).await?;
		Ok(())
	}
	pub async fn status(&self, name: String) -> anyhow::Result<Option<InstanceState>>{
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
						LxdCommand::Define(instance_config) => {
							if let Err(e) = self.instance_define(instance_config).await{
								warn!("failed to define instance: {:?}", e);
							}
							info!("lxd command define");
						},
						LxdCommand::DeleteInterface(instance, interface_name) => {
							if let Err(e) = self.delete_interface(instance, interface_name).await{
								warn!("failed to delete interface: {:?}", e);
							}
							info!("lxd command delete interface");
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
							|| interfaces_different(instance_state.network.clone(), instance.as_ref().unwrap().network.clone())
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
					}
				}
			}
		}	
	}

	pub async fn delete_interface(&mut self, instance: Instance, interface_name: String) -> anyhow::Result<()>{
		let instance_name = instance.metadata.name.clone().unwrap();
		let mut cmd = Command::new("lxc");
		cmd.arg("config").
			arg("device").
			arg("remove").
			arg(&instance_name).
			arg(&interface_name);
		let res = cmd.output().await;
		match res {
			Ok(res) => {
				if !res.status.success(){
					let stderr = std::str::from_utf8(&res.stderr).unwrap();
					return Err(io::Error::new(io::ErrorKind::Other, stderr).into());
				} else {
					info!("interface removed");
					return Ok(());
				}
			},
			Err(e) => {
				return Err(anyhow!(e));
			}
		}
	}

	pub async fn instance_define(&mut self, instance_config: InstanceConfig) -> anyhow::Result<()>{
		let instance_name = instance_config.instance.metadata.name.clone().unwrap();
		let instance_image = instance_config.instance.spec.image.clone();
		let lxd_instance_config: LxdInstanceConfig = instance_config.clone().into();
		let lxd_instance_config_str = serde_yaml::to_string(&lxd_instance_config)?;
		std::fs::write(format!("/tmp/{}.yaml", instance_name), lxd_instance_config_str.clone())?;

		let mut cmd = Command::new("lxc")
        	.stdin(Stdio::piped())
        	.stdout(Stdio::piped())
			.arg("launch")
			.arg(instance_image)
			.arg(instance_name.clone())
			.arg("--vm")
        	.spawn()?;
    	let mut child_stdin = cmd.stdin.take().unwrap();
    	child_stdin.write_all(lxd_instance_config_str.as_bytes()).await?;
		drop(child_stdin);
		let res = cmd.wait_with_output().await;
        match res {
            Ok(res) => {
                if !res.status.success(){
                    let stderr = std::str::from_utf8(&res.stderr).unwrap();
                    return Err(io::Error::new(io::ErrorKind::Other, stderr).into());
                } else {
					info!("instance successfully defined");
					self.instance_states.insert(instance_name.clone(), None);
                }
            },
            Err(e) => {
                return Err(anyhow!(e));
            }
        };

		/*

		let cloud_init = CloudInit::from(instance_config);
		let cloud_init_str = serde_yaml::to_string(&cloud_init)?;
		let mut cmd = Command::new("lxc")
			.stdin(Stdio::piped())
			.stdout(Stdio::piped())
			.arg("config")
			.arg("set")
			.arg(&instance_name)
			.arg("cloud-init.network-config")
			.spawn()?;
		let mut child_stdin = cmd.stdin.take().unwrap();
		child_stdin.write_all(cloud_init_str.as_bytes()).await?;
		drop(child_stdin);
		let res = cmd.wait_with_output().await;
		match res {
			Ok(res) => {
				if !res.status.success(){
					let stderr = std::str::from_utf8(&res.stderr).unwrap();
					return Err(io::Error::new(io::ErrorKind::Other, stderr).into());
				} else {
					info!("cloud init successfully set");
				}
			},
			Err(e) => {
				return Err(anyhow!(e));
			}
		};

		let cmd = Command::new("lxc")
			.arg("start")
			.arg(&instance_name)
			.spawn()?;
		let res = cmd.wait_with_output().await;
		match res {
			Ok(res) => {
				if !res.status.success(){
					let stderr = std::str::from_utf8(&res.stderr).unwrap();
					return Err(io::Error::new(io::ErrorKind::Other, stderr).into());
				} else {
					info!("instance successfully started");
				}
			},
			Err(e) => {
				return Err(anyhow!(e));
			}
		};
		*/

		Ok(())
	}

    pub async fn instance_create(&mut self, instance: &Instance) -> anyhow::Result<()>{
        let mut cmd = Command::new("lxc");
        cmd.arg("init").
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
    pub async fn instance_delete(&mut self, name: &str) -> anyhow::Result<()>{
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
					self.instance_states.remove(name);
                    return Ok(());
                }
            },
            Err(e) => {
                return Err(anyhow!(e));
            }
        }
    }
}

fn interfaces_different(existing_network: Option<HashMap<String, instance::InstanceInterface>>, new_network: Option<HashMap<String, instance::InstanceInterface>>) -> bool{
	if existing_network.is_none() && new_network.is_some(){
		return true;
	}
	if existing_network.is_some() && new_network.is_none(){
		return true;
	}
	if existing_network.is_some() && new_network.is_some(){
		let existing_network = existing_network.unwrap();
		let new_network = new_network.unwrap();
		if existing_network.len() != new_network.len(){
			return true;
		}
		for (interface_name, existing_interface) in existing_network{
			if let Some(new_interface) = new_network.get(&interface_name){
				if existing_interface.addresses.len() != new_interface.addresses.len(){
					return true;
				}
				for (i, existing_address) in existing_interface.addresses.iter().enumerate(){
					if let Some(new_address) = new_interface.addresses.get(i){
						if existing_address.address != new_address.address{
							return true;
						}
					} else {
						return true;
					}
				}
			} else {
				return true;
			}
		}
	}
	false	
}


pub async fn instance_status(name: &str) -> anyhow::Result<Option<InstanceState>>{
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
				let lxd_instance_state: InstanceState = serde_json::from_str(stdout)?;
				return Ok(Some(lxd_instance_state));
			}
		},
		Err(e) => {
			return Err(anyhow!(e));
		}
	}
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct LxdInstanceConfig{
	config: Config,
	devices: HashMap<String, DeviceType>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct Config{
	#[serde(rename = "limits.cpu")]
	pub limits_cpu: String,
	#[serde(rename = "limits.memory")]
	pub limits_memory: String,
	#[serde(rename = "cloud-init.network-config")]
	pub cloud_init_network_config: String,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
#[serde(untagged)]
pub enum DeviceType{
	Nic(NicConfig),
	Disk(DiscConfig),
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct NicConfig{
	#[serde(rename = "host_name")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub host_name: Option<String>,
	#[serde(rename = "hwaddr")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub hwaddr: Option<String>,
	#[serde(rename = "ipv4.address")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub ipv4_address: Option<String>,
	#[serde(rename = "ipv4.gateway")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub ipv4_gateway: Option<String>,
	#[serde(rename = "ipv6.gateway")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub ipv6_gateway: Option<String>,
	#[serde(rename = "mtu")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub mtu: Option<String>,
	#[serde(rename = "name")]
	pub name: String,
	#[serde(rename = "network")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub network: Option<String>,
	#[serde(rename = "nictype")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub nictype: Option<String>,
	#[serde(rename = "type")]
	pub r#type: String,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct DiscConfig{
	#[serde(rename = "path")]
	pub path: String,
	#[serde(rename = "pool")]
	pub pool: String,
	#[serde(rename = "type")]
	pub r#type: String,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct InstanceConfig{
	pub instance: Instance,
	pub interfaces: HashMap<String, InterfaceConfigType>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub enum InterfaceConfigType{
	Network{
		ipv4: String,
		prefix_len: u8,
		mac: String,
	},
	Mgmt{
		network: String,
	}
}

impl From<InstanceConfig> for LxdInstanceConfig{
	fn from(instance_config: InstanceConfig) -> Self {
		let instance = instance_config.instance;

		let mut device_config_map = HashMap::new();
		let mut ethernets_config_map = HashMap::new();
		for (idx, interface) in instance.spec.interfaces.iter().enumerate(){
			let interface_type = match instance_config.interfaces.get(&interface.name){
				Some(interface_type) => {
					interface_type.clone()
				},
				None => {
					warn!("interface type not found");
					continue;
				}
			};
			match interface_type{
				InterfaceConfigType::Mgmt { network } => {
					let nic_config = NicConfig{
						name: interface.name.clone(),
						r#type: "nic".to_string(),
						network: Some(network.clone()),
						mtu: None,
						..Default::default()
					};
					let nic_device = DeviceType::Nic(nic_config);
					device_config_map.insert(format!("eth{}",idx), nic_device);
					let ethernet_config = CloudInitEthernet{
						routes: None,
						addresses: None,
						dhcp4: Some(true),
						dhcp_identifier: Some("mac".to_string()),
						r#match: None,
					};
					ethernets_config_map.insert(interface.name.clone(), ethernet_config);
				},
				InterfaceConfigType::Network { ipv4, prefix_len, mac } => {
					let nic_config = NicConfig{
						host_name: Some(format!("{}-{}", instance.metadata.name.clone().unwrap(), interface.name.clone())),
						hwaddr: Some(mac.clone()),
						ipv4_address: Some(ipv4.clone()),
						ipv4_gateway: Some("none".to_string()),
						ipv6_gateway: Some("none".to_string()),
						mtu: Some(format!("{}",interface.mtu.clone())),
						name: interface.name.clone(),
						nictype: Some("routed".to_string()),
						r#type: "nic".to_string(),
						network: None,
					};
					let nic_device = DeviceType::Nic(nic_config);
					device_config_map.insert(format!("eth{}",idx), nic_device);
		
					let ethernet_config = CloudInitEthernet{
						routes: None,
						addresses: Some(vec![format!("{}/{}", ipv4.clone(), prefix_len)]),
						dhcp4: None,
						dhcp_identifier: None,
						r#match: Some(CloudInitMatch{
							macaddress: mac.clone(),
						}),
					};
					ethernets_config_map.insert(interface.name.clone(), ethernet_config);
				}
			}
		}

		let root_config = DiscConfig{
			path: "/".to_string(),
			pool: "default".to_string(),
			r#type: "disk".to_string(),
		};
		let root_device = DeviceType::Disk(root_config);
		device_config_map.insert("root".to_string(), root_device);
		LxdInstanceConfig{
			config: Config{
				limits_cpu: instance.spec.vcpu.to_string(),
				limits_memory: instance.spec.memory.to_string(),
				cloud_init_network_config: serde_yaml::to_string(&CloudInit{
					network: CloudInitNetwork{
						version: 2,
						ethernets: ethernets_config_map,
					}
				}).unwrap(),
			},
			devices: device_config_map,
		}
	}
}
/*
network:
  version: 2
  ethernets:
    enp5s0:
      routes:
      - to: default
        via: 169.254.0.1
        on-link: true
      - to: default
        via: fe80::1
        on-link: true
      addresses:
      - 192.0.2.2/32
      - 2001:db8::2/128

*/



#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CloudInit{
	network: CloudInitNetwork,
}
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CloudInitNetwork{
	version: u8,
	ethernets: HashMap<String, CloudInitEthernet>,
}
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CloudInitEthernet{
	#[serde(skip_serializing_if = "Option::is_none")]
	r#match: Option<CloudInitMatch>,
	#[serde(skip_serializing_if = "Option::is_none")]
	routes: Option<Vec<CloudInitRoute>>,
	#[serde(skip_serializing_if = "Option::is_none")]
	addresses: Option<Vec<String>>,
	#[serde(skip_serializing_if = "Option::is_none")]
	dhcp4: Option<bool>,
	#[serde(rename = "dhcp-identifier")]
	#[serde(skip_serializing_if = "Option::is_none")]
	dhcp_identifier: Option<String>
}
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CloudInitMatch{
	macaddress: String,
}
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CloudInitRoute{
	to: String,
	via: String,
	on_link: bool,
}
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CloudInitAddress{
	address: String,
}
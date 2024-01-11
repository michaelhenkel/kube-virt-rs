use kube_runtime::reflector::ObjectRef;
use serde::{Deserialize, Serialize};
use tokio::{process::Command, io::AsyncWriteExt};
use anyhow::anyhow;
use tracing::{info, warn, error};
use std::{io::{self}, collections::HashMap, process::Stdio};
use futures::channel::mpsc;
use tokio::time::Duration;
use pnet;

use crate::{
	instance::instance::{self, Instance, InstanceConfig, InstanceInterface, InstanceState},
	interface::interface::Interface,
};

pub struct LxdManager{
	pub instance_configs: HashMap<String, Option<String>>,
	pub interface_configs: HashMap<(String, String), Option<InstanceInterface>>,
	pub interface_states: HashMap<(String, String), Option<InstanceInterface>>,
	pub client: LxdClient,
	pub rx: tokio::sync::mpsc::Receiver<LxdCommand>,
	pub instance_update_tx: mpsc::Sender<ObjectRef<Instance>>,
	pub interface_update_tx: mpsc::Sender<ObjectRef<Interface>>,
	pub lxd_monitor_client: LxdMonitorClient,
}

#[derive(Clone)]
pub struct LxdClient{
	tx: tokio::sync::mpsc::Sender<LxdCommand>,
}

pub enum LxdCommand{
	Define(
		Instance,
		tokio::sync::oneshot::Sender<anyhow::Result<()>>,
	),
	Start(
		Instance,
		tokio::sync::oneshot::Sender<anyhow::Result<()>>,
	),
	DefineInterface(
		String,
		InterfaceConfig,
		tokio::sync::oneshot::Sender<anyhow::Result<()>>,
	),
	Delete(
		String,
		tokio::sync::oneshot::Sender<anyhow::Result<()>>,
	),
	DeleteInterface(Instance, String),
	Status(
		String,
		tokio::sync::oneshot::Sender<Option<String>>,
	),
	InterfaceConfig(
		String,
		String,
		tokio::sync::oneshot::Sender<Option<InstanceInterface>>,
	),
	InterfaceState(
		String,
		String,
		tokio::sync::oneshot::Sender<Option<InstanceInterface>>,
	),
	InterfaceCount(
		String,
		tokio::sync::oneshot::Sender<Option<u32>>,
	),
}

impl LxdClient{
	pub fn new(tx: tokio::sync::mpsc::Sender<LxdCommand>) -> LxdClient{
		LxdClient{
			tx
		}
	}
	pub async fn define(&self, instance: Instance) -> anyhow::Result<()>{
		let (tx, rx) = tokio::sync::oneshot::channel();
		self.tx.send(LxdCommand::Define(instance, tx)).await?;
		match rx.await{
			Ok(res) => {
				res
			},
			Err(e) => {
				warn!("lxd client define request received");
				Err(anyhow!(e))
			}
		}
	}

	pub async fn start(&self, instance: Instance) -> anyhow::Result<()>{
		let (tx, rx) = tokio::sync::oneshot::channel();
		self.tx.send(LxdCommand::Start(instance, tx)).await?;
		match rx.await{
			Ok(res) => {
				res
			},
			Err(e) => {
				warn!("lxd client start request received");
				Err(anyhow!(e))
			}
		}
	}

	pub async fn define_interface(&self, instance_name: String, interface_config: InterfaceConfig) -> anyhow::Result<()>{
		let (tx, rx) = tokio::sync::oneshot::channel();
		self.tx.send(LxdCommand::DefineInterface(instance_name, interface_config, tx)).await?;
		match rx.await{
			Ok(res) => {
				res
			},
			Err(e) => {
				warn!("lxd client define interface request received");
				Err(anyhow!(e))
			}
		}
	}
	pub async fn delete(&self, name: String) -> anyhow::Result<()>{
		let (tx, rx) = tokio::sync::oneshot::channel();
		self.tx.send(LxdCommand::Delete(name, tx)).await?;
		match rx.await{
			Ok(res) => {
				res
			},
			Err(e) => {
				warn!("lxd client delete request received");
				Err(anyhow!(e))
			}
		}
	}
	pub async fn delete_interface(&self,instance: Instance, interface_name: String) -> anyhow::Result<()>{
		self.tx.send(LxdCommand::DeleteInterface(instance, interface_name)).await?;
		Ok(())
	}
	pub async fn status(&self, name: String) -> anyhow::Result<Option<String>>{
		let (tx, rx) = tokio::sync::oneshot::channel();
		self.tx.send(LxdCommand::Status(name.clone(), tx)).await?;
		let instance_state = rx.await?;
		Ok(instance_state)
	}

	pub async fn interface_config(&self, instance_name: String, interface_name: String) -> anyhow::Result<Option<InstanceInterface>>{
		let (tx, rx) = tokio::sync::oneshot::channel();
		self.tx.send(LxdCommand::InterfaceConfig(instance_name.clone(), interface_name.clone(), tx)).await?;
		let interface_state = rx.await?;
		Ok(interface_state)
	}

	pub async fn interface_state(&self, instance_name: String, interface_name: String) -> anyhow::Result<Option<InstanceInterface>>{
		let (tx, rx) = tokio::sync::oneshot::channel();
		self.tx.send(LxdCommand::InterfaceState(instance_name.clone(), interface_name.clone(), tx)).await?;
		let interface_state = rx.await?;
		Ok(interface_state)
	}

	pub async fn get_host_interface_index_mac(&self, interface_name: &str) -> anyhow::Result<(i32, String)>{
		pnet::datalink::interfaces().iter().find_map(| interface| {
			if interface.name == interface_name{
				let mac = interface.mac.unwrap_or_default().to_string();
				Some((interface.index as i32, mac))
			} else {
				None
			}
		}).ok_or_else(|| anyhow!("interface not found"))
	}

	pub async fn get_interface_count(&self, instance_name: &str) -> anyhow::Result<Option<u32>>{
		let (tx, rx) = tokio::sync::oneshot::channel();
		self.tx.send(LxdCommand::InterfaceCount(instance_name.to_string(), tx)).await?;
		Ok(rx.await?)
	}
	
	pub async fn get_instance_interface_index(&self, interface_name: &str, instance_name: &str) -> anyhow::Result<i32> {
		let mut cmd = Command::new("lxc");
		let child = cmd.stdin(Stdio::piped()).
			stdout(Stdio::piped()).
			arg("exec").
			arg(instance_name).
			arg("cat").
			arg(format!("/sys/class/net/{}/ifindex", interface_name)).
			spawn()?;
		info!("executing command: {:?}", cmd);

		/*
		let mut cmd_stdin = child.stdin.take().unwrap();
		if let Err(e) = cmd_stdin.write_all(format!("/sys/class/net/{}/ifindex", interface_name).as_bytes()).await{
			return Err(anyhow!(e));
		}
		drop(cmd_stdin);
		*/
		let res = child.wait_with_output().await;
		match res {
			Ok(res) => {
				if !res.status.success(){
					let stderr = std::str::from_utf8(&res.stderr).unwrap();
					warn!("failed to get instance {} interface {} index", instance_name, interface_name);
					return Err(io::Error::new(io::ErrorKind::Other, stderr).into());
				} else {
					let stdout = std::str::from_utf8(&res.stdout).unwrap();
					match stdout.trim().parse::<i32>(){
						Ok(host_ifidx) => {
							return Ok(host_ifidx);
						},
						Err(e) => {
							return Err(anyhow!(e));
						}
					}
				}
			},
			Err(e) => {
				return Err(anyhow!(e));
			}
		}	
	}
}

impl LxdManager{
    pub fn new(instance_update_tx: mpsc::Sender<ObjectRef<Instance>>, interface_update_tx: mpsc::Sender<ObjectRef<Interface>>, lxd_monitor_client: LxdMonitorClient) -> LxdManager{
		let (tx, rx) = tokio::sync::mpsc::channel(1);
        LxdManager{
			instance_configs: HashMap::new(),
			interface_configs: HashMap::new(),
			interface_states: HashMap::new(),
			client: LxdClient::new(tx),
			rx,
			instance_update_tx,
			interface_update_tx,
			lxd_monitor_client,
		}
    }

	pub async fn start(&mut self){
		let mut interval = tokio::time::interval(Duration::from_millis(1500));
		let now = tokio::time::Instant::now();
		loop {
			tokio::select! {
				Some(cmd) = self.rx.recv() => {
					match cmd {
						LxdCommand::InterfaceCount(instance_name, tx) => {
							let interface_count = self.interface_count(instance_name).await;
							let count = match interface_count{
								Ok(interface_count) => {
									Some(interface_count)
								},
								Err(e) => {
									None
								},
							};
							if let Err(e) = tx.send(count){
								error!("failed to send interface count: {:?}", e);
							}
						},
						LxdCommand::Delete(name, tx) => {
							match self.instance_delete(&name, now).await{
								Ok(_) => {
									if let Err(e) = self.lxd_monitor_client.delete_instance(name.clone()).await{
										error!("failed to delete instance from monitor: {:?}", e);
									}
									self.instance_configs.remove(&name);
									for ((instance_name, interface_name), _) in self.interface_configs.clone(){
										if instance_name == name{
											self.interface_configs.remove(&(instance_name, interface_name));
										}
									}
									for ((instance_name, interface_name), _) in self.interface_states.clone(){
										if instance_name == name{
											self.interface_states.remove(&(instance_name, interface_name));
										}
									}
									if let Err(e) = self.lxd_monitor_client.delete_instance(name.clone()).await{
										error!("failed to delete instance from monitor: {:?}", e);
									}
									if let Err(e) = tx.send(Ok(())){
										error!("failed to send delete reply: {:?}", e);
									}
								},
								Err(e) => {
									error!("lxd command delete failed: {:?}", e);
									if let Err(e) = tx.send(Err(e)){
										warn!("failed to send delete reply: {:?}", e);
									}
								}
							}

						},
						LxdCommand::Start(instance_config, tx) => {
							let res = self.instance_start(instance_config.clone(), now).await;
							if let Err(e) = tx.send(res){
								warn!("failed to send start reply: {:?}", e);
							}
						},
						LxdCommand::Define(instance_config, tx) => {
							match self.instance_define(instance_config.clone(), now).await{
								Ok(_) => {
									self.instance_configs.insert(instance_config.metadata.name.clone().unwrap(), None);
									if let Err(e) = self.lxd_monitor_client.add_instance(instance_config.metadata.name.clone().unwrap()).await{
										error!("failed add instance to monitor: {:?}", e);
									}
									if let Err(e) = tx.send(Ok(())){
										error!("failed to send define reply: {:?}", e);
									}
								},
								Err(e) => {
									error!("lxd command define failed: {:?}", e);
									if let Err(e) = tx.send(Err(e)){
										error!("failed to send define reply: {:?}", e);
									}
								}
							}
						},
						LxdCommand::DefineInterface(instance_name, interface_config, tx) => {
							let res = self.interface_define(instance_name.clone(), interface_config.clone(), now).await;
							if let Err(e) = tx.send(res){
								error!("failed to send interface define reply: {:?}", e);
							}
							self.interface_configs.insert((instance_name.clone(), interface_config.name.clone()), None);
							self.interface_states.insert((instance_name, interface_config.name.clone()), None);
						},
						LxdCommand::DeleteInterface(instance, interface_name) => {
							let res = self.delete_interface(instance, interface_name, now).await;
							if let Err(e) = res{
								error!("failed to delete interface: {:?}", e);
							}
						},
						LxdCommand::Status(name, reply_tx) => {
							if let Some(res) = self.instance_configs.get(&name).cloned(){
								if let Err(_e) = reply_tx.send(res){
									error!("failed to send instance state");
								}
							} else {
								if let Err(_e) = reply_tx.send(None){
									error!("failed to send instance state");
								}
							}
						},
						LxdCommand::InterfaceConfig(instance_name, interface_name, reply_tx) => {
							if let Some(interface_state) = self.interface_configs.get(&(instance_name.clone(), interface_name.clone())).cloned(){
								if let Err(_e) = reply_tx.send(interface_state.clone()){
									warn!("failed to send interface state");
								}
							} else {
								if let Err(_e) = reply_tx.send(None){
									warn!("failed to send interface state");
								}
							}
						},
						LxdCommand::InterfaceState(instance_name, interface_name, reply_tx) => {
							if let Some(interface_state) = self.interface_states.get(&(instance_name.clone(), interface_name.clone())).cloned(){
								if let Err(_e) = reply_tx.send(interface_state.clone()){
									error!("failed to send interface state");
								}
							} else {
								if let Err(_e) = reply_tx.send(None){
									error!("failed to send interface state");
								}
							}
						}
					}
				},

				_ = interval.tick() => {
					for (instance_name, instance_state) in &mut self.instance_configs{
						if let Ok(new_instance_state) = self.lxd_monitor_client.get_instance_config(instance_name).await{
							if instance_state.as_ref() != new_instance_state.as_ref(){
								*instance_state = new_instance_state;
								let instance_ref = ObjectRef::new(instance_name).within("default");
								if let Err(e) = self.instance_update_tx.try_send(instance_ref){
									error!("failed to send instance update: {:?}", e);
								}
							}
						}
					}
					for ((instance_name, interface_name), interface_state) in &mut self.interface_configs{
						if let Ok(new_interface_state) = self.lxd_monitor_client.get_interface_config(instance_name.clone(), interface_name.clone()).await{
							if interface_state.as_ref() != new_interface_state.as_ref(){
								*interface_state = new_interface_state;
								let interface_ref = ObjectRef::new(&format!("{}-{}", instance_name, interface_name)).within("default");
								if let Err(e) = self.interface_update_tx.try_send(interface_ref){
									error!("failed to send interface update: {:?}", e);
								}
							}
						}
					}
					for ((instance_name, interface_name), interface_state) in &mut self.interface_states{
						if let Ok(new_interface_state) = self.lxd_monitor_client.get_interface_state(instance_name.clone(), interface_name.clone()).await{
							if interface_state.as_ref() != new_interface_state.as_ref(){
								*interface_state = new_interface_state;
								let interface_ref = ObjectRef::new(&format!("{}-{}", instance_name, interface_name)).within("default");
								if let Err(e) = self.interface_update_tx.try_send(interface_ref){
									error!("failed to send interface update: {:?}", e);
								}
							}
						}
					}
				},
			}
		}	
	}

	pub async fn run_cmd(&mut self, cmd: &mut Command, now: tokio::time::Instant) -> anyhow::Result<()>{
		let res = cmd.output().await;
		match res {
			Ok(res) => {
				if !res.status.success(){
					let stderr = std::str::from_utf8(&res.stderr).unwrap();
					error!("failed to execute command: {:?}", stderr);
					return Err(io::Error::new(io::ErrorKind::Other, stderr).into());
				} else {
					info!("command executed successfully at {}\n{:?}", now.elapsed().as_millis(), cmd);
				}
			},
			Err(e) => {
				error!("failed to execute command: {:?}", e);
				return Err(anyhow!(e));
			}
		}
		tokio::time::sleep(Duration::from_millis(500)).await;
		Ok(())
	}

	pub async fn interface_count(&mut self, instance_name: String) -> anyhow::Result<u32>{
		if let Some(instance_state) = self.lxd_monitor_client.get_state(instance_name).await?{
			if let Some(devices) = instance_state.devices{
				return Ok(devices.len() as u32);
			}
		}
		Ok(0)
	}

	pub async fn delete_interface(&mut self, instance: Instance, interface_name: String, now: tokio::time::Instant) -> anyhow::Result<()>{
		let instance_name = instance.metadata.name.clone().unwrap();
		let mut cmd = Command::new("lxc");
		cmd.arg("config").
			arg("device").
			arg("remove").
			arg(&instance_name).
			arg(&interface_name);
		self.run_cmd(&mut cmd, now).await
	}

	pub async fn instance_define(&mut self, instance: Instance, now: tokio::time::Instant) -> anyhow::Result<()>{
		let mut cmd = Command::new("lxc");
        cmd.arg("launch").
            arg(instance.spec.image.clone()).
            arg(instance.metadata.name.clone().unwrap()).
            arg("--vm").
            arg("-c").
            arg(format!("limits.cpu={}", instance.spec.vcpu)).
            arg("-c").
            arg(format!("limits.memory={}", instance.spec.memory));
		self.run_cmd(&mut cmd, now).await
	}

	pub async fn instance_start(&mut self, instance: Instance, now: tokio::time::Instant) -> anyhow::Result<()>{
		let mut cmd = Command::new("lxc");
        cmd.arg("start").
            arg(instance.metadata.name.clone().unwrap());
		self.run_cmd(&mut cmd, now).await
	}

	pub async fn interface_define(&mut self, instance_name: String, interface_config: InterfaceConfig, now: tokio::time::Instant) -> anyhow::Result<()>{
		let idx = self.interface_count(instance_name.clone()).await?;
		let mut cmd = Command::new("lxc");
		cmd.arg("config")
			.arg("device")
			.arg("add")
			.arg(&instance_name)
			//.arg(format!("eth{}", interface_config.pci_idx + 1))
			.arg(format!("eth{}", idx + 1))
			.arg("nic")
			.arg("nictype=routed")
			.arg(format!("ipv4.address={}", interface_config.ipv4))
			.arg("ipv4.gateway=none")
			.arg("ipv6.gateway=none")
			.arg(format!("hwaddr={}", interface_config.mac))
			.arg(format!("name={}", interface_config.name))
			.arg(format!("host_name={}-{}",instance_name, interface_config.name));
		for i in 0..5{
			if let Err(_) = self.run_cmd(&mut cmd, now).await{
				tokio::time::sleep(Duration::from_millis(1000)).await;
				if i == 4 {
					return Err(anyhow!("failed to define interface: {:?}", cmd));
				}
			} else {
				break;
			}
		}
		Ok(())
		//self.run_cmd(&mut cmd, now).await
	}

    pub async fn instance_delete(&mut self, name: &str, now: tokio::time::Instant) -> anyhow::Result<()>{
        let mut cmd = Command::new("lxc");
        cmd.arg("delete").
            arg(name).
            arg("--force");
		self.run_cmd(&mut cmd, now).await
    }
}

fn _interfaces_different(existing_network: Option<HashMap<String, instance::InstanceInterface>>, new_network: Option<HashMap<String, instance::InstanceInterface>>) -> bool{
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
				if existing_interface.addresses.is_none() && new_interface.addresses.is_some(){
					return true;
				}
				if existing_interface.addresses.is_some() && new_interface.addresses.is_none(){
					return true;
				}
				if let Some(existing_addresses) = existing_interface.addresses{
					if let Some(new_addresses) = &new_interface.addresses{
						if existing_addresses.len() != new_addresses.len(){
							return true;
						}
						for (i, existing_address) in existing_addresses.iter().enumerate(){
							if let Some(new_address) = new_addresses.get(i){
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
			} else {
				return true;
			}
		}
	}
	false	
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct InterfaceConfig{
	pub ipv4: String,
	pub prefix_len: u8,
	pub mac: String,
	pub pci_idx: u32,
	pub mtu: u32,
	pub name: String,
}


pub struct LxdMonitor{
	configs: HashMap<String, InstanceConfig>,
	states: HashMap<String, InstanceState>,
	rx: tokio::sync::mpsc::Receiver<LxdMonitorCommand>,
	pub client: LxdMonitorClient,
}

#[derive(Clone)]
pub struct LxdMonitorClient{
	tx: tokio::sync::mpsc::Sender<LxdMonitorCommand>,
}

impl LxdMonitorClient{
	pub async fn get_instance_config(&self, instance_name: &str) -> anyhow::Result<Option<String>>{
		let (tx, rx) = tokio::sync::oneshot::channel();
		self.tx.send(LxdMonitorCommand::GetInstanceConfig(instance_name.to_string(), tx)).await?;
		let instance_config = rx.await?;
		Ok(instance_config)
	}

	pub async fn get_interface_config(&self, instance_name: String, interface_name: String) -> anyhow::Result<Option<InstanceInterface>>{
		let (tx, rx) = tokio::sync::oneshot::channel();
		self.tx.send(LxdMonitorCommand::GetInterfaceConfig(instance_name, interface_name, tx)).await?;
		let interface_config = rx.await?;
		Ok(interface_config)
	}

	pub async fn get_interface_state(&self, instance_name: String, interface_name: String) -> anyhow::Result<Option<InstanceInterface>>{
		let (tx, rx) = tokio::sync::oneshot::channel();
		self.tx.send(LxdMonitorCommand::GetInterfaceState(instance_name, interface_name, tx)).await?;
		let interface_state = rx.await?;
		Ok(interface_state)
	}

	pub async fn get_state(&self, instance_name: String) -> anyhow::Result<Option<InstanceConfig>>{
		let (tx, rx) = tokio::sync::oneshot::channel();
		if let Err(e) = self.tx.send(LxdMonitorCommand::GetState(instance_name, tx)).await{
			error!("failed to send get state command: {:?}", e);
		}
		match rx.await{
			Err(e) => {
				error!("failed to receive get state command: {:?}", e);
				Ok(None)
			},
			Ok(instance_state) => {
				Ok(instance_state)
			}
		}
	}

	pub async fn add_instance(&self, instance_name: String) -> anyhow::Result<()>{
		self.tx.send(LxdMonitorCommand::AddInstance(instance_name)).await.map_err(|e| anyhow!(e))
	}

	pub async fn delete_instance(&self, instance_name: String) -> anyhow::Result<()> {
		self.tx.send(LxdMonitorCommand::DeleteInstance(instance_name)).await.map_err(|e| anyhow!(e))
	}
}

pub enum LxdMonitorCommand{
	GetInstanceConfig(String, tokio::sync::oneshot::Sender<Option<String>>),
	GetState(String, tokio::sync::oneshot::Sender<Option<InstanceConfig>>),
	GetInterfaceConfig(String, String, tokio::sync::oneshot::Sender<Option<InstanceInterface>>),
	GetInterfaceState(String, String, tokio::sync::oneshot::Sender<Option<InstanceInterface>>),
	AddInstance(String),
	DeleteInstance(String),
}

impl LxdMonitor {
	pub fn new() -> LxdMonitor{
		let (tx, rx) = tokio::sync::mpsc::channel(1);
		LxdMonitor{
			configs: HashMap::new(),
			states: HashMap::new(),
			rx,
			client: LxdMonitorClient { tx }
		}
	}
	pub async fn run(&mut self){
		let mut config_interval = tokio::time::interval(Duration::from_millis(2000));
		//let mut state_interval = tokio::time::interval(Duration::from_secs(1));
		loop {
			tokio::select! {
				Some(cmd) = self.rx.recv() => {
					match cmd{
						LxdMonitorCommand::AddInstance(instance_name) => {
							if !self.configs.contains_key(instance_name.as_str()){
								self.configs.insert(instance_name.clone(), InstanceConfig::default());
							}
							if !self.states.contains_key(instance_name.as_str()){
								self.states.insert(instance_name, InstanceState::default());
							}
						},
						LxdMonitorCommand::DeleteInstance(instance_name) => {
							self.configs.remove(&instance_name);
							self.states.remove(&instance_name);
						},
						LxdMonitorCommand::GetState(instance_name, reply_tx) => {
							let instance_state = self.configs.get(&instance_name).cloned();
							if let Err(_e) = reply_tx.send(instance_state){
								warn!("failed to send instance state");
							}
						},
						LxdMonitorCommand::GetInstanceConfig(instance_name, reply_tx) => {
							let instance_config = self.configs.get(&instance_name).cloned();
							let mut state = None;
							if let Some(instance_config) = instance_config{
								state = Some(instance_config.status);
							}
							if let Err(_e) = reply_tx.send(state){
								warn!("failed to send instance config");
							}
						},
						LxdMonitorCommand::GetInterfaceConfig(instance_name, interface_name, reply_tx) => {
							let mut instance_interface_config = None;
							if let Some(instance_config) = self.configs.get(&instance_name).cloned(){
								if let Some(devices) = instance_config.devices{
									for (_, device) in &devices {
										if device.name.as_ref() == Some(&interface_name) {
											instance_interface_config = Some(device.clone());
											break;
										}
									}
								}
							}
							if let Err(_e) = reply_tx.send(instance_interface_config){
								warn!("failed to send interface state");
							}
						},
						LxdMonitorCommand::GetInterfaceState(instance_name, interface_name, reply_tx) => {
							let mut instance_interface_state = None;
							if let Some(instance_state) = self.states.get(&instance_name).cloned(){
								if let Some(networks) = instance_state.network{
									if let Some(network) = networks.get(&interface_name){
										instance_interface_state = Some(network.clone());
									}
								}
							}
							if let Err(_e) = reply_tx.send(instance_interface_state){
								warn!("failed to send interface state");
							}
						}

					}
				},
				_ = config_interval.tick() => {
					for (instance_name, instance_config) in &mut self.configs{
						let mut cmd = Command::new("lxc");
						cmd.arg("query").
							arg(format!("/1.0/virtual-machines/{}",instance_name));
						let res = cmd.output().await;
						match res {
							Ok(res) => {
								if !res.status.success(){
									let stderr = std::str::from_utf8(&res.stderr).unwrap();
									if !stderr.contains("Instance not found"){
										warn!("failed to get instance config: {:?}", stderr);
									}
								} else {
									let stdout = std::str::from_utf8(&res.stdout).unwrap();
									match serde_json::from_str(stdout){
										Ok(lxd_instance_config) => { *instance_config = lxd_instance_config;},
										Err(e) => {
											error!("failed to parse instance config: {:?}", e);
										}
									}
								}
							},
							Err(e) => {
								error!("failed to get instance config: {:?}", e);
							}
						}
						tokio::time::sleep(Duration::from_millis(1)).await;
					}
					for (instance_name, instance_state) in &mut self.states{
						let mut cmd = Command::new("lxc");
						cmd.arg("query").
							arg(format!("/1.0/virtual-machines/{}/state",instance_name));
						let res = cmd.output().await;
					
						match res {
							Ok(res) => {
								if !res.status.success(){
									let stderr = std::str::from_utf8(&res.stderr).unwrap();
									if !stderr.contains("Instance not found"){
										warn!("failed to get instance state: {:?}", stderr);
									}
								} else {
									let stdout = std::str::from_utf8(&res.stdout).unwrap();
									match serde_json::from_str(stdout){
										Ok(lxd_instance_state) => { *instance_state = lxd_instance_state;},
										Err(e) => {
											error!("failed to parse instance state: {:?}", e);
										}
									}
								}
							},
							Err(e) => {
								error!("failed to get instance state: {:?}", e);
							}
						}
						
						tokio::time::sleep(Duration::from_millis(1)).await;
					}
				},
				/*
				_ = state_interval.tick() => {
					for (instance_name, instance_state) in &mut self.states{
						let mut cmd = Command::new("lxc");
						cmd.arg("query").
							arg(format!("/1.0/virtual-machines/{}/state",instance_name));
						let res = cmd.output().await;
						match res {
							Ok(res) => {
								if !res.status.success(){
									let stderr = std::str::from_utf8(&res.stderr).unwrap();
									if !stderr.contains("Instance not found"){
										warn!("failed to get instance state: {:?}", stderr);
									}
								} else {
									let stdout = std::str::from_utf8(&res.stdout).unwrap();
									let lxd_instance_state: InstanceState = serde_json::from_str(stdout)?;
									*instance_state = lxd_instance_state;
								}
							},
							Err(e) => {
								return Err(anyhow!(e));
							}
						}
					}
				}
				*/
			}
		}
	}
}
use garde::error;
use kube_runtime::reflector::ObjectRef;
use serde::{Deserialize, Serialize};
use tokio::{process::Command, io::AsyncWriteExt, sync::RwLock};
use std::{process::{Command as StdCommand, Stdio}, io::Write, collections::{HashSet, VecDeque}, sync::Arc};
use anyhow::anyhow;
use tracing::{info, warn, error};
use std::{io::{self}, collections::HashMap};
use futures::channel::mpsc;
use tokio::time::Duration;
use pnet;

use crate::{
	instance::instance::{self, Instance, InstanceState, InstanceSpec, InstanceInterface},
	interface::interface::Interface,
	network
};

pub struct LxdManager{
	pub instance_states: HashMap<String, Option<String>>,
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
	InterfaceStatus(
		String,
		String,
		tokio::sync::oneshot::Sender<Option<InstanceInterface>>,
	),
}

impl LxdClient{
	pub fn new(tx: tokio::sync::mpsc::Sender<LxdCommand>) -> LxdClient{
		LxdClient{
			tx
		}
	}
	pub async fn define(&self, instance: Instance) -> anyhow::Result<()>{
		info!("lxd client define request");
		let (tx, rx) = tokio::sync::oneshot::channel();
		self.tx.send(LxdCommand::Define(instance, tx)).await?;
		info!("lxd client define request sent");
		match rx.await{
			Ok(res) => {
				info!("lxd client define request received");
				res
			},
			Err(e) => {
				warn!("lxd client define request received");
				Err(anyhow!(e))
			}
		}
	}

	pub async fn start(&self, instance: Instance) -> anyhow::Result<()>{
		info!("lxd client start request");
		let (tx, rx) = tokio::sync::oneshot::channel();
		self.tx.send(LxdCommand::Start(instance, tx)).await?;
		info!("lxd client start request sent");
		match rx.await{
			Ok(res) => {
				info!("lxd client start request received");
				res
			},
			Err(e) => {
				warn!("lxd client start request received");
				Err(anyhow!(e))
			}
		}
	}

	pub async fn define_interface(&self, instance_name: String, interface_config: InterfaceConfig) -> anyhow::Result<()>{
		info!("lxd client define interface request");
		let (tx, rx) = tokio::sync::oneshot::channel();
		self.tx.send(LxdCommand::DefineInterface(instance_name, interface_config, tx)).await?;
		info!("lxd client define interface request sent");
		match rx.await{
			Ok(res) => {
				info!("lxd client define interface request received");
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
				info!("lxd client delete request received");
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
		info!("lxd client status request for {}", name);
		let (tx, rx) = tokio::sync::oneshot::channel();
		self.tx.send(LxdCommand::Status(name.clone(), tx)).await?;
		info!("lxd client status request sent for {}" , name);
		let instance_state = rx.await?;
		info!("lxd client status request received for {}", name);
		Ok(instance_state)
	}

	pub async fn interface_status(&self, instance_name: String, interface_name: String) -> anyhow::Result<Option<InstanceInterface>>{
		info!("lxd client interface status request for {} {}", instance_name, interface_name);
		let (tx, rx) = tokio::sync::oneshot::channel();
		self.tx.send(LxdCommand::InterfaceStatus(instance_name.clone(), interface_name.clone(), tx)).await?;
		info!("lxd client interface status request sent for {} {}", instance_name, interface_name);
		let interface_state = rx.await?;
		info!("lxd client interface status request received for {} {}", instance_name, interface_name);
		Ok(interface_state)
	}
}

impl LxdManager{
    pub fn new(instance_update_tx: mpsc::Sender<ObjectRef<Instance>>, interface_update_tx: mpsc::Sender<ObjectRef<Interface>>, lxd_monitor_client: LxdMonitorClient) -> LxdManager{
		let (tx, rx) = tokio::sync::mpsc::channel(1);
        LxdManager{
			instance_states: HashMap::new(),
			interface_states: HashMap::new(),
			client: LxdClient::new(tx),
			rx,
			instance_update_tx,
			interface_update_tx,
			lxd_monitor_client,
		}
    }

	pub async fn start(&mut self) -> anyhow::Result<()>{
		let mut interval = tokio::time::interval(Duration::from_secs(1));
		let now = tokio::time::Instant::now();
		loop {
			tokio::select! {
				Some(cmd) = self.rx.recv() => {
					match cmd {
						LxdCommand::Delete(name, tx) => {
							info!("lxd command delete");
							match self.instance_delete(&name, now).await{
								Ok(_) => {
									info!("lxd command delete success");
									if let Err(e) = self.lxd_monitor_client.delete_instance(name.clone()).await{
										error!("failed to delete instance from monitor: {:?}", e);
									}
									self.instance_states.remove(&name);
									if let Err(e) = tx.send(Ok(())){
										warn!("failed to send delete reply: {:?}", e);
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
							info!("lxd command start");

						},
						LxdCommand::Define(instance_config, tx) => {
							info!("lxd command define cmd received at {} for {}", now.elapsed().as_millis(), instance_config.metadata.name.clone().unwrap());
							match self.instance_define(instance_config.clone(), now).await{
								Ok(_) => {
									info!("lxd command define success");
									self.instance_states.insert(instance_config.metadata.name.clone().unwrap(), None);
									if let Err(e) = self.lxd_monitor_client.add_instance(instance_config.metadata.name.clone().unwrap()).await{
										error!("failed add instance to monitor: {:?}", e);
									}
									if let Err(e) = tx.send(Ok(())){
										warn!("failed to send define reply: {:?}", e);
									}
								},
								Err(e) => {
									error!("lxd command define failed: {:?}", e);
									if let Err(e) = tx.send(Err(e)){
										warn!("failed to send define reply: {:?}", e);
									}
								}
							}
						},
						LxdCommand::DefineInterface(instance_name, interface_config, tx) => {
							info!("lxd command interface define cmd received at {} for {}/{}", now.elapsed().as_millis(), instance_name, interface_config.name);
							let res = self.interface_define(instance_name.clone(), interface_config.clone(), now).await;
							if let Err(e) = tx.send(res){
								warn!("failed to send interface define reply: {:?}", e);
							}
							info!("lxd command interface define");
						},
						LxdCommand::DeleteInterface(instance, interface_name) => {
							let res = self.delete_interface(instance, interface_name, now).await;
							if let Err(e) = res{
								error!("failed to delete interface: {:?}", e);
							}
							info!("lxd command delete interface");
						},
						LxdCommand::Status(name, reply_tx) => {
							info!("lxd command instance status received for {}", name);
							if let Some(res) = self.instance_states.get(&name).cloned(){
								if let Err(_e) = reply_tx.send(res){
									warn!("failed to send instance state");
								}
							} else {
								if let Err(_e) = reply_tx.send(None){
									warn!("failed to send instance state");
								}
							}
							info!("lxd command instance status replied for {}", name);
						},
						LxdCommand::InterfaceStatus(instance_name, interface_name, reply_tx) => {
							info!("lxd command interface status received for {} {}", instance_name, interface_name);
							if let Some(interface_state) = self.interface_states.get(&(instance_name.clone(), interface_name.clone())).cloned(){
								if let Err(_e) = reply_tx.send(interface_state.clone()){
									warn!("failed to send interface state");
								}
							} else {
								if let Err(_e) = reply_tx.send(None){
									warn!("failed to send interface state");
								}
							}
							info!("lxd command interface status replied for {} {}", instance_name, interface_name);
						}
					}
				},

				_ = interval.tick() => {
					for (instance_name, instance_state) in &mut self.instance_states{
						if let Ok(new_instance_state) = self.lxd_monitor_client.get_instance_state(instance_name).await{
							if instance_state.as_ref() != new_instance_state.as_ref(){
								info!("instance state changed for instance {} : {:?}",instance_name, new_instance_state);
								*instance_state = new_instance_state;
								let instance_ref = ObjectRef::new(instance_name).within("default");
								if let Err(e) = self.instance_update_tx.try_send(instance_ref){
									warn!("failed to send instance update: {:?}", e);
								}
							}
						}
					}
					for ((instance_name, interface_name), interface_state) in &mut self.interface_states{
						if let Ok(new_interface_state) = self.lxd_monitor_client.get_interface_state(instance_name.clone(), interface_name.clone()).await{
							if interface_state.as_ref() != new_interface_state.as_ref(){
								info!("interface state changed for instance {} interface {} : {:?}",instance_name, interface_name, new_interface_state);
								*interface_state = new_interface_state;
								let interface_ref = ObjectRef::new(&format!("{}-{}", instance_name, interface_name)).within("default");
								if let Err(e) = self.interface_update_tx.try_send(interface_ref){
									warn!("failed to send interface update: {:?}", e);
								}
							}
						}
					}
				},
			}
		}	
	}

	pub async fn run_cmd(&mut self, mut cmd: Command, now: tokio::time::Instant) -> anyhow::Result<()>{
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

	pub async fn delete_interface(&mut self, instance: Instance, interface_name: String, now: tokio::time::Instant) -> anyhow::Result<()>{
		let instance_name = instance.metadata.name.clone().unwrap();
		let mut cmd = Command::new("lxc");
		cmd.arg("config").
			arg("device").
			arg("remove").
			arg(&instance_name).
			arg(&interface_name);
		self.run_cmd(cmd, now).await
	}

	pub async fn instance_define(&mut self, instance: Instance, now: tokio::time::Instant) -> anyhow::Result<()>{
		let mut cmd = Command::new("lxc");
        cmd.arg("init").
            arg(instance.spec.image.clone()).
            arg(instance.metadata.name.clone().unwrap()).
            arg("--vm").
            arg("-c").
            arg(format!("limits.cpu={}", instance.spec.vcpu)).
            arg("-c").
            arg(format!("limits.memory={}", instance.spec.memory));
		self.run_cmd(cmd, now).await
	}

	pub async fn instance_start(&mut self, instance: Instance, now: tokio::time::Instant) -> anyhow::Result<()>{
		let mut cmd = Command::new("lxc");
        cmd.arg("start").
            arg(instance.metadata.name.clone().unwrap());
		self.run_cmd(cmd, now).await
	}

	pub async fn interface_define(&mut self, instance_name: String, interface_config: InterfaceConfig, now: tokio::time::Instant) -> anyhow::Result<()>{
		let mut cmd = Command::new("lxc");
		cmd.arg("config")
			.arg("device")
			.arg("add")
			.arg(&instance_name)
			.arg(format!("eth{}", interface_config.pci_idx + 1))
			.arg("nic")
			.arg("nictype=routed")
			.arg(format!("ipv4.address={}", interface_config.ipv4))
			.arg("ipv4.gateway=none")
			.arg("ipv6.gateway=none")
			.arg(format!("hwaddr={}", interface_config.mac))
			.arg(format!("name={}", interface_config.name))
			.arg(format!("host_name={}-{}",instance_name, interface_config.name));
		self.run_cmd(cmd, now).await
	}

    pub async fn instance_delete(&mut self, name: &str, now: tokio::time::Instant) -> anyhow::Result<()>{
        let mut cmd = Command::new("lxc");
        cmd.arg("delete").
            arg(name).
            arg("--force");
		self.run_cmd(cmd, now).await
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

async fn get_host_interface_index_mac(interface_name: &str) -> anyhow::Result<(i32, String)>{
	pnet::datalink::interfaces().iter().find_map(| interface| {
		if interface.name == interface_name{
			let mac = interface.mac.unwrap_or_default().to_string();
			Some((interface.index as i32, mac))
		} else {
			None
		}
	}).ok_or_else(|| anyhow!("interface not found"))
}

pub async fn get_instance_interface_index(interface_name: &str, instance_name: &str) -> anyhow::Result<i32> {
	let mut cmd = Command::new("lxc");
	cmd.arg("exec").
		arg(instance_name).
		arg("cat").
		arg(format!("/sys/class/net/{}/ifindex", interface_name));
	let res = cmd.output().await;
	match res {
		Ok(res) => {
			if !res.status.success(){
				let stderr = std::str::from_utf8(&res.stderr).unwrap();
				warn!("failed to get instance {} interface {} index", instance_name, interface_name);
				return Err(io::Error::new(io::ErrorKind::Other, stderr).into());
			} else {
				let stdout = std::str::from_utf8(&res.stdout).unwrap();
				let host_ifidx = stdout.trim().parse::<i32>()?;
				return Ok(host_ifidx);
			}
		},
		Err(e) => {
			return Err(anyhow!(e));
		}
	}	
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
	states: HashMap<String, InstanceState>,
	rx: tokio::sync::mpsc::Receiver<LxdMonitorCommand>,
	pub client: LxdMonitorClient,
}

#[derive(Clone)]
pub struct LxdMonitorClient{
	tx: tokio::sync::mpsc::Sender<LxdMonitorCommand>,
}

impl LxdMonitorClient{
	pub async fn get_instance_state(&self, instance_name: &str) -> anyhow::Result<Option<String>>{
		let (tx, rx) = tokio::sync::oneshot::channel();
		self.tx.send(LxdMonitorCommand::GetInstanceState(instance_name.to_string(), tx)).await?;
		let instance_state = rx.await?;
		Ok(instance_state)
	}

	pub async fn get_interface_state(&self, instance_name: String, interface_name: String) -> anyhow::Result<Option<InstanceInterface>>{
		let (tx, rx) = tokio::sync::oneshot::channel();
		self.tx.send(LxdMonitorCommand::GetInterfaceState(instance_name, interface_name, tx)).await?;
		let interface_state = rx.await?;
		Ok(interface_state)
	}

	pub async fn get_state(&self, instance_name: String) -> anyhow::Result<Option<InstanceState>>{
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
	GetInstanceState(String, tokio::sync::oneshot::Sender<Option<String>>),
	GetState(String, tokio::sync::oneshot::Sender<Option<InstanceState>>),
	GetInterfaceState(String, String, tokio::sync::oneshot::Sender<Option<InstanceInterface>>),
	AddInstance(String),
	DeleteInstance(String),
}

impl LxdMonitor {
	pub fn new() -> LxdMonitor{
		let (tx, rx) = tokio::sync::mpsc::channel(1);
		LxdMonitor{
			states: HashMap::new(),
			rx,
			client: LxdMonitorClient { tx }
		}
	}
	pub async fn run(&mut self) -> anyhow::Result<()>{
		let mut interval = tokio::time::interval(Duration::from_secs(1));
		loop {
			tokio::select! {
				Some(cmd) = self.rx.recv() => {
					match cmd{
						LxdMonitorCommand::AddInstance(instance_name) => {
							if !self.states.contains_key(instance_name.as_str()){
								self.states.insert(instance_name, InstanceState::default());
							}
						},
						LxdMonitorCommand::DeleteInstance(instance_name) => {
							self.states.remove(&instance_name);
						},
						LxdMonitorCommand::GetState(instance_name, reply_tx) => {
							let instance_state = self.states.get(&instance_name).cloned();
							if let Err(_e) = reply_tx.send(instance_state){
								warn!("failed to send instance state");
							}
						},
						LxdMonitorCommand::GetInstanceState(instance_name, reply_tx) => {
							let instance_state = self.states.get(&instance_name).cloned();
							let mut state = None;
							if let Some(instance_state) = instance_state{
								state = Some(instance_state.status);
							}
							if let Err(_e) = reply_tx.send(state){
								warn!("failed to send instance state");
							}
						},
						LxdMonitorCommand::GetInterfaceState(instance_name, interface_name, reply_tx) => {
							let mut instance_interface_state = None;
							if let Some(instance_state) = self.states.get(&instance_name).cloned(){
								if let Some(device) = instance_state.devices{
									if let Some(interface_state) = device.get(&interface_name){
										instance_interface_state = Some(interface_state.clone());
									}
								}
							}
							if let Err(_e) = reply_tx.send(instance_interface_state){
								warn!("failed to send interface state");
							}
						}

					}
				},
				_ = interval.tick() => {
					for (instance_name, instance_state) in &mut self.states{
						let mut cmd = Command::new("lxc");
						cmd.arg("query").
							arg(format!("/1.0/virtual-machines/{}/state",instance_name));
						let res = cmd.output().await;
						match res {
							Ok(res) => {
								if !res.status.success(){
									let stderr = std::str::from_utf8(&res.stderr).unwrap();
									warn!("failed to get instance state: {:?}", stderr);
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
			}
		}
	}
}
// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock, mpsc};
use std::time::Duration;

use rocksdb::DB;
use super::cluster::{Simulator, Cluster};
use tikv::server::{Node, Config, create_raft_storage};
use tikv::raftstore::{Error, Result};
use tikv::raftstore::store;
use tikv::util::HandyRwLock;
use kvproto::raft_serverpb;
use kvproto::msgpb::{Message, MessageType};
use kvproto::raft_cmdpb::*;
use super::pd::TestPdClient;
use super::pd_ask::run_ask_loop;
use super::transport_simulate::{Strategy, SimulateTransport, Filter};
use tikv::server::http_handler::Handler;
use tikv::server::http_server::{Server, Listening, V1_MSG_PATH};
use tikv::server::http_client::Client;
use tikv::server::http_transport::{HttpTransport, PdStoreAddrPeeker};
use tikv::server::Error as ServerError;

type SimulateServerTransport = SimulateTransport<HttpTransport>;

pub struct ServerCluster {
    cluster_id: u64,
    servers: HashMap<u64, (Node<TestPdClient>, Listening)>,
    addrs: HashMap<u64, SocketAddr>,

    sim_trans: HashMap<u64, Arc<RwLock<SimulateServerTransport>>>,

    pd_client: Arc<RwLock<TestPdClient>>,
    client: Arc<Client>,
}

impl ServerCluster {
    pub fn new(cluster_id: u64, pd_client: Arc<RwLock<TestPdClient>>) -> ServerCluster {
        ServerCluster {
            cluster_id: cluster_id,
            servers: HashMap::new(),
            addrs: HashMap::new(),
            sim_trans: HashMap::new(),
            pd_client: pd_client,
            client: Arc::new(Client::new().unwrap()),
        }
    }
}

impl Simulator for ServerCluster {
    #[allow(useless_format)]
    fn run_node(&mut self,
                node_id: u64,
                cfg: Config,
                engine: Arc<DB>,
                strategy: Vec<Strategy>)
                -> u64 {
        assert!(node_id == 0 || !self.servers.contains_key(&node_id));

        let mut cfg = cfg;

        // Now we cache the store address, so here we should re-use last
        // listening address for the same store. Maybe we should enable
        // reuse_socket?
        if let Some(addr) = self.addrs.get(&node_id) {
            cfg.addr = format!("{}", addr)
        }

        let mut store_event_loop = store::create_event_loop(&cfg.store_cfg).unwrap();
        let mut node = Node::new(&mut store_event_loop,
                                 cfg.cluster_id,
                                 self.pd_client.clone());

        let router = node.raft_store_router();
        let store = create_raft_storage(router.clone(), engine.clone()).unwrap();

        let handler = Handler::new(store, router.clone());
        let mut server = Server::new(handler);
        let listening = server.http((&*cfg.addr).parse().unwrap()).unwrap();
        let addr = listening.addr;
        cfg.addr = format!("{}", addr);

        let peeker = PdStoreAddrPeeker::new(self.cluster_id, self.pd_client.clone());
        let trans = HttpTransport::new(self.client.clone(), peeker, router).unwrap();
        let trans = Arc::new(RwLock::new(trans));
        let simulate_trans = Arc::new(RwLock::new(SimulateTransport::new(strategy, trans)));
        
        node.set_advertise_addr(&cfg);
        node.start(store_event_loop, &cfg, engine, simulate_trans.clone())
            .unwrap();

        assert!(node_id == 0 || node_id == node.id());
        let node_id = node.id();

        self.sim_trans.insert(node_id, simulate_trans);

        self.servers.insert(node_id, (node, listening));
        self.addrs.insert(node_id, addr);

        node_id
    }

    fn stop_node(&mut self, node_id: u64) {
        let (node, listening) = self.servers.remove(&node_id).unwrap();
        listening.close();
        drop(node);
    }

    fn get_node_ids(&self) -> HashSet<u64> {
        self.servers.keys().cloned().collect()
    }

    fn call_command(&self,
                    store_id: u64,
                    request: RaftCmdRequest,
                    timeout: Duration)
                    -> Result<RaftCmdResponse> {
        let addr = self.addrs.get(&store_id).unwrap();

        let url = format!("http://{}{}", addr, V1_MSG_PATH).parse().unwrap();

        let mut msg = Message::new();
        msg.set_msg_type(MessageType::Cmd);
        msg.set_cmd_req(request);

        let mut resp_msg = match self.client.post_message_timeout(url, msg, timeout) {
            Ok(Some(resp_msg)) => resp_msg,
            Ok(None) => return Err(box_err!("request failed, get none result")),
            Err(ServerError::Timeout(_)) => return Err(Error::Timeout("request time".to_owned())),
            Err(e) => return Err(box_err!("request failed {:?}", e)),
        };

        assert_eq!(resp_msg.get_msg_type(), MessageType::CmdResp);

        Ok(resp_msg.take_cmd_resp())
    }

    fn send_raft_msg(&self, raft_msg: raft_serverpb::RaftMessage) -> Result<()> {
        let store_id = raft_msg.get_message().get_to();
        let addr = self.addrs.get(&store_id).unwrap();
        let url = format!("http://{}{}", addr, V1_MSG_PATH).parse().unwrap();

        let mut msg = Message::new();
        msg.set_msg_type(MessageType::Raft);
        msg.set_raft(raft_msg);

        if let Err(e) = self.client.post_message_timeout(url, msg, Duration::from_secs(3)) {
            return Err(box_err!("send raft msg failed {:?}", e));
        }

        Ok(())
    }

    fn hook_transport(&self, node_id: u64, filters: Vec<RwLock<Box<Filter>>>) {
        let trans = self.sim_trans.get(&node_id).unwrap();
        trans.wl().set_filters(filters);
    }
}

pub fn new_server_cluster(id: u64, count: usize) -> Cluster<ServerCluster> {
    let (tx, rx) = mpsc::channel();
    let pd_client = Arc::new(RwLock::new(TestPdClient::new(tx)));
    let sim = Arc::new(RwLock::new(ServerCluster::new(id, pd_client.clone())));
    run_ask_loop(pd_client.clone(), sim.clone(), rx);
    Cluster::new(id, count, sim, pd_client)
}

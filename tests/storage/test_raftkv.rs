use tikv::storage::*;
use tikv::raftserver::server::Config as ServerConfig;
use tikv::raftserver;
use tikv::pd::PdClient;
use tikv::util::codec::rpc;
use tikv::util::HandyRwLock;
use kvproto::metapb;
use kvproto::kvrpcpb::KeyAddress;
use kvproto::raft_serverpb::{self, Message as ServerMessage, MessageType};
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};

use tempdir::TempDir;
use std::sync::{Arc, RwLock, Mutex};
use std::sync::mpsc;
use std::collections::HashSet;
use std::time::Duration;
use std::thread;
use std::net::TcpStream;
use rocksdb::DB;

use raftserver::pd::TestPdClient;
use raftserver::pd_ask::run_ask_loop;
use raftserver::cluster::Simulator;
use raftserver::util::new_server_config;

fn new_key(region_id: u64, leader: &metapb::Peer, key: &[u8]) -> Key {
    let mut key_addr = KeyAddress::new();
    key_addr.set_region_id(region_id);
    key_addr.set_peer(leader.clone());
    key_addr.set_key(key.to_vec());
    Key::new(key_addr)
}

/// A simple transport that forward request from TestPdClient to raft kv server.
struct PdTransport {
    cluster_id: u64,
    pd_client: Arc<RwLock<TestPdClient>>,
    msg_id: Mutex<u64>,
}

impl PdTransport {
    fn new(cluster_id: u64, pd_client: Arc<RwLock<TestPdClient>>) -> PdTransport {
        PdTransport {
            cluster_id: cluster_id,
            msg_id: Mutex::new(0),
            pd_client: pd_client,
        }
    }

    fn alloc_msg_id(&self) -> u64 {
        let mut msg_id = self.msg_id.lock().unwrap();
        *msg_id += 1;
        *msg_id
    }
}

impl Simulator for PdTransport {
    fn run_node(&mut self, _: u64, _: ServerConfig, _: Arc<DB>) -> u64 {
        unimplemented!();
    }
    fn stop_node(&mut self, _: u64) {
        unimplemented!();
    }
    fn get_node_ids(&self) -> HashSet<u64> {
        unimplemented!();
    }
    fn call_command(&self,
                    request: RaftCmdRequest,
                    timeout: Duration)
                    -> raftserver::Result<RaftCmdResponse> {
        let node_id = request.get_header().get_peer().get_node_id();
        let node = self.pd_client.rl().get_node(self.cluster_id, node_id).unwrap();
        let addr = node.get_address();
        let mut conn = TcpStream::connect(addr).unwrap();

        conn.set_write_timeout(Some(timeout)).unwrap();

        let mut msg = ServerMessage::new();
        msg.set_msg_type(MessageType::Cmd);
        msg.set_cmd_req(request);

        let msg_id = self.alloc_msg_id();
        try!(rpc::encode_msg(&mut conn, msg_id, &msg));

        conn.set_read_timeout(Some(timeout)).unwrap();

        let mut resp_msg = ServerMessage::new();
        let get_msg_id = try!(rpc::decode_msg(&mut conn, &mut resp_msg));

        assert_eq!(resp_msg.get_msg_type(), MessageType::CmdResp);
        assert_eq!(msg_id, get_msg_id);

        Ok(resp_msg.take_cmd_resp())
    }

    fn send_raft_msg(&self, _: raft_serverpb::RaftMessage) -> raftserver::Result<()> {
        unimplemented!();
    }
}

type EngineInfo = (u64, metapb::Peer, RaftKv<TestPdClient>);

/// Build an engine with given path as rocksdb directory.
fn build_engine(pathes: Vec<TempDir>) -> EngineInfo {
    let mut cfg = RaftKvConfig::default();
    let cluster_id = 1;
    cfg.server_cfg = new_server_config(cluster_id);
    cfg.server_cfg.store_cfg.replica_check_tick_interval = 100;
    cfg.store_pathes = pathes.iter().map(|p| p.path().to_str().unwrap().to_owned()).collect();

    let (tx, rx) = mpsc::channel();
    let pd_client = Arc::new(RwLock::new(TestPdClient::new(tx)));

    let sim = Arc::new(RwLock::new(PdTransport::new(cluster_id, pd_client.clone())));
    run_ask_loop(pd_client.clone(), sim.clone(), rx);

    let raft_kv = RaftKv::new(&cfg, pd_client.clone()).unwrap();

    let region = pd_client.rl().get_region(cluster_id, b"").unwrap();
    assert_eq!(region.get_peers().len(), 1);

    let res = (region.get_region_id(),
               region.get_peers()[0].clone(),
               raft_kv);

    let region_id = region.get_region_id();
    let expect_count = pd_client.rl().get_cluster_meta(cluster_id).unwrap().get_max_peer_number();
    // wait for at most 1 sec for all replica being setup.
    let mut actual_count = 0;
    for _ in 0..50 {
        let region = pd_client.rl().get_region_by_id(cluster_id, region_id).unwrap();
        actual_count = region.get_peers().len() as u32;
        if actual_count == expect_count {
            return res;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("replica doesn't setup as expect: want {}, got {}",
           expect_count,
           actual_count);
}

fn put(engine: &Engine, k: &Key, v: &[u8]) {
    let put = Modify::Put((k.clone(), v.to_vec()));
    engine.write(vec![put]).unwrap();
}

fn delete(engine: &Engine, k: &Key) {
    let delete = Modify::Delete(k.clone());
    engine.write(vec![delete]).unwrap();
}

#[test]
fn test_normal() {
    let pathes = (0..5).map(|_| TempDir::new("test-raftkv").unwrap()).collect();
    let (region_id, leader, engine) = build_engine(pathes);

    thread::sleep(Duration::from_secs(1));

    let k1 = new_key(region_id, &leader, b"b");
    assert_eq!(engine.get(&k1).unwrap(), None);

    put(&engine, &k1, b"b");
    assert_eq!(engine.get(&k1).unwrap(), Some(b"b".to_vec()));

    put(&engine, &k1, b"c");
    assert_eq!(engine.get(&k1).unwrap(), Some(b"c".to_vec()));

    assert_eq!(engine.seek(&k1).unwrap(),
               Some((b"b".to_vec(), b"c".to_vec())));
    assert_eq!(engine.seek(&new_key(region_id, &leader, b"a")).unwrap(),
               Some((b"b".to_vec(), b"c".to_vec())));
    assert_eq!(engine.seek(&new_key(region_id, &leader, b"b\0")).unwrap(),
               None);

    // it's ok to delete a non-exist key.
    let k2 = new_key(region_id, &leader, b"c");
    assert_eq!(engine.get(&k2).unwrap(), None);
    delete(&engine, &k2);

    delete(&engine, &k1);
    assert_eq!(engine.get(&k1).unwrap(), None);
}

#[test]
fn test_batch() {
    let pathes = (0..5).map(|_| TempDir::new("test-raftkv").unwrap()).collect();
    let (region_id, leader, engine) = build_engine(pathes);

    thread::sleep(Duration::from_secs(1));

    let mut mutation = vec![];
    for i in 1..100 {
        let k = new_key(region_id, &leader, i.to_string().as_bytes());
        let put = Modify::Put((k, i.to_string().into_bytes()));
        mutation.push(put);
    }
    engine.write(mutation).unwrap();

    for i in 1..100 {
        let k = new_key(region_id, &leader, i.to_string().as_bytes());
        assert_eq!(engine.get(&k).unwrap(), Some(i.to_string().into_bytes()));
    }

    let mut mutation = vec![];
    for i in 1..100 {
        let k = new_key(region_id, &leader, i.to_string().as_bytes());
        let delete = Modify::Delete(k);
        mutation.push(delete);
    }
    engine.write(mutation).unwrap();

    for i in 1..100 {
        let k = new_key(region_id, &leader, i.to_string().as_bytes());
        assert_eq!(engine.get(&k).unwrap(), None);
    }
}
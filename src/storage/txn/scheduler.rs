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

use std::thread;
use std::time::Duration;
use std::boxed::{Box, FnBox};

use std::sync::Arc;
use threadpool::ThreadPool;
use storage::{Engine, Command, Snapshot};
use kvproto::kvrpcpb::Context;
use storage::mvcc::{MvccTxn, MvccSnapshot, Error as MvccError, MvccCursor};
use storage::{Key, Value, KvPair, Mutation, Callback};
use std::collections::HashMap;
use std::collections::LinkedList;
use std::collections::BTreeSet;
use mio::{self, EventLoop, EventLoopBuilder, NotifyError, Timeout};

use super::store::SnapshotStore;
use super::mem_rowlock::RowLocks;
use super::{Error, Result};

const MAX_SEND_RETRY_CNT: i32 = 20;

pub enum ProcessResult {
    ResultSet {
        result: Vec<Result<()>>,
    },

    Value {
        value: Option<Value>,
    },

    Nothing {},
}

pub enum Tick {
    Nothing {}
}

pub enum Msg {
    Quit {},

    CMD {
        cmd: Command,
    },

    CheckLock {
        cid: u64,
    },

    GetSnapshot {
        cid: u64,
    },

    SnapshotFinish {
        cid: u64,
        snapshot: Result<Box<Snapshot>>,
    },

    WriteFinish {
        cid: u64,
        pr: ProcessResult,
        result: Result<()>,
    },
}

// send_msg wraps Sender and retries some times if queue is full.
pub fn send_msg<M: Send>(ch: &mio::Sender<M>, mut msg: M) -> Result<()> {
    for _ in 0..MAX_SEND_RETRY_CNT {
        let r = ch.send(msg);
        if r.is_ok() {
            return Ok(());
        }

        match r.unwrap_err() {
            NotifyError::Full(m) => {
                warn!("notify queue is full, sleep and retry");
                thread::sleep(Duration::from_millis(100));
                msg = m;
                continue;
            }
            e => {
                return Err(box_err!("{:?}", e));
            }
        }
    }

    // TODO: if we refactor with quick_error, we can use NotifyError instead later.
    Err(box_err!("notify channel is full"))
}

#[derive(Debug)]
pub struct SendCh {
    ch: mio::Sender<Msg>,
}

impl Clone for SendCh {
    fn clone(&self) -> SendCh {
        SendCh { ch: self.ch.clone() }
    }
}

impl SendCh {
    pub fn new(ch: mio::Sender<Msg>) -> SendCh {
        SendCh { ch: ch }
    }

    pub fn send(&self, msg: Msg) -> Result<()> {
        try!(send_msg(&self.ch, msg));
        Ok(())
    }
}

type LockIdxs = Vec<usize>;

#[derive(Debug)]
struct RunningCtx {
    cid: u64,
    cmd: Command,
    needed_locks: LockIdxs,
    owned_lock_count: usize,
}

impl RunningCtx {
    pub fn new(cid: u64, cmd: Command, needed_locks: LockIdxs) -> RunningCtx {
        RunningCtx {
            cid: cid,
            cmd: cmd,
            needed_locks: needed_locks,
            owned_lock_count: 0,
        }
    }

    pub fn cid(&self) -> u64 {
        self.cid
    }

    pub fn cmd(&self) -> &Command {
        &self.cmd
    }

    pub fn needed_locks(&self) -> &LockIdxs {
        self.needed_locks
    }

    pub fn owned_lock_count(&self) -> usize {
        self.owned_lock_count
    }

    pub fn incr_owned_lock(&mut self, by: usize) {
        self.lock_ctx.incr_owned(by);
    }

    pub fn all_lock_acquired(&self) -> bool {
        self.needed_locks.len() == self.owned_lock_count
    }
}

pub struct Scheduler {
    engine: Arc<Box<Engine>>,

    // cid -> context
    cmd_ctxs: HashMap<u64, RunningCtx>,

    //
    sendch: SendCh,

    // cmd id generator
    idalloc: u64,

    // simulate memory row locks
    rowlocks: RowLocks,

    //
    handle: Option<thread::JoinHandle<()>>,

    // statistics for flow control
    running_cmd_count: u64,
    pending_cmd_count: u64,
}

fn readonly_cmd(cmd: &Command) -> bool {
    match cmd {
        Command::Get { .. }|
        Command::BatchGet { .. } |
        Command::Scan { .. } => { return true; }
        _ => { return false; }
    }
}

impl Scheduler {
    pub fn new(engine: Arc<Box<Engine>>, event_loop: &mut EventLoop<Self>, concurrency: usize) -> Scheduler {
        let sendch = SendCh::new(event_loop.channel());

        Scheduler {
            engine: engine,
            cmd_ctxs: HashMap::new(),
            sendch: sendch,
            idalloc: 0,
            rowlocks: RowLocks::new(concurrency),
            handle: None,
            running_cmd_count: 0,
            pending_cmd_count: 0,
        }
    }

    pub fn start(&mut self, event_loop: &mut EventLoop<Self>) -> Result<()> {

        let builder = thread::Builder::new().name(thd_name!(format!("scheduler")));
        let h = try!(builder.spawn(move || {
            if let Err(e) = event_loop.run(self) {
                error!("scheduler run err {:?}", e);
                Err(e.into())
            }
            info!("scheduler running");
        }));
        self.handle = Some(h);

        Ok(())
    }

    fn next_id(&mut self) -> u64 {
        self.idalloc += 1;
        self.idalloc
    }

    fn save_cmd_context(&mut self, cid: u64, ctx: RunningCtx) {
        match self.cmd_ctxs.insert(cid, ctx) {
            Some(_) => {
                panic!("cid = {} existed, fatal", cid);
            }
            None => {}
        }
    }

    fn finish_cmd(&mut self, cid: u64) {
        let ctx = self.cmd_ctxs.get(&cid).unwrap();

        assert_eq!(cid, ctx.cid);

        // release lock and wake up waiting commands
        let wakeup_list = self.rowlocks.release_by_indexs(ctx.needed_locks(), &cid);
        for cid in wakeup_list {
            self.sendch.send(Msg::CheckLock{ cid: cid });
        }

        // remove context
        self.cmd_ctxs.remove(&cid);
    }

    pub fn dispatch_cmd(&self, cmd: Command) {
        // Todo: flow control

        self.sendch.send(Msg::CMD{ cmd: cmd });
    }

    fn calc_lock_indexs(&self, cmd: Command) -> Vec<usize> {
        match cmd {
            Command::Prewrite { ctx, mutations, primary, start_ts, callback } => {
                let locked_keys: Vec<&Key> = mutations.iter().map(|x| x.key()).collect();
                self.rowlocks.calc_lock_indexs(&locked_keys)
            }
            Command::Commit { ctx, keys, lock_ts, commit_ts, callback } => {
                self.rowlocks.calc_lock_indexs(&keys)
            }
            Command::CommitThenGet { ctx, key, lock_ts, commit_ts, get_ts, callback } => {
                self.rowlocks.calc_lock_indexs(&[&key])
            }
            Command::Cleanup { ctx, key, start_ts, callback } => {
                self.rowlocks.calc_lock_indexs(&[&key])
            }
            Command::Rollback { ctx, keys, start_ts, callback } => {
                self.rowlocks.calc_lock_indexs(&keys)
            }
            Command::RollbackThenGet { ctx, key, lock_ts, callback } => {
                self.rowlocks.calc_lock_indexs(&[&key])
            }
            _ => {
                panic!("unsupport cmd that need lock");
            }
        }
    }

    fn process_cmd_with_snapshot(&self, cid: u64, snapshot: &Snapshot) -> Result<()> {
        let cmd_ctx = self.cmd_ctxs.get(&cid).unwrap();

        assert_eq!(cid, cmd_ctx.cid());
        debug!("process cmd with snapshot, cid = {}, cmd_ctx = {:?}", cid, cmd_ctx);

        match cmd_ctx.cmd {
            Command::Get { ctx, key, start_ts, callback } => {
                let snap_store = SnapshotStore::new(&snapshot, start_ts);
                callback(snap_store.get(&key).map_err(::storage::Error::from));
            }

            Command::BatchGet { ctx, keys, start_ts, callback } => {
                let snap_store = SnapshotStore::new(&snapshot, start_ts);
                callback(match snap_store.batch_get(&keys) {
                    Ok(results) => {
                        let mut res = vec![];
                        for (k, v) in keys.into_iter().zip(results.into_iter()) {
                            match v {
                                Ok(Some(x)) => res.push(Ok((k.raw().unwrap(), x))),
                                Ok(None) => {}
                                Err(e) => res.push(Err(::storage::Error::from(e))),
                            }
                        }
                        Ok(res)
                    }
                    Err(e) => Err(e.into()),
                });
            }

            Command::Scan { ctx, start_key, limit, start_ts, callback } => {
                let snap_store = SnapshotStore::new(&snapshot, start_ts);
                let mut scanner = try!(snap_store.scanner());
                callback(match snap_store.scan(start_key, limit) {
                    Ok(mut results) => {
                        Ok(results.drain(..).map(|x| x.map_err(::storage::Error::from)).collect())
                    }
                    Err(e) => Err(e.into()),
                });
            }

            Command::Prewrite { ctx, mutations, primary, start_ts, callback } => {
                let mut txn = MvccTxn::new(&snapshot, start_ts);

                let mut results = vec![];
                for m in mutations {
                    match txn.prewrite(m, &primary) {
                        Ok(_) => results.push(Ok(())),
                        e @ Err(MvccError::KeyIsLocked { .. }) => results.push(e.map_err(Error::from)),
                        Err(e) => return Err(Error::from(e)),
                    }
                }

                let pr: ProcessResult = ProcessResult::ResultSet {
                    result: results,
                };

                let cb = self.make_write_cb(pr, cid);
                self.engine.async_write(ctx, txn.modifies(), cb);
            }

            Command::Commit { ctx, keys, lock_ts, commit_ts, callback } => {
                let engine = self.engine.as_ref().as_ref();
                let mut txn = MvccTxn::new(&snapshot, lock_ts);

                for k in keys {
                    try!(txn.commit(&k, commit_ts));
                }

                let pr: ProcessResult = ProcessResult::Nothing {};

                let cb = self.make_write_cb(pr, cid);
                self.engine.async_write(ctx, txn.modifies(), cb);
            }

            Command::CommitThenGet { ctx, key, lock_ts, commit_ts, get_ts, callback } => {
                let mut txn = MvccTxn::new(&snapshot, lock_ts);

                let val = try!(txn.commit_then_get(&key, commit_ts, get_ts));

                let pr: ProcessResult = ProcessResult::Value { value: val };

                let cb = self.make_write_cb(pr, cid);
                self.engine.async_write(ctx, txn.modifies(), cb);
            }

            Command::Cleanup { ctx, key, start_ts, callback } => {
                let mut txn = MvccTxn::new(&snapshot, start_ts);

                try!(txn.rollback(&key));

                let pr: ProcessResult = ProcessResult::Nothing {};
                let cb = self.make_write_cb(pr, cid);
                self.engine.async_write(ctx, txn.modifies(), cb);
            }
            Command::Rollback { ctx, keys, start_ts, callback } => {
                let mut txn = MvccTxn::new(&snapshot, start_ts);

                for k in keys {
                    try!(txn.rollback(&k));
                }

                let pr: ProcessResult = ProcessResult::Nothing {};
                let cb = self.make_write_cb(pr, cid);
                self.engine.async_write(ctx, txn.modifies(), cb);
            }
            Command::RollbackThenGet { ctx, key, lock_ts, callback } => {
                let mut txn = MvccTxn::new(&snapshot, lock_ts);

                let val = try!(txn.rollback_then_get(&key));

                let pr: ProcessResult = ProcessResult::Value { value: val };
                let cb = self.make_write_cb(pr, cid);
                self.engine.async_write(ctx, txn.modifies(), cb);
            }
            _ => {
                panic!("unsupported commamd");
            }
        }

        Ok(())
    }

    fn process_write_finish(&self, cid: u64, pr: ProcessResult, result: Result<()>) {
        let cmd_ctx = self.cmd_ctxs.get(&cid).unwrap();
        assert_eq!(cid, cmd_ctx.cid);
        match cmd_ctx.cmd {
            Command::Prewrite { ctx, mutations, primary, start_ts, callback } => {
                callback(match result {
                    Ok(()) => {
                        match pr {
                            ProcessResult::ResultSet { result } => {
                                Ok(result.drain(..).map(|x| x.map_err(::storage::Error::from)).collect())
                            }
                            _ => { panic!("prewrite return but process result is not result set."); }
                        }
                    }
                    Err(e) => Err(e.into())
                });
            }
            Command::Commit { ctx, keys, lock_ts, commit_ts, callback } => {
                callback(result.map_err(::storage::Error::from));
            }
            Command::CommitThenGet { ctx, key, lock_ts, commit_ts, get_ts, callback } => {
                callback( match result {
                    Ok(()) => {
                        match pr {
                            ProcessResult::Value { value } => { Ok(value) }
                            _ => { panic!("commit then get return but process result is not value."); }
                        }
                    }
                    Err(()) => {
                        ::storage::Error::from
                    }
                });
            }
            Command::Cleanup { ctx, key, start_ts, callback } => {
                callback(result.map_err(::storage::Error::from));
            }
            Command::Rollback { ctx, keys, start_ts, callback } => {
                callback(result.map_err(::storage::Error::from));
            }
            Command::RollbackThenGet { ctx, key, lock_ts, callback } => {
                callback( match result {
                    Ok(()) => {
                        match pr {
                            ProcessResult::Value { value } => { Ok(value) }
                            _ => { panic!("rollback then get return but process result is not value."); }
                        }
                    }
                    Err(()) => {
                        ::storage::Error::from
                    }
                });
            }
            _ => { panic!("unsupported write cmd"); }
        }
    }

    fn process_failed_cmd(&self, cmd: Command, res: Result<()>) {
        match cmd {
            Command::Get { callback, .. } => {
                callback(res.map_err(::storage::Error::from));
            }
            Command::BatchGet { callback, .. } => {
                callback(match res {
                    Ok(()) => { panic!("failed means occur some error"); }
                    Err(e) => Err(e.into())
                });
            }
            Command::Scan { callback, .. } => {
                callback(match res {
                    Ok(()) => { panic!("failed means occur some error"); }
                    Err(e) => Err(e.into())
                });
            }
            Command::Prewrite { callback, .. } => {
                callback(match res {
                    Ok(()) => { panic!("failed means occur some error"); }
                    Err(e) => Err(e.into())
                });
            }
            Command::Commit { callback, .. } => {
                callback(res.map_err(::storage::Error::from));
            }
            Command::CommitThenGet { callback, .. } => {
                callback(res.map_err(::storage::Error::from));
            }
            Command::Cleanup { callback, .. } => {
                callback(res.map_err(::storage::Error::from));
            }
            Command::Rollback { callback, .. } => {
                callback(res.map_err(::storage::Error::from));
            }
            Command::RollbackThenGet { callback, .. } => {
                callback(res.map_err(::storage::Error::from));
            }
            _ => {
                panic!("unsupported write cmd");
            }
        }
    }

    fn make_write_cb(&self, pr: ProcessResult, cid: u64) -> Callback<()> {
        let ch = self.sendch.clone();
        Box::new(move |result: Result<()>| {
            if let Err(e) = ch.send(Msg::WriteFinish{
                cid: cid,
                pr: &pr,
                result: result,
            }) {
                error!("send GotSnap failed cmd id {}, err {:?}",
                cid,
                e);
            }
        })
    }

    fn extract_cmd_context_by_id(&self, cid: u64) -> Context {
        let rctx: RunningCtx = self.cmd_ctxs.get(cid).unwrap();
        match *rctx.cmd() {
            Command::Get { ref ctx, .. } => {
                ctx
            }
            Command::BatchGet { ref ctx, .. } => {
                ctx
            }
            Command::Scan { ref ctx, .. } => {
                ctx
            }
            Command::Prewrite { ref ctx, .. } => {
                ctx
            }
            Command::Commit { ref ctx, .. } => {
                ctx
            }
            Command::CommitThenGet { ref ctx, .. } => {
                ctx
            }
            Command::Cleanup { ref ctx, .. } => {
                ctx
            }
            Command::Rollback { ref ctx, .. } => {
                ctx
            }
            Command::RollbackThenGet { ref ctx, .. } => {
                ctx
            }
            _ => {
                panic!("unsupported write cmd");
            }
        }
    }
}

impl mio::Handler for Scheduler {

    type Timeout = Tick;
    type Message = Msg;

    fn timeout(&mut self, event_loop: &mut EventLoop<Self>, timeout: Tick) {
        //
    }

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Msg) {
        match msg {
            Msg::Quit => {
                info!("receive quit message");
                event_loop.shutdown();
            }

            Msg::CMD { cmd } => {
                // save cmd context and step forward
                let cid = self.next_id();
                if readonly_cmd(cmd) {
                    let ctx: RunningCtx = RunningCtx::new(cid, cmd, vec![]);
                    self.save_cmd_context(cid, ctx);

                    self.sendch.send(Msg::GetSnapshot{ cid: cid });
                } else {
                    let lock_idxs = self.calc_lock_indexs(cmd);
                    let mut ctx: RunningCtx = RunningCtx::new(cid, &cmd, &lock_idxs);

                    // if acquire all locks then get snapshot, or this command
                    // will be wake up by who owned lock after release lock
                    let acquire_count = self.rowlocks.aquire_by_indexs(&lock_idxs, cid);
                    ctx.incr_owned_lock(acquire_count);
                    self.save_cmd_context(cid, ctx);
                    if ctx.all_lock_acquired() {
                        self.sendch.send(Msg::GetSnapshot{ cid: cid });
                    }
                }
            }

            Msg::AcquireLock { cid } => {
                debug!("receive checklock msg cid = {}", cid);
                let mut ctx: RunningCtx = self.cmd_ctxs.get_mut(&cid).unwrap();

                let needed_locks = ctx.needed_locks();
                let owned_count = ctx.owned_lock_count();
                let new_acquired = self.rowlocs.acquire_by_indexs(&needed_locks[owned_count..], &cid);
                ctx.incr_owned_lock(new_acquired);

                if ctx.all_lock_acquired() {
                    self.sendch.send(Msg::GetSnapshot{ cid: &cid });
                }
            }

            Msg::GetSnapshot { cid } => {
                let ch = self.sendch.clone();
                let cb = box move |snapshot: Result<Box<Snapshot>>| {
                    if let Err(e) = ch.send(Msg::SnapshotFinish{
                        cid: cid,
                        snapshot: snapshot,
                    }) {
                        error!("send GotSnap failed cmd id {}, err {:?}",
                        cid,
                        e);
                    }
                };

                let ctx: &Context = self.extract_cmd_context_by_id(cid);
                self.engine.async_snapshot(&ctx, cb);
            }

            Msg::SnapshotFinish { cid, snapshot } => {
                let cmd_ctx = self.cmd_ctxs.get(&cid).unwrap();
                let cmd = cmd_ctx.cmd();
                assert_eq!(cid, cmd_ctx.cid);

                match snapshot {
                    Ok(Box(Snapshot)) => {
                        let res = self.process_cmd_with_snapshot(cid, &cmd, &snapshot);
                        let mut finished: bool = false;
                        match res {
                            Ok(()) => {
                                if readonly_cmd(&cmd) {
                                    finished = true;
                                }
                            }
                            Err(e) => {
                                self.process_failed_cmd(&cmd, Err(e.into()));
                                finished = true;
                            }
                        }

                        if finished {
                            self.finish_cmd(cid);
                        }
                    }
                    Err(e) => {
                        self.process_failed_cmd(cmd, Err(e.into()));
                        self.finish_cmd(cid);
                    }
                }
            }

            Msg::WriteFinish { cid, pr, result } => {
                self.process_write_finish(cid, pr, result);
                self.finish_cmd(cid);
            }
        }
    }

    fn tick(&mut self, event_loop: &mut EventLoop<Self>) {
        if !event_loop.is_running() {
            // stop work threads if has
        }
    }
}

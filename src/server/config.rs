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

pub use raftstore::store::Config as StoreConfig;
use super::Result;

const DEFAULT_CLUSTER_ID: u64 = 0;
pub const DEFAULT_LISTENING_ADDR: &'static str = "127.0.0.1:20160";
const DEFAULT_ADVERTISE_LISTENING_ADDR: &'static str = "";

#[derive(Clone, Debug)]
pub struct Config {
    pub cluster_id: u64,

    // Server listening address.
    pub addr: String,

    // Server advertise listening address for outer communication.
    // If not set, we will use listening address instead.
    // The correct format for advertise_addr is http://host:port or
    // https://host:port, if only host:port supplied, a default scheme
    // http or https if we use SSL will be added.
    pub advertise_addr: String,

    pub store_cfg: StoreConfig, // TODO: add SSL configuration later.
}

impl Default for Config {
    fn default() -> Config {
        Config {
            cluster_id: DEFAULT_CLUSTER_ID,
            addr: DEFAULT_LISTENING_ADDR.to_owned(),
            advertise_addr: DEFAULT_ADVERTISE_LISTENING_ADDR.to_owned(),
            store_cfg: StoreConfig::default(),
        }
    }
}

impl Config {
    pub fn new() -> Config {
        Config::default()
    }

    pub fn validate(&mut self) -> Result<()> {
        try!(self.store_cfg.validate());
        Ok(())
    }
}

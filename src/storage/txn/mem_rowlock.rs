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


use std::collections::{LinkedList, BTreeSet};
use std::hash::{Hash, SipHasher, Hasher};

pub struct RowLocks {
    // linked list store waiting commands, tree set use to check existed
    locks: Vec<(LinkedList<u64>, BTreeSet<u64>)>,
    size: usize,
}

impl RowLocks {
    pub fn new(size: usize) -> RowLocks {
        RowLocks {
            locks: (0..size).map(|_|  (LinkedList::new(), BTreeSet::new())).collect(),
            size: size,
        }
    }

    pub fn calc_lock_indexs<H>(&self, keys: &[H]) -> Vec<usize>
        where H: Hash
    {
        let mut indices: Vec<usize> = keys.iter().map(|x| self.calc_index(x)).collect();
        indices.sort();
        indices.dedup();
        indices
    }

    pub fn acquire_by_indexs(&self, indexs: &[usize], who: u64) -> usize {

        let mut acquired_count: usize = 0;
        for i in 0..indexs.len() {
            let (mut waiting_list, mut sets) = self.locks[i];

            match waiting_list.front() {
                Some(cid) => {
                    if cid == who {
                        acquired_count += 1;
                    } else {
                        if !sets.contains(cid) {
                            waiting_list.push_back(who);
                            sets.insert(who);
                        }
                        return acquired_count;
                    }
                }
                None => {
                    waiting_list.push_back(who);
                    sets.insert(who);
                    acquired_count += 1;
                }
            }
        }

        acquired_count
    }

    // release all locks owned, and return wakeup list
    pub fn release_by_indexs(&self, indexs: &[usize], who: u64) -> Vec<u64> {
        let mut wakeup_list = vec![];

        for i in indexs {
            let (mut waiting_list, mut sets) = self.locks[i];

            match waiting_list.pop_front() {
                Some(head) => {
                    if head != who {
                        panic!("release lock not owned");
                    }
                }
                None => {
                    panic!("should not happen");
                }
            }
            sets.remove(who);

            match waiting_list.front() {
                Some(wakeup) => {
                    wakeup_list.push_back();
                }
                None => {}
            }
        }

        wakeup_list
    }

    fn calc_index<H>(&self, key: &H) -> usize
        where H: Hash
    {
        let mut s = SipHasher::new();
        key.hash(&mut s);
        (s.finish() as usize) % self.size
    }
}

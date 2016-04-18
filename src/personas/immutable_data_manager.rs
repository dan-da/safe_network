// Copyright 2015 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under (1) the MaidSafe.net Commercial License,
// version 1.0 or later, or (2) The General Public License (GPL), version 3, depending on which
// licence you accepted on initial access to the Software (the "Licences").
//
// By contributing code to the SAFE Network Software, or to this project generally, you agree to be
// bound by the terms of the MaidSafe Contributor Agreement, version 1.0.  This, along with the
// Licenses can be found in the root directory of this project at LICENSE, COPYING and CONTRIBUTOR.
//
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.
//
// Please review the Licences for the specific language governing permissions and limitations
// relating to use of the SAFE Network Software.

use std::mem;
use std::convert::From;
use std::collections::{HashMap, HashSet};

use error::InternalError;
use itertools::Itertools;
use kademlia_routing_table::GROUP_SIZE;
use safe_network_common::client_errors::GetError;
use timed_buffer::TimedBuffer;
use maidsafe_utilities::serialisation;
use routing::{self, Authority, Data, DataIdentifier, ImmutableData, ImmutableDataBackup,
              ImmutableDataSacrificial, MessageId, PlainData, RequestContent, RequestMessage,
              ResponseContent, ResponseMessage};
use std::time::Duration;
use types::{Refresh, RefreshValue};
use vault::RoutingNode;
use xor_name::{self, XorName};

pub const REPLICANTS: usize = 2;

/// State of data_holder.
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug, RustcEncodable, RustcDecodable)]
pub enum DataHolderState {
    Good,
    Failed,
    Pending,
}

/// This is the name of a PmidNode which has been chosen to store the data on.  It is associated with
/// a specific piece of `ImmutableData`.  It is marked as `Pending` until the response of the Put
/// request is received, when it is then marked as `Good` or `Failed` depending on the response
/// result.  It remains `Good` until it fails a Get request, at which time it is deemed `Failed`, or
/// until it disconnects or moves out of the close group for the chunk, when it is removed from the
/// list of holders.
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug, RustcEncodable, RustcDecodable)]
pub struct DataHolder {
    name: XorName,
    state: DataHolderState,
}

/// Collection of PmidNodes holding a copy of the chunk
#[derive(Clone, PartialEq, Eq, Debug, RustcEncodable, RustcDecodable)]
pub struct Account {
    data_name: DataIdentifier,
    data_holders: HashSet<DataHolder>,
}

impl Account {
    pub fn new(data_name: DataIdentifier, data_holders: HashSet<DataHolder>) -> Account {
        Account {
            data_name: data_name,
            data_holders: data_holders,
        }
    }

    pub fn data_holders(&self) -> &HashSet<DataHolder> {
        &self.data_holders
    }

    pub fn name(&self) -> XorName {
        self.data_name.name()
    }

    pub fn data_type_name(&self) -> DataIdentifier {
        self.data_name
    }

    pub fn data_holders_mut(&mut self) -> &mut HashSet<DataHolder> {
        &mut self.data_holders
    }
}

pub struct ImmutableDataManager {
    accounts: HashSet<Account>,
    ongoing_gets: TimedBuffer<(DataIdentifier, MessageId), RequestMessage>,
    data_cache: HashMap<DataIdentifier, Data>,
}

impl ImmutableDataManager {
    pub fn new() -> ImmutableDataManager {
        ImmutableDataManager {
            accounts: HashMap::new(),
            ongoing_gets: TimedBuffer::new(Duration::minutes(5)),
            data_cache: HashMap::new(),
        }
    }
    // ######################### Get ################################
    pub fn handle_get(&mut self,
                      routing_node: &RoutingNode,
                      request: &RequestMessage,
                      data_request: &DataIdentifier,
                      message_id: &MessageId)
                      -> Result<(), InternalError> {

        // If the account doesn't exist, respond with GetFailure
        let data_holders = if let Some(account) = self.accounts.get(data_request.name()) {
            account
        } else {
            self.send_get_failure(routing_node, request, message_id)
        };

        // If data in data_Cache, return it from here
        if let Some(immutable_data) = self.data_cache.get(data_request.name()) {
            self.send_get_success(routing_node,
                                  Data::Immutable(immutable_data.clone()),
                                  message_id)
        }

        // Request the data from our PmidNodes (We do not need to check here for dead holders, that
        // is managed via churn handling)
        for holder in data_holders {
            match holder.state {
                DataHolderState::Good | DataHolderState::Pending => {
                    self.routing_node.send_get_request(Authority::NaeManager(*data_request.name()),
                                                       Authority::ManagedNode(holder.name()),
                                                       *data_request,
                                                       message_id.clone())
                }
                DataHolderState::Failed => {} // could be full
            }
        }
        // Add to ongoing_gets and reply when we get the data
        self.ongoing_gets.insert(*data_request.name(), *message_id)
    }

    fn send_get_failure(&mut self,
                        routing_node: &RoutingNode,
                        request_msg: &RequestMessage,
                        message_id: MessageId)
                        -> Result<(), InternalError> {
        let src = request_msg.dst.clone();
        let dst = request_msg.src.clone();
        let error = GetError::NoSuchData;
        let external_error_indicator = try!(serialisation::serialise(&error));
        routing_node.send_get_failure(src,
                                      dst,
                                      request_msg.clone(),
                                      external_error_indicator,
                                      *message_id)
    }

    fn send_get_success(&mut self,
                        routing_node: &RoutingNode,
                        data: Data,
                        request_msg: &RequestMessage,
                        message_id: MessageId)
                        -> Result<(), InternalError> {
        let src = request_msg.dst.clone();
        let dst = request_msg.src.clone();
        routing_node.send_get_success(src, dst, data, message_id)

    }
    /// recieved data we requested
    pub fn handle_client_get_success(&mut self,
                                     routing_node: &RoutingNode,
                                     response: &ResponseMessage,
                                     data: &Data,
                                     message_id: &MessageId)
                                     -> Result<(), InternalError> {

        // make sure we are still managing this group
        let _ = try!(routing_node.close_group(data.name()));
        self.find_and_reply_to_requestor(routing_node, response, data, message_id)
            .and(self.update_good_dataholder_in_account(routing_node, response, data))
    }

    fn find_and_reply_to_requestor(&mut self,
                                   routing_node: &RoutingNode,
                                   response: &ResponseMessage,
                                   data: &Data,
                                   message_id: &MessageId)
                                   -> Result<(), InternalError> {
        if let Some(request) = self.ongoing_gets.remove((data.identifier(), message_id)) {
            let src = request.dst.clone();
            let dst = request.src;
            trace!("Sending GetSuccess back to {:?}", dst);
            routing_node.send_get_success(src, dst, data, message_id);
        }
        Ok(())
    }

    fn update_good_dataholder_in_account(&mut self,
                                         routing_node: &RoutingNode,
                                         response: &ResponseMessage,
                                         data: &Data)
                                         -> Result<(), InternalError> {
        match self.accounts.get_mut(data.identify()) {
            Some(acc) => {
                // make sure data holder is marked good in the account.
                let holder = DataHolder {
                    name: response.src.name(),
                    state: DataHolderState::Good,
                };
                acc.data_holders_mut().insert(&holder);
            }
            None => {
                // We need to create the account and mark this holder as good
                // OK as routing guarantees we asked for this Get request!
                let hold = HashSet::new();
                let holder = DataHolder {
                    name: response.src.name(),
                    state: DataHolderState::Good,
                };
                let _ = hold.insert(&holder);
                let acc = Account::new(data.identifier(), hold);
                self.accounts.insert(acc);
            }
        }
    }

    pub fn handle_get_success_from_data_managers(&mut self,
                                                 routing_node: &RoutingNode,
                                                 response: &ResponseMessage,
                                                 data: &Data,
                                                 message_id: &MessageId)
                                                 -> Result<(), InternalError> {
        uimplemented!()
        // [TODO]: check data type, check all conversions and if we should be managing that data - 2016-04-17 10:21pm
    }

    pub fn handle_get_failure(&mut self,
                              routing_node: &RoutingNode,
                              pmid_node: &XorName,
                              message_id: &MessageId,
                              request: &RequestMessage,
                              _external_error_indicator: &[u8])
                              -> Result<(), InternalError> {
        let mut metadata_message_id = None;
        let data_name = if let Ok((data_name, metadata)) =
                               self.find_ongoing_get_after_failure(request) {
            metadata_message_id = Some(metadata.message_id);

            // Mark the responder as "failed"
            let _ = metadata.data_holders.insert(DataHolder {
                name: *pmid_node,
                state: DataHolderState::Failed,
            });

            trace!("Metadata for Get {} updated to {:?}", data_name, metadata);
            data_name
        } else {
            if let RequestContent::Get(ref data_request, _) = request.content {
                data_request.name()
            } else {
                return Err(InternalError::InvalidResponse);
            }
        };

        // Mark the responder as "failed" in the account if it was previously marked "good"
        if let Some(account) = self.accounts.get_mut(&data_name) {
            let _ = account.data_holders.insert(DataHolder {
                name: *pmid_node,
                state: DataHolderState::Failed,
            });
            trace!("Account for {} updated to {:?}", data_name, account);
        }

        if let Some(msg_id) = metadata_message_id {
            try!(self.check_and_replicate_after_get(routing_node, &data_name, &msg_id));
            Ok(())
        } else {
            Err(InternalError::FailedToFindCachedRequest(*message_id))
        }
    }


    // ##################### Put ###############################

    pub fn handle_put(&mut self,
                      routing_node: &RoutingNode,
                      full_pmid_nodes: &HashSet<XorName>,
                      request: &RequestMessage,
                      orig_data: Data,
                      message_id: routing::MessageId)
                      -> Result<(), InternalError> {

        let data_name = orig_data.name();
        // Only send success response if src is ClientManager.
        if let Authority::ClientManager(_) = request.src {
            let src = request.dst.clone();
            let dst = request.src.clone();
            let _ = routing_node.send_put_success(src, dst, data_name, message_id);
        }

        // If the data already exists, we are finished
        if self.accounts.contains_key(&data_name) {
            return Ok(());
        }

        // Choose the PmidNodes to store the data on, and add them in a new database entry.
        // This can potentially return an empty list if all the nodes are full.
        let target_data_holders = try!(self.choose_initial_data_holders(routing_node,
                                                                        full_pmid_nodes,
                                                                        &data_name));
        trace!("ImmutableDataManager chosen {:?} as data_holders for chunk {:?}",
               target_data_holders,
               orig_data);
        let _ = self.accounts.insert(data_name,
                                     Account::new(orig_data.get_type_tag(),
                                                  target_data_holders.clone()));
        let _ = self.data_cache.insert(orig_data.name(), orig_data.clone());

        // Send the message on to the PmidNodes' managers.
        for pmid_node in target_data_holders {
            let src = Authority::NaeManager(data_name);
            let dst = Authority::NodeManager(pmid_node.name);
            let _ = routing_node.send_put_request(src, dst, orig_data.clone(), message_id);
        }

        // If this is a "Normal" copy, we need to Put the "Backup" and "Sacrificial" copies too.
        if let Data::Immutable(data) = orig_data {
            let backup = ImmutableDataBackup::new(data.clone());
            let _ = routing_node.send_put_request(request.dst.clone(),
                                                  Authority::NaeManager(backup.name()),
                                                  Data::Immutable(backup),
                                                  message_id);
            let sacrificial = ImmutableDataSacrificial::new(data.clone());
            let _ = routing_node.send_put_request(request.dst.clone(),
                                                  Authority::NaeManager(sacrificial.name()),
                                                  Data::Immutable(sacrificial),
                                                  message_id);
        }

        Ok(())
    }

    pub fn handle_put_success(&mut self,
                              pmid_node: &XorName,
                              data_name: &XorName)
                              -> Result<(), InternalError> {
        // TODO: Check that the data_name is correct.
        let account = if let Some(account) = self.accounts.get_mut(&data_name) {
            account
        } else {
            debug!("Don't have account for {}", data_name);
            return Err(InternalError::InvalidResponse);
        };

        if !account.data_holders_mut().remove(&DataHolder::Pending(*pmid_node)) {
            debug!("Failed to remove {} - {:?}", pmid_node, account);
            return Err(InternalError::InvalidResponse);
        }
        account.data_holders_mut().insert(DataHolder::Good(*pmid_node));
        let _ = self.data_cache.remove(&data_name);

        Ok(())
    }

    pub fn handle_put_failure(&mut self,
                              routing_node: &RoutingNode,
                              pmid_node: &XorName,
                              immutable_data: &ImmutableData,
                              message_id: &MessageId)
                              -> Result<(), InternalError> {
        let account = if let Some(account) = self.accounts.get_mut(&immutable_data.name()) {
            account
        } else {
            debug!("Don't have account for {}", immutable_data.name());
            return Err(InternalError::InvalidResponse);
        };

        // Mark the holder as Failed
        if !account.data_holders_mut().remove(&DataHolder::Pending(*pmid_node)) {
            debug!("Failed to remove {} - {:?}", pmid_node, account);
            return Err(InternalError::InvalidResponse);
        }
        account.data_holders_mut().insert(DataHolder::Failed(*pmid_node));

        // Find a replacement - first node in close_group not already tried
        let data_name = immutable_data.name();
        match try!(routing_node.close_group(data_name)) {
            Some(target_data_holders) => {
                if let Some(new_holder) = target_data_holders.iter()
                                                             .filter(|elt| {
                                                                 !account.data_holders()
                                                                         .iter()
                                                                         .any(|exclude| {
                                                                             elt == &exclude.name()
                                                                         })
                                                             })
                                                             .next() {
                    let src = Authority::NaeManager(immutable_data.name());
                    let dst = Authority::NodeManager(*new_holder);
                    let data = Data::Immutable(immutable_data.clone());
                    let _ = routing_node.send_put_request(src, dst, data, *message_id);
                    account.data_holders_mut().insert(DataHolder::Pending(*new_holder));
                } else {
                    error!("Failed to find a new storage node for {}.", data_name);
                    return Err(InternalError::UnableToAllocateNewPmidNode);
                }
            }
            None => return Err(InternalError::NotInCloseGroup),
        }

        Ok(())
    }

    pub fn check_timeout(&mut self, routing_node: &RoutingNode) {
        for data_name in &self.ongoing_gets.get_expired() {
            let message_id;
            {
                // Safe to unwrap here as we just got all these keys via `get_expired`
                let mut metadata = self.ongoing_gets
                                       .get_mut(data_name)
                                       .expect("Logic error in TimedBuffer");
                for pmid_node in &mut metadata.data_holders {
                    // Get timed-out PmidNodes
                    if let DataHolder::Pending(name) = *pmid_node {
                        warn!("PmidNode {} failed to reply to Get request for {}.",
                              name,
                              data_name);
                        // Mark it as failed in the cache
                        *pmid_node = DataHolder::Failed(name);
                        // Mark it as "failed" in the account if it was previously marked "good"
                        if let Some(account) = self.accounts.get_mut(data_name) {
                            if account.data_holders_mut().remove(&DataHolder::Good(name)) {
                                account.data_holders_mut().insert(DataHolder::Failed(name));
                            }
                        }
                    }
                }
                message_id = metadata.message_id;
            }
            // let _ = self.ongoing_gets.insert(*data_name, metadata);
            let _ = self.check_and_replicate_after_get(routing_node, data_name, &message_id);
        }
    }
    // ################################# Churn ##################################
    pub fn handle_refresh(&mut self, data_name: XorName, account: Account) {
        let _ = self.accounts.insert(data_name, account);
    }

    pub fn handle_node_added(&mut self, routing_node: &RoutingNode, node_name: &XorName) {
        let message_id = MessageId::from_added_node(*node_name);
        // Remove entries from `data_cache` that we are not responsible for any more.
        let data_cache = mem::replace(&mut self.data_cache, HashMap::new());
        self.data_cache = data_cache.into_iter()
                                    .filter(|&(ref data_name, _)| {
                                        self.close_group_to(routing_node, data_name)
                                            .is_some()
                                    })
                                    .collect();
        // Remove entries from `ongoing_gets` that we are not responsible for any more.
        self.ongoing_gets
            .remove_keys(|&data_name| {
                match routing_node.close_group(*data_name) {
                    Ok(Some(_)) => false,
                    _ => true,
                }
            });
        // Only retain accounts for which we're still in the close group.
        let accounts = mem::replace(&mut self.accounts, HashMap::new());
        self.accounts = accounts.into_iter()
                                .filter_map(|(data_name, mut account)| {
                                    let close_group = if let Some(group) =
                                                             self.close_group_to(routing_node,
                                                                                 &data_name) {
                                        group
                                    } else {
                                        return None;
                                    };
                                    if close_group.contains(node_name) {
                                        *account.data_holders_mut() =
                    account.data_holders()
                           .iter()
                           .filter(|pmid_node| {
                               // Remove this data holder if it has been pushed out of the close
                               // group by the new node, i. e. if it is now too far away from the
                               // data. If Routing would suppress NodeAdded events on the side of
                               // the joining node, we could instead do this here:
                               // close_group.contains(pmid_node.name())
                               close_group.get(GROUP_SIZE - 1).into_iter().all(|name| {
                                   xor_name::closer_to_target_or_equal(pmid_node.name(),
                                                                       name,
                                                                       &data_name)
                               })
                           })
                           .cloned()
                           .collect();
                                    }
                                    let _ = self.handle_churn_for_account(routing_node,
                                                                          &data_name,
                                                                          &message_id,
                                                                          close_group,
                                                                          &mut account);
                                    Some((data_name, account))
                                })
                                .collect();
    }

    fn handle_churn_for_account(&mut self,
                                routing_node: &RoutingNode,
                                data_name: &XorName,
                                message_id: &MessageId,
                                close_group: Vec<XorName>,
                                account: &mut Account)
                                -> Option<(XorName, Account)> {
        trace!("Churning for {} - holders after: {:?}", data_name, account);

        // Check to see if the chunk should be replicated
        let new_replicants_count = Self::new_replicants_count(&account);
        if new_replicants_count > 0 {
            trace!("Need {} more replicant(s) for {}",
                   new_replicants_count,
                   data_name);
            if !self.handle_churn_for_ongoing_puts(routing_node,
                                                   data_name,
                                                   message_id,
                                                   account,
                                                   &close_group,
                                                   new_replicants_count) &&
               !self.handle_churn_for_ongoing_gets(data_name, &close_group) {
                // Create a new entry and send Get requests to each of the current holders
                let entry = PendingGetRequest::new(message_id, &account);
                trace!("Created ongoing get entry for {} - {:?}", data_name, entry);
                entry.send_get_requests(routing_node, data_name, *message_id);
                let _ = self.ongoing_gets.insert(*data_name, entry);
            }
        }

        self.send_refresh(routing_node, &data_name, &account, &message_id);
        Some((*data_name, account.clone()))
    }

    fn close_group_to(&self,
                      routing_node: &RoutingNode,
                      data_name: &XorName)
                      -> Option<Vec<XorName>> {
        match routing_node.close_group(*data_name) {
            Ok(None) => {
                trace!("No longer a DM for {}", data_name);
                None
            }
            Ok(Some(close_group)) => Some(close_group),
            Err(error) => {
                error!("Failed to get close group: {:?} for {}", error, data_name);
                None
            }
        }
    }

    pub fn handle_node_lost(&mut self, routing_node: &RoutingNode, node_name: &XorName) {
        let message_id = MessageId::from_lost_node(*node_name);
        let mut accounts = mem::replace(&mut self.accounts, HashMap::new());
        accounts.iter_mut().foreach(|(data_name, account)| {
            *account.data_holders_mut() = account.data_holders()
                                                 .iter()
                                                 .filter(|pmid_node| pmid_node.name() != node_name)
                                                 .cloned()
                                                 .collect();
            if let Some(close_group) = self.close_group_to(routing_node, &data_name) {
                let _ = self.handle_churn_for_account(routing_node,
                                                      data_name,
                                                      &message_id,
                                                      close_group,
                                                      account);
            }
        });
        let _ = mem::replace(&mut self.accounts, accounts);
    }


    fn new_replicants_count(account: &Account) -> usize {
        let mut holder_count = 0;
        for pmid_node in account.data_holders() {
            match *pmid_node {
                DataHolder::Pending(_) |
                DataHolder::Good(_) => holder_count += 1,
                DataHolder::Failed(_) => (),
            }
        }
        if holder_count < REPLICANTS {
            REPLICANTS - holder_count
        } else {
            0
        }
    }

    fn handle_churn_for_ongoing_puts(&mut self,
                                     routing_node: &RoutingNode,
                                     data_name: &XorName,
                                     message_id: &MessageId,
                                     account: &mut Account,
                                     close_group: &[XorName],
                                     mut new_replicants_count: usize)
                                     -> bool {
        let data = match self.data_cache.get(data_name) {
            Some(data) => data,
            None => return false,
        };

        // We have an entry in the `data_cache`, so replicate to new peers
        for group_member in close_group {
            if account.data_holders()
                      .iter()
                      .any(|&pmid_node| pmid_node.name() == group_member) {
                // This is already a holder - skip
                continue;
            }
            trace!("Replicating {} - sending Put to {}",
                   data_name,
                   group_member);
            let src = Authority::NaeManager(*data_name);
            let dst = Authority::NodeManager(*group_member);
            let _ = routing_node.send_put_request(src,
                                                  dst,
                                                  Data::Immutable(data.clone()),
                                                  *message_id);
            account.data_holders_mut().insert(DataHolder::Pending(*group_member));
            new_replicants_count -= 1;
            if new_replicants_count == 0 {
                return true;
            }
        }
        warn!("Failed to find a new close group member to replicate {} to",
              data_name);
        true
    }

    fn handle_churn_for_ongoing_gets(&mut self,
                                     data_name: &XorName,
                                     close_group: &[XorName])
                                     -> bool {
        if let Some(mut metadata) = self.ongoing_gets.get_mut(&data_name) {
            trace!("Already getting {} - {:?}", data_name, metadata);
            // Remove any holders which no longer belong in the cache entry
            metadata.data_holders
                    .retain(|pmid_node| {
                        close_group.get(GROUP_SIZE - 1).into_iter().all(|name| {
                            xor_name::closer_to_target_or_equal(pmid_node.name(), name, data_name)
                        })
                    });
            trace!("Updated ongoing get for {} to {:?}", data_name, metadata);
            true
        } else {
            false
        }
    }

    fn send_refresh(&self,
                    routing_node: &RoutingNode,
                    data_name: &XorName,
                    account: &Account,
                    message_id: &MessageId) {
        let src = Authority::NaeManager(*data_name);
        let refresh = Refresh::new(data_name,
                                   RefreshValue::ImmutableDataManagerAccount(account.clone()));
        if let Ok(serialised_refresh) = serialisation::serialise(&refresh) {
            trace!("ImmutableDataManager sending refresh for account {:?}",
                   src.name());
            let _ = routing_node.send_refresh_request(src.clone(),
                                                      src.clone(),
                                                      serialised_refresh,
                                                      *message_id);
        }
    }

    fn reply_with_data_else_cache_request(routing_node: &RoutingNode,
                                          request: &RequestMessage,
                                          message_id: &MessageId,
                                          metadata: &mut PendingGetRequest) {
        // If we've already received the chunk, send it to the new requester.  Otherwise add the
        // request to the others for later handling.
        if let Some(ref data) = metadata.data {
            let src = request.dst.clone();
            let dst = request.src.clone();
            let _ = routing_node.send_get_success(src,
                                                  dst,
                                                  Data::Immutable(data.clone()),
                                                  *message_id);
        } else {
            metadata.requests.push((*message_id, request.clone()));
        }
    }

    fn check_and_replicate_after_get(&mut self,
                                     routing_node: &RoutingNode,
                                     data_name: &XorName,
                                     message_id: &MessageId)
                                     -> Result<(), InternalError> {
        let mut finished = false;
        let mut new_data_holders = HashSet::<DataHolder>::new();
        if let Some(metadata) = self.ongoing_gets.get_mut(&data_name) {
            // Count the good holders, but just return from this function if any queried holders
            // haven't responded yet
            let mut good_holder_count = 0;
            for queried_pmid_node in &metadata.data_holders {
                match *queried_pmid_node {
                    DataHolder::Pending(_) => return Ok(()),
                    DataHolder::Good(_) => good_holder_count += 1,
                    DataHolder::Failed(_) => (),
                }
            }
            trace!("Have {} good holders for {}", good_holder_count, data_name);

            if good_holder_count >= REPLICANTS {
                // We can now delete this cached get request with no need for further action
                finished = true;
            } else if let Some(ref data) = metadata.data {
                assert_eq!(*data_name, data.name());
                // Put to new close peers and delete this cached get request
                new_data_holders = try!(Self::replicate_after_get(routing_node,
                                                                  data,
                                                                  &metadata.data_holders,
                                                                  message_id));
                finished = true;
            } else {
                // Recover the data from backup and/or sacrificial locations
                Self::recover_from_other_locations(routing_node, metadata, data_name, message_id);
            }
        } else {
            warn!("Failed to find metadata for check_and_replicate_after_get of {}",
                  data_name);
        }

        if finished {
            let _ = self.ongoing_gets.remove(data_name);
        }

        if !new_data_holders.is_empty() {
            trace!("Replicating {} - new holders: {:?}",
                   data_name,
                   new_data_holders);
            if let Some(account) = self.accounts.get_mut(data_name) {
                trace!("Replicating {} - account before: {:?}", data_name, account);
                *account.data_holders_mut() = account.data_holders()
                                                     .union(&new_data_holders)
                                                     .cloned()
                                                     .collect();
                trace!("Replicating {} - account after:  {:?}", data_name, account);
            }
        }

        Ok(())
    }

    fn replicate_after_get(routing_node: &RoutingNode,
                           data: &ImmutableData,
                           queried_data_holders: &[DataHolder],
                           message_id: &MessageId)
                           -> Result<HashSet<DataHolder>, InternalError> {
        let mut good_nodes = HashSet::<DataHolder>::new();
        let mut nodes_to_exclude = HashSet::<XorName>::new();
        for queried_pmid_node in queried_data_holders {
            match *queried_pmid_node {
                DataHolder::Good(name) => {
                    let _ = good_nodes.insert(DataHolder::Good(name));
                    let _ = nodes_to_exclude.insert(name);
                }
                DataHolder::Failed(name) => {
                    let _ = nodes_to_exclude.insert(name);
                }
                DataHolder::Pending(_) => unreachable!(),
            }
        }
        let data_name = data.name();
        trace!("Replicating {} - good nodes: {:?}", data_name, good_nodes);
        trace!("Replicating {} - nodes to be excluded: {:?}",
               data_name,
               nodes_to_exclude);
        let target_data_holders = match try!(routing_node.close_group(data_name)) {
            Some(target_data_holders) => {
                target_data_holders.into_iter()
                                   .filter(|elt| !nodes_to_exclude.contains(elt))
                                   .take(REPLICANTS - good_nodes.len())
                                   .map(DataHolder::Pending)
                                   .collect::<HashSet<DataHolder>>()
            }
            None => return Err(InternalError::NotInCloseGroup),
        };

        trace!("Replicating {} - target nodes: {:?}",
               data_name,
               target_data_holders);
        for new_pmid_node in &target_data_holders {
            trace!("Replicating {} - sending Put to {}",
                   data_name,
                   new_pmid_node.name());
            let src = Authority::NaeManager(data_name);
            let dst = Authority::NodeManager(*new_pmid_node.name());
            let _ = routing_node.send_put_request(src,
                                                  dst,
                                                  Data::Immutable(data.clone()),
                                                  *message_id);
        }
        Ok(target_data_holders)
    }

    fn recover_from_other_locations(routing_node: &RoutingNode,
                                    metadata: &mut PendingGetRequest,
                                    data_name: &XorName,
                                    message_id: &MessageId) {
        metadata.data_holders.clear();
        // If this Vault is a Backup or Sacrificial manager just return failure to any requesters
        // waiting for responses.
        match metadata.requested_data_type {
            ImmutableDataType::Backup |
            ImmutableDataType::Sacrificial => {
                Self::send_get_failures(routing_node, metadata);
            }
            _ => (),
        }
        metadata.send_get_requests(routing_node, data_name, *message_id);
    }

    fn send_get_failures(routing_node: &RoutingNode, metadata: &mut PendingGetRequest) {
        while let Some((original_message_id, request)) = metadata.requests.pop() {
            let src = request.dst.clone();
            let dst = request.src.clone();
            trace!("Sending GetFailure back to {:?}", dst);
            let error = GetError::NoSuchData;
            if let Ok(external_error_indicator) = serialisation::serialise(&error) {
                let _ = routing_node.send_get_failure(src,
                                                      dst,
                                                      request,
                                                      external_error_indicator,
                                                      original_message_id);
            }
        }
    }

    fn choose_initial_data_holders(&self,
                                   routing_node: &RoutingNode,
                                   full_pmid_nodes: &HashSet<XorName>,
                                   data_name: &XorName)
                                   -> Result<HashSet<DataHolder>, InternalError> {
        match try!(routing_node.close_group(*data_name)) {
            Some(mut target_data_holders) => {
                target_data_holders.retain(|target| !full_pmid_nodes.contains(target));
                target_data_holders.truncate(REPLICANTS);
                Ok(target_data_holders.into_iter()
                                      .map(DataHolder::Pending)
                                      .collect::<HashSet<DataHolder>>())
            }
            None => Err(InternalError::NotInCloseGroup),
        }
    }
}

impl Default for ImmutableDataManager {
    fn default() -> ImmutableDataManager {
        ImmutableDataManager::new()
    }
}



#[cfg(test)]
#[cfg_attr(feature="clippy", allow(indexing_slicing))]
#[cfg(not(feature="use-mock-crust"))]
mod test {
    use super::*;

    use std::collections::HashSet;
    use std::mem;
    use std::sync::mpsc;

    use maidsafe_utilities::{log, serialisation};
    use rand::distributions::{IndependentSample, Range};
    use rand::{random, thread_rng};
    use routing::{Authority, Data, DataIdentifier, ImmutableData, ImmutableDataType, MessageId,
                  RequestContent, RequestMessage, ResponseContent, ResponseMessage};
    use safe_network_common::client_errors::GetError;
    use sodiumoxide::crypto::sign;
    use types::{Refresh, RefreshValue};
    use utils::generate_random_vec_u8;
    use vault::RoutingNode;
    use xor_name::XorName;

    struct PutEnvironment {
        pub client_manager: Authority,
        pub im_data: ImmutableData,
        pub message_id: MessageId,
        pub incoming_request: RequestMessage,
        pub outgoing_requests: Vec<RequestMessage>,
        pub initial_holders: HashSet<DataHolder>,
    }

    struct GetEnvironment {
        pub client: Authority,
        pub message_id: MessageId,
        pub request: RequestMessage,
    }

    struct Environment {
        pub routing: RoutingNode,
        pub immutable_data_manager: ImmutableDataManager,
    }

    impl Environment {
        pub fn new() -> Environment {
            let _ = log::init(false);
            let env = Environment {
                routing: unwrap_result!(RoutingNode::new(mpsc::channel().0, false)),
                immutable_data_manager: ImmutableDataManager::new(),
            };
            env
        }

        pub fn get_close_data(&self) -> ImmutableData {
            loop {
                let im_data = ImmutableData::new(ImmutableDataType::Normal,
                                                 generate_random_vec_u8(1024));
                if let Ok(Some(_)) = self.routing.close_group(im_data.name()) {
                    return im_data;
                }
            }
        }

        pub fn get_close_node(&self) -> XorName {
            loop {
                let name = random::<XorName>();
                if let Ok(Some(_)) = self.routing.close_group(name) {
                    return name;
                }
            }
        }

        fn lose_close_node(&self, target: &XorName) -> XorName {
            if let Ok(Some(close_group)) = self.routing.close_group(*target) {
                let mut rng = thread_rng();
                let range = Range::new(0, close_group.len());
                let our_name = if let Ok(ref name) = self.routing.name() {
                    *name
                } else {
                    unreachable!()
                };
                loop {
                    let index = range.ind_sample(&mut rng);
                    if close_group[index] != our_name {
                        return close_group[index];
                    }
                }
            } else {
                random::<XorName>()
            }
        }

        pub fn put_im_data(&mut self) -> PutEnvironment {
            let im_data = self.get_close_data();
            let message_id = MessageId::new();
            let content = RequestContent::Put(Data::Immutable(im_data.clone()), message_id);
            let client_manager = Authority::ClientManager(random());
            let client_request = RequestMessage {
                src: client_manager.clone(),
                dst: Authority::NaeManager(im_data.name()),
                content: content.clone(),
            };
            let full_pmid_nodes = HashSet::new();
            unwrap_result!(self.immutable_data_manager
                               .handle_put(&self.routing, &full_pmid_nodes, &client_request));
            let outgoing_requests = self.routing.put_requests_given();
            assert_eq!(outgoing_requests.len(), REPLICANTS + 2);
            let initial_holders = outgoing_requests.iter()
                                                   .map(|put_request| {
                                                       DataHolder::Pending(put_request.dst
                                                                                      .name()
                                                                                      .clone())
                                                   })
                                                   .take(REPLICANTS)
                                                   .collect();
            PutEnvironment {
                client_manager: client_manager,
                im_data: im_data,
                message_id: message_id,
                incoming_request: client_request,
                outgoing_requests: outgoing_requests,
                initial_holders: initial_holders,
            }
        }

        pub fn get_im_data(&mut self, data_name: XorName) -> GetEnvironment {
            let message_id = MessageId::new();
            let content = RequestContent::Get(DataIdentifier::Immutable(data_name.clone(),
                                                                        ImmutableDataType::Normal),
                                              message_id);
            let keys = sign::gen_keypair();
            let from = random();
            let client = Authority::Client {
                client_key: keys.0,
                peer_id: random(),
                proxy_node_name: from,
            };
            let request = RequestMessage {
                src: client.clone(),
                dst: Authority::NaeManager(data_name.clone()),
                content: content.clone(),
            };
            let _ = self.immutable_data_manager.handle_get(&self.routing, &request);
            GetEnvironment {
                client: client,
                message_id: message_id,
                request: request,
            }
        }
    }

    #[test]
    fn handle_put() {
        let mut env = Environment::new();
        let put_env = env.put_im_data();
        for (index, req) in put_env.outgoing_requests.iter().enumerate() {
            assert_eq!(req.src, Authority::NaeManager(put_env.im_data.name()));
            if index < REPLICANTS {
                if let Authority::NodeManager(_) = req.dst {} else {
                    panic!()
                }
                assert_eq!(req.content,
                           RequestContent::Put(Data::Immutable(put_env.im_data.clone()),
                                               put_env.message_id.clone()));
            } else if index == REPLICANTS {
                let backup = ImmutableData::new(ImmutableDataType::Backup,
                                                put_env.im_data.value().clone());
                assert_eq!(req.dst, Authority::NaeManager(backup.name()));
                assert_eq!(req.content,
                           RequestContent::Put(Data::Immutable(backup), put_env.message_id));
            } else {
                let sacrificial = ImmutableData::new(ImmutableDataType::Sacrificial,
                                                     put_env.im_data.value().clone());
                assert_eq!(req.dst, Authority::NaeManager(sacrificial.name()));
                assert_eq!(req.content,
                           RequestContent::Put(Data::Immutable(sacrificial), put_env.message_id));
            }
        }
        let put_successes = env.routing.put_successes_given();
        assert_eq!(put_successes.len(), 1);
        assert_eq!(put_successes[0].content,
                   ResponseContent::PutSuccess(put_env.im_data.name(), put_env.message_id));
        assert_eq!(put_env.client_manager, put_successes[0].dst);
        assert_eq!(Authority::NaeManager(put_env.im_data.name()),
                   put_successes[0].src);
    }

    #[test]
    fn get_non_existing_data() {
        let mut env = Environment::new();
        let im_data = env.get_close_data();
        let get_env = env.get_im_data(im_data.name());
        assert!(env.routing.get_requests_given().is_empty());
        assert!(env.routing.get_successes_given().is_empty());
        let get_failure = env.routing.get_failures_given();
        assert_eq!(get_failure.len(), 1);
        if let ResponseContent::GetFailure { ref external_error_indicator, ref id, .. } =
               get_failure[0].content.clone() {
            assert_eq!(get_env.message_id, *id);
            let parsed_error = unwrap_result!(serialisation::deserialise(external_error_indicator));
            assert_eq!(GetError::NoSuchData, parsed_error);
        } else {
            panic!("Received unexpected response {:?}", get_failure[0]);
        }
        assert_eq!(get_env.client, get_failure[0].dst);
        assert_eq!(Authority::NaeManager(im_data.name()), get_failure[0].src);
    }

    #[test]
    fn get_immediately_after_put() {
        let mut env = Environment::new();
        let put_env = env.put_im_data();

        let get_env = env.get_im_data(put_env.im_data.name());
        assert!(env.routing.get_requests_given().is_empty());
        assert!(env.routing.get_failures_given().is_empty());
        let get_success = env.routing.get_successes_given();
        assert_eq!(get_success.len(), 1);
        if let ResponseMessage { content: ResponseContent::GetSuccess(response_data, id), .. } =
               get_success[0].clone() {
            assert_eq!(Data::Immutable(put_env.im_data.clone()), response_data);
            assert_eq!(get_env.message_id, id);
        } else {
            panic!("Received unexpected response {:?}", get_success[0]);
        }
    }

    #[test]
    fn get_after_put_success() {
        let mut env = Environment::new();
        let put_env = env.put_im_data();
        for data_holder in &put_env.initial_holders {
            let _ = env.immutable_data_manager
                       .handle_put_success(data_holder.name(), &put_env.im_data.name());
        }

        let get_env = env.get_im_data(put_env.im_data.name());
        assert!(env.routing.get_successes_given().is_empty());
        assert!(env.routing.get_failures_given().is_empty());
        let get_requests = env.routing.get_requests_given();
        assert_eq!(get_requests.len(), REPLICANTS);
        for get_request in &get_requests {
            if let RequestContent::Get(data_request, message_id) = get_request.content.clone() {
                assert_eq!(put_env.im_data.name(), data_request.name());
                assert_eq!(get_env.message_id, message_id);
            } else {
                panic!("Received unexpected request {:?}", get_request);
            }
            assert_eq!(Authority::NaeManager(put_env.im_data.name()),
                       get_request.src);
            assert!(put_env.initial_holders
                           .contains(&DataHolder::Pending(*get_request.dst.name())));
        }
    }

    #[test]
    fn handle_put_failure() {
        let mut env = Environment::new();
        let put_env = env.put_im_data();
        let mut current_put_request_count = put_env.outgoing_requests.len();
        let mut current_holders = put_env.initial_holders.clone();
        for data_holder in &put_env.initial_holders {
            let _ = env.immutable_data_manager
                       .handle_put_failure(&env.routing,
                                           data_holder.name(),
                                           &put_env.im_data,
                                           &put_env.message_id);
            let put_requests = env.routing.put_requests_given();
            let last_put_request = unwrap_option!(put_requests.last(), "");
            assert_eq!(put_requests.len(), current_put_request_count + 1);
            assert_eq!(last_put_request.src,
                       Authority::NaeManager(put_env.im_data.name()));
            assert_eq!(last_put_request.content,
                       RequestContent::Put(Data::Immutable(put_env.im_data.clone()),
                                           put_env.message_id.clone()));
            let new_holder = DataHolder::Pending(last_put_request.dst.name().clone());
            assert!(!current_holders.contains(&new_holder));
            current_put_request_count += 1;
            current_holders.insert(new_holder);
        }
    }

    #[test]
    fn handle_get_failure() {
        let mut env = Environment::new();
        let put_env = env.put_im_data();
        for data_holder in &put_env.initial_holders {
            let _ = env.immutable_data_manager
                       .handle_put_success(data_holder.name(), &put_env.im_data.name());
        }

        let get_env = env.get_im_data(put_env.im_data.name());
        let mut get_requests = env.routing.get_requests_given();
        assert_eq!(get_requests.len(), REPLICANTS);

        // The first holder responds with failure - no further Puts or Gets triggered
        {
            let get_request = unwrap_option!(get_requests.first(), "");
            unwrap_result!(env.immutable_data_manager.handle_get_failure(&env.routing,
                                                                         get_request.dst.name(),
                                                                         &get_env.message_id,
                                                                         &get_request,
                                                                         &[]));
            assert_eq!(env.routing.put_requests_given().len(), REPLICANTS + 2);
            assert_eq!(env.routing.get_requests_given().len(), REPLICANTS);
            assert!(env.routing.get_successes_given().is_empty());
            assert!(env.routing.get_failures_given().is_empty());
        }

        // The second holder responds with failure - should trigger Gets from Backup and Sacrificial
        // DMs
        {
            let get_request = unwrap_option!(get_requests.get(1), "");
            unwrap_result!(env.immutable_data_manager.handle_get_failure(&env.routing,
                                                                         get_request.dst.name(),
                                                                         &get_env.message_id,
                                                                         &get_request,
                                                                         &[]));
        }
        assert_eq!(env.routing.put_requests_given().len(), REPLICANTS + 2);
        assert!(env.routing.get_successes_given().is_empty());
        assert!(env.routing.get_failures_given().is_empty());
        get_requests = env.routing.get_requests_given();
        assert_eq!(get_requests.len(), REPLICANTS + 2);

        let backup_get_request = unwrap_option!(get_requests.get(REPLICANTS), "");
        let backup = ImmutableData::new(ImmutableDataType::Backup, put_env.im_data.value().clone());
        assert_eq!(backup_get_request.dst, Authority::NaeManager(backup.name()));
        let mut expected_message_id = MessageId::increment_first_byte(&get_env.message_id);
        assert_eq!(backup_get_request.content,
                   RequestContent::Get(DataIdentifier::Immutable(backup.name(),
                                                                 ImmutableDataType::Backup),
                                       expected_message_id));

        let sacrificial_get_request = unwrap_option!(get_requests.last(), "");
        let sacrificial = ImmutableData::new(ImmutableDataType::Sacrificial,
                                             put_env.im_data.value().clone());
        assert_eq!(sacrificial_get_request.dst,
                   Authority::NaeManager(sacrificial.name()));
        expected_message_id = MessageId::increment_first_byte(&expected_message_id);
        assert_eq!(sacrificial_get_request.content,
                   RequestContent::Get(DataIdentifier::Immutable(sacrificial.name(),
                                                                 ImmutableDataType::Sacrificial),
                                       expected_message_id));

        assert_eq!(env.routing.put_requests_given().len(), REPLICANTS + 2);
        assert_eq!(env.routing.get_requests_given().len(), REPLICANTS + 2);
        assert!(env.routing.get_successes_given().is_empty());
        assert!(env.routing.get_failures_given().is_empty());

        assert_eq!(env.routing.put_requests_given().len(), REPLICANTS + 2);
        assert_eq!(env.routing.get_requests_given().len(), REPLICANTS + 2);
        assert!(env.routing.get_successes_given().is_empty());

        let get_failures = env.routing.get_failures_given();
        assert_eq!(get_failures.len(), 1);
        let get_failure = unwrap_option!(get_failures.first(), "");
        if let ResponseContent::GetFailure { ref external_error_indicator, ref id, .. } =
               get_failure.content.clone() {
            assert_eq!(get_env.message_id, *id);
            let parsed_error = unwrap_result!(serialisation::deserialise(external_error_indicator));
            if let GetError::NoSuchData = parsed_error {} else {
                panic!("Received unexpected external_error_indicator with parsed error as {:?}",
                       parsed_error);
            }
        } else {
            panic!("Received unexpected response {:?}", get_failure);
        }
        assert_eq!(get_env.client, get_failure.dst);
        assert_eq!(Authority::NaeManager(put_env.im_data.name()),
                   get_failure.src);
    }

    #[test]
    fn handle_get_success() {
        let mut env = Environment::new();
        let put_env = env.put_im_data();
        for data_holder in &put_env.initial_holders {
            let _ = env.immutable_data_manager
                       .handle_put_success(data_holder.name(), &put_env.im_data.name());
        }

        let get_env = env.get_im_data(put_env.im_data.name());
        let get_requests = env.routing.get_requests_given();
        assert_eq!(get_requests.len(), REPLICANTS);
        let mut success_count = 0;
        for get_request in &get_requests {
            let response = ResponseMessage {
                src: get_request.dst.clone(),
                dst: get_request.src.clone(),
                content: ResponseContent::GetSuccess(Data::Immutable(put_env.im_data.clone()),
                                                     get_env.message_id),
            };
            let _ = env.immutable_data_manager.handle_get_success(&env.routing, &response);
            success_count += 1;
            assert_eq!(env.routing.put_requests_given().len(), REPLICANTS + 2);
            assert_eq!(env.routing.get_requests_given().len(), REPLICANTS);
            assert!(env.routing.get_failures_given().is_empty());
            if success_count == 1 {
                let get_success = env.routing.get_successes_given();
                assert_eq!(get_success.len(), 1);
                if let ResponseMessage {
                    content: ResponseContent::GetSuccess(response_data, id),
                    ..
                } = get_success[0].clone() {
                    assert_eq!(Data::Immutable(put_env.im_data.clone()), response_data);
                    assert_eq!(get_env.message_id, id);
                } else {
                    panic!("Received unexpected response {:?}", get_success[0]);
                }
            } else {
                assert_eq!(env.routing.get_successes_given().len(), 1);
            }
        }
    }

    #[test]
    fn handle_refresh() {
        let mut env = Environment::new();
        let data = env.get_close_data();
        let mut data_holders: HashSet<DataHolder> = HashSet::new();
        for _ in 0..REPLICANTS {
            data_holders.insert(DataHolder::Good(env.get_close_node()));
        }
        let _ = env.immutable_data_manager.handle_refresh(data.name(),
                                                          Account::new(&ImmutableDataType::Normal,
                                                                       data_holders.clone()));
        let _get_env = env.get_im_data(data.name());
        let get_requests = env.routing.get_requests_given();
        assert_eq!(get_requests.len(), REPLICANTS);
        let pmid_nodes: Vec<XorName> = get_requests.into_iter()
                                                   .map(|request| *request.dst.name())
                                                   .collect();
        for data_holder in &data_holders {
            assert!(pmid_nodes.contains(data_holder.name()));
        }
    }

    #[test]
    fn churn_during_put() {
        let _ = ::maidsafe_utilities::log::init(false);
        let mut env = Environment::new();
        let put_env = env.put_im_data();
        let mut account = Account::new(&ImmutableDataType::Normal, put_env.initial_holders.clone());
        let mut churn_count = 0;
        let mut replicants = REPLICANTS;
        let mut put_request_len = REPLICANTS + 2;
        let mut replication_put_message_id: MessageId;
        for data_holder in &put_env.initial_holders {
            churn_count += 1;
            if churn_count % 2 == 0 {
                let lost_node = env.lose_close_node(&put_env.im_data.name());
                let _ = env.immutable_data_manager
                           .handle_put_success(data_holder.name(), &put_env.im_data.name());
                env.routing.remove_node_from_routing_table(&lost_node);
                let _ = env.immutable_data_manager.handle_node_lost(&env.routing, &lost_node);
                let temp_account = mem::replace(&mut account,
                                                Account::new(&ImmutableDataType::Normal,
                                                             HashSet::new()));
                *account.data_holders_mut() =
                    temp_account.data_holders()
                                .into_iter()
                                .filter_map(|holder| {
                                    if *holder.name() == lost_node {
                                        if let DataHolder::Failed(_) = *holder {} else {
                                            replicants -= 1;
                                        }
                                        None
                                    } else if holder == data_holder {
                                        Some(DataHolder::Good(*holder.name()))
                                    } else {
                                        Some(*holder)
                                    }
                                })
                                .collect();
                replication_put_message_id = MessageId::from_lost_node(lost_node);
            } else {
                let new_node = env.get_close_node();
                let data = put_env.im_data.clone();
                let _ = env.immutable_data_manager.handle_put_failure(&env.routing,
                                                                      data_holder.name(),
                                                                      &data,
                                                                      &put_env.message_id);
                env.routing.add_node_into_routing_table(&new_node);
                let _ = env.immutable_data_manager.handle_node_added(&env.routing, &new_node);

                if let Ok(None) = env.routing.close_group(put_env.im_data.name()) {
                    // No longer being the DM of the data, expecting no refresh request
                    assert_eq!(env.routing.refresh_requests_given().len(), churn_count - 1);
                    return;
                }

                let temp_account = mem::replace(&mut account,
                                                Account::new(&ImmutableDataType::Normal,
                                                             HashSet::new()));
                *account.data_holders_mut() =
                    temp_account.data_holders()
                                .into_iter()
                                .filter_map(|holder| {
                                    if holder == data_holder {
                                        replicants -= 1;
                                        Some(DataHolder::Failed(*holder.name()))
                                    } else {
                                        Some(*holder)
                                    }
                                })
                                .collect();
                replication_put_message_id = put_env.message_id.clone();
            }
            if replicants < REPLICANTS {
                put_request_len += REPLICANTS - replicants;
                replicants += 1;
                let requests = env.routing.put_requests_given();
                assert_eq!(requests.len(), put_request_len);
                let put_request = unwrap_option!(requests.last(), "");
                assert_eq!(put_request.src,
                           Authority::NaeManager(put_env.im_data.name()));
                assert_eq!(put_request.content,
                           RequestContent::Put(Data::Immutable(put_env.im_data.clone()),
                                               replication_put_message_id));
                account.data_holders_mut().insert(DataHolder::Pending(*put_request.dst.name()));
            }

            let refreshs = env.routing.refresh_requests_given();
            assert_eq!(refreshs.len(), churn_count);
            let received_refresh = unwrap_option!(refreshs.last(), "");
            if let RequestContent::Refresh(received_serialised_refresh, _) =
                   received_refresh.content.clone() {
                let parsed_refresh = unwrap_result!(serialisation::deserialise::<Refresh>(
                        &received_serialised_refresh[..]));
                assert_eq!(parsed_refresh.value,
                           RefreshValue::ImmutableDataManagerAccount(account.clone()));
            } else {
                panic!("Received unexpected refresh {:?}", received_refresh);
            }
        }
    }

    #[test]
    fn churn_after_put() {
        let mut env = Environment::new();
        let put_env = env.put_im_data();
        let mut good_holders = HashSet::new();
        for data_holder in &put_env.initial_holders {
            unwrap_result!(env.immutable_data_manager
                              .handle_put_success(data_holder.name(), &put_env.im_data.name()));
            good_holders.insert(DataHolder::Good(*data_holder.name()));
        }

        let mut account = Account::new(&ImmutableDataType::Normal, good_holders.clone());
        let mut churn_count = 0;
        let mut get_message_id: MessageId;
        let mut get_requests_len = 0;
        let mut replicants = REPLICANTS;
        for _data_holder in &good_holders {
            churn_count += 1;
            if churn_count % 2 == 0 {
                let lost_node = env.lose_close_node(&put_env.im_data.name());
                env.routing.remove_node_from_routing_table(&lost_node);
                let _ = env.immutable_data_manager.handle_node_lost(&env.routing, &lost_node);
                get_message_id = MessageId::from_lost_node(lost_node);

                let temp_account = mem::replace(&mut account,
                                                Account::new(&ImmutableDataType::Normal,
                                                             HashSet::new()));
                *account.data_holders_mut() = temp_account.data_holders()
                                                          .into_iter()
                                                          .filter_map(|holder| {
                                                              if *holder.name() == lost_node {
                                                                  replicants -= 1;
                                                                  None
                                                              } else {
                                                                  Some(*holder)
                                                              }
                                                          })
                                                          .collect();
            } else {
                let new_node = env.get_close_node();
                env.routing.add_node_into_routing_table(&new_node);
                let _ = env.immutable_data_manager.handle_node_added(&env.routing, &new_node);
                get_message_id = MessageId::from_added_node(new_node);

                if let Ok(None) = env.routing.close_group(put_env.im_data.name()) {
                    // No longer being the DM of the data, expecting no refresh request
                    assert_eq!(env.routing.refresh_requests_given().len(), churn_count - 1);
                    return;
                }
            }

            if replicants < REPLICANTS && get_requests_len == 0 {
                get_requests_len = account.data_holders().len();
                let get_requests = env.routing.get_requests_given();
                assert_eq!(get_requests.len(), get_requests_len);
                for get_request in &get_requests {
                    assert_eq!(get_request.src,
                               Authority::NaeManager(put_env.im_data.name()));
                    assert_eq!(get_request.content,
                               RequestContent::Get(DataIdentifier::Immutable(put_env.im_data.name(),
                                                                     ImmutableDataType::Normal),
                                                   get_message_id));
                }
            } else {
                assert_eq!(env.routing.get_requests_given().len(), get_requests_len);
            }

            let refreshs = env.routing.refresh_requests_given();
            assert_eq!(refreshs.len(), churn_count);
            let received_refresh = unwrap_option!(refreshs.last(), "");
            if let RequestContent::Refresh(received_serialised_refresh, _) =
                   received_refresh.content.clone() {
                let parsed_refresh = unwrap_result!(serialisation::deserialise::<Refresh>(
                        &received_serialised_refresh[..]));
                assert_eq!(parsed_refresh.value,
                           RefreshValue::ImmutableDataManagerAccount(account.clone()));
            } else {
                panic!("Received unexpected refresh {:?}", received_refresh);
            }
        }
    }

    #[test]
    fn churn_during_get() {
        let mut env = Environment::new();
        let put_env = env.put_im_data();
        let mut good_holders = HashSet::new();
        for data_holder in &put_env.initial_holders {
            unwrap_result!(env.immutable_data_manager
                              .handle_put_success(data_holder.name(), &put_env.im_data.name()));
            good_holders.insert(DataHolder::Good(*data_holder.name()));
        }

        let get_env = env.get_im_data(put_env.im_data.name());
        let get_requests = env.routing.get_requests_given();

        let mut account = Account::new(&ImmutableDataType::Normal, good_holders.clone());
        let mut churn_count = 0;
        let mut get_response_len = 0;
        for get_request in &get_requests {
            churn_count += 1;
            if churn_count % 2 == 0 {
                let lost_node = env.lose_close_node(&put_env.im_data.name());
                let get_response = ResponseMessage {
                    src: get_request.dst.clone(),
                    dst: get_request.src.clone(),
                    content: ResponseContent::GetSuccess(Data::Immutable(put_env.im_data.clone()),
                                                         get_env.message_id.clone()),
                };
                let _ = env.immutable_data_manager.handle_get_success(&env.routing, &get_response);
                env.routing.remove_node_from_routing_table(&lost_node);
                let _ = env.immutable_data_manager.handle_node_lost(&env.routing, &lost_node);
                let temp_account = mem::replace(&mut account,
                                                Account::new(&ImmutableDataType::Normal,
                                                             HashSet::new()));
                *account.data_holders_mut() = temp_account.data_holders()
                                                          .into_iter()
                                                          .filter_map(|holder| {
                                                              if *holder.name() == lost_node {
                                                                  None
                                                              } else {
                                                                  Some(*holder)
                                                              }
                                                          })
                                                          .collect();
                get_response_len = 1;
            } else {
                let new_node = env.get_close_node();
                let _ = env.immutable_data_manager.handle_get_failure(&env.routing,
                                                                      get_request.dst.name(),
                                                                      &get_env.message_id,
                                                                      &get_request,
                                                                      &[]);
                env.routing.add_node_into_routing_table(&new_node);
                let _ = env.immutable_data_manager.handle_node_added(&env.routing, &new_node);

                if let Ok(None) = env.routing.close_group(put_env.im_data.name()) {
                    // No longer being the DM of the data, expecting no refresh request
                    assert_eq!(env.routing.refresh_requests_given().len(), churn_count - 1);
                    return;
                }

                let temp_account = mem::replace(&mut account,
                                                Account::new(&ImmutableDataType::Normal,
                                                             HashSet::new()));
                *account.data_holders_mut() =
                    temp_account.data_holders()
                                .into_iter()
                                .filter_map(|holder| {
                                    if holder.name() == get_request.dst.name() {
                                        Some(DataHolder::Failed(*holder.name()))
                                    } else {
                                        Some(*holder)
                                    }
                                })
                                .collect();
            }
            if get_response_len == 1 {
                let get_success = env.routing.get_successes_given();
                assert_eq!(get_success.len(), 1);
                if let ResponseMessage { content: ResponseContent::GetSuccess(response_data,
                                                                              id), .. } =
                       get_success[0].clone() {
                    assert_eq!(Data::Immutable(put_env.im_data.clone()), response_data);
                    assert_eq!(get_env.message_id, id);
                } else {
                    panic!("Received unexpected response {:?}", get_success[0]);
                }
            }
            assert_eq!(env.routing.get_successes_given().len(), get_response_len);

            let refreshs = env.routing.refresh_requests_given();
            assert_eq!(refreshs.len(), churn_count);
            let received_refresh = unwrap_option!(refreshs.last(), "");
            if let RequestContent::Refresh(received_serialised_refresh, _) =
                   received_refresh.content.clone() {
                let parsed_refresh = unwrap_result!(serialisation::deserialise::<Refresh>(
                        &received_serialised_refresh[..]));
                if let RefreshValue::ImmutableDataManagerAccount(received_account) =
                       parsed_refresh.value.clone() {
                    if churn_count == REPLICANTS ||
                       env.immutable_data_manager.ongoing_gets.len() == 0 {
                        // A replication after ongoing_get get cleared picks up a new data_holder.
                        assert_eq!(env.routing.put_requests_given().len(), (2 * REPLICANTS) + 1);
                        assert!(received_account.data_holders().len() >= REPLICANTS);
                        return;
                    } else {
                        assert_eq!(received_account, account);
                    }
                } else {
                    panic!("Received unexpected refresh value {:?}", parsed_refresh);
                }
            } else {
                panic!("Received unexpected refresh {:?}", received_refresh);
            }
        }
    }
}

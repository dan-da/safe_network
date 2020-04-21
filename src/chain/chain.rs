// Copyright 2018 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{
    shared_state::SharedState, stats::Stats, AccumulatedEvent, AccumulatingEvent, EldersChange,
    GenesisPfxInfo, NetworkEvent, NetworkParams, Proof, ProofSet,
};
use crate::{
    consensus::{AccumulatingProof, ConsensusEngine, DkgResult, DkgResultWrapper, InsertError},
    error::{Result, RoutingError},
    id::{FullId, P2pNode, PublicId},
    location::{DstLocation, SrcLocation},
    messages::{AccumulatingMessage, PlainMessage, Variant},
    relocation::RelocateDetails,
    rng::MainRng,
    section::{EldersInfo, MemberState, SectionKeyInfo, SectionProofBlock, SectionProofSlice},
    xor_space::Xorable,
    Prefix, XorName,
};
use bincode::serialize;
use itertools::Itertools;
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    mem,
    net::SocketAddr,
};

/// Returns the delivery group size based on the section size `n`
pub const fn delivery_group_size(n: usize) -> usize {
    // this is an integer that is ≥ n/3
    (n + 2) / 3
}

/// Data chain.
pub struct Chain {
    /// The consensus engine.
    pub consensus_engine: ConsensusEngine,
    /// Network parameters
    network_params: NetworkParams,
    /// This node's public ID.
    our_id: PublicId,
    /// Our current Section BLS keys.
    our_section_bls_keys: SectionKeys,
    /// The shared state of the section.
    state: SharedState,
    /// Marker indicating we are processing churn event
    churn_in_progress: bool,
    /// Marker indicating that elders may need to change,
    members_changed: bool,
    /// The new dkg key to use when SectionInfo completes. For lookup, use the XorName of the
    /// first member in DKG participants and new ElderInfo. We only store 2 items during split, and
    /// then members are disjoint. We are working around not having access to the prefix for the
    /// DkgResult but only the list of participants.
    new_section_bls_keys: BTreeMap<XorName, DkgResult>,
    // The accumulated info during a split pfx change.
    split_cache: Option<SplitCache>,
}

#[allow(clippy::len_without_is_empty)]
impl Chain {
    /// Returns the number of elders per section
    pub fn elder_size(&self) -> usize {
        self.network_params.elder_size
    }

    /// Returns the safe section size.
    pub fn safe_section_size(&self) -> usize {
        self.network_params.safe_section_size
    }

    /// Returns the full `NetworkParams` structure (if present)
    pub fn network_params(&self) -> NetworkParams {
        self.network_params
    }

    /// Returns the shared section state.
    pub fn state(&self) -> &SharedState {
        &self.state
    }

    pub fn our_section_bls_keys(&self) -> &bls::PublicKeySet {
        &self.our_section_bls_keys.public_key_set
    }

    pub fn our_section_bls_secret_key_share(&self) -> Result<&SectionKeyShare, RoutingError> {
        self.our_section_bls_keys
            .secret_key_share
            .as_ref()
            .ok_or(RoutingError::InvalidElderDkgResult)
    }

    /// Create a new chain given genesis information
    pub fn new(
        rng: &mut MainRng,
        network_params: NetworkParams,
        our_full_id: FullId,
        gen_info: GenesisPfxInfo,
        secret_key_share: Option<bls::SecretKeyShare>,
    ) -> Self {
        // TODO validate `gen_info` to contain adequate proofs
        let our_id = *our_full_id.public_id();
        let secret_key_share = secret_key_share
            .and_then(|key| SectionKeyShare::new(key, &our_id, &gen_info.elders_info));
        let consensus_engine = ConsensusEngine::new(rng, our_full_id, &gen_info);

        Self {
            network_params,
            our_id,
            our_section_bls_keys: SectionKeys {
                public_key_set: gen_info.public_keys.clone(),
                secret_key_share,
            },
            state: SharedState::new(gen_info.elders_info, gen_info.public_keys, gen_info.ages),
            consensus_engine,
            churn_in_progress: false,
            members_changed: false,
            new_section_bls_keys: Default::default(),
            split_cache: None,
        }
    }

    /// Handles an accumulated parsec Observation for genesis.
    ///
    /// The related_info is the serialized shared state that will be the starting
    /// point when processing parsec data.
    pub fn handle_genesis_event(
        &mut self,
        _group: &BTreeSet<PublicId>,
        related_info: &[u8],
    ) -> Result<(), RoutingError> {
        // `related_info` is empty only if this is the `first` node.
        let new_state = if !related_info.is_empty() {
            Some(bincode::deserialize(related_info)?)
        } else {
            None
        };

        // On split membership may need to be checked again.
        self.members_changed = true;
        self.state.update(new_state);

        Ok(())
    }

    /// Handles a completed parsec DKG Observation.
    pub fn handle_dkg_result_event(
        &mut self,
        participants: &BTreeSet<PublicId>,
        dkg_result: &DkgResultWrapper,
    ) -> Result<(), RoutingError> {
        if let Some(first) = participants.iter().next() {
            if self
                .new_section_bls_keys
                .insert(*first.name(), dkg_result.0.clone())
                .is_some()
            {
                log_or_panic!(log::Level::Error, "Ejected previous DKG result");
            }
        }

        Ok(())
    }

    /// Get the serialized shared state that will be the starting point when processing
    /// parsec data
    pub fn get_genesis_related_info(&self) -> Result<Vec<u8>, RoutingError> {
        Ok(serialize(&self.state)?)
    }

    /// Handles an opaque parsec Observation as a NetworkEvent.
    pub fn handle_opaque_event(
        &mut self,
        event: &NetworkEvent,
        proof: Proof,
    ) -> Result<(), RoutingError> {
        let (acc_event, signature) = AccumulatingEvent::from_network_event(event.clone());
        match self.consensus_engine.add_proof(acc_event, proof, signature) {
            Ok(()) | Err(InsertError::AlreadyComplete) => {
                // Proof added or event already completed.
            }
            Err(InsertError::ReplacedAlreadyInserted) => {
                // TODO: If detecting duplicate vote from peer, penalise.
                log_or_panic!(
                    log::Level::Warn,
                    "Duplicate proof for {:?} in accumulator. [{:?}]",
                    event,
                    self.consensus_engine.incomplete_events().format(", ")
                );
            }
        }
        Ok(())
    }

    /// Returns the next accumulated event.
    ///
    /// If the event is a `SectionInfo` or `NeighbourInfo`, it also updates the corresponding
    /// containers.
    pub fn poll_accumulated(&mut self) -> Result<Option<PollAccumulated>, RoutingError> {
        if let Some(event) = self.poll_churn_event_backlog() {
            return Ok(Some(PollAccumulated::AccumulatedEvent(event)));
        }

        // Note: it's important that `promote_and_demote_elders` happens before `poll_relocation`,
        // otherwise we might relocate a node that we still need.
        if let Some(new_infos) = self.promote_and_demote_elders()? {
            return Ok(Some(PollAccumulated::PromoteDemoteElders(new_infos)));
        }

        if let Some(details) = self.poll_relocation() {
            return Ok(Some(PollAccumulated::RelocateDetails(details)));
        }

        let (event, proofs) = match self.poll_accumulator() {
            None => return Ok(None),
            Some((event, proofs)) => (event, proofs),
        };

        let event = match self.process_accumulating(event, proofs)? {
            None => return Ok(None),
            Some(event) => event,
        };

        if let Some(event) = self.check_ready_or_backlog_churn_event(event)? {
            return Ok(Some(PollAccumulated::AccumulatedEvent(event)));
        }

        Ok(None)
    }

    fn poll_accumulator(&mut self) -> Option<(AccumulatingEvent, AccumulatingProof)> {
        let opt_event = self
            .consensus_engine
            .incomplete_events()
            .find(|(event, proofs)| self.is_valid_transition(event, proofs.parsec_proof_set()))
            .map(|(event, _)| event.clone());

        opt_event.and_then(|event| {
            self.consensus_engine
                .poll_event(event, self.state.our_info().member_ids().cloned().collect())
        })
    }

    fn process_accumulating(
        &mut self,
        event: AccumulatingEvent,
        proofs: AccumulatingProof,
    ) -> Result<Option<AccumulatedEvent>, RoutingError> {
        match event {
            AccumulatingEvent::SectionInfo(ref info, ref key_info) => {
                let change = EldersChangeBuilder::new(self);
                if self.add_elders_info(info.clone(), key_info.clone(), proofs)? {
                    let change = change.build(self);
                    return Ok(Some(
                        AccumulatedEvent::new(event).with_elders_change(change),
                    ));
                } else {
                    return Ok(None);
                }
            }
            AccumulatingEvent::NeighbourInfo(ref info) => {
                let change = EldersChangeBuilder::new(self);
                self.state.sections.add_neighbour(info.clone());
                let change = change.build(self);

                return Ok(Some(
                    AccumulatedEvent::new(event).with_elders_change(change),
                ));
            }
            AccumulatingEvent::TheirKeyInfo(ref key_info) => {
                self.state.sections.update_keys(key_info);
            }
            AccumulatingEvent::AckMessage(ref ack_payload) => {
                self.state
                    .sections
                    .update_knowledge(ack_payload.src_prefix, ack_payload.ack_version);
            }
            AccumulatingEvent::ParsecPrune => {
                if self.churn_in_progress {
                    return Ok(None);
                }
            }
            AccumulatingEvent::Online(_)
            | AccumulatingEvent::Offline(_)
            | AccumulatingEvent::StartDkg(_)
            | AccumulatingEvent::User(_)
            | AccumulatingEvent::Relocate(_)
            | AccumulatingEvent::RelocatePrepare(_, _)
            | AccumulatingEvent::SendAckMessage(_) => (),
        }

        Ok(Some(AccumulatedEvent::new(event)))
    }

    pub fn poll_churn_event_backlog(&mut self) -> Option<AccumulatedEvent> {
        if self.can_poll_churn() {
            if let Some(event) = self.state.churn_event_backlog.pop_back() {
                trace!(
                    "churn backlog poll {:?}, Others: {:?}",
                    event,
                    self.state.churn_event_backlog
                );
                return Some(event);
            }
        }

        None
    }

    pub fn check_ready_or_backlog_churn_event(
        &mut self,
        event: AccumulatedEvent,
    ) -> Result<Option<AccumulatedEvent>, RoutingError> {
        let start_churn_event = match &event.content {
            AccumulatingEvent::Online(_)
            | AccumulatingEvent::Offline(_)
            | AccumulatingEvent::Relocate(_) => true,
            _ => false,
        };

        if start_churn_event && !self.can_poll_churn() {
            trace!(
                "churn backlog {:?}, Other: {:?}",
                event,
                self.state.churn_event_backlog
            );
            self.state.churn_event_backlog.push_front(event);
            return Ok(None);
        }

        Ok(Some(event))
    }

    /// Returns the details of the next scheduled relocation to be voted for, if any.
    fn poll_relocation(&mut self) -> Option<RelocateDetails> {
        // Delay relocation until all backlogged churn events have been handled and no
        // additional churn is in progress. Only allow one relocation at a time.
        if !self.can_poll_churn() || !self.state.churn_event_backlog.is_empty() {
            return None;
        }

        let details = loop {
            if let Some(details) = self.state.relocate_queue.pop_back() {
                if self.state.our_members.contains(&details.pub_id) {
                    break details;
                } else {
                    trace!("Not relocating {} - not a member", details.pub_id);
                }
            } else {
                return None;
            }
        };

        if self.state.is_peer_our_elder(&details.pub_id) {
            warn!(
                "Not relocating {} - The peer is still our elder.",
                details.pub_id,
            );

            // Keep the details in the queue so when the node is demoted we can relocate it.
            self.state.relocate_queue.push_back(details);
            return None;
        }

        trace!("relocating member {}", details.pub_id);

        Some(details)
    }

    fn can_poll_churn(&self) -> bool {
        self.state.handled_genesis_event && !self.churn_in_progress
    }

    /// Adds a member to our section.
    ///
    /// # Panics
    ///
    /// Panics if churn is in progress
    pub fn add_member(&mut self, p2p_node: P2pNode, age: u8) -> bool {
        assert!(!self.churn_in_progress);

        let added = self
            .state
            .add_member(p2p_node, age, self.safe_section_size());

        if added {
            self.members_changed = true;
        }

        added
    }

    /// Remove a member from our section. Returns the SocketAddr and the state of the member before
    /// the removal.
    ///
    /// # Panics
    ///
    /// Panics if churn is in progress
    pub fn remove_member(&mut self, pub_id: &PublicId) -> (Option<SocketAddr>, MemberState) {
        assert!(!self.churn_in_progress);

        let (addr, state) = self.state.remove_member(pub_id, self.safe_section_size());

        if addr.is_some() {
            self.members_changed = true;
        }

        (addr, state)
    }

    /// Generate a new section info based on the current set of members.
    /// Returns a set of EldersInfos to vote for.
    fn promote_and_demote_elders(&mut self) -> Result<Option<Vec<EldersInfo>>, RoutingError> {
        if !self.members_changed || !self.can_poll_churn() {
            // Nothing changed that could impact elder set, or we cannot process it yet.
            return Ok(None);
        }

        if self.should_split() {
            let (our_info, other_info) = self.split_self()?;
            self.members_changed = false;
            self.churn_in_progress = true;
            return Ok(Some(vec![our_info, other_info]));
        }

        let expected_elders_map = self.our_expected_elders();
        let expected_elders: BTreeSet<_> = expected_elders_map.values().cloned().collect();
        let current_elders: BTreeSet<_> = self.state.our_info().member_nodes().cloned().collect();

        if expected_elders == current_elders {
            self.members_changed = false;
            Ok(None)
        } else {
            let old_size = self.state.our_info().len();

            let new_info = EldersInfo::new(
                expected_elders_map,
                *self.state.our_info().prefix(),
                Some(self.state.our_info()),
            )?;

            if self.state.our_info().len() < self.elder_size() && old_size >= self.elder_size() {
                panic!(
                    "Merging situation encountered! Not supported: {:?}: {:?}",
                    self.our_id(),
                    self.state.our_info()
                );
            }

            self.members_changed = false;
            self.churn_in_progress = true;
            Ok(Some(vec![new_info]))
        }
    }

    /// Gets the data needed to initialise a new Parsec instance
    pub fn prepare_parsec_reset(
        &mut self,
        parsec_version: u64,
    ) -> Result<ParsecResetData, RoutingError> {
        let remaining = self.consensus_engine.reset_accumulator(&self.our_id);

        self.state.handled_genesis_event = false;

        Ok(ParsecResetData {
            gen_pfx_info: GenesisPfxInfo {
                elders_info: self.state.our_info().clone(),
                public_keys: self.our_section_bls_keys().clone(),
                state_serialized: self.get_genesis_related_info()?,
                ages: self.state.our_members.get_age_counters(),
                parsec_version,
            },
            cached_events: remaining.cached_events,
            completed_events: remaining.completed_events,
        })
    }

    /// Finalises a split or merge - creates a `GenesisPfxInfo` for the new graph and returns the
    /// cached and currently accumulated events.
    pub fn finalise_prefix_change(
        &mut self,
        parsec_version: u64,
    ) -> Result<ParsecResetData, RoutingError> {
        // TODO: Bring back using their_knowledge to clean_older section in our_infos
        self.state.sections.prune_neighbours();

        info!("finalise_prefix_change: {:?}", self.state.our_prefix());
        trace!("finalise_prefix_change state: {:?}", self.state);

        self.prepare_parsec_reset(parsec_version)
    }

    /// Returns our public ID
    pub fn our_id(&self) -> &PublicId {
        &self.our_id
    }

    pub fn get_p2p_node(&self, name: &XorName) -> Option<&P2pNode> {
        self.state
            .our_members
            .get_p2p_node(name)
            .or_else(|| self.state.get_our_elder_p2p_node(name))
            .or_else(|| self.state.sections.get_elder(name))
            .or_else(|| self.state.our_members.get_post_split_sibling_p2p_node(name))
    }

    /// Returns whether we are elder in our section.
    pub fn is_self_elder(&self) -> bool {
        self.state.is_peer_our_elder(&self.our_id)
    }

    fn our_expected_elders(&self) -> BTreeMap<XorName, P2pNode> {
        let mut elders = self.state.our_members.elder_candidates(self.elder_size());

        // Ensure that we can still handle one node lost when relocating.
        // Ensure that the node we eject are the one we want to relocate first.
        let missing = self.elder_size().saturating_sub(elders.len());
        elders.extend(self.state.elder_candidates_from_relocating(missing));
        elders
    }

    /// Returns an iterator over the members that have not state == `Left`.
    pub fn our_active_members(&self) -> impl Iterator<Item = &P2pNode> {
        self.state.our_members.active().map(|info| &info.p2p_node)
    }

    // Signs and proves the given message and wraps it in `AccumulatingMessage`.
    pub fn to_accumulating_message(
        &self,
        dst: DstLocation,
        variant: Variant,
        node_knowledge_override: Option<u64>,
    ) -> Result<AccumulatingMessage> {
        let proof = self.prove(&dst, node_knowledge_override);
        let pk_set = self.our_section_bls_keys().clone();
        let secret_key = self.our_section_bls_secret_key_share()?;

        let content = PlainMessage {
            src: *self.state.our_prefix(),
            dst,
            variant,
        };

        AccumulatingMessage::new(content, secret_key, pk_set, proof)
    }

    /// Provide a SectionProofSlice that proves the given signature to the given destination
    /// location.
    /// If `node_knowledge_override` is `Some`, it is used when calculating proof for
    /// `DstLocation::Node` instead of the stored knowledge. Has no effect for other location types.
    pub fn prove(
        &self,
        target: &DstLocation,
        node_knowledge_override: Option<u64>,
    ) -> SectionProofSlice {
        let first_index = self.knowledge_index(target, node_knowledge_override);
        self.state.our_history.slice_from(first_index as usize)
    }

    /// Provide a start index of a SectionProofSlice that proves the given signature to the given
    /// destination location.
    /// If `node_knowledge_override` is `Some`, it is used when calculating proof for
    /// `DstLocation::Node` instead of the stored knowledge. Has no effect for other location types.
    pub fn knowledge_index(
        &self,
        target: &DstLocation,
        node_knowledge_override: Option<u64>,
    ) -> u64 {
        match (target, node_knowledge_override) {
            (DstLocation::Node(_), Some(knowledge)) => knowledge,
            _ => self.state.sections.proving_index(target),
        }
    }

    /// Check which nodes are unresponsive.
    pub fn check_vote_status(&mut self) -> BTreeSet<PublicId> {
        let members = self.state.our_info().member_ids();
        self.consensus_engine.check_vote_status(members)
    }

    /// If given `NetworkEvent` is a `EldersInfo`, returns `true` if we have the previous
    /// `EldersInfo` in our_infos/neighbour_infos OR if its a valid neighbour pfx
    /// we do not currently have in our chain.
    /// Returns `true` for other types of `NetworkEvent`.
    fn is_valid_transition(&self, network_event: &AccumulatingEvent, proofs: &ProofSet) -> bool {
        match *network_event {
            AccumulatingEvent::SectionInfo(ref info, _) => {
                if !self.state.our_info().is_quorum(proofs) {
                    return false;
                }

                if !info.is_successor_of(self.state.our_info()) {
                    log_or_panic!(
                        log::Level::Error,
                        "We shouldn't have a SectionInfo that is not a direct descendant. our: \
                         {:?}, new: {:?}",
                        self.state.our_info(),
                        info
                    );
                }

                true
            }
            AccumulatingEvent::NeighbourInfo(ref info) => {
                if !self.state.our_info().is_quorum(proofs) {
                    return false;
                }

                // Do not process yet any version that is not the immediate follower of the one we have.
                let not_follow = |i: &EldersInfo| {
                    info.prefix().is_compatible(i.prefix()) && info.version() != (i.version() + 1)
                };
                if self
                    .state
                    .sections
                    .compatible(info.prefix())
                    .into_iter()
                    .any(not_follow)
                {
                    return false;
                }

                true
            }

            AccumulatingEvent::Online(_)
            | AccumulatingEvent::Offline(_)
            | AccumulatingEvent::TheirKeyInfo(_)
            | AccumulatingEvent::AckMessage(_)
            | AccumulatingEvent::ParsecPrune
            | AccumulatingEvent::Relocate(_)
            | AccumulatingEvent::RelocatePrepare(_, _)
            | AccumulatingEvent::User(_) => self.state.our_info().is_quorum(proofs),

            AccumulatingEvent::SendAckMessage(_) => {
                // We may not reach consensus if malicious peer, but when we do we know all our
                // nodes have updated `their_keys`.
                self.state.our_info().is_total_consensus(proofs)
            }

            AccumulatingEvent::StartDkg(_) => {
                unreachable!("StartDkg present in the chain accumulator")
            }
        }
    }

    /// Handles our own section info, or the section info of our sibling directly after a split.
    /// Returns whether the event should be handled by the caller.
    pub fn add_elders_info(
        &mut self,
        elders_info: EldersInfo,
        key_info: SectionKeyInfo,
        proofs: AccumulatingProof,
    ) -> Result<bool, RoutingError> {
        // Split handling alone. wouldn't cater to merge
        if elders_info
            .prefix()
            .is_extension_of(self.state.our_prefix())
        {
            match self.split_cache.take() {
                None => {
                    self.split_cache = Some(SplitCache {
                        elders_info,
                        key_info,
                        proofs,
                    });
                    Ok(false)
                }
                Some(cache) => {
                    let cache_pfx = *cache.elders_info.prefix();

                    // Add our_info first so when we add sibling info, its a valid neighbour prefix
                    // which does not get immediately purged.
                    if cache_pfx.matches(self.our_id.name()) {
                        self.do_add_elders_info(cache.elders_info, cache.key_info, cache.proofs)?;
                        self.state.sections.add_neighbour(elders_info);
                    } else {
                        self.do_add_elders_info(elders_info, key_info, proofs)?;
                        self.state.sections.add_neighbour(cache.elders_info);
                    }
                    Ok(true)
                }
            }
        } else {
            self.do_add_elders_info(elders_info, key_info, proofs)?;
            Ok(true)
        }
    }

    fn do_add_elders_info(
        &mut self,
        elders_info: EldersInfo,
        key_info: SectionKeyInfo,
        proofs: AccumulatingProof,
    ) -> Result<(), RoutingError> {
        let proof_block = self.combine_signatures_for_section_proof_block(key_info, proofs)?;
        let our_new_key =
            key_matching_first_elder_name(&elders_info, mem::take(&mut self.new_section_bls_keys))?;

        self.state.push_our_new_info(elders_info, proof_block);
        self.our_section_bls_keys =
            SectionKeys::new(our_new_key, self.our_id(), self.state.our_info());
        self.churn_in_progress = false;
        self.state.sections.prune_neighbours();
        self.state.remove_our_members_not_matching_our_prefix();
        Ok(())
    }

    pub fn combine_signatures_for_section_proof_block(
        &self,
        key_info: SectionKeyInfo,
        proofs: AccumulatingProof,
    ) -> Result<SectionProofBlock, RoutingError> {
        let signature = self
            .check_and_combine_signatures(&key_info, proofs)
            .ok_or(RoutingError::InvalidNewSectionInfo)?;
        Ok(SectionProofBlock::new(key_info, signature))
    }

    pub fn check_and_combine_signatures<S: Serialize + Debug>(
        &self,
        signed_payload: &S,
        proofs: AccumulatingProof,
    ) -> Option<bls::Signature> {
        let signed_bytes = serialize(signed_payload)
            .map_err(|err| {
                log_or_panic!(
                    log::Level::Error,
                    "Failed to serialise accumulated event: {:?} for {:?}",
                    err,
                    signed_payload
                );
                err
            })
            .ok()?;

        proofs
            .check_and_combine_signatures(
                self.state.our_info(),
                self.our_section_bls_keys(),
                &signed_bytes,
            )
            .or_else(|| {
                log_or_panic!(
                    log::Level::Error,
                    "Failed to combine signatures for accumulated event: {:?}",
                    signed_payload
                );
                None
            })
    }

    /// Returns whether we should split into two sections.
    fn should_split(&self) -> bool {
        let our_name = self.our_id.name();
        let our_prefix_bit_count = self.state.our_prefix().bit_count();
        let (our_new_size, sibling_new_size) = self
            .state
            .our_members
            .mature()
            .map(|p2p_node| our_name.common_prefix(p2p_node.name()) > our_prefix_bit_count)
            .fold((0, 0), |(ours, siblings), is_our_prefix| {
                if is_our_prefix {
                    (ours + 1, siblings)
                } else {
                    (ours, siblings + 1)
                }
            });

        // If either of the two new sections will not contain enough entries, return `false`.
        let safe_section_size = self.safe_section_size();
        our_new_size >= safe_section_size && sibling_new_size >= safe_section_size
    }

    /// Splits our section and generates new elders infos for the child sections.
    fn split_self(&mut self) -> Result<(EldersInfo, EldersInfo), RoutingError> {
        let next_bit = self.our_id.name().bit(self.state.our_prefix().bit_count());

        let our_prefix = self.state.our_prefix().pushed(next_bit);
        let other_prefix = self.state.our_prefix().pushed(!next_bit);

        let our_new_section = self
            .state
            .our_members
            .elder_candidates_matching_prefix(&our_prefix, self.elder_size());
        let other_section = self
            .state
            .our_members
            .elder_candidates_matching_prefix(&other_prefix, self.elder_size());

        let our_new_info =
            EldersInfo::new(our_new_section, our_prefix, Some(self.state.our_info()))?;
        let other_info = EldersInfo::new(other_section, other_prefix, Some(self.state.our_info()))?;

        Ok((our_new_info, other_info))
    }

    /// Returns a set of nodes to which a message for the given `DstLocation` could be sent
    /// onwards, sorted by priority, along with the number of targets the message should be sent to.
    /// If the total number of targets returned is larger than this number, the spare targets can
    /// be used if the message can't be delivered to some of the initial ones.
    ///
    /// * If the destination is an `DstLocation::Section`:
    ///     - if our section is the closest on the network (i.e. our section's prefix is a prefix of
    ///       the destination), returns all other members of our section; otherwise
    ///     - returns the `N/3` closest members to the target
    ///
    /// * If the destination is an `DstLocation::PrefixSection`:
    ///     - if the prefix is compatible with our prefix and is fully-covered by prefixes in our
    ///       RT, returns all members in these prefixes except ourself; otherwise
    ///     - if the prefix is compatible with our prefix and is *not* fully-covered by prefixes in
    ///       our RT, returns `Err(Error::CannotRoute)`; otherwise
    ///     - returns the `N/3` closest members of the RT to the lower bound of the target
    ///       prefix
    ///
    /// * If the destination is an individual node:
    ///     - if our name *is* the destination, returns an empty set; otherwise
    ///     - if the destination name is an entry in the routing table, returns it; otherwise
    ///     - returns the `N/3` closest members of the RT to the target
    pub fn targets(&self, dst: &DstLocation) -> Result<(Vec<P2pNode>, usize), RoutingError> {
        if !self.is_self_elder() {
            // We are not Elder - return all the elders of our section, so the message can be properly
            // relayed through them.
            let targets: Vec<_> = self.state.our_info().member_nodes().cloned().collect();
            let dg_size = targets.len();
            return Ok((targets, dg_size));
        }

        let (best_section, dg_size) = match dst {
            DstLocation::Node(target_name) => {
                if target_name == self.our_id().name() {
                    return Ok((Vec::new(), 0));
                }
                if let Some(node) = self.get_p2p_node(target_name) {
                    return Ok((vec![node.clone()], 1));
                }
                self.candidates(target_name)?
            }
            DstLocation::Section(target_name) => {
                let (prefix, section) = self.state.sections.closest(target_name);
                if prefix == self.state.our_prefix() || prefix.is_neighbour(self.state.our_prefix())
                {
                    // Exclude our name since we don't need to send to ourself
                    let our_name = self.our_id().name();

                    // FIXME: only doing this for now to match RT.
                    // should confirm if needed esp after msg_relay changes.
                    let section: Vec<_> = section
                        .member_nodes()
                        .filter(|node| node.name() != our_name)
                        .cloned()
                        .collect();
                    let dg_size = section.len();
                    return Ok((section, dg_size));
                }
                self.candidates(target_name)?
            }
            DstLocation::Prefix(prefix) => {
                if prefix.is_compatible(self.state.our_prefix())
                    || prefix.is_neighbour(self.state.our_prefix())
                {
                    // only route the message when we have all the targets in our chain -
                    // this is to prevent spamming the network by sending messages with
                    // intentionally short prefixes
                    if prefix.is_compatible(self.state.our_prefix())
                        && !prefix.is_covered_by(self.state.known_prefixes().iter())
                    {
                        return Err(RoutingError::CannotRoute);
                    }

                    let is_compatible = |(pfx, section)| {
                        if prefix.is_compatible(pfx) {
                            Some(section)
                        } else {
                            None
                        }
                    };

                    // Exclude our name since we don't need to send to ourself
                    let our_name = self.our_id().name();

                    let targets: Vec<_> = self
                        .state
                        .known_sections()
                        .filter_map(is_compatible)
                        .flat_map(EldersInfo::member_nodes)
                        .filter(|node| node.name() != our_name)
                        .cloned()
                        .collect();
                    let dg_size = targets.len();
                    return Ok((targets, dg_size));
                }
                self.candidates(&prefix.lower_bound())?
            }
            DstLocation::Direct => return Err(RoutingError::CannotRoute),
        };

        Ok((best_section, dg_size))
    }

    // Obtain the delivery group candidates for this target
    fn candidates(&self, target_name: &XorName) -> Result<(Vec<P2pNode>, usize), RoutingError> {
        let filtered_sections = self
            .state
            .sections
            .sorted_by_distance_to(target_name)
            .into_iter()
            .map(|(prefix, members)| (prefix, members.len(), members.member_nodes()));

        let mut dg_size = 0;
        let mut nodes_to_send = Vec::new();
        for (idx, (prefix, len, connected)) in filtered_sections.enumerate() {
            nodes_to_send.extend(connected.cloned());
            dg_size = delivery_group_size(len);

            if prefix == self.state.our_prefix() {
                // Send to all connected targets so they can forward the message
                let our_name = self.our_id().name();
                nodes_to_send.retain(|node| node.name() != our_name);
                dg_size = nodes_to_send.len();
                break;
            }
            if idx == 0 && nodes_to_send.len() >= dg_size {
                // can deliver to enough of the closest section
                break;
            }
        }
        nodes_to_send.sort_by(|lhs, rhs| target_name.cmp_distance(lhs.name(), rhs.name()));

        if dg_size > 0 && nodes_to_send.len() >= dg_size {
            Ok((nodes_to_send, dg_size))
        } else {
            Err(RoutingError::CannotRoute)
        }
    }

    // Returns the set of peers that are responsible for collecting signatures to verify a message;
    // this may contain us or only other nodes.
    pub fn signature_targets(&self, dst: &DstLocation) -> Vec<P2pNode> {
        let dst_name = match dst {
            DstLocation::Node(name) => *name,
            DstLocation::Section(name) => *name,
            DstLocation::Prefix(prefix) => prefix.name(),
            DstLocation::Direct => {
                log_or_panic!(
                    log::Level::Error,
                    "Invalid destination for signature targets: {:?}",
                    dst
                );
                return vec![];
            }
        };

        let mut list = self
            .state
            .our_elders()
            .cloned()
            .sorted_by(|lhs, rhs| dst_name.cmp_distance(lhs.name(), rhs.name()));
        list.truncate(delivery_group_size(list.len()));
        list
    }

    /// Returns whether we are a part of the given source.
    pub fn in_src_location(&self, src: &SrcLocation) -> bool {
        match src {
            SrcLocation::Node(name) => self.our_id().name() == name,
            SrcLocation::Section(prefix) => prefix.matches(self.our_id().name()),
        }
    }

    /// Returns whether we are a part of the given destination.
    pub fn in_dst_location(&self, dst: &DstLocation) -> bool {
        match dst {
            DstLocation::Node(name) => self.our_id().name() == name,
            DstLocation::Section(name) => self.state.our_prefix().matches(name),
            DstLocation::Prefix(prefix) => self.state.our_prefix().is_compatible(prefix),
            DstLocation::Direct => true,
        }
    }

    /// Compute an estimate of the size of the network from the size of our routing table.
    ///
    /// Return (estimate, exact), with exact = true iff we have the whole network in our
    /// routing table.
    pub fn network_size_estimate(&self) -> (u64, bool) {
        let known_prefixes = self.state.known_prefixes();
        let is_exact = Prefix::default().is_covered_by(known_prefixes.iter());

        // Estimated fraction of the network that we have in our RT.
        // Computed as the sum of 1 / 2^(prefix.bit_count) for all known section prefixes.
        let network_fraction: f64 = known_prefixes
            .iter()
            .map(|p| 1.0 / (p.bit_count() as f64).exp2())
            .sum();

        // Total size estimate = known_nodes / network_fraction
        let network_size = self.state.known_elders().count() as f64 / network_fraction;

        (network_size.ceil() as u64, is_exact)
    }

    /// Check if we know this node but have not yet processed it.
    pub fn is_in_online_backlog(&self, pub_id: &PublicId) -> bool {
        self.state.churn_event_backlog.iter().any(|evt| {
            if let AccumulatingEvent::Online(payload) = &evt.content {
                payload.p2p_node.public_id() == pub_id
            } else {
                false
            }
        })
    }

    /// Returns network statistics.
    pub fn stats(&self) -> Stats {
        let (total_elders, total_elders_exact) = self.network_size_estimate();

        Stats {
            known_elders: self.state.known_elders().count() as u64,
            total_elders,
            total_elders_exact,
        }
    }
}

#[cfg(any(test, feature = "mock_base"))]
impl Chain {
    /// Returns the members of the section with the given prefix (if it exists)
    pub fn get_section(&self, pfx: &Prefix<XorName>) -> Option<&EldersInfo> {
        if self.state.our_prefix() == pfx {
            Some(self.state.our_info())
        } else {
            self.state.sections.get(pfx)
        }
    }
}

#[cfg(feature = "mock_base")]
impl Chain {
    /// If our section is the closest one to `name`, returns all names in our section *including
    /// ours*, otherwise returns `None`.
    pub fn close_names(&self, name: &XorName) -> Option<Vec<XorName>> {
        if self.state.our_prefix().matches(name) {
            Some(self.state.our_info().member_names().copied().collect())
        } else {
            None
        }
    }

    /// Returns the age counter of the given member or `None` if not a member.
    pub fn member_age_counter(&self, name: &XorName) -> Option<u32> {
        self.state
            .our_members
            .get(name)
            .map(|member| member.age_counter_value())
    }
}

#[cfg(test)]
impl Chain {
    pub fn validate_our_history(&self) -> bool {
        self.state.our_history.validate()
    }
}

fn key_matching_first_elder_name(
    elders_info: &EldersInfo,
    mut name_to_key: BTreeMap<XorName, DkgResult>,
) -> Result<DkgResult, RoutingError> {
    let first_name = elders_info
        .member_names()
        .next()
        .ok_or(RoutingError::InvalidElderDkgResult)?;
    name_to_key
        .remove(first_name)
        .ok_or(RoutingError::InvalidElderDkgResult)
}

/// The outcome of successful accumulated poll
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum PollAccumulated {
    AccumulatedEvent(AccumulatedEvent),
    RelocateDetails(RelocateDetails),
    PromoteDemoteElders(Vec<EldersInfo>),
}

/// The outcome of a prefix change.
pub struct ParsecResetData {
    /// The new genesis prefix info.
    pub gen_pfx_info: GenesisPfxInfo,
    /// The cached events that should be revoted.
    pub cached_events: BTreeSet<NetworkEvent>,
    /// The completed events.
    pub completed_events: BTreeSet<AccumulatingEvent>,
}

/// The secret share of the section key.
#[derive(Clone)]
pub struct SectionKeyShare {
    /// Index used to combine signature share and get PublicKeyShare from PublicKeySet.
    pub index: usize,
    /// Secret Key share
    pub key: bls::SecretKeyShare,
}

impl SectionKeyShare {
    /// Create a new share with associated share index.
    #[cfg(any(test, feature = "mock_base"))]
    pub const fn new_with_position(index: usize, key: bls::SecretKeyShare) -> Self {
        Self { index, key }
    }

    /// create a new share finding the position wihtin the elders.
    pub fn new(
        key: bls::SecretKeyShare,
        our_id: &PublicId,
        new_elders_info: &EldersInfo,
    ) -> Option<Self> {
        Some(Self {
            index: new_elders_info.member_ids().position(|id| id == our_id)?,
            key,
        })
    }
}

/// All the key material needed to sign or combine signature for our section key.
#[derive(Clone)]
pub struct SectionKeys {
    /// Public key set to verify threshold signatures and combine shares.
    pub public_key_set: bls::PublicKeySet,
    /// Secret Key share and index. None if the node was not participating in the DKG.
    pub secret_key_share: Option<SectionKeyShare>,
}

impl SectionKeys {
    pub fn new(dkg_result: DkgResult, our_id: &PublicId, new_elders_info: &EldersInfo) -> Self {
        Self {
            public_key_set: dkg_result.public_key_set,
            secret_key_share: dkg_result
                .secret_key_share
                .and_then(|key| SectionKeyShare::new(key, our_id, new_elders_info)),
        }
    }
}

struct EldersChangeBuilder {
    old_neighbour: BTreeSet<P2pNode>,
}

impl EldersChangeBuilder {
    fn new(chain: &Chain) -> Self {
        Self {
            old_neighbour: chain.state.sections.other_elders().cloned().collect(),
        }
    }

    fn build(self, chain: &Chain) -> EldersChange {
        let new_neighbour: BTreeSet<_> = chain.state.sections.other_elders().cloned().collect();

        EldersChange {
            neighbour_added: new_neighbour
                .difference(&self.old_neighbour)
                .cloned()
                .collect(),
            neighbour_removed: self
                .old_neighbour
                .difference(&new_neighbour)
                .cloned()
                .collect(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct SplitCache {
    elders_info: EldersInfo,
    key_info: SectionKeyInfo,
    proofs: AccumulatingProof,
}

#[cfg(test)]
mod tests {
    use super::{super::GenesisPfxInfo, *};
    use crate::{
        consensus::generate_bls_threshold_secret_key,
        id::{FullId, P2pNode, PublicId},
        rng::{self, MainRng},
        section::{EldersInfo, MIN_AGE_COUNTER},
        unwrap,
        xor_space::{Prefix, XorName},
    };
    use rand::{seq::SliceRandom, Rng};
    use std::{
        collections::{BTreeMap, HashMap},
        str::FromStr,
    };

    enum SecInfoGen<'a> {
        New(Prefix<XorName>, usize),
        Add(&'a EldersInfo),
        Remove(&'a EldersInfo),
    }

    fn gen_section_info(
        rng: &mut MainRng,
        gen: SecInfoGen,
    ) -> (EldersInfo, HashMap<PublicId, FullId>) {
        match gen {
            SecInfoGen::New(pfx, n) => {
                let mut full_ids = HashMap::new();
                let mut members = BTreeMap::new();
                for _ in 0..n {
                    let some_id = FullId::within_range(rng, &pfx.range_inclusive());
                    let peer_addr = ([127, 0, 0, 1], 9999).into();
                    let pub_id = *some_id.public_id();
                    let _ = members.insert(*pub_id.name(), P2pNode::new(pub_id, peer_addr));
                    let _ = full_ids.insert(*some_id.public_id(), some_id);
                }
                (EldersInfo::new(members, pfx, None).unwrap(), full_ids)
            }
            SecInfoGen::Add(info) => {
                let mut members = info.member_map().clone();
                let some_id = FullId::within_range(rng, &info.prefix().range_inclusive());
                let peer_addr = ([127, 0, 0, 1], 9999).into();
                let pub_id = *some_id.public_id();
                let _ = members.insert(*pub_id.name(), P2pNode::new(pub_id, peer_addr));
                let mut full_ids = HashMap::new();
                let _ = full_ids.insert(pub_id, some_id);
                (
                    EldersInfo::new(members, *info.prefix(), Some(info)).unwrap(),
                    full_ids,
                )
            }
            SecInfoGen::Remove(info) => {
                let members = info.member_map().clone();
                (
                    EldersInfo::new(members, *info.prefix(), Some(info)).unwrap(),
                    Default::default(),
                )
            }
        }
    }

    fn add_neighbour_elders_info(chain: &mut Chain, neighbour_info: EldersInfo) {
        assert!(
            !neighbour_info.prefix().matches(chain.our_id.name()),
            "Only add neighbours."
        );
        chain.state.sections.add_neighbour(neighbour_info)
    }

    fn gen_chain<T>(
        rng: &mut MainRng,
        sections: T,
    ) -> (Chain, HashMap<PublicId, FullId>, bls::SecretKeySet)
    where
        T: IntoIterator<Item = (Prefix<XorName>, usize)>,
    {
        let mut full_ids = HashMap::new();
        let mut our_id = None;
        let mut section_members = vec![];
        for (pfx, size) in sections {
            let (info, ids) = gen_section_info(rng, SecInfoGen::New(pfx, size));
            if our_id.is_none() {
                our_id = Some(unwrap!(ids.values().next()).clone());
            }
            full_ids.extend(ids);
            section_members.push(info);
        }

        let our_id = unwrap!(our_id);
        let mut sections_iter = section_members.into_iter();

        let elders_info = sections_iter.next().expect("section members");
        let ages = elders_info
            .member_ids()
            .map(|pub_id| (*pub_id, MIN_AGE_COUNTER))
            .collect();

        let participants = elders_info.len();
        let our_id_index = 0;
        let secret_key_set = generate_bls_threshold_secret_key(rng, participants);
        let secret_key_share = secret_key_set.secret_key_share(our_id_index);
        let public_key_set = secret_key_set.public_keys();

        let genesis_info = GenesisPfxInfo {
            elders_info,
            public_keys: public_key_set,
            state_serialized: Vec::new(),
            ages,
            parsec_version: 0,
        };

        let mut chain = Chain::new(
            rng,
            Default::default(),
            our_id,
            genesis_info,
            Some(secret_key_share),
        );

        for neighbour_info in sections_iter {
            add_neighbour_elders_info(&mut chain, neighbour_info);
        }

        (chain, full_ids, secret_key_set)
    }

    fn gen_00_chain(rng: &mut MainRng) -> (Chain, HashMap<PublicId, FullId>, bls::SecretKeySet) {
        let elder_size: usize = 7;
        gen_chain(
            rng,
            vec![
                (Prefix::from_str("00").unwrap(), elder_size),
                (Prefix::from_str("01").unwrap(), elder_size),
                (Prefix::from_str("10").unwrap(), elder_size),
            ],
        )
    }

    fn check_infos_for_duplication(chain: &Chain) {
        let mut prefixes: Vec<Prefix<XorName>> = vec![];
        for (_, info) in chain.state.sections.all() {
            if let Some(pfx) = prefixes.iter().find(|x| x.is_compatible(info.prefix())) {
                panic!(
                    "Found compatible prefixes! {:?} and {:?}",
                    pfx,
                    info.prefix()
                );
            }
            prefixes.push(*info.prefix());
        }
    }

    #[test]
    fn generate_chain() {
        let mut rng = rng::new();

        let (chain, _, _) = gen_00_chain(&mut rng);
        let chain_id = *chain.our_id();

        assert_eq!(
            chain
                .get_section(&Prefix::from_str("00").unwrap())
                .map(|info| info.is_member(&chain_id)),
            Some(true)
        );
        assert_eq!(chain.get_section(&Prefix::from_str("").unwrap()), None);
        assert!(chain.validate_our_history());
        check_infos_for_duplication(&chain);
    }

    #[test]
    fn neighbour_info_cleaning() {
        let mut rng = rng::new();
        let (mut chain, _, _) = gen_00_chain(&mut rng);
        for _ in 0..100 {
            let (new_info, _new_ids) = {
                let old_info: Vec<_> = chain.state.sections.other().map(|(_, info)| info).collect();
                let info = old_info.choose(&mut rng).expect("neighbour infos");
                if rng.gen_bool(0.5) {
                    gen_section_info(&mut rng, SecInfoGen::Add(info))
                } else {
                    gen_section_info(&mut rng, SecInfoGen::Remove(info))
                }
            };

            add_neighbour_elders_info(&mut chain, new_info);
            assert!(chain.validate_our_history());
            check_infos_for_duplication(&chain);
        }
    }
}

// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::messaging::system::{NodeState, SystemMsg};
use crate::node::{api::cmds::Cmd, core::Node, Result, SectionAuthUtils};
use crate::types::Peer;

use sn_membership::{SignedVote, VoteResponse};
use std::vec;

// Message handling
impl Node {
    pub(crate) async fn handle_membership_vote(
        &self,
        peer: Peer,
        signed_vote: SignedVote<NodeState>,
    ) -> Result<Vec<Cmd>> {
        debug!("Received membership vote {:?} from {}", signed_vote, peer);

        let cmds = if let Some(membership) = self.membership.write().await.as_mut() {
            assert!(self.is_elder().await);
            match membership.handle_signed_vote(signed_vote) {
                Ok(VoteResponse::Broadcast(response_vote)) => {
                    vec![
                        self.send_msg_to_our_elders(SystemMsg::Membership(response_vote))
                            .await?,
                    ]
                }
                Ok(VoteResponse::WaitingForMoreVotes) => vec![],
                Err(e) => {
                    error!("Error while processing vote {:?}", e);
                    vec![]
                }
            }
        } else {
            assert!(self.is_not_elder().await);
            vec![]
        };

        Ok(cmds)
    }
}

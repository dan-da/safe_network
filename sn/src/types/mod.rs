// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! SAFE network data types.

/// Standardised log markers for various events
pub mod log_markers;
/// PrefixMap used in client and node.
pub mod prefix_map;
/// Register data type
pub mod register;
/// Encoding utils
pub mod utils;

mod address;
mod cache;
mod chunk;
mod connections;
mod errors;
mod keys;
mod peer;
mod token;

pub(crate) use connections::{PeerLinks, SendToOneError};

pub use address::{
    BytesAddress, ChunkAddress, DataAddress, RegisterAddress, ReplicatedDataAddress,
    SafeKeyAddress, Scope,
};
pub use cache::Cache;
pub use chunk::{Chunk, MAX_CHUNK_SIZE_IN_BYTES};
pub use errors::{convert_dt_error_to_error_msg, Error, Result};
pub use keys::{
    keypair::{BlsKeypairShare, Encryption, Keypair, OwnerType, Signing},
    node_keypairs::NodeKeypairs,
    public_key::PublicKey,
    secret_key::SecretKey,
    signature::{Signature, SignatureShare},
};
pub use peer::Peer;
pub use token::Token;

use crate::messaging::data::{RegisterCmd, ReplicatedRegisterLog};

use serde::{Deserialize, Serialize};
use xor_name::XorName;

///
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum ReplicatedData {
    /// A chunk of data.
    Chunk(Chunk),
    /// A single cmd for a register.
    RegisterWrite(RegisterCmd),
    /// An entire op log of a register.
    RegisterLog(ReplicatedRegisterLog),
}

impl ReplicatedData {
    pub(crate) fn name(&self) -> XorName {
        match self {
            Self::Chunk(chunk) => *chunk.name(),
            Self::RegisterLog(log) => *log.address.name(),
            Self::RegisterWrite(cmd) => *cmd.dst_address().name(),
        }
    }

    pub(crate) fn address(&self) -> ReplicatedDataAddress {
        match self {
            Self::Chunk(chunk) => ReplicatedDataAddress::Chunk(*chunk.address()),
            Self::RegisterLog(log) => ReplicatedDataAddress::Register(log.address),
            Self::RegisterWrite(cmd) => ReplicatedDataAddress::Register(cmd.dst_address()),
        }
    }
}

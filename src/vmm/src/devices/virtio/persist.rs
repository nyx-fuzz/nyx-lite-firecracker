// Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Defines the structures needed for saving/restoring Virtio primitives.

use std::num::Wrapping;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::devices::virtio::device::VirtioDevice;
use crate::devices::virtio::gen::virtio_ring::VIRTIO_RING_F_EVENT_IDX;
use crate::devices::virtio::mmio::MmioTransport;
use crate::devices::virtio::queue::Queue;
use crate::snapshot::Persist;
use crate::vstate::memory::{GuestAddress, GuestMemoryMmap};

/// Errors thrown during restoring virtio state.
#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum PersistError {
    /// Snapshot state contains invalid queue info.
    InvalidInput,
}

/// Queue information saved in snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueState {
    /// The maximal size in elements offered by the device
    max_size: u16,

    /// The queue size in elements the driver selected
    size: u16,

    /// Indicates if the queue is finished with configuration
    ready: bool,

    /// Guest physical address of the descriptor table
    desc_table: u64,

    /// Guest physical address of the available ring
    avail_ring: u64,

    /// Guest physical address of the used ring
    used_ring: u64,

    next_avail: Wrapping<u16>,
    next_used: Wrapping<u16>,

    uses_notif_suppression: bool, // NYX-LITE PATCH to allow restoring this field in queues as well.

    /// The number of added used buffers since last guest kick
    num_added: Wrapping<u16>,
}

impl Persist<'_> for Queue {
    type State = QueueState;
    type ConstructorArgs = ();
    type Error = ();

    fn save(&self) -> Self::State {
        QueueState {
            max_size: self.max_size,
            size: self.size,
            ready: self.ready,
            desc_table: self.desc_table.0,
            avail_ring: self.avail_ring.0,
            used_ring: self.used_ring.0,
            next_avail: self.next_avail,
            next_used: self.next_used,
            uses_notif_suppression: self.uses_notif_suppression, // NYX-LITE PATCH
            num_added: self.num_added,
        }
    }

    fn restore(_: Self::ConstructorArgs, state: &Self::State) -> Result<Self, Self::Error> {
        Ok(Queue {
            max_size: state.max_size,
            size: state.size,
            ready: state.ready,
            desc_table: GuestAddress(state.desc_table),
            avail_ring: GuestAddress(state.avail_ring),
            used_ring: GuestAddress(state.used_ring),
            next_avail: state.next_avail,
            next_used: state.next_used,
            uses_notif_suppression: state.uses_notif_suppression, // NYX-LITE PATCH
            num_added: state.num_added,
        })
    }
}

/// State of a VirtioDevice.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VirtioDeviceState {
    /// Device type.
    pub device_type: u32,
    /// Available virtio features.
    pub avail_features: u64,
    /// Negotiated virtio features.
    pub acked_features: u64,
    /// List of queues.
    pub queues: Vec<QueueState>,
    /// The MMIO interrupt status.
    pub interrupt_status: u32,
    /// Flag for activated status.
    pub activated: bool,
}

impl VirtioDeviceState {
    /// Construct the virtio state of a device.
    pub fn from_device(device: &dyn VirtioDevice) -> Self {
        VirtioDeviceState {
            device_type: device.device_type(),
            avail_features: device.avail_features(),
            acked_features: device.acked_features(),
            queues: device.queues().iter().map(Persist::save).collect(),
            interrupt_status: device.interrupt_status().load(Ordering::Relaxed),
            activated: device.is_activated(),
        }
    }

    /// Does sanity checking on the `self` state against expected values
    /// and builds queues from state.
    pub fn build_queues_checked(
        &self,
        mem: &GuestMemoryMmap,
        expected_device_type: u32,
        expected_num_queues: usize,
        expected_queue_max_size: u16,
    ) -> Result<Vec<Queue>, PersistError> {
        // Sanity check:
        // - right device type,
        // - acked features is a subset of available ones,
        // - right number of queues,
        if self.device_type != expected_device_type
            || (self.acked_features & !self.avail_features) != 0
            || self.queues.len() != expected_num_queues
        {
            return Err(PersistError::InvalidInput);
        }

        let uses_notif_suppression = (self.acked_features & 1u64 << VIRTIO_RING_F_EVENT_IDX) != 0;
        let queues: Vec<Queue> = self
            .queues
            .iter()
            .map(|queue_state| {
                // Safe to unwrap, `Queue::restore` has no error case.
                let mut queue = Queue::restore((), queue_state).unwrap();
                if uses_notif_suppression {
                    queue.enable_notif_suppression();
                }
                queue
            })
            .collect();

        for q in &queues {
            // Sanity check queue size and queue max size.
            if q.max_size != expected_queue_max_size || q.size > expected_queue_max_size {
                return Err(PersistError::InvalidInput);
            }
            // Snapshot can happen at any time, including during device configuration/activation
            // when fields are only partially configured.
            //
            // Only if the device was activated, check `q.is_valid()`.
            if self.activated && !q.is_valid(mem) {
                return Err(PersistError::InvalidInput);
            }
        }
        Ok(queues)
    }
}

/// Transport information saved in snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MmioTransportState {
    // The register where feature bits are stored.
    pub features_select: u32, // NYX-LITE PATCH
    // The register where features page is selected.
    pub acked_features_select: u32, // NYX-LITE PATCH
    pub queue_select: u32, // NYX-LITE PATCH
    pub device_status: u32, // NYX-LITE PATCH
    pub config_generation: u32, //NYX-LITE PATCH
}

/// Auxiliary structure for initializing the transport when resuming from a snapshot.
#[derive(Debug)]
pub struct MmioTransportConstructorArgs {
    /// Pointer to guest memory.
    pub mem: GuestMemoryMmap,
    /// Device associated with the current MMIO state.
    pub device: Arc<Mutex<dyn VirtioDevice>>,
    /// Is device backed by vhost-user.
    pub is_vhost_user: bool,
}

impl Persist<'_> for MmioTransport {
    type State = MmioTransportState;
    type ConstructorArgs = MmioTransportConstructorArgs;
    type Error = ();

    fn save(&self) -> Self::State {
        MmioTransportState {
            features_select: self.features_select,
            acked_features_select: self.acked_features_select,
            queue_select: self.queue_select,
            device_status: self.device_status,
            config_generation: self.config_generation,
        }
    }

    fn restore(
        constructor_args: Self::ConstructorArgs,
        state: &Self::State,
    ) -> Result<Self, Self::Error> {
        let mut transport = MmioTransport::new(
            constructor_args.mem,
            constructor_args.device,
            constructor_args.is_vhost_user,
        );
        transport.features_select = state.features_select;
        transport.acked_features_select = state.acked_features_select;
        transport.queue_select = state.queue_select;
        transport.device_status = state.device_status;
        transport.config_generation = state.config_generation;
        Ok(transport)
    }
}

#[cfg(test)]
mod tests {
    use utils::tempfile::TempFile;

    use super::*;
    use crate::devices::virtio::block::virtio::device::FileEngineType;
    use crate::devices::virtio::block::virtio::test_utils::default_block_with_path;
    use crate::devices::virtio::block::virtio::VirtioBlock;
    use crate::devices::virtio::mmio::tests::DummyDevice;
    use crate::devices::virtio::net::test_utils::default_net;
    use crate::devices::virtio::net::Net;
    use crate::devices::virtio::test_utils::default_mem;
    use crate::devices::virtio::vsock::{Vsock, VsockUnixBackend};
    use crate::snapshot::Snapshot;

    const DEFAULT_QUEUE_MAX_SIZE: u16 = 256;
    impl Default for QueueState {
        fn default() -> QueueState {
            QueueState {
                max_size: DEFAULT_QUEUE_MAX_SIZE,
                size: DEFAULT_QUEUE_MAX_SIZE,
                ready: false,
                desc_table: 0,
                avail_ring: 0,
                used_ring: 0,
                next_avail: Wrapping(0),
                next_used: Wrapping(0),
                uses_notif_suppression: false, // NYX-LITE PATCH
                num_added: Wrapping(0),
            }
        }
    }

    #[test]
    fn test_virtiodev_sanity_checks() {
        let max_size = DEFAULT_QUEUE_MAX_SIZE;
        let mut state = VirtioDeviceState::default();
        let mem = default_mem();
        // Valid checks.
        state.build_queues_checked(&mem, 0, 0, max_size).unwrap();
        // Invalid dev-type.
        state
            .build_queues_checked(&mem, 1, 0, max_size)
            .unwrap_err();
        // Invalid num-queues.
        state
            .build_queues_checked(&mem, 0, 1, max_size)
            .unwrap_err();
        // Unavailable features acked.
        state.acked_features = 1;
        state
            .build_queues_checked(&mem, 0, 0, max_size)
            .unwrap_err();

        // Validate queue sanity checks.
        let mut state = VirtioDeviceState::default();
        let good_q = QueueState::default();
        state.queues = vec![good_q];
        // Valid.
        state
            .build_queues_checked(&mem, 0, state.queues.len(), max_size)
            .unwrap();

        // Invalid max queue size.
        let bad_q = QueueState {
            max_size: max_size + 1,
            ..Default::default()
        };
        state.queues = vec![bad_q];
        state
            .build_queues_checked(&mem, 0, state.queues.len(), max_size)
            .unwrap_err();

        // Invalid: size > max.
        let bad_q = QueueState {
            size: max_size + 1,
            ..Default::default()
        };
        state.queues = vec![bad_q];
        state
            .build_queues_checked(&mem, 0, state.queues.len(), max_size)
            .unwrap_err();

        // activated && !q.is_valid()
        let bad_q = QueueState::default();
        state.queues = vec![bad_q];
        state.activated = true;
        state
            .build_queues_checked(&mem, 0, state.queues.len(), max_size)
            .unwrap_err();
    }

    #[test]
    fn test_queue_persistence() {
        let queue = Queue::new(128);

        let mut mem = vec![0; 4096];

        Snapshot::serialize(&mut mem.as_mut_slice(), &queue.save()).unwrap();

        let restored_queue =
            Queue::restore((), &Snapshot::deserialize(&mut mem.as_slice()).unwrap()).unwrap();

        assert_eq!(restored_queue, queue);
    }

    #[test]
    fn test_virtio_device_state_serde() {
        let dummy = DummyDevice::new();
        let mut mem = vec![0; 4096];

        let state = VirtioDeviceState::from_device(&dummy);
        Snapshot::serialize(&mut mem.as_mut_slice(), &state).unwrap();

        let restored_state: VirtioDeviceState = Snapshot::deserialize(&mut mem.as_slice()).unwrap();
        assert_eq!(restored_state, state);
    }

    impl PartialEq for MmioTransport {
        fn eq(&self, other: &MmioTransport) -> bool {
            let self_dev_type = self.device().lock().unwrap().device_type();
            self.acked_features_select == other.acked_features_select &&
                self.features_select == other.features_select &&
                self.queue_select == other.queue_select &&
                self.device_status == other.device_status &&
                self.config_generation == other.config_generation &&
                self.interrupt_status.load(Ordering::SeqCst) == other.interrupt_status.load(Ordering::SeqCst) &&
                // Only checking equality of device type, actual device (de)ser is tested by that
                // device's tests.
                self_dev_type == other.device().lock().unwrap().device_type()
        }
    }

    fn generic_mmiotransport_persistence_test(
        mmio_transport: MmioTransport,
        mem: GuestMemoryMmap,
        device: Arc<Mutex<dyn VirtioDevice>>,
    ) {
        let mut buf = vec![0; 4096];

        Snapshot::serialize(&mut buf.as_mut_slice(), &mmio_transport.save()).unwrap();

        let restore_args = MmioTransportConstructorArgs {
            mem,
            device,
            is_vhost_user: false,
        };
        let restored_mmio_transport = MmioTransport::restore(
            restore_args,
            &Snapshot::deserialize(&mut buf.as_slice()).unwrap(),
        )
        .unwrap();

        assert_eq!(restored_mmio_transport, mmio_transport);
    }

    fn create_default_block() -> (MmioTransport, GuestMemoryMmap, Arc<Mutex<VirtioBlock>>) {
        let mem = default_mem();

        // Create backing file.
        let f = TempFile::new().unwrap();
        f.as_file().set_len(0x1000).unwrap();
        let block = default_block_with_path(
            f.as_path().to_str().unwrap().to_string(),
            FileEngineType::default(),
        );
        let block = Arc::new(Mutex::new(block));
        let mmio_transport = MmioTransport::new(mem.clone(), block.clone(), false);

        (mmio_transport, mem, block)
    }

    fn create_default_net() -> (MmioTransport, GuestMemoryMmap, Arc<Mutex<Net>>) {
        let mem = default_mem();
        let net = Arc::new(Mutex::new(default_net()));
        let mmio_transport = MmioTransport::new(mem.clone(), net.clone(), false);

        (mmio_transport, mem, net)
    }

    fn default_vsock() -> (
        MmioTransport,
        GuestMemoryMmap,
        Arc<Mutex<Vsock<VsockUnixBackend>>>,
    ) {
        let mem = default_mem();

        let guest_cid = 52;
        let mut temp_uds_path = TempFile::new().unwrap();
        // Remove the file so the path can be used by the socket.
        temp_uds_path.remove().unwrap();
        let uds_path = String::from(temp_uds_path.as_path().to_str().unwrap());
        let backend = VsockUnixBackend::new(guest_cid, uds_path).unwrap();
        let vsock = Vsock::new(guest_cid, backend).unwrap();
        let vsock = Arc::new(Mutex::new(vsock));
        let mmio_transport = MmioTransport::new(mem.clone(), vsock.clone(), false);

        (mmio_transport, mem, vsock)
    }

    #[test]
    fn test_block_over_mmiotransport_persistence() {
        let (mmio_transport, mem, block) = create_default_block();
        generic_mmiotransport_persistence_test(mmio_transport, mem, block);
    }

    #[test]
    fn test_net_over_mmiotransport_persistence() {
        let (mmio_transport, mem, net) = create_default_net();
        generic_mmiotransport_persistence_test(mmio_transport, mem, net);
    }

    #[test]
    fn test_vsock_over_mmiotransport_persistence() {
        let (mmio_transport, mem, vsock) = default_vsock();
        generic_mmiotransport_persistence_test(mmio_transport, mem, vsock);
    }
}

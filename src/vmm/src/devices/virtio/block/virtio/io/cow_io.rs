// BEGIN NYX-LITE PATCH
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::sync::Mutex;
use std::sync::Arc;
use std::ops::Bound::{Included, Excluded};
use vm_memory::bitmap::BitmapSlice;
use vm_memory::{GuestMemoryError, ReadVolatile, VolatileSlice, WriteVolatile};

use crate::vstate::memory::{GuestAddress, GuestMemory, GuestMemoryMmap};
use crate::devices::virtio::block::virtio::io::sync_io::SyncIoError;
use crate::devices::virtio::block::virtio::SECTOR_SIZE;

const SECTOR_SIZE_USIZE : usize = SECTOR_SIZE as usize;
const SECTOR_SIZE_U64 : u64 = SECTOR_SIZE as u64;


#[derive(Debug)]
pub struct CowCache{
    areas: Vec<[u8; SECTOR_SIZE_USIZE]>,
    disc_offset_to_area_idx: BTreeMap<u64, usize>,
}

impl CowCache{
    pub fn new() -> Self{
        Self{areas: vec![], disc_offset_to_area_idx: BTreeMap::new()}
    }

    pub fn write<Bitmap: BitmapSlice>(&mut self, offset: u64, src: &VolatileSlice<Bitmap>){
        assert_eq!(offset%SECTOR_SIZE_U64, 0,"read offset is not aligend to SECTOR_SIZE");
        assert_eq!(src.len()%SECTOR_SIZE_USIZE, 0, "len of data written is not aligned to SECTOR_SIZE");
        for sector_i in 0..(src.len() as u64)/SECTOR_SIZE_U64{
            let disc_offset = offset+sector_i*SECTOR_SIZE_U64;
            let area_idx = if let Some(area) = self.disc_offset_to_area_idx.get(&disc_offset){
                *area
            } else{
                let area_idx = self.areas.len();
                self.disc_offset_to_area_idx.insert(disc_offset, area_idx);
                self.areas.push([0; SECTOR_SIZE_USIZE]);
                area_idx
            };
            src.subslice((sector_i*SECTOR_SIZE_U64) as usize, SECTOR_SIZE as usize).unwrap().copy_to(&mut self.areas[area_idx]);
        }
    }

    pub fn read<Bitmap: BitmapSlice>(&self, file: &mut File, offset: u64, mut dest: &mut VolatileSlice<Bitmap>) {

        file.seek(SeekFrom::Start(offset)).unwrap();
            //.map_err(SyncIoError::Seek).;
        // TODO this is kinda inefficient if a lot of values have been written
        // before, as we copy a bunch from the file just to overwirite it from
        // the COW -- maybe profile & optimize 
        file.read_exact_volatile(&mut dest).unwrap();
        let bounds =  (Included(offset), Excluded(offset+(dest.len() as u64)));
        for (disc_offset, area_idx) in self.disc_offset_to_area_idx.range(bounds) {
            let slice_offset = disc_offset - offset;
            dest.subslice(slice_offset as usize, SECTOR_SIZE_USIZE).unwrap().copy_from(&self.areas[*area_idx]);
        }
    }
}

#[derive(Debug)]
pub struct CowFileEngine {
    file: File,
    cache: Arc<Mutex<CowCache>>,
}

// SAFETY: `File` is send and ultimately a POD.
unsafe impl Send for CowFileEngine {}

impl CowFileEngine {
    pub fn from_file(file: File) -> CowFileEngine {
        CowFileEngine { file, cache: Arc::new(Mutex::new(CowCache::new())) }
    }

    #[cfg(test)]
    pub fn file(&self) -> &File {
        &self.file
    }

    /// Update the backing file of the engine
    pub fn update_file(&mut self, file: File) {
        self.file = file
    }

    pub fn read(
        &mut self,
        offset: u64,
        mem: &GuestMemoryMmap,
        addr: GuestAddress,
        count: u32,
    ) -> Result<u32, SyncIoError> {

        mem.get_slice(addr, count as usize)
            .and_then(|mut slice| Ok(self.cache.lock().unwrap().read(&mut self.file, offset, &mut slice)))
            .map_err(SyncIoError::Transfer)?;
        Ok(count)
    }

    pub fn write(
        &mut self,
        offset: u64,
        mem: &GuestMemoryMmap,
        addr: GuestAddress,
        count: u32,
    ) -> Result<u32, SyncIoError> {
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(SyncIoError::Seek)?;
        mem.get_slice(addr, count as usize)
            .and_then(|slice| Ok(self.cache.lock().unwrap().write(offset, &slice)))
            .map_err(SyncIoError::Transfer)?;
        
        Ok(count)
    }

    pub fn flush(&mut self) -> Result<(), SyncIoError> {
        // since we never write to a file, flush is a NOP
        Ok(())
    }
}

// END NYX-LITE PATCH
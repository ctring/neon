//!
//! This provides an abstraction to store PostgreSQL relations and other files
//! in the key-value store
//!
//! (TODO: The line between PUT-functions here and walingest.rs is a bit blurry, as
//! walingest.rs handles a few things like implicit relation creation and extension.
//! Clarify that)
//!

use crate::relish::*;
use crate::repository::*;
use crate::repository::{Repository, Timeline};
use crate::walrecord::ZenithWalRecord;
use anyhow::{bail, Result};
use bytes::{Buf, Bytes};
use postgres_ffi::{pg_constants, Oid, TransactionId};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::{Arc, RwLockReadGuard};
use tracing::{debug, info, warn};
use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::{Lsn, RecordLsn};

/// Block number within a relation or SRU. This matches PostgreSQL's BlockNumber type.
pub type BlockNumber = u32;

pub struct DatadirTimeline<R>
where
    R: Repository,
{
    pub tline: Arc<R::Timeline>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DbDirectory {
    // (dbnode, spcnode)
    dbs: HashSet<(Oid, Oid)>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TwoPhaseDirectory {
    xids: HashSet<TransactionId>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RelDirectory {
    // Set of relations that exist. (relfilenode, forknum)
    //
    // TODO: Store it as a btree or radix tree or something else that spans multiple
    // key-value pairs, if you have a lot of relations
    rels: HashSet<(Oid, u8)>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RelSizeEntry {
    nblocks: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SlruSegmentDirectory {
    // Set of SLRU segments that exist.
    segments: HashSet<u32>,
}

static ZERO_PAGE: Bytes = Bytes::from_static(&[0u8; 8192]);

impl<R: Repository> DatadirTimeline<R> {
    pub fn new(tline: Arc<R::Timeline>) -> Self {
        DatadirTimeline { tline }
    }

    //------------------------------------------------------------------------------
    // Public GET functions
    //------------------------------------------------------------------------------

    /// Look up given page version.
    pub fn get_rel_page_at_lsn(&self, tag: RelTag, blknum: BlockNumber, lsn: Lsn) -> Result<Bytes> {
        let nblocks = self.get_rel_size(tag, lsn)?;
        if blknum >= nblocks {
            debug!(
                "read beyond EOF at {} blk {} at {}, size is {}: returning all-zeros page",
                tag, blknum, lsn, nblocks
            );
            return Ok(ZERO_PAGE.clone());
        }

        let key = rel_block_to_key(tag, blknum);
        self.tline.get(key, lsn)
    }

    /// Look up given page version.
    pub fn get_slru_page_at_lsn(
        &self,
        kind: SlruKind,
        segno: u32,
        blknum: BlockNumber,
        lsn: Lsn,
    ) -> Result<Bytes> {
        let key = slru_block_to_key(kind, segno, blknum);
        self.tline.get(key, lsn)
    }

    /// Get size of a relation file
    pub fn get_rel_size(&self, tag: RelTag, lsn: Lsn) -> Result<BlockNumber> {
        if (tag.forknum == pg_constants::FSM_FORKNUM
            || tag.forknum == pg_constants::VISIBILITYMAP_FORKNUM)
            && !self.get_rel_exists(tag, lsn)?
        {
            // FIXME: Postgres sometimes calls calls smgrcreate() to
            // create FSM, and smgrnblocks() on it immediately
            // afterwards, without extending it.  Tolerate that by
            // claiming that any non-existent FSM fork has size 0.
            return Ok(0);
        }

        let key = rel_size_to_key(tag);
        let mut buf = self.tline.get(key, lsn)?;
        Ok(buf.get_u32_le())
    }

    /// Get size of an SLRU segment
    pub fn get_slru_segment_size(
        &self,
        kind: SlruKind,
        segno: u32,
        lsn: Lsn,
    ) -> Result<BlockNumber> {
        let key = slru_segment_size_to_key(kind, segno);
        let mut buf = self.tline.get(key, lsn)?;
        Ok(buf.get_u32_le())
    }

    /// Get size of an SLRU segment
    pub fn get_slru_segment_exists(&self, kind: SlruKind, segno: u32, lsn: Lsn) -> Result<bool> {
        // fetch directory listing
        let key = slru_dir_to_key(kind);
        let buf = self.tline.get(key, lsn)?;
        let dir = SlruSegmentDirectory::des(&buf)?;

        let exists = dir.segments.get(&segno).is_some();
        Ok(exists)
    }

    /// Does relation exist?
    pub fn get_rel_exists(&self, tag: RelTag, lsn: Lsn) -> Result<bool> {
        // fetch directory listing
        let key = rel_dir_to_key(tag.spcnode, tag.dbnode);
        let buf = self.tline.get(key, lsn)?;
        let dir = RelDirectory::des(&buf)?;

        let exists = dir.rels.get(&(tag.relnode, tag.forknum)).is_some();

        info!("EXISTS: {} : {:?}", tag, exists);

        Ok(exists)
    }

    /// Get a list of all existing relations in given tablespace and database.
    pub fn list_rels(&self, spcnode: u32, dbnode: u32, lsn: Lsn) -> Result<HashSet<RelTag>> {
        // fetch directory listing
        let key = rel_dir_to_key(spcnode, dbnode);
        let buf = self.tline.get(key, lsn)?;
        let dir = RelDirectory::des(&buf)?;

        let rels: HashSet<RelTag> =
            HashSet::from_iter(dir.rels.iter().map(|(relnode, forknum)| RelTag {
                spcnode,
                dbnode,
                relnode: *relnode,
                forknum: *forknum,
            }));

        Ok(rels)
    }

    /// Get a list of SLRU segments
    pub fn list_slru_segments(&self, kind: SlruKind, lsn: Lsn) -> Result<HashSet<u32>> {
        // fetch directory entry
        let key = slru_dir_to_key(kind);

        let buf = self.tline.get(key, lsn)?;
        let dir = SlruSegmentDirectory::des(&buf)?;

        Ok(dir.segments)
    }

    pub fn get_relmap_file(&self, spcnode: Oid, dbnode: Oid, lsn: Lsn) -> Result<Bytes> {
        let key = relmap_file_key(spcnode, dbnode);

        let buf = self.tline.get(key, lsn)?;
        Ok(buf)
    }

    pub fn list_relmap_files(&self, lsn: Lsn) -> Result<HashSet<(Oid, Oid)>> {
        // fetch directory entry
        let buf = self.tline.get(DBDIR_KEY, lsn)?;
        let dir = DbDirectory::des(&buf)?;

        Ok(dir.dbs)
    }

    pub fn get_twophase_file(&self, xid: TransactionId, lsn: Lsn) -> Result<Bytes> {
        let key = twophase_file_key(xid);
        let buf = self.tline.get(key, lsn)?;
        Ok(buf)
    }

    pub fn list_twophase_files(&self, lsn: Lsn) -> Result<HashSet<TransactionId>> {
        // fetch directory entry
        let buf = self.tline.get(TWOPHASEDIR_KEY, lsn)?;
        let dir = TwoPhaseDirectory::des(&buf)?;

        Ok(dir.xids)
    }

    pub fn get_control_file(&self, lsn: Lsn) -> Result<Bytes> {
        self.tline.get(CONTROLFILE_KEY, lsn)
    }

    pub fn get_checkpoint(&self, lsn: Lsn) -> Result<Bytes> {
        self.tline.get(CHECKPOINT_KEY, lsn)
    }

    //------------------------------------------------------------------------------
    // Public PUT functions, to update the repository with new page versions.
    //
    // These are called by the WAL receiver to digest WAL records.
    //------------------------------------------------------------------------------

    /// Atomically get both last and prev.
    pub fn get_last_record_rlsn(&self) -> RecordLsn {
        self.tline.get_last_record_rlsn()
    }

    /// Get last or prev record separately. Same as get_last_record_rlsn().last/prev.
    pub fn get_last_record_lsn(&self) -> Lsn {
        self.tline.get_last_record_lsn()
    }

    pub fn get_prev_record_lsn(&self) -> Lsn {
        self.tline.get_prev_record_lsn()
    }

    pub fn get_disk_consistent_lsn(&self) -> Lsn {
        self.tline.get_disk_consistent_lsn()
    }

    /// This provides a "transaction-like" interface to updating the data
    ///
    /// To ingest a WAL record, call begin_record(lsn) to get a writer
    /// object. Use the functions in the writer-object to modify the
    /// repository state, updating all the pages and metadata that the
    /// WAL record affects. When you're done, call writer.finish() to
    /// commit the changes.
    ///
    /// Note that any pending modifications you make through the writer
    /// won't be visible to calls to the get functions until you finish!
    /// If you update the same page twice, the last update wins.
    ///
    pub fn begin_record(&self, lsn: Lsn) -> DatadirTimelineWriter<R> {
        DatadirTimelineWriter {
            tline: self,
            lsn,
            pending_updates: HashMap::new(),
            pending_deletions: Vec::new(),
        }
    }

    ///
    /// Check that it is valid to request operations with that lsn.
    pub fn check_lsn_is_in_scope(
        &self,
        lsn: Lsn,
        latest_gc_cutoff_lsn: &RwLockReadGuard<Lsn>,
    ) -> Result<()> {
        self.tline.check_lsn_is_in_scope(lsn, latest_gc_cutoff_lsn)
    }

    /// Retrieve current logical size of the timeline
    ///
    /// NOTE: counted incrementally, includes ancestors,
    /// doesnt support TwoPhase relishes yet
    pub fn get_current_logical_size(&self) -> usize {
        //todo!()
        0
    }

    /// Does the same as get_current_logical_size but counted on demand.
    /// Used in tests to ensure that incremental and non incremental variants match.
    pub fn get_current_logical_size_non_incremental(&self, _lsn: Lsn) -> Result<usize> {
        //todo!()
        Ok(0)
    }
}

pub struct DatadirTimelineWriter<'a, R: Repository> {
    tline: &'a DatadirTimeline<R>,

    lsn: Lsn,
    pending_updates: HashMap<Key, Value>,
    pending_deletions: Vec<Range<Key>>,
}

// TODO Currently, Deref is used to allow easy access to read methods from this trait.
// This is probably considered a bad practice in Rust and should be fixed eventually,
// but will cause large code changes.
impl<'a, R: Repository> std::ops::Deref for DatadirTimelineWriter<'a, R> {
    type Target = DatadirTimeline<R>;

    fn deref(&self) -> &Self::Target {
        self.tline
    }
}

/// Various functions to mutate the repository state.
impl<'a, R: Repository> DatadirTimelineWriter<'a, R> {
    pub fn init_empty(&mut self) -> Result<()> {
        let buf = DbDirectory::ser(&DbDirectory {
            dbs: HashSet::new(),
        })?;
        self.put(DBDIR_KEY, Value::Image(buf.into()));

        let buf = TwoPhaseDirectory::ser(&TwoPhaseDirectory {
            xids: HashSet::new(),
        })?;
        self.put(TWOPHASEDIR_KEY, Value::Image(buf.into()));

        let buf: Bytes = SlruSegmentDirectory::ser(&SlruSegmentDirectory {
            segments: HashSet::new(),
        })?
        .into();
        self.put(slru_dir_to_key(SlruKind::Clog), Value::Image(buf.clone()));
        self.put(
            slru_dir_to_key(SlruKind::MultiXactMembers),
            Value::Image(buf.clone()),
        );
        self.put(
            slru_dir_to_key(SlruKind::MultiXactOffsets),
            Value::Image(buf),
        );

        Ok(())
    }

    /// Put a new page version that can be constructed from a WAL record
    ///
    /// NOTE: this will *not* implicitly extend the relation, if the page is beyond the
    /// current end-of-file. It's up to the caller to check that the relation size
    /// matches the blocks inserted!
    pub fn put_rel_wal_record(
        &mut self,
        rel: RelTag,
        blknum: BlockNumber,
        rec: ZenithWalRecord,
    ) -> Result<()> {
        self.put(rel_block_to_key(rel, blknum), Value::WalRecord(rec));
        Ok(())
    }

    pub fn put_slru_wal_record(
        &mut self,
        kind: SlruKind,
        segno: u32,
        blknum: BlockNumber,
        rec: ZenithWalRecord,
    ) -> Result<()> {
        self.put(
            slru_block_to_key(kind, segno, blknum),
            Value::WalRecord(rec),
        );
        Ok(())
    }

    /// Like put_wal_record, but with ready-made image of the page.
    pub fn put_rel_page_image(
        &mut self,
        rel: RelTag,
        blknum: BlockNumber,
        img: Bytes,
    ) -> Result<()> {
        self.put(rel_block_to_key(rel, blknum), Value::Image(img));
        Ok(())
    }

    pub fn put_slru_page_image(
        &mut self,
        kind: SlruKind,
        segno: u32,
        blknum: BlockNumber,
        img: Bytes,
    ) -> Result<()> {
        self.put(slru_block_to_key(kind, segno, blknum), Value::Image(img));
        Ok(())
    }

    pub fn put_relmap_file(&mut self, spcnode: Oid, dbnode: Oid, img: Bytes) -> Result<()> {
        // Add it to the directory (if it doesn't exist already)
        let buf = self.get(DBDIR_KEY)?;
        let mut dir = DbDirectory::des(&buf)?;
        if dir.dbs.insert((spcnode, dbnode)) {
            let buf = DbDirectory::ser(&dir)?;
            self.put(DBDIR_KEY, Value::Image(buf.into()));
        }

        self.put(relmap_file_key(spcnode, dbnode), Value::Image(img));
        Ok(())
    }

    pub fn put_twophase_file(&mut self, xid: TransactionId, img: Bytes) -> Result<()> {
        // Add it to the directory entry
        let buf = self.get(TWOPHASEDIR_KEY)?;
        let mut dir = TwoPhaseDirectory::des(&buf)?;
        if !dir.xids.insert(xid) {
            bail!("twophase file for xid {} already exists", xid);
        }
        self.put(
            TWOPHASEDIR_KEY,
            Value::Image(Bytes::from(TwoPhaseDirectory::ser(&dir)?)),
        );

        self.put(twophase_file_key(xid), Value::Image(img));
        Ok(())
    }

    pub fn put_control_file(&mut self, img: Bytes) -> Result<()> {
        self.put(CONTROLFILE_KEY, Value::Image(img));
        Ok(())
    }

    pub fn put_checkpoint(&mut self, img: Bytes) -> Result<()> {
        self.put(CHECKPOINT_KEY, Value::Image(img));
        Ok(())
    }

    pub fn put_dbdir_creation(&mut self, spcnode: Oid, dbnode: Oid) -> Result<()> {
        // Create RelDirectory
        let dir_key = rel_dir_to_key(spcnode, dbnode);

        let dir = RelDirectory {
            rels: HashSet::new(),
        };
        let buf: Bytes = RelDirectory::ser(&dir)?.into();
        self.put(dir_key, Value::Image(buf));
        Ok(())
    }

    pub fn drop_dbdir(&mut self, spcnode: Oid, dbnode: Oid) -> Result<()> {
        // Remove entry from dbdir
        let buf = self.get(DBDIR_KEY)?;
        let mut dir = DbDirectory::des(&buf)?;
        if dir.dbs.remove(&(spcnode, dbnode)) {
            let buf = DbDirectory::ser(&dir)?;
            self.put(DBDIR_KEY, Value::Image(buf.into()));
        } else {
            warn!(
                "dropped dbdir for spcnode {} dbnode {} did not exist in db directory",
                spcnode, dbnode
            );
        }

        // Delete all relations and metadata files for the spcnode/dnode
        self.delete(dbdir_key_range(spcnode, dbnode));
        Ok(())
    }

    // When a new relish is created:
    // - create/update the directory entry to remember that it exists
    // - create relish header to indicate the size (0)

    // When a relish is extended:
    // - update relish header with new size
    // - insert the block

    // when a relish is truncated:
    // - delete truncated blocks
    // - update relish header with size

    pub fn put_rel_creation(&mut self, rel: RelTag, nblocks: BlockNumber) -> Result<()> {
        info!("CREAT: {}", rel);
        // Add it to the directory entry
        let dir_key = rel_dir_to_key(rel.spcnode, rel.dbnode);
        let buf = self.get(dir_key)?;
        let mut dir = RelDirectory::des(&buf)?;

        if !dir.rels.insert((rel.relnode, rel.forknum)) {
            bail!("rel {} already exists", rel);
        }
        self.put(dir_key, Value::Image(Bytes::from(RelDirectory::ser(&dir)?)));

        // Put size
        let size_key = rel_size_to_key(rel);
        let buf = nblocks.to_le_bytes();
        self.put(size_key, Value::Image(Bytes::from(buf.to_vec())));

        // even if nblocks > 0, we don't insert any actual blocks here

        Ok(())
    }

    /// Truncate relation
    pub fn put_rel_truncation(&mut self, rel: RelTag, nblocks: BlockNumber) -> Result<()> {
        // Put size
        let size_key = rel_size_to_key(rel);
        let buf = nblocks.to_le_bytes();
        self.put(size_key, Value::Image(Bytes::from(buf.to_vec())));
        Ok(())
    }

    pub fn put_slru_segment_creation(
        &mut self,
        kind: SlruKind,
        segno: u32,
        nblocks: BlockNumber,
    ) -> Result<()> {
        // Add it to the directory entry
        let dir_key = slru_dir_to_key(kind);
        let buf = self.get(dir_key)?;
        let mut dir = SlruSegmentDirectory::des(&buf)?;

        if !dir.segments.insert(segno) {
            bail!("slru segment {:?}/{} already exists", kind, segno);
        }
        self.put(
            dir_key,
            Value::Image(Bytes::from(SlruSegmentDirectory::ser(&dir)?)),
        );

        // Put size
        let size_key = slru_segment_size_to_key(kind, segno);
        let buf = nblocks.to_le_bytes();
        self.put(size_key, Value::Image(Bytes::from(buf.to_vec())));

        // even if nblocks > 0, we don't insert any actual blocks here

        Ok(())
    }

    /// Extend SLRU segment
    pub fn put_slru_extend(
        &mut self,
        kind: SlruKind,
        segno: u32,
        nblocks: BlockNumber,
    ) -> Result<()> {
        // Put size
        let size_key = slru_segment_size_to_key(kind, segno);
        let buf = nblocks.to_le_bytes();
        self.put(size_key, Value::Image(Bytes::from(buf.to_vec())));
        Ok(())
    }

    /// Extend relation
    pub fn put_rel_extend(&mut self, rel: RelTag, nblocks: BlockNumber) -> Result<()> {
        // Put size
        let size_key = rel_size_to_key(rel);
        let buf = nblocks.to_le_bytes();
        self.put(size_key, Value::Image(Bytes::from(buf.to_vec())));
        Ok(())
    }

    /// This method is used for marking dropped relations and truncated SLRU files and aborted two phase records
    pub fn put_rel_drop(&mut self, rel: RelTag) -> Result<()> {
        // Remove it from the directory entry
        let dir_key = rel_dir_to_key(rel.spcnode, rel.dbnode);
        let buf = self.get(dir_key)?;
        let mut dir = RelDirectory::des(&buf)?;

        if dir.rels.remove(&(rel.relnode, rel.forknum)) {
            self.put(dir_key, Value::Image(Bytes::from(RelDirectory::ser(&dir)?)));
        } else {
            warn!("dropped rel {} did not exist in rel directory", rel);
        }

        // Delete size entry, as well as all blocks
        self.delete(rel_key_range(rel));

        Ok(())
    }

    /// This method is used for marking dropped relations and truncated SLRU files and aborted two phase records
    pub fn drop_relmap_file(&mut self, _spcnode: Oid, _dbnode: Oid) -> Result<()> {
        // TODO
        Ok(())
    }

    /// This method is used for marking truncated SLRU files
    pub fn drop_slru_segment(&mut self, kind: SlruKind, segno: u32) -> Result<()> {
        // Remove it from the directory entry
        let dir_key = slru_dir_to_key(kind);
        let buf = self.get(dir_key)?;
        let mut dir = SlruSegmentDirectory::des(&buf)?;

        if !dir.segments.remove(&segno) {
            warn!("slru segment {:?}/{} does not exist", kind, segno);
        }
        self.put(
            dir_key,
            Value::Image(Bytes::from(SlruSegmentDirectory::ser(&dir)?)),
        );

        // Delete size entry, as well as all blocks
        self.delete(slru_segment_key_range(kind, segno));

        Ok(())
    }

    /// This method is used for marking truncated SLRU files
    pub fn drop_twophase_file(&mut self, xid: TransactionId) -> Result<()> {
        // Remove it from the directory entry
        let buf = self.get(TWOPHASEDIR_KEY)?;
        let mut dir = TwoPhaseDirectory::des(&buf)?;

        if !dir.xids.remove(&xid) {
            warn!("twophase file for xid {} does not exist", xid);
        }
        self.put(
            TWOPHASEDIR_KEY,
            Value::Image(Bytes::from(TwoPhaseDirectory::ser(&dir)?)),
        );

        // Delete it
        self.delete(twophase_key_range(xid));

        Ok(())
    }

    pub fn finish(self) -> Result<()> {
        let writer = self.tline.tline.writer();

        for (key, value) in self.pending_updates {
            writer.put(key, self.lsn, value)?;
        }
        for key_range in self.pending_deletions {
            writer.delete(key_range, self.lsn)?;
        }

        writer.advance_last_record_lsn(self.lsn);

        Ok(())
    }

    // Internal helper functions to batch the modifications

    fn get(&self, key: Key) -> Result<Bytes> {
        // Note: we don't check pending_deletions. It is an error to request a value
        // that has been removed, deletion only avoids leaking storage.

        if let Some(value) = self.pending_updates.get(&key) {
            if let Value::Image(img) = value {
                Ok(img.clone())
            } else {
                // Currently, we never need to read back a WAL record that we
                // inserted in the same "transaction". All the metadata updates
                // work directly with Images, and we never need to read actual
                // data pages. We could handle this if we had to, by calling
                // the walredo manager, but let's keep it simple for now.
                bail!("unexpected pending WAL record");
            }
        } else {
            let last_lsn = self.tline.get_last_record_lsn();
            self.tline.tline.get(key, last_lsn)
        }
    }

    fn put(&mut self, key: Key, val: Value) {
        self.pending_updates.insert(key, val);
    }

    fn delete(&mut self, key_range: Range<Key>) {
        info!("DELETE {}-{}", key_range.start, key_range.end);
        self.pending_deletions.push(key_range);
    }
}

// Utilities to pack stuff in Key

//
// Key space:
//
// blocky stuff: relations and SLRUs
//
// DbDir    () -> (dbnode, spcnode)
//
//   Filenodemap
//
//   RelDir   -> relnode forknum
//
//       RelBlocks
//
//       RelSize
//
// Slrus
//
// SlruDir  kind
//
//   SlruSegBlocks segno
//
//   SlruSegSize
//
// pg_twophase
//
// controlfile
// checkpoint
//

// DbDir:
// 00 00000000 00000000 00000000 00   00000000
//
// Filenodemap:
// 00 SPCNODE  DBNODE   00000000 00   00000000
//
// RelDir:
// 00 SPCNODE  DBNODE   00000000 00   00000001
//
// RelBlock:
// 00 SPCNODE  DBNODE   RELNODE  FORK BLKNUM
//
// RelSize:
// 00 SPCNODE  DBNODE   RELNODE  FORK FFFFFFFF
//
// SlruDir:
// 01 kind     00000000 00000000 00   00000000
//
// SlruSegBlock:
// 01 kind     00000001 SEGNO    00   BLKNUM
//
// SlruSegSize:
// 01 kind     00000001 SEGNO    00   FFFFFFFF
//
// TwoPhaseDir:
// 02 00000000 00000000 00000000 00   00000000
//
// TwoPhaseFile:
// 02 00000000 00000000 00000000 00   XID
//
// ControlFile:
// 03 00000000 00000000 00000000 00   00000000
//
// Checkpoint:
// 03 00000000 00000000 00000000 00   00000001

const DBDIR_KEY: Key = Key {
    field1: 0x00,
    field2: 0,
    field3: 0,
    field4: 0,
    field5: 0,
    field6: 0,
};

const TWOPHASEDIR_KEY: Key = Key {
    field1: 0x02,
    field2: 0,
    field3: 0,
    field4: 0,
    field5: 0,
    field6: 0,
};

const CONTROLFILE_KEY: Key = Key {
    field1: 0x03,
    field2: 0,
    field3: 0,
    field4: 0,
    field5: 0,
    field6: 0,
};

const CHECKPOINT_KEY: Key = Key {
    field1: 0x03,
    field2: 0,
    field3: 0,
    field4: 0,
    field5: 0,
    field6: 1,
};

pub fn rel_block_to_key(rel: RelTag, blknum: BlockNumber) -> Key {
    Key {
        field1: 0x00,
        field2: rel.spcnode,
        field3: rel.dbnode,
        field4: rel.relnode,
        field5: rel.forknum,
        field6: blknum,
    }
}

pub fn rel_dir_to_key(spcnode: Oid, dbnode: Oid) -> Key {
    Key {
        field1: 0x00,
        field2: spcnode,
        field3: dbnode,
        field4: 0,
        field5: 0,
        field6: 1,
    }
}

pub fn rel_size_to_key(rel: RelTag) -> Key {
    Key {
        field1: 0x00,
        field2: rel.spcnode,
        field3: rel.dbnode,
        field4: rel.relnode,
        field5: rel.forknum,
        field6: 0xffffffff,
    }
}

pub fn slru_dir_to_key(kind: SlruKind) -> Key {
    Key {
        field1: 0x01,
        field2: match kind {
            SlruKind::Clog => 0x00,
            SlruKind::MultiXactMembers => 0x01,
            SlruKind::MultiXactOffsets => 0x02,
        },
        field3: 0,
        field4: 0,
        field5: 0,
        field6: 0,
    }
}

pub fn slru_block_to_key(kind: SlruKind, segno: u32, blknum: BlockNumber) -> Key {
    Key {
        field1: 0x01,
        field2: match kind {
            SlruKind::Clog => 0x00,
            SlruKind::MultiXactMembers => 0x01,
            SlruKind::MultiXactOffsets => 0x02,
        },
        field3: 1,
        field4: segno,
        field5: 0,
        field6: blknum,
    }
}

pub fn rel_key_range(rel: RelTag) -> Range<Key> {
    Key {
        field1: 0x00,
        field2: rel.spcnode,
        field3: rel.dbnode,
        field4: rel.relnode,
        field5: rel.forknum,
        field6: 0,
    }..Key {
        field1: 0x00,
        field2: rel.spcnode,
        field3: rel.dbnode,
        field4: rel.relnode,
        field5: rel.forknum + 1,
        field6: 0,
    }
}

pub fn slru_segment_size_to_key(kind: SlruKind, segno: u32) -> Key {
    Key {
        field1: 0x01,
        field2: match kind {
            SlruKind::Clog => 0x00,
            SlruKind::MultiXactMembers => 0x01,
            SlruKind::MultiXactOffsets => 0x02,
        },
        field3: 1,
        field4: segno,
        field5: 0,
        field6: 0xffffffff,
    }
}

pub fn slru_segment_key_range(kind: SlruKind, segno: u32) -> Range<Key> {
    let field2 = match kind {
        SlruKind::Clog => 0x00,
        SlruKind::MultiXactMembers => 0x01,
        SlruKind::MultiXactOffsets => 0x02,
    };

    Key {
        field1: 0x01,
        field2,
        field3: segno,
        field4: 0,
        field5: 0,
        field6: 0,
    }..Key {
        field1: 0x01,
        field2,
        field3: segno,
        field4: 0,
        field5: 1,
        field6: 0,
    }
}

pub fn relmap_file_key(spcnode: Oid, dbnode: Oid) -> Key {
    Key {
        field1: 0x00,
        field2: spcnode,
        field3: dbnode,
        field4: 0,
        field5: 0,
        field6: 0,
    }
}

pub fn twophase_file_key(xid: TransactionId) -> Key {
    Key {
        field1: 0x02,
        field2: 0,
        field3: 0,
        field4: 0,
        field5: 0,
        field6: xid,
    }
}

pub fn twophase_key_range(xid: TransactionId) -> Range<Key> {
    let (next_xid, overflowed) = xid.overflowing_add(1);

    Key {
        field1: 0x02,
        field2: 0,
        field3: 0,
        field4: 0,
        field5: 0,
        field6: xid,
    }..Key {
        field1: 0x02,
        field2: 0,
        field3: 0,
        field4: 0,
        field5: if overflowed { 1 } else { 0 },
        field6: next_xid,
    }
}

pub fn key_to_rel_block(key: Key) -> Result<(RelTag, BlockNumber)> {
    Ok(match key.field1 {
        0x00 => (
            RelTag {
                spcnode: key.field2,
                dbnode: key.field3,
                relnode: key.field4,
                forknum: key.field5,
            },
            key.field6,
        ),
        _ => bail!("unexpected value kind 0x{:02x}", key.field1),
    })
}

pub fn key_to_slru_block(key: Key) -> Result<(SlruKind, u32, BlockNumber)> {
    Ok(match key.field1 {
        0x01 => {
            let kind = match key.field2 {
                0x00 => SlruKind::Clog,
                0x01 => SlruKind::MultiXactMembers,
                0x02 => SlruKind::MultiXactOffsets,
                _ => bail!("unrecognized slru kind 0x{:02x}", key.field2),
            };
            let segno = key.field4;
            let blknum = key.field6;

            (kind, segno, blknum)
        }
        _ => bail!("unexpected value kind 0x{:02x}", key.field1),
    })
}

pub fn key_to_relish_block(key: Key) -> Result<(RelishTag, BlockNumber)> {
    // FIXME: there's got to be a bitfields crate or something out there to do this for us..

    // This only works for keys for blocks that are handled by WalRedo manager.
    // TODO: assert that the other fields are zero

    Ok(match key.field1 {
        0x00 => (
            RelishTag::Relation(RelTag {
                spcnode: key.field2,
                dbnode: key.field3,
                relnode: key.field4,
                forknum: key.field5,
            }),
            key.field6,
        ),

        0x01 => (
            RelishTag::Slru {
                slru: match key.field2 {
                    0x00 => SlruKind::Clog,
                    0x01 => SlruKind::MultiXactMembers,
                    0x02 => SlruKind::MultiXactOffsets,
                    _ => bail!("unrecognized slru kind 0x{:02x}", key.field2),
                },
                segno: key.field4,
            },
            key.field6,
        ),

        _ => bail!("unrecognized value kind 0x{:02x}", key.field1),
    })
}

pub fn dbdir_key_range(spcnode: Oid, dbnode: Oid) -> Range<Key> {
    Key {
        field1: 0x00,
        field2: spcnode,
        field3: dbnode,
        field4: 0,
        field5: 0,
        field6: 0,
    }..Key {
        field1: 0x00,
        field2: spcnode,
        field3: dbnode,
        field4: 0xffffffff,
        field5: 0xff,
        field6: 0xffffffff,
    }
}

///
/// Tests that should work the same with any Repository/Timeline implementation.
///

#[cfg(test)]
pub fn create_test_timeline<R: Repository>(
    repo: R,
    timeline_id: zenith_utils::zid::ZTimelineId,
) -> Result<Arc<crate::DatadirTimeline<R>>> {
    let tline = repo.create_empty_timeline(timeline_id, Lsn(8))?;
    let tline = DatadirTimeline::new(tline);
    let mut writer = tline.begin_record(Lsn(8));
    writer.init_empty()?;

    writer.put_dbdir_creation(0, 111)?;

    writer.finish()?;
    Ok(Arc::new(tline))
}

#[allow(clippy::bool_assert_comparison)]
#[cfg(test)]
mod tests {
    //use super::repo_harness::*;
    //use super::*;

    /*
        fn assert_current_logical_size<R: Repository>(timeline: &DatadirTimeline<R>, lsn: Lsn) {
            let incremental = timeline.get_current_logical_size();
            let non_incremental = timeline
                .get_current_logical_size_non_incremental(lsn)
                .unwrap();
            assert_eq!(incremental, non_incremental);
        }
    */

    /*
    ///
    /// Test list_rels() function, with branches and dropped relations
    ///
    #[test]
    fn test_list_rels_drop() -> Result<()> {
        let repo = RepoHarness::create("test_list_rels_drop")?.load();
        let tline = create_empty_timeline(repo, TIMELINE_ID)?;
        const TESTDB: u32 = 111;

        // Import initial dummy checkpoint record, otherwise the get_timeline() call
        // after branching fails below
        let mut writer = tline.begin_record(Lsn(0x10));
        writer.put_checkpoint(ZERO_CHECKPOINT.clone())?;
        writer.finish()?;

        // Create a relation on the timeline
        let mut writer = tline.begin_record(Lsn(0x20));
        writer.put_rel_page_image(TESTREL_A, 0, TEST_IMG("foo blk 0 at 2"))?;
        writer.finish()?;

        let writer = tline.begin_record(Lsn(0x00));
        writer.finish()?;

        // Check that list_rels() lists it after LSN 2, but no before it
        assert!(!tline.list_rels(0, TESTDB, Lsn(0x10))?.contains(&TESTREL_A));
        assert!(tline.list_rels(0, TESTDB, Lsn(0x20))?.contains(&TESTREL_A));
        assert!(tline.list_rels(0, TESTDB, Lsn(0x30))?.contains(&TESTREL_A));

        // Create a branch, check that the relation is visible there
        repo.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Lsn(0x30))?;
        let newtline = match repo.get_timeline(NEW_TIMELINE_ID)?.local_timeline() {
            Some(timeline) => timeline,
            None => panic!("Should have a local timeline"),
        };
        let newtline = DatadirTimelineImpl::new(newtline);
        assert!(newtline
            .list_rels(0, TESTDB, Lsn(0x30))?
            .contains(&TESTREL_A));

        // Drop it on the branch
        let mut new_writer = newtline.begin_record(Lsn(0x40));
        new_writer.drop_relation(TESTREL_A)?;
        new_writer.finish()?;

        // Check that it's no longer listed on the branch after the point where it was dropped
        assert!(newtline
            .list_rels(0, TESTDB, Lsn(0x30))?
            .contains(&TESTREL_A));
        assert!(!newtline
            .list_rels(0, TESTDB, Lsn(0x40))?
            .contains(&TESTREL_A));

        // Run checkpoint and garbage collection and check that it's still not visible
        newtline.tline.checkpoint(CheckpointConfig::Forced)?;
        repo.gc_iteration(Some(NEW_TIMELINE_ID), 0, true)?;

        assert!(!newtline
            .list_rels(0, TESTDB, Lsn(0x40))?
            .contains(&TESTREL_A));

        Ok(())
    }
     */

    /*
    #[test]
    fn test_read_beyond_eof() -> Result<()> {
        let repo = RepoHarness::create("test_read_beyond_eof")?.load();
        let tline = create_test_timeline(repo, TIMELINE_ID)?;

        make_some_layers(&tline, Lsn(0x20))?;
        let mut writer = tline.begin_record(Lsn(0x60));
        walingest.put_rel_page_image(
            &mut writer,
            TESTREL_A,
            0,
            TEST_IMG(&format!("foo blk 0 at {}", Lsn(0x60))),
        )?;
        writer.finish()?;

        // Test read before rel creation. Should error out.
        assert!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x10)).is_err());

        // Read block beyond end of relation at different points in time.
        // These reads should fall into different delta, image, and in-memory layers.
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x20))?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x25))?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x30))?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x35))?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x40))?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x45))?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x50))?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x55))?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x60))?, ZERO_PAGE);

        // Test on an in-memory layer with no preceding layer
        let mut writer = tline.begin_record(Lsn(0x70));
        walingest.put_rel_page_image(
            &mut writer,
            TESTREL_B,
            0,
            TEST_IMG(&format!("foo blk 0 at {}", Lsn(0x70))),
        )?;
        writer.finish()?;

        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_B, 1, Lsn(0x70))?, ZERO_PAGE);

        Ok(())
    }
     */
}

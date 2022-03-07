//! An ImageLayer represents an image or a snapshot of a segment at one particular LSN.
//! It is stored in a file on disk.
//!
//! On disk, the image files are stored in timelines/<timelineid> directory.
//! Currently, there are no subdirectories, and each image layer file is named like this:
//!
//!    <key start>-<key end>__<LSN>
//!
//! For example:
//!
//!    000000067F000032BE0000400000000070B6-000000067F000032BE0000400000000080B6__00000000346BC568
//!
//! An image file is constructed using the 'bookfile' crate.
//!
//! Only metadata is loaded into memory by the load function.
//! When images are needed, they are read directly from disk.
//!
use crate::config::PageServerConf;
use crate::layered_repository::filename::{ImageFileName, PathOrConf};
use crate::layered_repository::storage_layer::{
    Layer, ValueReconstructResult, ValueReconstructState,
};
use crate::layered_repository::utils;
use crate::repository::{Key, Value};
use crate::virtual_file::VirtualFile;
use crate::{ZTenantId, ZTimelineId};
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use log::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufWriter, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use bookfile::{Book, BookWriter, ChapterWriter};

use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;

// Magic constant to identify a Zenith image layer file
// FIXME: bump all magics
pub const IMAGE_FILE_MAGIC: u32 = 0x5A616E01 + 1;

/// Mapping from (key, lsn) -> page/WAL record
/// byte ranges in VALUES_CHAPTER
static INDEX_CHAPTER: u64 = 1;

/// Contains each block in block # order
const VALUES_CHAPTER: u64 = 2;

/// Contains the [`Summary`] struct
const SUMMARY_CHAPTER: u64 = 3;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Summary {
    tenantid: ZTenantId,
    timelineid: ZTimelineId,
    key_range: Range<Key>,

    lsn: Lsn,
}

impl From<&ImageLayer> for Summary {
    fn from(layer: &ImageLayer) -> Self {
        Self {
            tenantid: layer.tenantid,
            timelineid: layer.timelineid,
            key_range: layer.key_range.clone(),

            lsn: layer.lsn,
        }
    }
}

///
/// ImageLayer is the in-memory data structure associated with an on-disk image
/// file.  We keep an ImageLayer in memory for each file, in the LayerMap. If a
/// layer is in "loaded" state, we have a copy of the index in memory, in 'inner'.
/// Otherwise the struct is just a placeholder for a file that exists on disk,
/// and it needs to be loaded before using it in queries.
///
pub struct ImageLayer {
    path_or_conf: PathOrConf,
    pub tenantid: ZTenantId,
    pub timelineid: ZTimelineId,
    pub key_range: Range<Key>,

    // This entry contains an image of all pages as of this LSN
    pub lsn: Lsn,

    inner: Mutex<ImageLayerInner>,
}

pub struct ImageLayerInner {
    /// If false, the 'index' has not been loaded into memory yet.
    loaded: bool,

    /// If None, the 'image_type' has not been loaded into memory yet. FIXME
    book: Option<Book<VirtualFile>>,

    /// offset of each value
    index: HashMap<Key, u64>,
}

impl Layer for ImageLayer {
    fn filename(&self) -> PathBuf {
        PathBuf::from(self.layer_name().to_string())
    }

    fn get_tenant_id(&self) -> ZTenantId {
        self.tenantid
    }

    fn get_timeline_id(&self) -> ZTimelineId {
        self.timelineid
    }

    fn get_key_range(&self) -> Range<Key> {
        self.key_range.clone()
    }

    fn get_lsn_range(&self) -> Range<Lsn> {
        // End-bound is exclusive
        self.lsn..(self.lsn + 1)
    }

    /// Look up given page in the file
    fn get_value_reconstruct_data(
        &self,
        lsn_floor: Lsn,
        reconstruct_state: &mut ValueReconstructState,
    ) -> Result<ValueReconstructResult> {
        assert!(lsn_floor <= self.lsn);
        assert!(self.key_range.contains(&reconstruct_state.key));
        assert!(reconstruct_state.lsn >= self.lsn);

        match reconstruct_state.img {
            Some((cached_lsn, _)) if self.lsn <= cached_lsn => {
                reconstruct_state.lsn = cached_lsn;
                return Ok(ValueReconstructResult::Complete);
            }
            _ => {}
        }

        let inner = self.load()?;

        if let Some(offset) = inner.index.get(&reconstruct_state.key) {
            let chapter = inner
                .book
                .as_ref()
                .unwrap()
                .chapter_reader(VALUES_CHAPTER)?;

            let blob = utils::read_blob_from_chapter(&chapter, *offset).with_context(|| {
                format!(
                    "failed to read value from data file {} at offset {}",
                    self.filename().display(),
                    offset
                )
            })?;
            let value = Bytes::from(blob);

            reconstruct_state.img = Some((self.lsn, value));
            reconstruct_state.lsn = self.lsn;
            Ok(ValueReconstructResult::Complete)
        } else {
            reconstruct_state.lsn = self.lsn;
            Ok(ValueReconstructResult::Missing)
        }
    }

    fn collect_keys(&self, key_range: &Range<Key>, keys: &mut HashSet<Key>) -> Result<()> {
        let inner = self.load()?;

        let index = &inner.index;

        keys.extend(index.keys().filter(|x| key_range.contains(x)));
        Ok(())
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Result<(Key, Lsn, Value)>>> {
        todo!();
    }
    
    fn unload(&self) -> Result<()> {
        // TODO: unload 'segs'. Or even better, don't hold it in memory but
        // access it directly from the file (using the buffer cache)
        let mut inner = self.inner.lock().unwrap();
        inner.index = HashMap::default();
        inner.loaded = false;

        Ok(())
    }

    fn delete(&self) -> Result<()> {
        // delete underlying file
        fs::remove_file(self.path())?;
        Ok(())
    }

    fn is_incremental(&self) -> bool {
        false
    }

    fn is_in_memory(&self) -> bool {
        false
    }

    /// debugging function to print out the contents of the layer
    fn dump(&self) -> Result<()> {
        println!(
            "----- image layer for ten {} tli {} key {}-{} at {} ----",
            self.tenantid, self.timelineid, self.key_range.start, self.key_range.end, self.lsn
        );

        let inner = self.load()?;

        let mut index_vec: Vec<(&Key, &u64)> = inner.index.iter().collect();
        index_vec.sort_by_key(|x| x.1);

        for (key, offset) in index_vec {
            println!("key: {} offset {}", key, offset);
        }

        Ok(())
    }
}

impl ImageLayer {
    fn path_for(
        path_or_conf: &PathOrConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        fname: &ImageFileName,
    ) -> PathBuf {
        match path_or_conf {
            PathOrConf::Path(path) => path.to_path_buf(),
            PathOrConf::Conf(conf) => conf
                .timeline_path(&timelineid, &tenantid)
                .join(fname.to_string()),
        }
    }

    ///
    /// Load the contents of the file into memory
    ///
    fn load(&self) -> Result<MutexGuard<ImageLayerInner>> {
        // quick exit if already loaded
        let mut inner = self.inner.lock().unwrap();

        if inner.loaded {
            return Ok(inner);
        }

        let path = self.path();

        // Open the file if it's not open already.
        if inner.book.is_none() {
            let file = VirtualFile::open(&path)
                .with_context(|| format!("Failed to open file '{}'", path.display()))?;
            inner.book = Some(Book::new(file).with_context(|| {
                format!("Failed to open file '{}' as a bookfile", path.display())
            })?);
        }
        let book = inner.book.as_ref().unwrap();

        match &self.path_or_conf {
            PathOrConf::Conf(_) => {
                let chapter = book.read_chapter(SUMMARY_CHAPTER)?;
                let actual_summary = Summary::des(&chapter)?;

                let expected_summary = Summary::from(self);

                if actual_summary != expected_summary {
                    bail!("in-file summary does not match expected summary. actual = {:?} expected = {:?}", actual_summary, expected_summary);
                }
            }
            PathOrConf::Path(path) => {
                let actual_filename = Path::new(path.file_name().unwrap());
                let expected_filename = self.filename();

                if actual_filename != expected_filename {
                    println!(
                        "warning: filename does not match what is expected from in-file summary"
                    );
                    println!("actual: {:?}", actual_filename);
                    println!("expected: {:?}", expected_filename);
                }
            }
        }

        let chapter = book.read_chapter(INDEX_CHAPTER)?;
        let index = HashMap::des(&chapter)?;

        info!("loaded from {}", &path.display());

        inner.index = index;
        inner.loaded = true;

        Ok(inner)
    }

    /// Create an ImageLayer struct representing an existing file on disk
    pub fn new(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        filename: &ImageFileName,
    ) -> ImageLayer {
        ImageLayer {
            path_or_conf: PathOrConf::Conf(conf),
            timelineid,
            tenantid,
            key_range: filename.key_range.clone(),
            lsn: filename.lsn,
            inner: Mutex::new(ImageLayerInner {
                book: None,
                index: HashMap::new(),
                loaded: false,
            }),
        }
    }

    /// Create an ImageLayer struct representing an existing file on disk.
    ///
    /// This variant is only used for debugging purposes, by the 'dump_layerfile' binary.
    pub fn new_for_path<F>(path: &Path, book: &Book<F>) -> Result<ImageLayer>
    where
        F: std::os::unix::prelude::FileExt,
    {
        let chapter = book.read_chapter(SUMMARY_CHAPTER)?;
        let summary = Summary::des(&chapter)?;

        Ok(ImageLayer {
            path_or_conf: PathOrConf::Path(path.to_path_buf()),
            timelineid: summary.timelineid,
            tenantid: summary.tenantid,
            key_range: summary.key_range,
            lsn: summary.lsn,
            inner: Mutex::new(ImageLayerInner {
                book: None,
                index: HashMap::new(),
                loaded: false,
            }),
        })
    }

    fn layer_name(&self) -> ImageFileName {
        ImageFileName {
            key_range: self.key_range.clone(),
            lsn: self.lsn,
        }
    }

    /// Path to the layer file in pageserver workdir.
    pub fn path(&self) -> PathBuf {
        Self::path_for(
            &self.path_or_conf,
            self.timelineid,
            self.tenantid,
            &self.layer_name(),
        )
    }
}

/// A builder object for constructing a new image layer.
///
/// Usage:
///
/// 1. Create the ImageLayerWriter by calling ImageLayerWriter::new(...)
///
/// 2. Write the contents by calling `put_page_image` for every page
///    in the segment.
///
/// 3. Call `finish`.
///
pub struct ImageLayerWriter {
    conf: &'static PageServerConf,
    path: PathBuf,
    timelineid: ZTimelineId,
    tenantid: ZTenantId,
    key_range: Range<Key>,
    lsn: Lsn,

    values_writer: Option<ChapterWriter<BufWriter<VirtualFile>>>,
    end_offset: u64,

    index: HashMap<Key, u64>,

    finished: bool,
}

impl ImageLayerWriter {
    pub fn new(
        conf: &'static PageServerConf,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
        key_range: &Range<Key>,
        lsn: Lsn,
    ) -> Result<ImageLayerWriter> {
        // Create the file
        //
        // Note: This overwrites any existing file. There shouldn't be any.
        // FIXME: throw an error instead?
        let path = ImageLayer::path_for(
            &PathOrConf::Conf(conf),
            timelineid,
            tenantid,
            &ImageFileName {
                key_range: key_range.clone(),
                lsn,
            },
        );
        info!("new image layer {}", path.display());
        let file = VirtualFile::create(&path)?;
        let buf_writer = BufWriter::new(file);
        let book = BookWriter::new(buf_writer, IMAGE_FILE_MAGIC)?;

        // Open the page-images chapter for writing. The calls to
        // `put_image` will use this to write the contents.
        let chapter = book.new_chapter(VALUES_CHAPTER);

        let writer = ImageLayerWriter {
            conf,
            path,
            timelineid,
            tenantid,
            key_range: key_range.clone(),
            lsn,
            values_writer: Some(chapter),
            index: HashMap::new(),
            end_offset: 0,
            finished: false,
        };

        Ok(writer)
    }

    ///
    /// Write next value to the file.
    ///
    /// The page versions must be appended in blknum order.
    ///
    pub fn put_image(&mut self, key: Key, img: &[u8]) -> Result<()> {
        assert!(self.key_range.contains(&key));
        let off = self.end_offset;

        if let Some(writer) = &mut self.values_writer {
            let len = utils::write_blob(writer, img)?;
            self.end_offset += len;

            let old = self.index.insert(key, off);
            assert!(old.is_none());
        } else {
            panic!()
        }

        Ok(())
    }

    pub fn finish(&mut self) -> Result<ImageLayer> {
        // Close the values chapter
        let book = self.values_writer.take().unwrap().close()?;

        // Write out the index
        let mut chapter = book.new_chapter(INDEX_CHAPTER);
        let buf = HashMap::ser(&self.index)?;
        chapter.write_all(&buf)?;
        let book = chapter.close()?;

        // Write out the summary chapter
        let mut chapter = book.new_chapter(SUMMARY_CHAPTER);
        let summary = Summary {
            tenantid: self.tenantid,
            timelineid: self.timelineid,
            key_range: self.key_range.clone(),
            lsn: self.lsn,
        };
        Summary::ser_into(&summary, &mut chapter)?;
        let book = chapter.close()?;

        // This flushes the underlying 'buf_writer'.
        book.close()?;

        // Note: Because we open the file in write-only mode, we cannot
        // reuse the same VirtualFile for reading later. That's why we don't
        // set inner.book here. The first read will have to re-open it.
        let layer = ImageLayer {
            path_or_conf: PathOrConf::Conf(self.conf),
            timelineid: self.timelineid,
            tenantid: self.tenantid,
            key_range: self.key_range.clone(),
            lsn: self.lsn,
            inner: Mutex::new(ImageLayerInner {
                book: None,
                loaded: false,
                index: HashMap::new(),
            }),
        };
        trace!("created image layer {}", layer.path().display());

        self.finished = true;

        Ok(layer)
    }
}

impl Drop for ImageLayerWriter {
    fn drop(&mut self) {
        if let Some(page_image_writer) = self.values_writer.take() {
            if let Ok(book) = page_image_writer.close() {
                let _ = book.close();
            }
        }
        if !self.finished {
            let _ = fs::remove_file(&self.path);
        }
    }
}

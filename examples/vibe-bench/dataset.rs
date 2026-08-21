//! VIBE dataset loading: download from Hugging Face and read the HDF5
//! layout the benchmark publishes.
//!
//! A VIBE dataset file carries root attributes `distance`, `dimension` and
//! `point_type`, and contiguous (uncompressed) datasets `train` (N x dim
//! float32), `test` (M x dim float32), `neighbors` (M x G integer row
//! indices into `train`, sorted by distance) and `distances` (M x G
//! float32). Only `point_type = "float"` files are supported here.
//!
//! turbovec's score is an (unbiased, quantized) inner product: the encoder
//! stores each row as a unit direction plus its norm and the kernel folds
//! the norm back in. That maps onto the VIBE `distance` attribute as:
//!
//! - `ip` / `normalized`: inner product directly (`normalized` datasets
//!   are unit-norm, so inner product is cosine).
//! - `cosine`: both sides are L2-normalized at load, then inner product.
//! - `euclidean` / `hamming`: not expressible as an inner product without
//!   changing the ranking, so loading fails by name rather than silently
//!   benchmarking the wrong metric.

use std::fs;
use std::path::{Path, PathBuf};

use hidefix::idx::DatasetD;
use hidefix::prelude::*;

/// Base URL the VIBE project publishes its precomputed datasets under.
const DATASET_URL: &str = "https://huggingface.co/datasets/vector-index-bench/vibe/resolve/main";

/// A loaded VIBE dataset, ready for benchmarking.
pub struct Dataset {
    /// Dataset name (the `{name}.hdf5` file stem).
    pub name: String,
    /// The `distance` attribute, as published.
    pub distance: String,
    /// Vector dimensionality, from the `train` shape.
    pub dim: usize,
    /// Train rows, row-major, `n_train * dim`. L2-normalized when
    /// `distance == "cosine"`.
    pub train: Vec<f32>,
    /// Number of train rows.
    pub n_train: usize,
    /// Query rows, row-major, `n_queries * dim`. Normalized like `train`.
    pub queries: Vec<f32>,
    /// Ground-truth neighbour row indices, row-major `n_queries * gt_depth`.
    pub neighbors: Vec<i64>,
    /// Ground-truth depth per query (G above, 100 in the published files).
    pub gt_depth: usize,
}

/// Download `{name}.hdf5` into `cache_dir` when absent and return its path.
///
/// The download goes to a `.part` sibling first and is renamed into place,
/// so an interrupted run never leaves a truncated file behind under the
/// final name.
pub fn ensure_cached(name: &str, cache_dir: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if name.contains('/') || name.contains('\\') || name.is_empty() {
        return Err(format!("invalid dataset name {name:?}: expected a bare file stem").into());
    }
    fs::create_dir_all(cache_dir)?;
    let path = cache_dir.join(format!("{name}.hdf5"));
    if path.exists() {
        return Ok(path);
    }
    let url = format!("{DATASET_URL}/{name}.hdf5");
    let part = cache_dir.join(format!("{name}.hdf5.part"));
    println!("downloading {url}");
    println!("        -> {}", path.display());
    let mut response = ureq::get(&url).call()?;
    let mut reader = response.body_mut().as_reader();
    {
        let mut file = fs::File::create(&part)?;
        std::io::copy(&mut reader, &mut file)?;
    }
    fs::rename(&part, &path)?;
    Ok(path)
}

/// Read one string-valued root attribute.
fn string_attr(file: &hdf5::File, name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let attr = file
        .attr(name)
        .map_err(|e| format!("missing root attribute {name:?}: {e}"))?;
    let value = attr
        .read_scalar::<hdf5::types::VarLenUnicode>()
        .map_err(|e| format!("root attribute {name:?} is not a string: {e}"))?;
    Ok(value.to_string())
}

/// Read a whole 2D float32 dataset, returning (rows, cols, values).
fn read_f32_2d(
    idx: &hidefix::idx::Index,
    name: &str,
) -> Result<(usize, usize, Vec<f32>), Box<dyn std::error::Error>> {
    let ds = idx
        .dataset(name)
        .ok_or_else(|| format!("dataset {name:?} not found in file"))?;
    let DatasetD::D2(d2) = ds else {
        return Err(format!("dataset {name:?} is not 2-dimensional").into());
    };
    if d2.dtype != Datatype::Float(4) {
        return Err(format!("dataset {name:?} is {:?}, expected float32", d2.dtype).into());
    }
    let rows = d2.shape[0] as usize;
    let cols = d2.shape[1] as usize;
    let mut reader = idx.reader(name)?;
    let values: Vec<f32> = reader.values(..)?;
    if values.len() != rows * cols {
        return Err(format!(
            "dataset {name:?}: read {} values, expected {rows} x {cols}",
            values.len()
        )
        .into());
    }
    Ok((rows, cols, values))
}

/// Read the ground-truth `neighbors` dataset as i64 row indices. The
/// published files store it as int32 or int64 depending on the generator.
fn read_neighbors(
    idx: &hidefix::idx::Index,
) -> Result<(usize, usize, Vec<i64>), Box<dyn std::error::Error>> {
    let ds = idx
        .dataset("neighbors")
        .ok_or("dataset \"neighbors\" not found in file")?;
    let DatasetD::D2(d2) = ds else {
        return Err("dataset \"neighbors\" is not 2-dimensional".into());
    };
    let rows = d2.shape[0] as usize;
    let cols = d2.shape[1] as usize;
    let mut reader = idx.reader("neighbors")?;
    let values: Vec<i64> = match d2.dtype {
        Datatype::Int(4) => reader
            .values::<i32, _>(..)?
            .into_iter()
            .map(i64::from)
            .collect(),
        Datatype::Int(8) => reader.values::<i64, _>(..)?,
        other => {
            return Err(
                format!("dataset \"neighbors\" is {other:?}, expected int32 or int64").into(),
            )
        }
    };
    Ok((rows, cols, values))
}

/// L2-normalize every row in place. Zero-norm rows are left as-is; they
/// score 0 against everything, matching turbovec's own convention.
fn normalize_rows(values: &mut [f32], dim: usize) {
    for row in values.chunks_exact_mut(dim) {
        let norm = row.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 1e-10 {
            for x in row.iter_mut() {
                *x /= norm;
            }
        }
    }
}

/// Load a VIBE dataset file, applying the metric mapping described in the
/// module docs.
pub fn load(name: &str, path: &Path) -> Result<Dataset, Box<dyn std::error::Error>> {
    let file = hdf5::File::open(path)?;
    let distance = string_attr(&file, "distance")?;
    let point_type = string_attr(&file, "point_type")?;
    if point_type != "float" {
        return Err(format!(
            "dataset {name} has point_type {point_type:?}; only \"float\" datasets are supported"
        )
        .into());
    }
    let normalize = match distance.as_str() {
        "ip" | "normalized" => false,
        "cosine" => true,
        other => {
            return Err(format!(
                "dataset {name} uses distance {other:?}; turbovec scores inner product, so only \
                 \"ip\", \"normalized\" and \"cosine\" ground truth can be reproduced \
                 (\"cosine\" runs on L2-normalized rows)"
            )
            .into())
        }
    };
    drop(file);

    let idx = hidefix::idx::Index::index(path)?;
    let (n_train, dim, mut train) = read_f32_2d(&idx, "train")?;
    let (n_queries, qdim, mut queries) = read_f32_2d(&idx, "test")?;
    if qdim != dim {
        return Err(format!("train dim {dim} and test dim {qdim} disagree").into());
    }
    let (gt_rows, gt_depth, neighbors) = read_neighbors(&idx)?;
    if gt_rows != n_queries {
        return Err(format!("test has {n_queries} rows but neighbors has {gt_rows}").into());
    }
    if normalize {
        println!("distance \"cosine\": L2-normalizing train and test rows");
        normalize_rows(&mut train, dim);
        normalize_rows(&mut queries, dim);
    }
    Ok(Dataset {
        name: name.to_string(),
        distance,
        dim,
        train,
        n_train,
        queries,
        neighbors,
        gt_depth,
    })
}

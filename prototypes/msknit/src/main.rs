mod msp;
mod network;
mod search;
mod similarity;
mod spectrum;

use clap::{Parser, Subcommand};
use log::info;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "msknit", about = "Molecular networking and spectral library search")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Build a molecular similarity network from MSP spectra
    Network {
        /// One or more MSP files
        #[arg(short, long, required = true, num_args = 1..)]
        input: Vec<PathBuf>,

        /// Output directory
        #[arg(short, long, default_value = ".")]
        outdir: PathBuf,

        /// Fragment m/z tolerance in Da
        #[arg(long, default_value_t = 0.02)]
        tolerance: f64,

        /// Minimum cosine similarity
        #[arg(long, default_value_t = 0.7)]
        min_cosine: f64,

        /// Minimum matched peaks
        #[arg(long, default_value_t = 4)]
        min_matched: usize,

        /// Max edges per node
        #[arg(long, default_value_t = 10)]
        top_k: usize,

        /// Similarity method: "modified" or "simple"
        #[arg(long, default_value = "modified")]
        similarity: String,

        /// Apply sqrt transform to intensities
        #[arg(long)]
        sqrt_transform: bool,

        /// Remove peaks below this fraction of base peak
        #[arg(long, default_value_t = 0.01)]
        noise_filter: f64,

        /// Write GraphML output
        #[arg(long)]
        graphml: bool,

        /// Number of threads (0 = all available)
        #[arg(long, default_value_t = 0)]
        threads: usize,

        /// Verbose logging
        #[arg(short, long)]
        verbose: bool,
    },

    /// Search query spectra against a reference library
    Search {
        /// Query MSP file(s) (experimental spectra)
        #[arg(short, long, required = true, num_args = 1..)]
        query: Vec<PathBuf>,

        /// Library MSP file(s) (reference spectra)
        #[arg(short, long, required = true, num_args = 1..)]
        library: Vec<PathBuf>,

        /// Output directory
        #[arg(short, long, default_value = ".")]
        outdir: PathBuf,

        /// Fragment m/z tolerance in Da
        #[arg(long, default_value_t = 0.02)]
        tolerance: f64,

        /// Precursor m/z tolerance in Da (0 = unlimited)
        #[arg(long, default_value_t = 0.5)]
        precursor_tol: f64,

        /// Minimum cosine similarity
        #[arg(long, default_value_t = 0.7)]
        min_cosine: f64,

        /// Minimum matched peaks
        #[arg(long, default_value_t = 4)]
        min_matched: usize,

        /// Top-K hits per query
        #[arg(long, default_value_t = 5)]
        top_k: usize,

        /// Similarity method: "modified" or "simple"
        #[arg(long, default_value = "modified")]
        similarity: String,

        /// Apply sqrt transform to intensities
        #[arg(long)]
        sqrt_transform: bool,

        /// Remove peaks below this fraction of base peak
        #[arg(long, default_value_t = 0.01)]
        noise_filter: f64,

        /// Number of threads (0 = all available)
        #[arg(long, default_value_t = 0)]
        threads: usize,

        /// Verbose logging
        #[arg(short, long)]
        verbose: bool,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Network {
            input,
            outdir,
            tolerance,
            min_cosine,
            min_matched,
            top_k,
            similarity,
            sqrt_transform,
            noise_filter,
            graphml,
            threads,
            verbose,
        } => run_network(
            input,
            outdir,
            tolerance,
            min_cosine,
            min_matched,
            top_k,
            similarity,
            sqrt_transform,
            noise_filter,
            graphml,
            threads,
            verbose,
        ),
        Commands::Search {
            query,
            library,
            outdir,
            tolerance,
            precursor_tol,
            min_cosine,
            min_matched,
            top_k,
            similarity,
            sqrt_transform,
            noise_filter,
            threads,
            verbose,
        } => run_search(
            query,
            library,
            outdir,
            tolerance,
            precursor_tol,
            min_cosine,
            min_matched,
            top_k,
            similarity,
            sqrt_transform,
            noise_filter,
            threads,
            verbose,
        ),
    }
}

fn run_network(
    input: Vec<PathBuf>,
    outdir: PathBuf,
    tolerance: f64,
    min_cosine: f64,
    min_matched: usize,
    top_k: usize,
    similarity: String,
    sqrt_transform: bool,
    noise_filter: f64,
    graphml: bool,
    threads: usize,
    verbose: bool,
) {
    init_logging(verbose);
    init_threads(threads);

    let mut spectra = load_spectra(&input);
    preprocess(&mut spectra, noise_filter, sqrt_transform);

    let n = spectra.len();
    let use_modified = similarity == "modified";
    eprintln!(
        "Computing pairwise {} cosine (tolerance={} Da, min_cosine={}, min_matched={})...",
        if use_modified { "modified" } else { "simple" },
        tolerance, min_cosine, min_matched
    );

    let mut edges = network::compute_edges(&spectra, tolerance, min_cosine, min_matched, use_modified);
    eprintln!("Found {} edges before top-K filtering", edges.len());

    network::top_k_filter(&mut edges, n, top_k);
    eprintln!("Retained {} edges after top-{} filtering", edges.len(), top_k);

    let components = network::connected_components(n, &edges);
    let n_components = components.iter().filter(|&&c| c >= 0).collect::<std::collections::HashSet<_>>().len();
    let n_singletons = components.iter().filter(|&&c| c == -1).count();
    eprintln!("{} connected components, {} singletons", n_components, n_singletons);

    let nodes = network::build_node_attributes(&spectra, &components);

    std::fs::create_dir_all(&outdir).expect("Failed to create output directory");
    network::write_edges_csv(&edges, &outdir.join("edges.csv"));
    eprintln!("Wrote {}", outdir.join("edges.csv").display());
    network::write_nodes_csv(&nodes, &outdir.join("nodes.csv"));
    eprintln!("Wrote {}", outdir.join("nodes.csv").display());

    if graphml {
        network::write_graphml(&nodes, &edges, &outdir.join("network.graphml"));
        eprintln!("Wrote {}", outdir.join("network.graphml").display());
    }
}

fn run_search(
    query_paths: Vec<PathBuf>,
    library_paths: Vec<PathBuf>,
    outdir: PathBuf,
    tolerance: f64,
    precursor_tol: f64,
    min_cosine: f64,
    min_matched: usize,
    top_k: usize,
    similarity: String,
    sqrt_transform: bool,
    noise_filter: f64,
    threads: usize,
    verbose: bool,
) {
    init_logging(verbose);
    init_threads(threads);

    let mut queries = load_spectra(&query_paths);
    let mut library = load_spectra(&library_paths);
    preprocess(&mut queries, noise_filter, sqrt_transform);
    preprocess(&mut library, noise_filter, sqrt_transform);

    eprintln!("Loaded {} query spectra, {} library spectra", queries.len(), library.len());

    let use_modified = similarity == "modified";
    eprintln!(
        "Searching with {} cosine (frag_tol={} Da, prec_tol={} Da, min_cosine={}, min_matched={})...",
        if use_modified { "modified" } else { "simple" },
        tolerance, precursor_tol, min_cosine, min_matched
    );

    let hits = search::search_library(
        &queries, &library, tolerance, min_cosine, min_matched, top_k, use_modified, precursor_tol,
    );

    let n_matched = hits.iter().map(|h| h.query_index).collect::<std::collections::HashSet<_>>().len();
    eprintln!("{} hits for {}/{} queries", hits.len(), n_matched, queries.len());

    std::fs::create_dir_all(&outdir).expect("Failed to create output directory");
    let hits_path = outdir.join("hits.csv");
    search::write_hits_csv(&hits, &hits_path);
    eprintln!("Wrote {}", hits_path.display());
}

fn init_logging(verbose: bool) {
    env_logger::Builder::new()
        .filter_level(if verbose { log::LevelFilter::Info } else { log::LevelFilter::Warn })
        .init();
}

fn init_threads(threads: usize) {
    if threads > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build_global()
            .expect("Failed to set thread pool size");
    }
}

fn load_spectra(paths: &[PathBuf]) -> Vec<spectrum::Spectrum> {
    let mut spectra = Vec::new();
    for path in paths {
        info!("Parsing {}", path.display());
        let mut batch = msp::parse_msp(path, spectra.len());
        info!("  {} spectra", batch.len());
        spectra.append(&mut batch);
    }
    let n = spectra.len();
    let n_pos = spectra.iter().filter(|s| s.ion_mode == "Positive").count();
    let n_neg = spectra.iter().filter(|s| s.ion_mode == "Negative").count();
    eprintln!("Loaded {} spectra ({} positive, {} negative)", n, n_pos, n_neg);
    spectra
}

fn preprocess(spectra: &mut [spectrum::Spectrum], noise_filter: f64, sqrt_transform: bool) {
    for s in spectra.iter_mut() {
        s.filter_noise(noise_filter);
        if sqrt_transform {
            s.sqrt_transform();
        }
    }
}

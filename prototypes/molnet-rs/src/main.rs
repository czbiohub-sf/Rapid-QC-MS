mod msp;
mod network;
mod similarity;
mod spectrum;

use clap::Parser;
use log::info;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "molnet", about = "Molecular networking from MSP spectral data")]
struct Cli {
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
}

fn main() {
    let cli = Cli::parse();

    env_logger::Builder::new()
        .filter_level(if cli.verbose {
            log::LevelFilter::Info
        } else {
            log::LevelFilter::Warn
        })
        .init();

    // Configure thread pool
    if cli.threads > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(cli.threads)
            .build_global()
            .expect("Failed to set thread pool size");
    }

    // Parse all MSP files
    let mut spectra = Vec::new();
    for path in &cli.input {
        info!("Parsing {}", path.display());
        let mut batch = msp::parse_msp(path, spectra.len());
        info!("  {} spectra", batch.len());
        spectra.append(&mut batch);
    }

    let n = spectra.len();
    let n_pos = spectra.iter().filter(|s| s.ion_mode == "Positive").count();
    let n_neg = spectra.iter().filter(|s| s.ion_mode == "Negative").count();
    eprintln!(
        "Loaded {} spectra ({} positive, {} negative)",
        n, n_pos, n_neg
    );

    // Preprocess
    for s in &mut spectra {
        s.filter_noise(cli.noise_filter);
        if cli.sqrt_transform {
            s.sqrt_transform();
        }
    }

    // Compute pairwise similarities
    let use_modified = cli.similarity == "modified";
    eprintln!(
        "Computing pairwise {} cosine (tolerance={} Da, min_cosine={}, min_matched={})...",
        if use_modified { "modified" } else { "simple" },
        cli.tolerance,
        cli.min_cosine,
        cli.min_matched
    );

    let mut edges = network::compute_edges(
        &spectra,
        cli.tolerance,
        cli.min_cosine,
        cli.min_matched,
        use_modified,
    );

    eprintln!("Found {} edges before top-K filtering", edges.len());

    // Top-K filter
    network::top_k_filter(&mut edges, n, cli.top_k);
    eprintln!("Retained {} edges after top-{} filtering", edges.len(), cli.top_k);

    // Connected components
    let components = network::connected_components(n, &edges);
    let n_components = components.iter().filter(|&&c| c >= 0).collect::<std::collections::HashSet<_>>().len();
    let n_singletons = components.iter().filter(|&&c| c == -1).count();
    eprintln!(
        "{} connected components, {} singletons",
        n_components, n_singletons
    );

    // Build node attributes
    let nodes = network::build_node_attributes(&spectra, &components);

    // Write output
    std::fs::create_dir_all(&cli.outdir).expect("Failed to create output directory");

    let edges_path = cli.outdir.join("edges.csv");
    network::write_edges_csv(&edges, &edges_path);
    eprintln!("Wrote {}", edges_path.display());

    let nodes_path = cli.outdir.join("nodes.csv");
    network::write_nodes_csv(&nodes, &nodes_path);
    eprintln!("Wrote {}", nodes_path.display());

    if cli.graphml {
        let graphml_path = cli.outdir.join("network.graphml");
        network::write_graphml(&nodes, &edges, &graphml_path);
        eprintln!("Wrote {}", graphml_path.display());
    }
}

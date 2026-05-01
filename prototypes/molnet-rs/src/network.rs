use crate::similarity::{modified_cosine, simple_cosine, SimilarityResult};
use crate::spectrum::Spectrum;
use rayon::prelude::*;
use serde::Serialize;
use std::io::Write;
use std::path::Path;

#[derive(Debug, Clone, Serialize)]
pub struct Edge {
    pub source: usize,
    pub target: usize,
    pub cosine_score: f64,
    pub matched_peaks: usize,
    pub precursor_mz_diff: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeAttributes {
    pub node_id: usize,
    pub precursor_mz: f64,
    pub rt: f64,
    pub polarity: String,
    pub adduct: String,
    pub name: String,
    pub alignment_id: String,
    pub component_id: i64,
}

/// Compute all pairwise similarities above threshold.
pub fn compute_edges(
    spectra: &[Spectrum],
    tolerance: f64,
    min_cosine: f64,
    min_matched: usize,
    use_modified: bool,
) -> Vec<Edge> {
    let n = spectra.len();

    (0..n)
        .into_par_iter()
        .flat_map(|i| {
            let mut local_edges = Vec::new();
            for j in (i + 1)..n {
                // Skip cross-polarity pairs
                if !spectra[i].ion_mode.is_empty()
                    && !spectra[j].ion_mode.is_empty()
                    && spectra[i].ion_mode != spectra[j].ion_mode
                {
                    continue;
                }

                // Skip spectra with too few peaks
                if spectra[i].peaks.len() < min_matched || spectra[j].peaks.len() < min_matched {
                    continue;
                }

                let result: SimilarityResult = if use_modified {
                    modified_cosine(&spectra[i], &spectra[j], tolerance)
                } else {
                    simple_cosine(&spectra[i], &spectra[j], tolerance)
                };

                if result.score >= min_cosine && result.matched_peaks >= min_matched {
                    local_edges.push(Edge {
                        source: i,
                        target: j,
                        cosine_score: result.score,
                        matched_peaks: result.matched_peaks,
                        precursor_mz_diff: (spectra[i].precursor_mz - spectra[j].precursor_mz)
                            .abs(),
                    });
                }
            }
            local_edges
        })
        .collect()
}

/// Keep only the top-K edges per node by cosine score.
pub fn top_k_filter(edges: &mut Vec<Edge>, n_nodes: usize, top_k: usize) {
    // Count edges per node, keep top-K for each
    let mut node_edges: Vec<Vec<usize>> = vec![Vec::new(); n_nodes];
    for (idx, e) in edges.iter().enumerate() {
        node_edges[e.source].push(idx);
        node_edges[e.target].push(idx);
    }

    let mut keep = vec![false; edges.len()];
    for node_list in &mut node_edges {
        node_list.sort_by(|&a, &b| {
            edges[b]
                .cosine_score
                .partial_cmp(&edges[a].cosine_score)
                .unwrap()
        });
        for &idx in node_list.iter().take(top_k) {
            keep[idx] = true;
        }
    }

    let mut i = 0;
    edges.retain(|_| {
        let k = keep[i];
        i += 1;
        k
    });
}

/// Assign connected component IDs using union-find. Singletons get -1.
pub fn connected_components(n_nodes: usize, edges: &[Edge]) -> Vec<i64> {
    let mut parent: Vec<usize> = (0..n_nodes).collect();

    fn find(parent: &mut [usize], x: usize) -> usize {
        if parent[x] != x {
            parent[x] = find(parent, parent[x]);
        }
        parent[x]
    }

    fn union(parent: &mut [usize], a: usize, b: usize) {
        let ra = find(parent, a);
        let rb = find(parent, b);
        if ra != rb {
            parent[ra] = rb;
        }
    }

    for e in edges {
        union(&mut parent, e.source, e.target);
    }

    // Compress paths
    for i in 0..n_nodes {
        find(&mut parent, i);
    }

    // Map root → component_id
    let mut root_to_id: std::collections::HashMap<usize, i64> = std::collections::HashMap::new();
    let mut next_id: i64 = 0;

    // Count members per root
    let mut root_count: std::collections::HashMap<usize, usize> =
        std::collections::HashMap::new();
    for i in 0..n_nodes {
        *root_count.entry(parent[i]).or_insert(0) += 1;
    }

    let mut components = vec![-1i64; n_nodes];
    for i in 0..n_nodes {
        let root = parent[i];
        if root_count[&root] == 1 {
            // Singleton
            components[i] = -1;
        } else {
            let id = root_to_id.entry(root).or_insert_with(|| {
                let id = next_id;
                next_id += 1;
                id
            });
            components[i] = *id;
        }
    }

    components
}

/// Write edges to CSV.
pub fn write_edges_csv(edges: &[Edge], path: &Path) {
    let mut wtr = csv::Writer::from_path(path).expect("Failed to create edges CSV");
    for e in edges {
        wtr.serialize(e).expect("Failed to write edge");
    }
    wtr.flush().expect("Failed to flush edges CSV");
}

/// Write node attributes to CSV.
pub fn write_nodes_csv(attrs: &[NodeAttributes], path: &Path) {
    let mut wtr = csv::Writer::from_path(path).expect("Failed to create nodes CSV");
    for a in attrs {
        wtr.serialize(a).expect("Failed to write node");
    }
    wtr.flush().expect("Failed to flush nodes CSV");
}

/// Build node attributes from spectra and component assignments.
pub fn build_node_attributes(spectra: &[Spectrum], components: &[i64]) -> Vec<NodeAttributes> {
    spectra
        .iter()
        .enumerate()
        .map(|(i, s)| NodeAttributes {
            node_id: i,
            precursor_mz: s.precursor_mz,
            rt: s.retention_time,
            polarity: s.ion_mode.clone(),
            adduct: s.precursor_type.clone(),
            name: s.name.clone(),
            alignment_id: s.alignment_id.clone(),
            component_id: components[i],
        })
        .collect()
}

/// Write a minimal GraphML file.
pub fn write_graphml(nodes: &[NodeAttributes], edges: &[Edge], path: &Path) {
    let mut f = std::fs::File::create(path).expect("Failed to create GraphML file");

    writeln!(f, r#"<?xml version="1.0" encoding="UTF-8"?>"#).unwrap();
    writeln!(
        f,
        r#"<graphml xmlns="http://graphml.graphstruct.org/graphml">"#
    )
    .unwrap();

    // Attribute declarations
    writeln!(f, r#"  <key id="mz" for="node" attr.name="precursor_mz" attr.type="double"/>"#)
        .unwrap();
    writeln!(f, r#"  <key id="rt" for="node" attr.name="rt" attr.type="double"/>"#).unwrap();
    writeln!(
        f,
        r#"  <key id="pol" for="node" attr.name="polarity" attr.type="string"/>"#
    )
    .unwrap();
    writeln!(
        f,
        r#"  <key id="adduct" for="node" attr.name="adduct" attr.type="string"/>"#
    )
    .unwrap();
    writeln!(
        f,
        r#"  <key id="name" for="node" attr.name="name" attr.type="string"/>"#
    )
    .unwrap();
    writeln!(
        f,
        r#"  <key id="aid" for="node" attr.name="alignment_id" attr.type="string"/>"#
    )
    .unwrap();
    writeln!(f, r#"  <key id="comp" for="node" attr.name="component_id" attr.type="long"/>"#)
        .unwrap();
    writeln!(
        f,
        r#"  <key id="cos" for="edge" attr.name="cosine_score" attr.type="double"/>"#
    )
    .unwrap();
    writeln!(
        f,
        r#"  <key id="mp" for="edge" attr.name="matched_peaks" attr.type="int"/>"#
    )
    .unwrap();

    writeln!(f, r#"  <graph id="G" edgedefault="undirected">"#).unwrap();

    for n in nodes {
        writeln!(f, r#"    <node id="n{}">"#, n.node_id).unwrap();
        writeln!(f, r#"      <data key="mz">{}</data>"#, n.precursor_mz).unwrap();
        writeln!(f, r#"      <data key="rt">{}</data>"#, n.rt).unwrap();
        writeln!(f, r#"      <data key="pol">{}</data>"#, n.polarity).unwrap();
        writeln!(f, r#"      <data key="adduct">{}</data>"#, xml_escape(&n.adduct)).unwrap();
        writeln!(f, r#"      <data key="name">{}</data>"#, xml_escape(&n.name)).unwrap();
        writeln!(f, r#"      <data key="aid">{}</data>"#, n.alignment_id).unwrap();
        writeln!(f, r#"      <data key="comp">{}</data>"#, n.component_id).unwrap();
        writeln!(f, "    </node>").unwrap();
    }

    for (i, e) in edges.iter().enumerate() {
        writeln!(
            f,
            r#"    <edge id="e{}" source="n{}" target="n{}">"#,
            i, e.source, e.target
        )
        .unwrap();
        writeln!(f, r#"      <data key="cos">{:.6}</data>"#, e.cosine_score).unwrap();
        writeln!(f, r#"      <data key="mp">{}</data>"#, e.matched_peaks).unwrap();
        writeln!(f, "    </edge>").unwrap();
    }

    writeln!(f, "  </graph>").unwrap();
    writeln!(f, "</graphml>").unwrap();
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

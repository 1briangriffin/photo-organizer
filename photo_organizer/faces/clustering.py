"""
Era-based face clustering.

Groups face embeddings into identity clusters within temporal eras using
HDBSCAN. Supports both standard time-based windows and birth-date-driven
developmental windows for children (faces change fastest in early years, so
those get tighter windows).

Clusters and memberships are proposals: rows land in face_clusters /
face_cluster_members with status='proposed' plus reviewable
face_cluster_assign run_actions. Re-clustering supersedes previous proposed
clusters for the model; accepted clusters are never touched.
"""
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import numpy as np

from ..database.db import DBManager
from ..database.ops import DBOperations
from . import config
from .db_ops import FaceDBOperations

# Injectable clustering backend: takes an (N, D) embedding matrix, returns
# (labels, probabilities-or-None). Label -1 is noise.
ClusterFn = Callable[[np.ndarray], tuple[np.ndarray, Optional[np.ndarray]]]


def compute_standard_eras(min_date: datetime, max_date: datetime,
                          era_size_years: float = config.DEFAULT_ERA_SIZE_YEARS,
                          ) -> list[tuple[datetime, datetime]]:
    """
    Partition a date range into overlapping time-based eras.

    Uses 50% overlap so faces near boundaries appear in two windows,
    preventing artificial identity splits.

    Returns:
        List of (start_date, end_date) tuples — always at least one, even
        when every face shares a single capture timestamp.
    """
    if era_size_years <= 0:
        raise ValueError(
            f"era_size_years must be positive, got {era_size_years!r} "
            f"(a zero step is an infinite loop)")
    window = timedelta(days=int(era_size_years * 365))
    step = timedelta(days=int(era_size_years * 365 * config.ERA_OVERLAP_FRACTION))

    eras = []
    current = min_date
    # `or not eras`: min_date == max_date (single-timestamp collection)
    # must still yield one window instead of silently clustering nothing.
    while current < max_date or not eras:
        end = min(current + window, max_date + timedelta(days=1))
        eras.append((current, end))
        current += step

    return eras


def compute_child_eras(birth_date: datetime,
                       min_date: datetime, max_date: datetime,
                       boundaries: list[int] = config.CHILD_ERA_BOUNDARIES,
                       ) -> list[tuple[datetime, datetime]]:
    """
    Generate developmental era windows for a child based on birth date.

    Creates tighter clustering windows during the years when faces
    change fastest (infancy, toddlerhood, childhood).

    Args:
        birth_date: The child's date of birth.
        min_date: Earliest photo date in the collection.
        max_date: Latest photo date in the collection.
        boundaries: Age boundaries in years (e.g., [0, 2, 5, 10, 15]).

    Returns:
        List of (start_date, end_date) tuples for this child's eras, each
        never wider than DEFAULT_ERA_SIZE_YEARS: a boundary-to-boundary
        span wider than that (5-10 and 10-15 are each 5 years — WIDER than
        the 2.5-year standard window, the opposite of "tighter") is
        subdivided with the same overlap fraction standard eras use,
        instead of clustering the whole span as one window. A 5-year
        window swept in every face captured across those calendar years
        regardless of whose developmental stage it targeted — including
        other children's entire infancies — which is how unrelated babies
        ended up chained into one cluster on the real catalog.
    """
    eras = []
    for i in range(len(boundaries) - 1):
        start_age = boundaries[i]
        end_age = boundaries[i + 1]

        era_start = birth_date + timedelta(days=int(start_age * 365.25))
        era_end = birth_date + timedelta(days=int(end_age * 365.25))

        # Only include eras that overlap with the photo collection
        if era_end < min_date or era_start > max_date:
            continue

        era_start = max(era_start, min_date)
        era_end = min(era_end, max_date + timedelta(days=1))

        span_years = end_age - start_age
        if span_years > config.DEFAULT_ERA_SIZE_YEARS:
            eras.extend(compute_standard_eras(
                era_start, era_end,
                era_size_years=config.DEFAULT_ERA_SIZE_YEARS))
        else:
            eras.append((era_start, era_end))

    # Deliberately NO open-ended era beyond the last boundary: standard
    # windows already cover adult dates, and a birth+15y..end-of-collection
    # window spans decades — wide enough for HDBSCAN to chain unrelated
    # look-alikes (e.g. different babies years apart all sitting inside one
    # seeded adult's giant window). Adult faces change slowly, so the
    # 2.5-year standard windows are the right resolution for them.
    return eras


def _mutual_knn_graph(embeddings: np.ndarray, k: int, min_sim: float,
                      ) -> tuple[list[list[int]], dict[int, dict[int, float]]]:
    """Mutual-kNN graph over one proposed cluster's members (L2-normalized
    embeddings). An edge exists when each member is in the other's top-k
    AND their cosine similarity clears min_sim. Returns (connected
    components as index lists, adjacency {i: {j: sim}})."""
    n = len(embeddings)
    sims = embeddings @ embeddings.T
    np.fill_diagonal(sims, -1.0)
    k = min(k, n - 1)
    top_k = np.argpartition(sims, -k, axis=1)[:, -k:]
    knn = [set(int(j) for j in row) for row in top_k]

    adjacency: dict[int, dict[int, float]] = {i: {} for i in range(n)}
    for i in range(n):
        for j in knn[i]:
            if j > i and i in knn[j] and sims[i, j] >= min_sim:
                adjacency[i][j] = float(sims[i, j])
                adjacency[j][i] = float(sims[i, j])

    # Near-tied similarities can leave a member with no MUTUAL top-k edge
    # (every neighbor has k closer friends). Isolation would trim a
    # legitimate member to noise, so edge-less nodes attach to their single
    # best neighbor when it clears the floor — one edge cannot bridge two
    # subgroups, so the anti-chaining property survives.
    for i in range(n):
        if not adjacency[i]:
            j = int(np.argmax(sims[i]))
            if sims[i, j] >= min_sim:
                adjacency[i][j] = float(sims[i, j])
                adjacency[j][i] = float(sims[i, j])

    components: list[list[int]] = []
    seen: set = set()
    for start in range(n):
        if start in seen:
            continue
        component = [start]
        seen.add(start)
        frontier = [start]
        while frontier:
            node = frontier.pop()
            for neighbor in adjacency[node]:
                if neighbor not in seen:
                    seen.add(neighbor)
                    component.append(neighbor)
                    frontier.append(neighbor)
        components.append(sorted(component))
    return components, adjacency


def _articulation_points(adjacency: dict[int, dict[int, float]]) -> set:
    """Articulation points of an undirected graph (iterative Tarjan):
    members whose removal disconnects their component — the topological
    signature of a face chaining two subgroups together."""
    visited: set = set()
    disc: dict[int, int] = {}
    low: dict[int, int] = {}
    parent: dict[int, int] = {}
    points: set = set()
    timer = 0

    for root in adjacency:
        if root in visited:
            continue
        visited.add(root)
        disc[root] = low[root] = timer
        timer += 1
        root_children = 0
        stack = [(root, iter(adjacency[root]))]
        while stack:
            node, neighbors = stack[-1]
            descended = False
            for neighbor in neighbors:
                if neighbor not in visited:
                    parent[neighbor] = node
                    if node == root:
                        root_children += 1
                    visited.add(neighbor)
                    disc[neighbor] = low[neighbor] = timer
                    timer += 1
                    stack.append((neighbor, iter(adjacency[neighbor])))
                    descended = True
                    break
                if neighbor != parent.get(node):
                    low[node] = min(low[node], disc[neighbor])
            if not descended:
                stack.pop()
                up = parent.get(node)
                if up is not None:
                    low[up] = min(low[up], low[node])
                    if up != root and low[node] >= disc[up]:
                        points.add(up)
        if root_children > 1:
            points.add(root)
    return points


def _sklearn_hdbscan(min_cluster_size: int, min_samples: int) -> ClusterFn:
    """Default clustering backend. Lazily imports scikit-learn."""
    try:
        from sklearn.cluster import HDBSCAN
    except ImportError:
        raise ImportError(
            "scikit-learn is required for clustering. "
            "Install with: uv sync --extra faces"
        )

    def cluster(embeddings: np.ndarray) -> tuple[np.ndarray, Optional[np.ndarray]]:
        # On L2-normalized vectors, euclidean distance is monotonically
        # related to cosine distance: ||a-b||^2 = 2(1 - cos(a,b))
        clusterer = HDBSCAN(
            min_cluster_size=min_cluster_size,
            min_samples=min_samples,
            metric='euclidean',
            cluster_selection_method='eom',
            # Parallelize the core-distance kNN phase; the MST build itself
            # is inherently sequential.
            n_jobs=-1,
            # Explicit to keep behavior stable across the sklearn 1.10 default
            # flip (and to silence the per-era FutureWarning meanwhile).
            copy=True,
        )
        labels = clusterer.fit_predict(embeddings)
        probabilities = getattr(clusterer, "probabilities_", None)
        return labels, probabilities

    return cluster


class FaceClusterPipeline:
    """
    Proposes identity clusters from scanned embeddings:
    supersede prior proposals -> build era windows -> HDBSCAN per era ->
    persist proposed clusters, memberships, and run actions.
    """

    def __init__(self, db_path: Path,
                 era_size_years: float = config.DEFAULT_ERA_SIZE_YEARS,
                 min_cluster_size: int = config.HDBSCAN_MIN_CLUSTER_SIZE,
                 min_samples: int = config.HDBSCAN_MIN_SAMPLES,
                 pca_dims: int = config.CLUSTER_PCA_DIMS,
                 min_det_score: float = config.MIN_WORKING_DET_SCORE,
                 min_member_sim: float = config.MIN_MEMBER_SIMILARITY,
                 cohesion_edge_sim: float = config.COHESION_MIN_EDGE_SIM,
                 cohesion_knn: int = config.COHESION_KNN,
                 cluster_fn: Optional[ClusterFn] = None):
        self.db_manager = DBManager(db_path)
        self.era_size = era_size_years
        self.min_cluster_size = min_cluster_size
        self.pca_dims = pca_dims
        self.min_det_score = min_det_score
        self.min_member_sim = min_member_sim
        self.cohesion_edge_sim = cohesion_edge_sim
        self.cohesion_knn = cohesion_knn
        self.cluster_fn = cluster_fn or _sklearn_hdbscan(min_cluster_size, min_samples)

    def _maybe_reduce(self, embeddings: np.ndarray) -> np.ndarray:
        """PCA-reduce the embedding matrix for clustering only.

        Fit globally (not per era) so distances stay comparable across
        windows. Skipped when disabled, when the data has no room to reduce,
        or when scikit-learn is unavailable (injected backends in tests).
        Representatives are always computed from the original embeddings.
        """
        n_samples, n_features = embeddings.shape
        target = min(self.pca_dims, n_samples)
        if self.pca_dims <= 0 or target >= n_features:
            return embeddings
        try:
            from sklearn.decomposition import PCA
        except ImportError:
            logging.debug("scikit-learn unavailable; clustering on full dims.")
            return embeddings
        logging.info(f"Reducing embeddings {n_features} -> {target} dims for "
                     f"clustering (PCA)...")
        return PCA(n_components=target, random_state=0).fit_transform(
            embeddings).astype(np.float32)

    def run(self, *, run_id: int) -> Dict[str, Any]:
        try:
            from tqdm import tqdm
        except ImportError:  # pragma: no cover - tqdm is a core dependency
            tqdm = lambda x, **kw: x  # noqa: E731

        stats = {
            "detections_total": 0,
            "detections_undated": 0,
            "eras_processed": 0,
            "clusters_proposed": 0,
            "memberships_proposed": 0,
            "members_trimmed_incoherent": 0,
            "members_trimmed_cannot_link": 0,
            "members_trimmed_label_conflict": 0,
            "members_trimmed_incohesive": 0,
            "members_flagged_articulation": 0,
            "clusters_split_incohesive": 0,
            "clusters_dropped_incoherent": 0,
            "clusters_superseded": 0,
        }

        with self.db_manager as conn:
            face_ops = FaceDBOperations(DBOperations(conn))

            stats["clusters_superseded"] = face_ops.supersede_proposed_clusters(
                run_id=run_id,
                model_name=config.MODEL_NAME,
                model_version=config.MODEL_VERSION_TAG,
            )

            logging.info("Loading embeddings...")
            rows = face_ops.get_embeddings_with_capture_dates(
                model_name=config.MODEL_NAME,
                model_version=config.MODEL_VERSION_TAG,
                min_det_score=self.min_det_score or None,
            )
            stats["detections_total"] = len(rows)

            dated: list[tuple[int, np.ndarray, datetime]] = []
            for detection_id, embedding, capture_str in rows:
                dt = _parse_capture(capture_str)
                if dt is None:
                    stats["detections_undated"] += 1
                    continue
                dated.append((detection_id, np.asarray(embedding, dtype=np.float32), dt))

            if not dated:
                conn.commit()
                logging.info("No dated detections to cluster.")
                return stats

            detection_ids = [item[0] for item in dated]
            dates = [item[2] for item in dated]
            original = np.stack([item[1] for item in dated])
            reduced = self._maybe_reduce(original)

            # Cannot-link constraints from past rejections/evictions: a
            # proposed cluster must never re-join faces a human separated.
            constraints = [c for c in face_ops.get_active_cannot_links()
                           if c["detections_b"] is not None]
            # Positive labels are constraints too: faces labeled to
            # DIFFERENT named persons can never be one identity. (Anonymous
            # machine-made groups don't count — only human-confirmed names.)
            named_detections = face_ops.get_named_person_detections()

            # Accepted clusters are immutable; a new generation that lands
            # on the same deterministic era key must take a suffixed key
            # instead of writing into the accepted row.
            accepted_keys = face_ops.get_accepted_cluster_keys(
                model_name=config.MODEL_NAME,
                model_version=config.MODEL_VERSION_TAG,
            )

            min_date, max_date = min(dates), max(dates)
            logging.info(
                f"Clustering {len(dated)} detection(s) "
                f"from {min_date.date()} to {max_date.date()}"
            )

            # Track where each window came from so progress output is
            # self-explanatory. Identical windows from multiple sources
            # (e.g. twins) share one entry with combined origins.
            window_origins: dict[tuple[datetime, datetime], list[str]] = {}
            for era in compute_standard_eras(min_date, max_date, self.era_size):
                window_origins.setdefault(era, []).append("standard")
            for person_id, display_name, birth_date in face_ops.get_persons_with_birth_dates():
                try:
                    bd = datetime.strptime(birth_date, "%Y-%m-%d")
                except (TypeError, ValueError):
                    logging.warning(
                        f"Person #{person_id} has unparseable birth_date "
                        f"{birth_date!r}; skipping developmental eras."
                    )
                    continue
                child_eras = compute_child_eras(bd, min_date, max_date)
                name = display_name or f"person #{person_id}"
                for era_start, era_end in child_eras:
                    start_age = round((era_start - bd).days / 365.25)
                    end_age = round((era_end - bd).days / 365.25)
                    window_origins.setdefault((era_start, era_end), []).append(
                        f"{name} {start_age}-{end_age}y"
                    )
                logging.info(
                    f"Added {len(child_eras)} developmental era(s) for "
                    f"{name} (born {birth_date})"
                )

            eras = sorted(window_origins, key=lambda e: e[0])
            logging.info(f"Processing {len(eras)} era window(s)...")

            # HDBSCAN offers no intra-fit progress, so windows are the unit of
            # work; the postfix shows how much each one contributed.
            progress = tqdm(eras, desc="Clustering era windows", unit="era")
            for era_start, era_end in progress:
                origin = ", ".join(window_origins[(era_start, era_end)])
                if hasattr(progress, "set_postfix"):
                    progress.set_postfix(
                        window=f"{era_start:%Y-%m}..{era_end:%Y-%m} ({origin})",
                        clusters=stats["clusters_proposed"],
                    )
                proposed = self._cluster_era(
                    face_ops, detection_ids, dates, original, reduced,
                    era_start, era_end, run_id, constraints=constraints,
                    named_detections=named_detections,
                    accepted_keys=accepted_keys,
                )
                if proposed:
                    if proposed["clusters"]:
                        stats["eras_processed"] += 1
                    stats["clusters_proposed"] += proposed["clusters"]
                    stats["memberships_proposed"] += proposed["memberships"]
                    stats["members_trimmed_incoherent"] += proposed["trimmed_incoherent"]
                    stats["members_trimmed_cannot_link"] += proposed["trimmed_cannot_link"]
                    stats["members_trimmed_label_conflict"] += proposed["trimmed_label_conflict"]
                    stats["members_trimmed_incohesive"] += proposed["trimmed_incohesive"]
                    stats["members_flagged_articulation"] += proposed["members_flagged_articulation"]
                    stats["clusters_split_incohesive"] += proposed["clusters_split_incohesive"]
                    stats["clusters_dropped_incoherent"] += proposed["dropped_incoherent"]
                    conn.commit()

            conn.commit()
            logging.info(
                f"Clustering complete: {stats['clusters_proposed']} cluster(s) "
                f"proposed ({stats['eras_processed']} of {len(eras)} window(s) "
                f"produced clusters); coherence gate trimmed "
                f"{stats['members_trimmed_incoherent']} member(s) and "
                f"dissolved {stats['clusters_dropped_incoherent']} cluster(s); "
                f"{stats['members_trimmed_cannot_link']} member(s) removed by "
                f"cannot-links, {stats['members_trimmed_label_conflict']} by "
                f"label conflicts; cohesion gate split "
                f"{stats['clusters_split_incohesive']} extra cluster(s), "
                f"returned {stats['members_trimmed_incohesive']} member(s) to "
                f"noise, flagged {stats['members_flagged_articulation']} "
                f"articulation member(s); "
                f"{stats['clusters_superseded']} prior proposal(s) superseded."
            )
        return stats

    def _cluster_era(self, face_ops: FaceDBOperations,
                     all_detection_ids: list[int],
                     all_dates: list[datetime],
                     original: np.ndarray,
                     reduced: np.ndarray,
                     era_start: datetime, era_end: datetime,
                     run_id: int,
                     constraints: Optional[list[dict]] = None,
                     named_detections: Optional[dict[int, int]] = None,
                     accepted_keys: Optional[set] = None,
                     ) -> Optional[Dict[str, int]]:
        """Cluster one era window. Returns a counter dict (see run()) or
        None when the window has nothing to cluster.

        Clustering runs on the (possibly PCA-reduced) matrix; representative
        embeddings always come from the original full-dimension vectors so
        linking and refinement compare in the model's native space.

        Each HDBSCAN label passes four gates, human evidence before
        heuristics: centroid coherence, cannot-link constraints, named-label
        conflicts, then the mutual-kNN cohesion gate (which may split one
        label into several clusters and assigns membership confidence).
        """
        era_indices = [i for i, dt in enumerate(all_dates)
                       if era_start <= dt < era_end]
        if len(era_indices) < self.min_cluster_size:
            return None

        detection_ids = [all_detection_ids[i] for i in era_indices]
        embeddings = original[era_indices]
        labels, probabilities = self.cluster_fn(reduced[era_indices])
        labels = np.asarray(labels)

        unique_labels = sorted(set(int(lbl) for lbl in labels) - {-1})
        if not unique_labels:
            return None

        era_label = f"{era_start:%Y%m%d}-{era_end:%Y%m%d}"
        counters = {
            "clusters": 0,
            "memberships": 0,
            "trimmed_incoherent": 0,
            "dropped_incoherent": 0,
            "trimmed_cannot_link": 0,
            "trimmed_label_conflict": 0,
            "trimmed_incohesive": 0,
            "clusters_split_incohesive": 0,
            "members_flagged_articulation": 0,
        }
        for label in unique_labels:
            mask = labels == label

            # Coherence gate: density clustering degenerates on sparse
            # windows and can chain unrelated faces into one blob. Every
            # member must actually resemble the cluster's centroid (in full
            # embedding space); trimmed members return to noise, and a
            # cluster that loses too many members dissolves entirely.
            if self.min_member_sim > 0:
                indices = np.flatnonzero(mask)
                rep0 = embeddings[indices].mean(axis=0)
                norm0 = np.linalg.norm(rep0)
                if norm0 > 0:
                    rep0 = rep0 / norm0
                sims = embeddings[indices] @ rep0
                keep = sims >= self.min_member_sim
                if keep.sum() < len(indices):
                    # Second pass against the centroid of the survivors so a
                    # few outliers can't drag the mean toward themselves.
                    survivors = indices[keep]
                    if len(survivors):
                        rep1 = embeddings[survivors].mean(axis=0)
                        norm1 = np.linalg.norm(rep1)
                        if norm1 > 0:
                            rep1 = rep1 / norm1
                        keep = (embeddings[indices] @ rep1) >= self.min_member_sim
                    counters["trimmed_incoherent"] += int(
                        len(indices) - keep.sum())
                    mask = np.zeros_like(mask)
                    mask[indices[keep]] = True
                if mask.sum() < self.min_cluster_size:
                    counters["dropped_incoherent"] += 1
                    continue

            # Cannot-link enforcement: never re-join faces a human
            # separated. Drop the smaller offending side per constraint
            # (evictions record the intruders as side A, which loses ties),
            # then re-check viability.
            if constraints:
                member_set = {detection_ids[i] for i in np.flatnonzero(mask)}
                remove: set = set()
                for constraint in constraints:
                    in_a = (member_set - remove) & constraint["detections_a"]
                    in_b = (member_set - remove) & constraint["detections_b"]
                    if in_a and in_b:
                        remove |= in_a if len(in_a) <= len(in_b) else in_b
                if remove:
                    counters["trimmed_cannot_link"] += len(remove)
                    keep_ids = member_set - remove
                    new_mask = np.zeros_like(mask)
                    for i in np.flatnonzero(mask):
                        if detection_ids[i] in keep_ids:
                            new_mask[i] = True
                    mask = new_mask
                    if mask.sum() < self.min_cluster_size:
                        counters["dropped_incoherent"] += 1
                        continue

            # Named-label enforcement: faces the user labeled to DIFFERENT
            # people can never be one cluster. Keep the plurality person's
            # faces (plus unlabeled members); trim the rest to noise.
            if named_detections:
                persons_in: dict[int, list[int]] = {}
                for i in np.flatnonzero(mask):
                    person = named_detections.get(detection_ids[i])
                    if person is not None:
                        persons_in.setdefault(person, []).append(i)
                if len(persons_in) > 1:
                    keep_person = max(
                        persons_in,
                        key=lambda p: (len(persons_in[p]), -p))
                    conflict = [i for person, member_idx in persons_in.items()
                                if person != keep_person
                                for i in member_idx]
                    counters["trimmed_label_conflict"] += len(conflict)
                    mask = mask.copy()
                    mask[conflict] = False
                    if mask.sum() < self.min_cluster_size:
                        counters["dropped_incoherent"] += 1
                        continue

            # Cohesion gate: validate the cluster as a mutual-kNN graph in
            # full embedding space. Weakly-chained blobs split into their
            # real components; membership confidence comes from the graph
            # (mean similarity to neighbors, penalized for articulation
            # members — the faces that chain subgroups together).
            indices = np.flatnonzero(mask)
            groups: list[tuple[list[int], dict[int, float]]] = []
            if self.cohesion_edge_sim > 0 and len(indices) > 1:
                components, adjacency = _mutual_knn_graph(
                    embeddings[indices], self.cohesion_knn,
                    self.cohesion_edge_sim)
                cut_vertices = _articulation_points(adjacency)
                for component in components:
                    if len(component) < self.min_cluster_size:
                        counters["trimmed_incohesive"] += len(component)
                        continue
                    confidences: dict[int, float] = {}
                    for local in component:
                        neighbor_sims = adjacency[local].values()
                        confidence = sum(neighbor_sims) / len(neighbor_sims)
                        if local in cut_vertices:
                            confidence *= config.ARTICULATION_PENALTY
                            counters["members_flagged_articulation"] += 1
                        confidences[int(indices[local])] = min(
                            max(confidence, 0.0), 1.0)
                    groups.append(
                        ([int(indices[local]) for local in component],
                         confidences))
                if len(groups) > 1:
                    counters["clusters_split_incohesive"] += len(groups) - 1
            else:
                groups.append((
                    [int(i) for i in indices],
                    {int(i): (float(probabilities[i])
                              if probabilities is not None else None)
                     for i in indices},
                ))

            for group_index, (member_indices, confidences) in enumerate(groups):
                # HDBSCAN label numbering isn't stable across runs anyway,
                # so split components just take a suffix.
                suffix = "" if group_index == 0 else f"-{group_index}"
                cluster_key = f"era:{era_label}#{label:03d}{suffix}"
                if accepted_keys and cluster_key in accepted_keys:
                    # Never write into an accepted cluster's key: this
                    # generation gets its own row; the duplicate tier will
                    # propose the merge if the faces really match.
                    cluster_key = f"{cluster_key}@{run_id}"
                rep = embeddings[member_indices].mean(axis=0)
                norm = np.linalg.norm(rep)
                if norm > 0:
                    rep = rep / norm
                face_ops.upsert_cluster(
                    run_id=run_id,
                    cluster_key=cluster_key,
                    model_name=config.MODEL_NAME,
                    model_version=config.MODEL_VERSION_TAG,
                    era_start=era_start.isoformat(),
                    era_end=era_end.isoformat(),
                    representative_embedding=[float(v) for v in rep],
                    payload={"era_label": era_label,
                             "face_count": len(member_indices)},
                )
                for i in member_indices:
                    face_ops.propose_cluster_assignment(
                        run_id=run_id,
                        detection_id=detection_ids[i],
                        cluster_key=cluster_key,
                        model_name=config.MODEL_NAME,
                        model_version=config.MODEL_VERSION_TAG,
                        confidence=confidences[i],
                    )
                    counters["memberships"] += 1
                counters["clusters"] += 1

        noise = int((labels == -1).sum())
        logging.debug(
            f"Era {era_label}: {counters['clusters']} cluster(s) from "
            f"{len(era_indices)} detection(s) ({noise} noise, "
            f"{counters['trimmed_incoherent']} trimmed incoherent, "
            f"{counters['trimmed_cannot_link']} removed by cannot-links, "
            f"{counters['trimmed_label_conflict']} label conflicts, "
            f"{counters['trimmed_incohesive']} incohesive)"
        )
        return counters


def _parse_capture(capture_str: Optional[str]) -> Optional[datetime]:
    if not capture_str:
        return None
    try:
        return datetime.fromisoformat(capture_str)
    except ValueError:
        return None

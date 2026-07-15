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
        List of (start_date, end_date) tuples.
    """
    window = timedelta(days=int(era_size_years * 365))
    step = timedelta(days=int(era_size_years * 365 * config.ERA_OVERLAP_FRACTION))

    eras = []
    current = min_date
    while current < max_date:
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
        List of (start_date, end_date) tuples for this child's eras.
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
        eras.append((era_start, era_end))

    # Add open-ended era for the oldest boundary onward (standard windows
    # take over for adult faces, which change slowly).
    last_boundary = boundaries[-1]
    adult_start = birth_date + timedelta(days=int(last_boundary * 365.25))
    if adult_start < max_date:
        eras.append((max(adult_start, min_date), max_date + timedelta(days=1)))

    return eras


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
                 cluster_fn: Optional[ClusterFn] = None):
        self.db_manager = DBManager(db_path)
        self.era_size = era_size_years
        self.min_cluster_size = min_cluster_size
        self.pca_dims = pca_dims
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

            min_date, max_date = min(dates), max(dates)
            logging.info(
                f"Clustering {len(dated)} detection(s) "
                f"from {min_date.date()} to {max_date.date()}"
            )

            eras = compute_standard_eras(min_date, max_date, self.era_size)
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
                eras.extend(child_eras)
                logging.info(
                    f"Added {len(child_eras)} developmental era(s) for "
                    f"{display_name or f'person #{person_id}'} (born {birth_date})"
                )

            eras = sorted(set(eras), key=lambda e: e[0])
            logging.info(f"Processing {len(eras)} era window(s)...")

            # HDBSCAN offers no intra-fit progress, so windows are the unit of
            # work; the postfix shows how much each one contributed.
            progress = tqdm(eras, desc="Clustering era windows", unit="era")
            for era_start, era_end in progress:
                if hasattr(progress, "set_postfix"):
                    progress.set_postfix(
                        window=f"{era_start:%Y-%m}..{era_end:%Y-%m}",
                        clusters=stats["clusters_proposed"],
                    )
                proposed = self._cluster_era(
                    face_ops, detection_ids, dates, original, reduced,
                    era_start, era_end, run_id,
                )
                if proposed:
                    stats["eras_processed"] += 1
                    stats["clusters_proposed"] += proposed[0]
                    stats["memberships_proposed"] += proposed[1]
                    conn.commit()

            conn.commit()
            logging.info(
                f"Clustering complete: {stats['clusters_proposed']} cluster(s) "
                f"proposed across {stats['eras_processed']} era(s); "
                f"{stats['clusters_superseded']} prior proposal(s) superseded."
            )
        return stats

    def _cluster_era(self, face_ops: FaceDBOperations,
                     all_detection_ids: list[int],
                     all_dates: list[datetime],
                     original: np.ndarray,
                     reduced: np.ndarray,
                     era_start: datetime, era_end: datetime,
                     run_id: int) -> Optional[tuple[int, int]]:
        """Cluster one era window. Returns (clusters, memberships) or None.

        Clustering runs on the (possibly PCA-reduced) matrix; representative
        embeddings always come from the original full-dimension vectors so
        linking and refinement compare in the model's native space.
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
        clusters = 0
        memberships = 0
        for label in unique_labels:
            mask = labels == label
            member_ids = [detection_ids[i] for i in np.flatnonzero(mask)]
            rep = embeddings[mask].mean(axis=0)
            norm = np.linalg.norm(rep)
            if norm > 0:
                rep = rep / norm

            cluster_key = f"era:{era_label}#{label:03d}"
            face_ops.upsert_cluster(
                run_id=run_id,
                cluster_key=cluster_key,
                model_name=config.MODEL_NAME,
                model_version=config.MODEL_VERSION_TAG,
                era_start=era_start.isoformat(),
                era_end=era_end.isoformat(),
                representative_embedding=[float(v) for v in rep],
                payload={"era_label": era_label, "face_count": len(member_ids)},
            )
            for i in np.flatnonzero(mask):
                confidence = (
                    float(probabilities[i]) if probabilities is not None else None
                )
                face_ops.propose_cluster_assignment(
                    run_id=run_id,
                    detection_id=detection_ids[i],
                    cluster_key=cluster_key,
                    model_name=config.MODEL_NAME,
                    model_version=config.MODEL_VERSION_TAG,
                    confidence=confidence,
                )
                memberships += 1
            clusters += 1

        noise = int((labels == -1).sum())
        logging.debug(
            f"Era {era_label}: {clusters} cluster(s) from {len(era_indices)} "
            f"detection(s) ({noise} noise)"
        )
        return clusters, memberships


def _parse_capture(capture_str: Optional[str]) -> Optional[datetime]:
    if not capture_str:
        return None
    try:
        return datetime.fromisoformat(capture_str)
    except ValueError:
        return None

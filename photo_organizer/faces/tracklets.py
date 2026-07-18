"""
Same-event tracklets: sequence evidence for face identity.

Photos taken minutes apart at one event carry a signal embeddings alone
miss: the same person, in the same clothes and lighting, photographed
repeatedly. Grouping photos into events by capture-time gaps and matching
faces between nearby photos (mutual nearest neighbor over embeddings)
builds short "tracklets" — chains of detections that are physically the
same person even when their embeddings fall into different density modes.

Two rules embody the depicted-faces design constraint:

- Faces in the SAME photo are never auto-linked. Same-photo uniqueness is
  a prior, not a law — framed photos on a wall, mirrors, screens, and twins
  all legitimately put one face in a photo twice.
- Same-photo pairs whose similarity is suspiciously high are flagged for
  human review (almost always a depiction or a twin), never decided.

Tracklet output feeds the link phase: a tracklet spanning two clusters is
strong merge evidence, including for clusters in the SAME era window that
HDBSCAN split into separate density modes.
"""
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Sequence

import numpy as np

from . import config


@dataclass
class TrackletResult:
    """Output of build_tracklets.

    tracklets:        lists of detection ids (each len >= 2), one per
                      connected component of accepted cross-photo matches.
    edges:            {(det_lo, det_hi): cosine similarity} for every
                      accepted cross-photo match (the evidence trail).
    same_photo_flags: (file_id, det_a, det_b, similarity) for same-photo
                      pairs above the review threshold — likely depictions
                      (framed photos, screens, mirrors) or twins.
    events:           number of multi-photo events found.
    photos_grouped:   number of photos that landed in multi-photo events.
    """
    tracklets: list[list[int]] = field(default_factory=list)
    edges: dict[tuple[int, int], float] = field(default_factory=dict)
    same_photo_flags: list[tuple[int, int, int, float]] = field(default_factory=list)
    events: int = 0
    photos_grouped: int = 0


class _UnionFind:
    """Minimal union-find over detection ids (linking.py has its own for the
    accept phase; duplicated here to keep the import graph acyclic)."""

    def __init__(self):
        self.parent: dict[int, int] = {}

    def find(self, x: int) -> int:
        root = x
        while self.parent.setdefault(root, root) != root:
            root = self.parent[root]
        while self.parent[x] != root:  # path compression
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, x: int, y: int):
        self.parent[self.find(y)] = self.find(x)


def _parse_capture(capture_str: Optional[str]) -> Optional[datetime]:
    if not capture_str:
        return None
    try:
        return datetime.fromisoformat(capture_str)
    except ValueError:
        return None


def group_into_events(photos: Sequence[tuple[int, datetime]],
                      gap_minutes: float = config.EVENT_GAP_MINUTES,
                      ) -> list[list[int]]:
    """
    Split photos into events: runs of capture times where consecutive
    photos are at most gap_minutes apart. Returns file_id lists in capture
    order; only multi-photo events are useful to callers but singletons are
    returned too so callers can count coverage.
    """
    ordered = sorted(photos, key=lambda p: p[1])
    gap = timedelta(minutes=gap_minutes)
    events: list[list[int]] = []
    previous: Optional[datetime] = None
    for file_id, captured in ordered:
        if previous is None or captured - previous > gap:
            events.append([file_id])
        else:
            events[-1].append(file_id)
        previous = captured
    return events


def build_tracklets(rows: Sequence[tuple[int, int, Optional[str], np.ndarray]],
                    *,
                    gap_minutes: float = config.EVENT_GAP_MINUTES,
                    min_similarity: float = config.TRACKLET_MIN_SIMILARITY,
                    file_adjacency: int = config.TRACKLET_FILE_ADJACENCY,
                    same_photo_review_sim: float = config.SAME_PHOTO_REVIEW_SIMILARITY,
                    ) -> TrackletResult:
    """
    Build same-event tracklets from detection rows
    (detection_id, file_id, capture_datetime_iso, L2-normalized embedding).

    Within each event, every photo is matched against the next
    `file_adjacency` photos: a detection pair links when each is the
    other's best match between the two photos (mutual nearest neighbor)
    and their cosine similarity clears `min_similarity`. Rows without a
    capture date are skipped — no timestamp, no sequence evidence.
    """
    result = TrackletResult()

    # Group detections by photo; a photo's capture time is shared by all
    # its detections.
    by_file: dict[int, list[tuple[int, np.ndarray]]] = {}
    file_dates: dict[int, datetime] = {}
    for detection_id, file_id, capture_str, embedding in rows:
        captured = _parse_capture(capture_str)
        if captured is None:
            continue
        by_file.setdefault(file_id, []).append((detection_id, embedding))
        file_dates[file_id] = captured

    if not by_file:
        return result

    # Same-photo review flags are independent of event membership: a lone
    # photo of a wall of framed portraits still deserves the flag.
    for file_id, faces in by_file.items():
        if len(faces) < 2:
            continue
        ids = [det_id for det_id, _ in faces]
        matrix = np.stack([emb for _, emb in faces])
        sims = matrix @ matrix.T
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                sim = float(sims[i, j])
                if sim >= same_photo_review_sim:
                    result.same_photo_flags.append(
                        (file_id, ids[i], ids[j], round(sim, 3))
                    )

    events = group_into_events(list(file_dates.items()), gap_minutes)
    uf = _UnionFind()

    for event in events:
        if len(event) < 2:
            continue
        result.events += 1
        result.photos_grouped += len(event)
        for i, file_a in enumerate(event):
            faces_a = by_file[file_a]
            ids_a = [det_id for det_id, _ in faces_a]
            matrix_a = np.stack([emb for _, emb in faces_a])
            for file_b in event[i + 1:i + 1 + file_adjacency]:
                faces_b = by_file[file_b]
                ids_b = [det_id for det_id, _ in faces_b]
                matrix_b = np.stack([emb for _, emb in faces_b])

                sims = matrix_a @ matrix_b.T
                best_b = np.argmax(sims, axis=1)   # a's best match in b
                best_a = np.argmax(sims, axis=0)   # b's best match in a
                for ai, bi in enumerate(best_b):
                    sim = float(sims[ai, bi])
                    if sim < min_similarity or best_a[bi] != ai:
                        continue
                    det_a, det_b = ids_a[ai], ids_b[int(bi)]
                    uf.union(det_a, det_b)
                    key = (det_a, det_b) if det_a < det_b else (det_b, det_a)
                    # Keep the strongest observation of a pair (adjacency
                    # depth can match the same pair via two photo pairs).
                    if sim > result.edges.get(key, 0.0):
                        result.edges[key] = sim

    components: dict[int, list[int]] = {}
    for det_id in uf.parent:
        components.setdefault(uf.find(det_id), []).append(det_id)
    result.tracklets = [sorted(members) for members in components.values()
                        if len(members) >= 2]
    result.tracklets.sort()

    logging.info(
        f"Tracklets: {result.events} multi-photo event(s) covering "
        f"{result.photos_grouped} photo(s) -> {len(result.tracklets)} "
        f"tracklet(s) from {len(result.edges)} matched pair(s); "
        f"{len(result.same_photo_flags)} same-photo pair(s) flagged for "
        f"review (possible depictions or twins)."
    )
    for file_id, det_a, det_b, sim in result.same_photo_flags:
        logging.info(
            f"  same-photo flag: file {file_id} detections {det_a}/{det_b} "
            f"similarity {sim} — check for framed photo/screen/mirror or twins"
        )
    return result

"""
In-memory set of file_ids this pipeline run is working on.

The pipeline seeds this set from:
- Phase A discovery (ids of files the discoverer accepted into the working
  set — every upserted id, including re-upserts of rows that already existed).
- Apply-deferred-changes (rename-target ids produced by
  DestinationSyncer.apply_renames).
- Later Phase B writers (planner / linked-destination assignment) add their
  outputs so the next writer operates on the accumulated-so-far set.

Phase B writers (planner, sidecar/PSD/output linkers, mover) accept this set
as an *input filter*: writes only happen against ids in the set. Rows outside
the set — stale renames from interrupted prior runs, leftover dest_path=NULL
entries, or anything else the catalog carries — are invisible to Phase B.

Tier 2 (deferred) will promote this to a durable `run_actions` table. An
in-memory set is sufficient for Tier 1 because the mover always runs in the
same process that built the set.
"""
from dataclasses import dataclass, field
from typing import Iterable, Set


@dataclass
class PipelineCandidates:
    """Thin wrapper around a Set[int] so callers can pass a typed object
    rather than a bare set — makes the pipeline's input-filter contract
    readable at call sites and gives Tier 2 a single place to attach
    run_actions persistence if/when that work happens."""

    _ids: Set[int] = field(default_factory=set)

    def add(self, file_id: int) -> None:
        self._ids.add(int(file_id))

    def add_many(self, ids: Iterable[int]) -> None:
        for file_id in ids:
            self._ids.add(int(file_id))

    def ids(self) -> Set[int]:
        """Returns a copy so callers can't mutate internal state through this
        view. Mutation goes through add / add_many."""
        return set(self._ids)

    def __len__(self) -> int:
        return len(self._ids)

    def __contains__(self, file_id: int) -> bool:
        return int(file_id) in self._ids

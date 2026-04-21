# Plan (Tier 1): Fix `--ingest-dest` dry-run resurrection and DPP linking

**Scope:** smallest set of changes that makes `--ingest-dest --dry-run --move`
accurate for the DPP workflow and safe to promote to a real run.

**Non-goals:** the `run_actions` / `run_observations` architecture Codex
proposed. That's Tier 2, planned separately after this ships.

---

## Revised root-cause summary

Direct probing of the production catalog changed the diagnosis from the
previous draft. The verified findings:

1. **Hash-lookup in rename detection worked for *these specific 3 rows* —
   but it is broken for 44% of the catalog.** Rows 19318-19320 happen to
   have both full and sparse hashes populated; running
   `DestinationSyncer.sync_destinations([dest], dry_run=True)` against the
   live DB today correctly returns `renamed=3, new=3`. That made my earlier
   "sparse-only lookup miss" theory look wrong. It's not wrong — it's
   narrower than I thought. Catalog-wide distribution:

   | Hash state | Count | % |
   |---|---:|---:|
   | Both full + sparse | 22,675 | 22% |
   | Full only | 30,855 | 31% |
   | **Sparse only** | **44,434** | **44%** |
   | Neither (all type=`other`) | 2,973 | 3% |

   For any of the 44,434 sparse-only rows (including 33,487 RAWs), a rename
   fails detection exactly the way originally theorized: the hasher
   escalates to full hash on the "known sparse" collision, the lookup uses
   full, and misses. The three CR2s the user renamed happened to be in the
   22% "both" bucket, which is why this worked today — but any DPP workflow
   touching a sparse-only RAW hits it. **Change 4 is a primary correctness
   fix, not defensive work.**

2. **The real bug is dry-run rename invisibility.**
   [CataloguedTreeDiscoverer.discover()](photo_organizer/pipeline/discovery.py#L104-L136)
   calls `sync_destinations(dry_run=True)` during Phase A. Rename detection
   succeeds, but
   [path_sync.py:234](photo_organizer/sync/path_sync.py#L234) guards
   `update_dest_path_atomic` on `not dry_run`, so the catalog's
   `files.dest_path` stays pointed at the old filenames. Phase B then runs
   against stale state: linker reads stale `orig_name`, mover's global
   `get_pending_moves()` finds the 3 stale rows have missing dest files and
   plans `shutil.move(Seed_path → old_dest_name)` for each. A real `--move`
   run would duplicate at old names.

3. **Linker stem matching breaks on DPP renames.**
   [linking.py:219-234](photo_organizer/metadata/linking.py#L219-L234) pulls
   `f.orig_name` for both RAW and output sides. DPP's workflow (rename RAW in
   place, export JPEG with `_0001` suffix) produces stems that:
   - Differ between `orig_name` ("EOS 5D Mark II_…") and current on-disk name
     ("Evie_napping_…").
   - Even after a rename update, differ by a trailing `_NNNN` export suffix.

   All four matching strategies require stem equality (strategies 2 and 4
   guard on `raw_stem == out_stem` explicitly). No link proposed → observed
   `inserted=0` for the 3 new JPEGs.

4. **Catalog is not contaminated.** The failed dry-run created 3 legitimate
   new JPEG rows (dest_path=NULL, ingest_mode=True) and did not create
   duplicate CR2 rows. `upsert_file_record` matched by full hash but skipped
   canonical overwrite because existing rows are `is_seed=1`. No repair step
   is needed.

---

## Design changes

### Change 1 — Move rename reconciliation into Phase B savepoint

**Problem:** Phase A commits rename updates (or doesn't, on dry-run), so Phase
B sees whichever version has been committed. On dry-run, Phase B sees stale
state.

**Fix:** Split `sync_destinations` usage into detect (Phase A) + apply
(Phase B).

- `DestinationSyncer.sync_destinations` gains an `apply_renames: bool =
  True` parameter. **Default `True`** preserves today's behavior for
  every non-pipeline caller — notably
  `PhotoOrganizerApp.sync_dest()` at
  [core.py:351](photo_organizer/core.py#L351), which drives the
  user-facing `--sync-dest` command and must continue to apply
  renames as part of its job. `CataloguedTreeDiscoverer` is the only
  caller that passes `apply_renames=False`, because for ingest-dest
  the application step moves into the Phase B savepoint.
  When `apply_renames=False`, the syncer returns a `SyncReport` with
  the rename records but never calls `update_dest_path_atomic`.
- **`SyncReport` gains structured rename records.** Today
  `renamed_files: List[Tuple[str, str]]` is display-only and insufficient
  for later application. Add:

  ```python
  @dataclass
  class RenameRecord:
      file_id: int
      old_path: str
      new_path: str
      mtime: float
      size_bytes: int
      # Hash identity observed during rename detection:
      #   - matched_hash is whichever key hit the lookup (full preferred,
      #     sparse fallback per Change 5's priority rule).
      #   - matched_hash_is_sparse records the kind of key, so the
      #     downstream file_occurrences row can set hash_is_sparse
      #     correctly.
      #   - observed_full_hash is the full hash if FileHasher computed
      #     one during the collision-path escalation, otherwise None.
      #     When populated AND files.hash is NULL on this row,
      #     apply_renames triggers Change 5's upgrade. When populated,
      #     apply_renames also prefers it over matched_hash for
      #     file_occurrences persistence — occurrence records always
      #     store the best observed hash (hash_is_sparse=0) when a full
      #     hash was computed, regardless of which key matched the
      #     catalog row.
      matched_hash: str
      matched_hash_is_sparse: bool
      observed_full_hash: Optional[str]
  ```

  `SyncReport.renames: List[RenameRecord]`. The existing
  `renamed_files: List[Tuple[str, str]]` stays for CSV/logging
  back-compat, derived from `renames`.
- **`SyncReport` also gains `imported_file_ids: Set[int]`.**
  `DestinationSyncer.import_new_files()` currently returns a count; it
  must be changed to return `Set[int]` and populate
  `SyncReport.imported_file_ids` on the in-out report it mutates.
  **Contract:** the set represents *every file_id this run has
  accepted into its ingest working set* — not only rows it freshly
  created. `DBOperations.upsert_file_record()` returns an existing row
  id when the file is already catalogued
  ([ops.py:83](photo_organizer/database/ops.py#L83)), and in the
  post-failed-dry-run world this matters: the three JPEGs the user's
  first dry-run created are now catalog rows with `dest_path=NULL`,
  and a rerun's discovery will re-upsert them and get the same ids
  back. Those ids MUST land in `imported_file_ids` so planner, linker,
  and mover still see them as this run's candidates. The distinction
  between "created by this run" vs "already existed but still
  ingest-working-set" is not visible to the candidate-set and should
  not be — it's the working set that matters.
- A new `DestinationSyncer.apply_renames(report) -> Set[int]` method
  iterates `report.renames` and calls `update_dest_path_atomic` with the
  structured fields. Returns the set of file_ids it updated.
- **Discoverer protocol gains `apply_deferred_changes(db_ops, conn,
  result) -> Set[int]`.** `_run_pipeline` must not import
  `DestinationSyncer` or otherwise reach into sync machinery — the
  whole point of the `Discoverer` abstraction is to keep
  scanner/syncer concerns out of `core.py`. Instead:
  - `UntrackedTreeDiscoverer.apply_deferred_changes()`: no-op,
    returns `set()`. Organize has no deferred Phase B work.
  - `CataloguedTreeDiscoverer.apply_deferred_changes()`: constructs
    (or retains from `discover()`) a `DestinationSyncer`, calls
    `apply_renames(result.sync_report)`, returns the set of updated
    file_ids.

  `_run_pipeline` then calls `discoverer.apply_deferred_changes(...)`
  as the first step inside the Phase B savepoint and feeds the
  returned ids into `PipelineCandidates`. This keeps the savepoint
  boundary in `core.py` while the rename-application mechanics stay
  with the discoverer that owns them.
- `CataloguedTreeDiscoverer.discover()` calls detection only. The returned
  `DiscoveryResult` carries the `SyncReport` AND a new
  `imported_file_ids: Set[int]` field copied from
  `sync_report.imported_file_ids`.
- **`UntrackedTreeDiscoverer` must also populate
  `DiscoveryResult.imported_file_ids`.** `_run_pipeline` is shared by
  `organize()` AND `ingest_dest()`, and Change 2's candidate-scoped
  `plan_all` / `_assign_linked_destinations` / `FileMover.execute`
  will no-op for the organize path if `imported_file_ids` is only
  filled for ingest-dest. The fix is trivial — the organize
  discoverer already has `file_id = db_ops.upsert_file_record(record)`
  at [discovery.py:61](photo_organizer/pipeline/discovery.py#L61);
  add `result.imported_file_ids.add(file_id)` alongside the existing
  `result.imported_paths.append(...)` call. Same working-set
  semantics as the catalogued discoverer: *every* upserted id goes in
  (including re-upserts of already-catalogued rows), because the
  organize pipeline's candidate scope is exactly "the files this scan
  touched".
- **`_run_pipeline`'s `skip_if_no_imports` early return must be
  broadened.** Today
  [core.py:192](photo_organizer/core.py#L192) returns before Phase B
  when `not result.imported_paths`. With detection moved out of
  Phase A, a rename-only run (no imports, but renames pending) would
  skip Phase B and `apply_renames()` would never fire. The guard
  changes to:

  ```python
  if skip_if_no_imports \
          and not result.imported_paths \
          and not result.sync_report.renames:
      logging.info("No new files or pending renames — pipeline exiting early.")
      return stats
  ```

  Rename-only runs now proceed into Phase B, apply renames inside the
  savepoint, and exit cleanly (planner/linker/mover run against the
  candidate set seeded from the renames, which may legitimately
  produce zero work).
- `_run_pipeline` invokes
  `discoverer.apply_deferred_changes(db_ops, conn, result)` as the
  first step inside the Phase B savepoint and adds the returned ids
  to `PipelineCandidates`. Organize runs get back `set()` from the
  untracked discoverer's no-op implementation; ingest-dest runs get
  back the renamed file_ids from the catalogued discoverer.

Effect: on a dry-run, renames are visible to linker/planner/mover and roll
back with the savepoint. On a real run, they release with Phase B's commit.
Single scan; no duplicate I/O. No path-string reconstruction at apply time.

### Change 2 — Scope Phase B to per-run candidate file_ids (input filter, not output collector)

**Problem:** `FileMover.execute` calls
[`get_pending_moves()`](photo_organizer/database/ops.py#L176-L180) which is
`SELECT * FROM files WHERE dest_path IS NOT NULL`. It then filters by
`Path(dest).exists()`. Any catalog row whose file is absent from its stored
`dest_path` — regardless of cause — becomes a resurrection candidate.

**Stronger requirement (per Codex review):** scoping the mover alone is
insufficient. `DestinationPlanner.plan_all()`,
`_assign_linked_destinations()`, and `FileLinker.link_raw_outputs()` also
query globally today. If we only scope the mover, those earlier Phase B
writers will plan/assign/link against unrelated rows (interrupted prior
runs, null-dest stragglers), and the mover's post-hoc scope filter won't
help — the in-savepoint mutations already fired. Scoping must be an **input
filter** on every Phase B writer, not just an output collector.

**Fix (Tier 1 shape):** pass a `PipelineCandidates` set through the pipeline
and require every Phase B writer to accept it as a filter.

- `PipelineCandidates` dataclass (stand-alone in `pipeline/candidates.py`)
  with `.add(file_id)` / `.add_many(ids)` / `.ids() -> Set[int]`. Seeded
  from Phase A with `result.imported_file_ids`.
- `DBOperations.get_pending_moves_for_ids(file_ids)` — same SELECT as
  today, constrained by `id IN (…)`.
- `FileMover.execute(file_ids, move_mode, dry_run)`. Idempotency filter
  (`Path(dest).exists()`) unchanged.
- `DestinationPlanner.plan_all(dest_root, candidate_file_ids=None) -> Set[int]`.
  When candidates are provided, the internal "unassigned files to plan" query
  is constrained by `id IN (…)`. Returns the set of file_ids it assigned
  (still useful for downstream steps that may care).
- `_assign_linked_destinations(db_ops, candidate_file_ids=None) -> Set[int]`.
  Each SELECT is gated so only sidecars/PSDs whose parent RAW/source is in
  the candidate set (OR whose own id is in the candidate set — covers both
  net-new imports and pre-existing sidecars that got re-parented by a
  candidate RAW's rename) get an assignment. Returns assigned ids.
- `FileLinker.link_raw_outputs(candidate_output_file_ids=None, dry_run=False)`.
  Outputs query is constrained to `id IN (candidate_output_file_ids)` when
  provided. RAW side remains global — a new output can legitimately match
  any pre-existing RAW. Effect: proposed count goes from ~19,286 (today) to
  just the links involving this run's new outputs, making the log and the
  CSV actually reviewable. Must also propagate the pipeline's `dry_run` so
  the inserts are suppressed on preview (today it ignores caller intent — a
  latent issue surfaced by the user's run).
- `FileLinker.link_raw_sidecars(candidate_file_ids=None)` and
  `FileLinker.link_raw_sidecars_by_dest(candidate_file_ids=None)`.
  Today both write globally
  ([linking.py:26](photo_organizer/metadata/linking.py#L26),
  [linking.py:64](photo_organizer/metadata/linking.py#L64)) and are
  invoked by `sidecar_linker(linker)` in `_run_pipeline` before
  `link_raw_outputs()`
  ([core.py:199](photo_organizer/core.py#L199)). Scope the sidecar
  SELECT to `id IN (candidate_file_ids)` when provided — matching the
  same "new output side scoped, pre-existing RAW side global" pattern
  as `link_raw_outputs`. A sidecar that's pre-existing but whose
  parent RAW was just renamed this run also counts as a candidate
  (covered because the renamed RAW's file_id is in the set and any
  sidecar `UNION` variant can pick it up by parent-relationship —
  but in practice sidecar scoping is driven by the sidecar id
  itself, not the RAW's).
- `FileLinker.link_psds(candidate_psd_file_ids=None)`. Today PSD
  selection is global
  ([linking.py:106](photo_organizer/metadata/linking.py#L106)).
  Scope the PSD SELECT to `id IN (candidate_psd_file_ids)` when
  provided. Source side (RAW/JPEG) remains global — a new PSD can
  legitimately match any pre-existing source.

  **Why scope the link writers at all** — they write into
  `raw_sidecars`, `raw_outputs`, and `psd_source_links`. Without
  scoping, a real-run `ingest-dest` would rebuild global link tables
  every time, which is (a) wasteful and (b) hides regressions: a
  spurious link proposed on this run is indistinguishable from one
  that has always been there. Scoping makes this run's link
  contribution auditable.

**Candidate seeding points:**
- `DiscoveryResult.imported_file_ids: Set[int]` — populated by **both
  discoverers** with the run's full ingest working set (every upserted
  file_id, not just net-new rows — see Change 1 for the contract).
  Without this, organize runs (which use `UntrackedTreeDiscoverer`)
  would get an empty candidate set and the scoped planner/linker/mover
  would no-op. Paths alone are insufficient — the pipeline needs the
  actual catalog ids.
- `apply_renames` return value — renamed file_ids.
- `plan_all` and `_assign_linked_destinations` return values — files the
  pipeline just assigned a destination to.

All contribute to `PipelineCandidates` *before* the next writer consumes it,
so each step operates on the accumulated-so-far set.

Effect: Phase B only touches rows this run is actually working on. Stale
rows — from anywhere, any cause — are invisible to planner, linked-dest
assignment, linker output side, and mover. This is the narrowest change
that converts "global scan" into "run-scoped plan" without introducing a
`run_actions` table.

**Explicit non-goal:** we do NOT add a `pending_action_run_id` column on
`files`. An in-memory `PipelineCandidates` is sufficient because the mover
always runs inside the same process that built the set. If a future command
needs to preview a plan, stop, and resume — that's when `run_actions` earns
its schema change, in Tier 2.

### Change 3 — Linker matches editor exports via current-stem + capture_datetime

**Root insight:** at the moment an editor (DPP, Lightroom, Photoshop, etc.)
exports a derivative next to a renamed RAW, the export's stem equals the
RAW's current on-disk name — i.e. the RAW's `dest_path` stem after
`sync-dest` reconciliation. The output's `orig_name` preserves that
identity. Stem drift only appears later, when the pipeline normalizes the
output's planned `dest_path` by appending a date/time component. So the
deterministic signature of an editor export is:

    output.orig_name_stem == raw.dest_path_stem
    AND output.capture_datetime == raw.capture_datetime

Both sides are location-independent. The user may let DPP write to a
`\jpeg` subfolder, into `\output\YYYY\YYYY-MM\`, or anywhere else under
dest; the stem-plus-capture-time signature holds in every case. The only
assumptions are (1) the editor preserves EXIF DateTimeOriginal (effectively
universal) and (2) the user's rename gave the RAW a stem that is unique
across the catalog for that capture_datetime (true by construction —
originals aren't duplicate-named).

**Part A: read `dest_path` on the RAW side.**

```sql
SELECT f.id, f.orig_name, f.dest_path, m.capture_datetime, m.camera_model
FROM files f
LEFT JOIN media_metadata m ON f.id = m.file_id
WHERE f.type = 'raw'
```

```python
def _current_stem(orig_name: str, dest_path: Optional[str]) -> str:
    name = Path(dest_path).name if dest_path else orig_name
    return self._normalize_stem(Path(name).stem)
```

Paired with Change 1, the RAW's rename is visible even on dry-run, so the
current-stem derivation works consistently in preview and real runs.

**Part B: new strategy — editor-export identity.**

Insert as strategy 5, after strategy 4. Confidence **100** — this is a
deterministic signal, not a heuristic.

**Ambiguity guard.** Because camera is not required in the key, two RAW
rows could in principle share the same `(current_stem,
capture_datetime)` (e.g. same user-chosen stem reused across a burst,
or two cameras triggered at exactly the same timestamp into rows with
the same post-rename stem). `raw_outputs` is keyed by `(raw_file_id,
output_file_id)` and would happily accept both, producing a spurious
many-RAWs-to-one-output link. The strategy must refuse to auto-link
when the RAW-side key resolves to more than one RAW.

Implementation: build a RAW-side index `raw_key_to_ids:
Dict[(stem, dt), List[raw_id]]` in the same pass that already indexes
the RAW-side data. For each output lookup, only propose a link when
the RAW list has length 1. Length >1 gets logged (`WARNING` with both
RAW ids and the key) and skipped — it is a signal the operator should
investigate, not auto-resolve.

```python
# Strategy 5: editor-export identity
# output.orig_name_stem == raw.dest_path_stem (the RAW's current on-disk
# name, which is what the editor saw at export time) AND matching
# capture_datetime. No camera-model requirement: equal stem + equal
# capture time is already a stronger signature than strategy 4's
# datetime+camera pair, and some third-party exports don't carry the
# Model EXIF forward.
key = (raw_current_stem, raw_dt)
raw_candidates = raw_key_to_ids.get(key, [])
if len(raw_candidates) > 1:
    logging.warning(
        "Strategy 5 ambiguous: multiple RAWs share key (stem=%r, dt=%s): %s "
        "— skipping auto-link.",
        raw_current_stem, raw_dt, raw_candidates,
    )
elif key in output_stem_dt_index:
    for out_id, out_name, out_dt_str, out_camera in output_stem_dt_index[key]:
        proposed_links.append((
            raw_id, raw_name, out_id, out_name,
            raw_dt_str, out_dt_str, raw_camera or '', out_camera or '',
            100, 'editor_export_identity'
        ))
```

The index is built over outputs keyed by `(normalized orig_name stem,
capture_datetime)`. RAW side uses `_current_stem` (reads `dest_path` when
present). Parent-dir is deliberately *not* part of the key — it would
bake in assumptions about where any given editor puts its exports.

**No DPP-suffix stripping.** The `_NNNN` handling in an earlier draft of
this plan solved the wrong drift point: it tried to recover the match
*after* the output's planned `dest_path` was normalized. Matching on
pre-normalization `orig_name` makes that unnecessary and lifts the
confidence to 100.

### Change 4 — Dual-key hash lookup in rename detection

**Problem:** 44% of this catalog (44,434 rows, 33,487 of them RAWs) is
sparse-hash-only — `files.hash=NULL`, only `sparse_hash` populated. For those
rows, `hash_to_file` contains only a sparse entry. The hasher on
rename-detection escalates to full hash because the sparse collides with
the row's own sparse entry in `known_sparse_hashes`. `hash_value =
full_hash` then misses, and the renamed file is misclassified as NEW.
The 3 CR2s in the user's workflow happened to be in the 22% "both hashes"
subset, which is why sync classified them correctly — but this is the
primary correctness blocker for any DPP-style workflow on the other 44%.

**Fix:** dual-key lookup at
[path_sync.py:211](photo_organizer/sync/path_sync.py#L211):

```python
matches: List[Tuple[int, str]] = []
seen: Set[int] = set()
for key in (hash_result.full_hash, hash_result.sparse_hash):
    if not key:
        continue
    for file_id, dest_path in hash_to_file.get(key, []):
        if file_id not in seen:
            seen.add(file_id)
            matches.append((file_id, dest_path))
```

Behavior unchanged when the catalog has the full hash on file. Recovers the
sparse-only case.

### Change 5 — Persist full hash when rename match came via sparse (with conflict handling)

**Problem:** if Change 4's dual-key lookup matched via `sparse_hash` but the
hasher had also computed `full_hash` (collision path), the catalog's
`files.hash` remains NULL. Next run will go through the same
sparse-collision-fallback-to-full dance.

**Uniqueness constraint:** `files.hash TEXT UNIQUE` at
[schema.py:42](photo_organizer/database/schema.py#L42). A naive upgrade can
collide if two sparse-only rows turn out to share a full hash (true content
duplicates that were never resolved because each was sparse-unique at its
own ingest time). The upgrade must not crash the pipeline on that case.

**Fix:** `update_dest_path_atomic` accepts an optional `full_hash` parameter
and attempts the upgrade with conflict handling:

1. **Lookup priority (Change 4 refinement):** when both full and sparse
   lookups hit, prefer the full-hash match. Full-hash identity is stronger
   than sparse; if a row already has the full hash we just computed, that
   row is the correct rename target. This makes sparse-match-then-upgrade
   a disjoint case from full-match.
2. **Upgrade path, conflict-safe:**

   ```python
   try:
       cur.execute(
           "UPDATE files SET hash = ? WHERE id = ? AND hash IS NULL",
           (full_hash, file_id),
       )
   except sqlite3.IntegrityError:
       # Another row already owns this full hash — the match we just
       # applied via sparse is a content duplicate of an existing row.
       # Leave files.hash NULL on this row and log for later audit.
       logging.warning(
           "Full-hash upgrade blocked by UNIQUE conflict on file_id=%d "
           "(full_hash=%s). Row remains sparse-only; likely duplicate of "
           "another catalog row.",
           file_id, full_hash,
       )
   ```

   Alternative formulation using a `NOT EXISTS` guard achieves the same
   effect without exception handling but is harder to observe — the log
   line is deliberate. Tier 2's identity-confidence work can upgrade this
   to a structured `possible_duplicate` marker.

3. The caller in `_scan_and_match` passes `hash_result.full_hash` when it is
   populated, regardless of which key the lookup matched on.

Small, additive, strengthens identity confidence over time — across the
44,434 sparse-only rows, every rename through this path is a free upgrade
opportunity. Conflict case is rare but must be graceful.

---

## Code change summary

| File | Change |
|---|---|
| `photo_organizer/sync/path_sync.py` | Dual-key lookup (C4). Split detect/apply into two methods. Apply records full_hash when available (C5). |
| `photo_organizer/database/ops.py` | `get_pending_moves_for_ids(ids)`. `update_dest_path_atomic` accepts optional `full_hash`. |
| `photo_organizer/organization/mover.py` | `execute(file_ids, move_mode, dry_run)`. Use `get_pending_moves_for_ids`. |
| `photo_organizer/organization/rules.py` | `plan_all(dest_root) -> Set[int]` returns assigned file_ids. |
| `photo_organizer/pipeline/discovery.py` | `CataloguedTreeDiscoverer` stops calling `apply_renames` in Phase A. Both discoverers populate `DiscoveryResult.imported_file_ids` (untracked: every upserted id; catalogued: `sync_report.imported_file_ids`). New `apply_deferred_changes(db_ops, conn, result) -> Set[int]` on both discoverers: no-op on untracked, calls `apply_renames` on catalogued. `DiscoveryResult` gains `sync_report` field. |
| `photo_organizer/core.py` | `_run_pipeline` threads `PipelineCandidates` through Phase B. Invokes `discoverer.apply_deferred_changes(...)` as the first savepoint step. Passes candidate set into every Phase B writer. Broadens `skip_if_no_imports` guard to also check `sync_report.renames`. |
| `photo_organizer/metadata/linking.py` | Query `dest_path`. Derive stem via `_current_stem`. Add strategy 5. Scope `link_raw_sidecars`, `link_raw_sidecars_by_dest`, `link_psds`, and `link_raw_outputs` by candidate file_ids. |
| `photo_organizer/models.py` (or `pipeline/candidates.py`) | New `PipelineCandidates` dataclass. |

No schema migration. No new tables. No `command_runs` changes. No CLI
surface changes.

---

## Tests

### `tests/test_path_sync.py`

**Fixture note:** tests that exercise sparse-hash logic must either (a) use
real fixture files large enough to cross `SPARSE_HASH_THRESHOLD` and let
`FileHasher` compute the actual sparse hash, then seed the catalog with
that same value, or (b) monkeypatch `FileHasher.compute_hash` to return a
known `HashResult`. Seeding a fabricated `'s-abc'` against a small fixture
file will compute a real sparse hash on disk that doesn't match, and the
lookup miss will be for the wrong reason. Prefer (a) for realism; use (b)
for speed when the hash itself isn't the thing under test.

1. **Detect-only mode leaves DB unchanged.** Call new detect-only variant,
   verify rename is in the report but `files.dest_path` unchanged and no
   `file_occurrences` writes.
2. **`apply_renames` applies reported renames.** Build a `SyncReport` with a
   `RenameRecord` manually and verify `dest_path` is updated.
3. **Dual-key lookup — sparse-only catalog row.** Seed a large fixture
   file; compute its real sparse hash; seed the catalog row with
   `hash=NULL, sparse_hash=<real>`; rename the file on disk under the same
   parent; call detect; verify `len(report.renames) == 1`.
4. **Full-hash upgrade on sparse-fallback match.** Same setup as 3; after
   apply, verify `files.hash` is populated with the full hash.
5. **Full-hash upgrade conflict is logged, not fatal.** Seed two rows with
   distinct sparse hashes but (artificially) the same full hash on the
   second row. Rename the first row's file. Apply. Verify: first row still
   has `hash=NULL`, warning logged, no exception raised, `dest_path`
   updated as expected.

### `tests/test_mover_scope.py` (new)

6. **Mover ignores rows outside candidate set.** Seed two cataloged files,
   one with its dest file missing (stale). Call
   `mover.execute(file_ids={<other>}, …)`. Verify the stale row is not
   touched — `planned=0` for that branch.
7. **Mover processes only candidates.** Seed one legit pending move, pass it
   in. Verify `planned=1`.

### `tests/test_linking.py`

8. **Renamed RAW: current-stem match via dest_path.** RAW `orig_name`
   `IMG_1234.CR2`, `dest_path=.../renamed_shot.CR2`. JPEG `orig_name`
   `renamed_shot.JPG`, same datetime. Expect strategy-5 link
   (confidence 100, method `editor_export_identity`).
9. **Editor-export identity fires across parent dirs.** Same setup as 8,
   but the JPEG's parent dir is `.../jpeg/` while the RAW's is `.../raw/…`.
   Expect the same strategy-5 link — location independence.
10. **No capture_datetime: no strategy-5 match.** JPEG has `capture_datetime
    = NULL`. Expect strategy-5 does not fire (the deterministic signature
    requires both sides of the key). Earlier strategies may still match on
    other signals — the assertion is only that strategy 5 stays silent.
11. **Burst-shot no cross-link.** Two RAWs at same `capture_datetime`,
    `dest_path` stems `A` and `B`. JPEG `orig_name` stem `A`, same
    datetime. Expect exactly one strategy-5 link: (RAW_A, JPEG). B
    unchanged.
12. **Strategy-5 ambiguity guard.** Two RAWs with the SAME `dest_path` stem
    AND same `capture_datetime` (pathological but possible if a user
    reuses stems or two cameras triggered identical timestamps). One
    JPEG with that stem + datetime. Expect: zero strategy-5 links
    proposed, one WARNING logged naming both RAW ids and the
    `(stem, dt)` key. Neither RAW gets an auto-link.

### `tests/test_pipeline_integration.py` (new)

13. **Ingest-dest dry-run against renamed RAW + new JPEG.** Build a catalog
    with one RAW (dest_path old name), rename that RAW on disk, add a new
    JPEG next to it with the same stem as the RAW's new name and matching
    `capture_datetime`. Run `ingest_dest(dry_run=True)`. Verify **all** of
    the following:
    - `stats["renamed"] == 1, stats["imported"] == 1`.
    - `stats["planned"] == 1` — only the JPEG route into `output/`.
    - `stats["skipped"]` includes the renamed RAW (its new `dest_path`
      exists on disk, so the mover skips it — this is correct behavior,
      not a resurrection). The test should assert `skipped >= 1` and
      inspect `mover`'s logged actions to confirm the skipped row's dest
      path is the *new* name, not the old one.
    - **Zero planned actions target the old RAW filename.** Explicitly
      query any "planned" entry and assert `old_RAW_dest_path` does not
      appear as a `dest` target. This is the resurrection-proof assertion.
    - Catalog unchanged after rollback: `files.dest_path` for the RAW
      still the old name (rolled back), `raw_outputs` unchanged, no new
      `file_occurrences` for the JPEG's proposed output path.
    - `link_raw_outputs` was called with `candidate_output_file_ids={new_jpeg_id}`
      — verified via spy or by asserting the proposed-link count in the
      log is 1, not global.

    Test 13 is the integration regression for the exact bug the user
    hit, with assertions precise enough to distinguish "skipped
    renamed RAW" (correct) from "planned old-name resurrection" (the
    bug).

14. **Rename-only ingest-dest run proceeds into Phase B.** Build a catalog
    with one RAW (dest_path old name). Rename it on disk. Do NOT add any
    new files. Run `ingest_dest(dry_run=False)`. Verify:
    - The pipeline does NOT hit the early return — Phase B runs.
    - `stats["renamed"] == 1`, `stats["imported"] == 0`.
    - After commit, `files.dest_path` for the RAW reflects the new name.
    - `apply_renames` was called exactly once (spy or instrumentation).

    This locks in the broadened early-return condition from Change 1.

15. **App-level: Phase B failure after apply_renames rolls back the
    rename.** Drive `PhotoOrganizerApp.ingest_dest()` directly,
    `dry_run=False`. Same setup as 13. Monkeypatch `FileMover.execute`
    to raise after renames have been applied and linking has run.
    Expect:
    - The exception propagates out of `ingest_dest()`.
    - `files.dest_path` for the RAW is the OLD name — Phase A
      committed scan data, but rename application now lives inside
      the Phase B savepoint and rolls back with it.
    - No `raw_outputs` rows inserted.
    - No new `file_occurrences` rows for the post-rename path beyond
      what Phase A wrote.

    This locks in the deliberate real-run atomicity change: rename
    application is no longer independently committed in Phase A, so a
    later Phase B failure now rolls back the rename too. The prior
    behavior would have persisted the rename and left the catalog in a
    half-applied state.

### `tests/test_main_wrapper.py` (extend)

16. **CLI-level: Phase B failure records `exit_status='error'` in
    `command_runs` with `db_mutates=False`, `files_mutate=False`.**
    Drive through `main()` (not `PhotoOrganizerApp` directly) because
    `command_runs` is written by `RunRecorder` in
    `photo_organizer/main.py`, not by the app. Use the same failure
    injection as test 15, invoke via `_invoke_main(..., ["photo-organizer",
    "--ingest-dest", dest, "--db", db_path])`. Assert the row's
    `exit_status`, `db_mutates`, and `files_mutate` columns.

    Splitting the app-level rollback assertion (test 15) from the
    CLI-level recorder assertion (test 16) keeps each test honest
    about what layer it's exercising.

### `tests/test_pipeline_integration.py` (add)

17. **Rerun after failed dry-run — pre-existing orphan JPEG re-enters
    the candidate set.** Regression for the working-set-not-created
    contract in Change 1. Setup mirrors the user's post-failed-dry-run
    catalog state:
    - RAW row in the catalog (dest_path = *new* name, already
      applied by a prior real `--sync-dest`).
    - JPEG row already in the catalog with `dest_path IS NULL` and
      `ingest_mode=1` — left over from a previous failed ingest-dest
      dry-run. Its `orig_path` points at the on-disk JPEG sitting
      next to the renamed RAW.
    - No net-new files on disk since that prior run.

    Run `ingest_dest(dry_run=True)`. Verify:
    - The discoverer re-upserts the JPEG; `upsert_file_record`
      returns the existing id.
    - That id is in `result.imported_file_ids` (working-set contract
      — not "created this run").
    - `stats["imported"] == 1` (the JPEG counts as this run's
      imported working set even though no new row was created).
    - Phase B's scoped `link_raw_outputs` is invoked with the JPEG's
      id in `candidate_output_file_ids`.
    - A proposed strategy-5 link (RAW → JPEG, confidence 100,
      `editor_export_identity`) appears in the preview CSV.
    - `stats["planned"] == 1` — the JPEG gets a planned
      `output/…` dest_path.
    - After rollback, catalog state is unchanged (JPEG still
      `dest_path IS NULL`, no `raw_outputs` row).

    This is the exact scenario that prompted this plan; without test
    17, the working-set contract can silently regress into a
    "created-only" implementation that would re-break the user's
    catalog.

---

## Order of work

1. **Mechanical refactor — detect/apply split + candidate set plumbing**
   (Changes 1, 2). Dry-run behavior shifts from "renames invisible to
   Phase B" to "renames visible and rolled back with the savepoint".
   Real-run behavior also shifts in one intentional way: rename
   application moves inside the Phase B savepoint, so a later
   planner/linker/mover failure now rolls back the rename. This is the
   right atomicity (either the whole run takes effect or none of it
   does), but it is a real change from today's "rename committed
   eagerly in Phase A" behavior and is covered by test 15.
2. **Linker fixes** (Change 3), including the ambiguity guard covered
   by test 12.
3. **Defensive hash fixes** (Changes 4, 5).
4. **Tests 1-17.**
5. **Rerun the user's `--ingest-dest --dry-run --move` against the real
   library.** Expected stats: `renamed=3, imported=3, planned=3`. No
   resurrection plan. Linker proposes 3 new links via strategy 5 at
   confidence 100.
6. **If the dry-run verifies clean, run real** `--ingest-dest --move`. This
   should persist the renames, import the 3 JPEGs with `output/…`
   destinations, link them to their RAWs at confidence 100, and move them.
7. Commit as a single PR off `feature/catalog-maintenance`:
   `feature/ingest-dest-linking-fixes`.

---

## Open questions for review

- **Strategy-5 confidence = 100.** This is a deterministic signature
  (equal pre-normalization stem + equal capture_datetime), so it should
  sit at the top of the confidence band. Worth sanity-checking that
  nothing downstream assumes "confidence < 100 means heuristic".
- **`apply_renames` commit semantics.** Proposal: no internal commit; rely on
  the outer savepoint / pipeline commit. Alternative: commit internally on
  `real` runs to match today's `auto-sync` behavior. I prefer the savepoint
  route — it collapses the "when does Phase A end" question cleanly.
- **`PipelineCandidates` location.** Stand-alone module, or live in
  `core.py`? Leaning stand-alone (`pipeline/candidates.py`) so Tier 2 can
  promote it to a `RunContext` without touching core.
- **Do we need Change 5 (full-hash upgrade)?** Given the 44% sparse-only
  population, this is more valuable than I initially framed it: every rename
  through Change 4's dual-key path is an opportunity to strengthen identity
  on a row that currently can't be matched by full-hash lookup. It gradually
  raises the "both hashes" percentage over time with zero extra I/O (full
  hash was already computed during the sparse collision fallback). I'd keep
  it in.

---

## What Tier 2 looks like (so we know what we're deferring)

Not in this PR. Noting here so the boundary is explicit:

- `run_actions` table as durable plan/apply record, replacing the in-memory
  `PipelineCandidates`.
- `run_observations` table — only if a future command (face detection?)
  genuinely needs resumable scans or cross-run diffs. Requires an explicit
  retention policy.
- `RunContext` object promoted from `PipelineCandidates`, passed instead of
  loose kwargs.
- `created_by_run_id` columns on `raw_outputs`, `raw_sidecars`,
  `psd_source_links`, and (incoming) face tables.
- Identity-confidence policy hardening; sparse-hash-as-identity downgraded
  to sparse-hash-as-candidate.

If Tier 1 lands cleanly and the mover-scope work feels natural, Tier 2 is a
smaller incremental step than it looks.

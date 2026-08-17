# Phase 0A Pre-Registration — Age-Banded Template Spike

Status: **draft, not yet locked**. 2026-07-30.
Gates Phase 0A of `PLAN_face_identity_enrollment.md` (§8).

This document fixes the experimental procedure **before** the experiment runs.
Once locked, §§3–9 do not change; only §10 (threshold values) is filled in from
the profiling pass, and it is filled in once.

Phase 0A is **read-only**: a script opening the catalog read-only, writing
nothing to the database. Outputs are files under a results directory.

---

## 1. What this gates

**Question:** does conditioning person templates on age carry real signal on
*this* catalog, over and above simply giving each person more prototypes?

The plan's remaining 8 phases assume it does. If it does not, most of that work
is unjustified — but §9 defines four distinct "no" outcomes, and three of them
still leave valuable, much cheaper work on the table. This is not a binary
go/no-go; it identifies **which part of the plan is load-bearing**.

---

## 2. Reproducibility manifest

Recorded in the results directory before any metric is computed:

| Field | Source |
|---|---|
| Catalog snapshot | DB path, file size, SHA-256, capture date of the copy |
| Schema version | `schema_version` table |
| Code commit | `git rev-parse HEAD`, dirty-tree flag |
| Embedding model | `model_name` / `model_version` (`buffalo_l` / `buffalo_l_v1`) |
| Random seed | Fixed constant, recorded |
| Split identity | Hash of the ordered fold assignment |
| Every SQL query | Verbatim, with bound parameters |
| Row counts | Per tier, per filter, per fold — before and after each exclusion |

The analysis runs against a **copy** of the catalog, not the live database.

---

## 3. Data selection and label trust tiers

`get_labeled_person_embeddings` unions detection-level and cluster-level links,
which would mix human ground truth with machine inference. Phase 0A separates
them using `face_person_links.link_method` and link shape:

| Tier | Definition | Use in 0A |
|---|---|---|
| **A — direct human** | `detection_id IS NOT NULL` AND `link_method IN ('photo_label','manual_review')` AND `status='accepted'` | **Primary evidence.** Templates and evaluation both drawn from here |
| **B — human-approved machine** | `detection_id IS NOT NULL` AND `link_method IN ('auto_assign_accept','merge_accept')` | Secondary run only; reported separately, never pooled with A |
| **C — cluster-derived / absorbed** | `cluster_id IS NOT NULL`, or `link_method='merge_absorb'` | **Excluded from the primary result.** Reported as a sensitivity run |

The headline number is Tier A only. If Tier A is too small to support the
analysis, that is itself a finding and routes to outcome **E** (§9).

**Additional filters**, each with before/after counts recorded:

- `face_detections.status = 'observed'` (excludes not-a-face, depiction, doll)
- `confidence >= MIN_WORKING_DET_SCORE` (0.7)
- Person has a `birth_date` (required to compute a band at all)
- Detection has a usable capture date (see §4.2)

**RAW files are out of scope.** `--include-raw` is *not* used for this or any
prior scan (0 RAW detections as of 2026-07-21), and must not be enabled for Phase
0A. Three independent reasons: unprocessed RAWs carry no human labels, so they
add zero evaluation data; rawpy/LibRaw applies the camera's embedded orientation
flag rather than any later DPP4 correction, so RAW-derived embeddings can be
misoriented with no available fix — the same defect §4.1 excludes; and such
detections are destined for retirement once their JPEG/TIFF export is linked
(`retire-superseded-raw-detections`). The RAW backlog is an **enrollment
coverage** question, not a Phase 0A question — see §12 note.

---

## 4. Confounds and their handling

Three known contaminants. Each is measured, reported, and excluded from the
primary analysis.

### 4.1 Orientation-era detections

Detections computed before the EXIF orientation fix (`a2c57de`, 2026-07-20) may
have been embedded from wrongly-rotated pixels. `face_detections.created_at` and
`observed_by_run_id` date every detection.

**Handling:** detections created before the orientation fix, and not superseded
by a `rescan-misoriented` run, are **excluded from the primary analysis** and
reported as a separate stratum. If exclusion removes most of the evidence, that
routes to **E**.

### 4.2 Date provenance — mtime fallback

`filesystem.py:181-182` substitutes file mtime when EXIF yields nothing, so
`media_metadata.capture_datetime` silently mixes true capture time with
filesystem time. A face whose date came from mtime has an **unreliable band
assignment**, which is precisely the variable under test.

**`files` has no `mtime` column**, so provenance cannot be reconstructed from the
database. Definitive method, cheap at spike scale:

> Re-extract EXIF `DateTimeOriginal` for every file in the sample. A photo whose
> stored `capture_datetime` has no EXIF backing is marked `date_source=fallback`.

Fallback-dated detections are **excluded from the primary analysis** and reported
as a stratum. Their count is a deliverable in its own right — it sizes the
scanned-print problem and directly informs Phase 2's date work.

### 4.3 Same-person label propagation

Tier A excludes cluster-level links, but a `photo_label` applied with the
whole-cluster checkbox on (`streamlit_app.py:1143`, default true) writes a
detection-level link for the visible face **and** a cluster-level link for the
rest. The detection-level row is genuine; the cluster-level rows are Tier C and
already excluded. No further handling needed, but the ratio is reported — it
sizes how much of the existing corpus is compound rather than atomic.

---

## 5. Splitting protocol

**Split unit: the calendar day.** All detections from photos captured on the same
day go to the same fold.

Rationale: 15-minute events (`EVENT_GAP_MINUTES`) under-split. A birthday party
spanning three hours fragments into several "events" that share one haircut, one
outfit, one lighting setup, one camera — train/test leakage that would inflate
every variant equally but flatter the whole exercise.

- **5-fold cross-validation** over days, stratified so each person's days are
  distributed across folds where possible.
- A person needs days in **≥2 folds** to be evaluable; persons below that are
  reported as unevaluable rather than silently dropped.
- Templates are built from training-fold days only; evaluation uses held-out days
  only. No detection appears on both sides.
- **Sensitivity analysis**: repeat at 15-minute event grain and report the delta.
  A large gap between day-level and event-level results is itself evidence of
  leakage sensitivity and gets reported.

---

## 6. Model variants

Four template models × {date gate on, off} = 8 configurations.

| # | Model | Templates per person |
|---|---|---|
| M1 | Lifetime single mean (current behavior) | 1 |
| M2 | Age-agnostic multi-medoid | `K · B_p` |
| M3 | Age-banded mean | 1 per occupied band = `B_p` |
| M4 | Age-banded multi-medoid | `K` per occupied band = `K · B_p` |

### 6.1 Equal template budget — the definition that makes M2 vs M4 fair

For person `p` with `L_p` training faces occupying `B_p` distinct age bands:

- **M4** gets `K` medoids per occupied band → `K · B_p` total.
- **M2** gets **exactly `K · B_p` medoids**, chosen by k-medoids over the
  person's *entire* training set with no age information.

Budget is matched **per person**, not globally. Without this, M4 could win purely
by having more prototypes, and the result would be uninterpretable — this is the
single most important control in the design.

`K = 3` for the primary run; `K ∈ {1, 3, 5}` reported as a sensitivity sweep.

### 6.2 Scoring

- Cosine similarity, **full 512 dimensions** (no PCA — `CLUSTER_PCA_DIMS=64` was
  validated for HDBSCAN cost, not for exact identity matching).
- Score against a person = **max over that person's medoids**.
- Max-over-medoids favors persons with more templates; because budget is matched
  per person in §6.1, the bias is equalized across M2/M4. Reported anyway as a
  per-person template-count vs accuracy correlation.
- Adjacent-band scoring at a penalty is **disabled** for the primary run — it is a
  tuning knob and would confound the structural question. Reported as a
  sensitivity run.

### 6.3 Bands

Bands as specified in the plan §5.3. Band edges are **an input to this
experiment, not an output** — the per-band accuracy breakdown (§7) informs
whether the edges are placed sensibly, but the boundaries are not tuned against
the test folds.

### 6.4 Date gate

Off: score against all enrolled persons.
On: score only against persons whose birth date precedes the capture date
(conservative edge of the birth-precision interval).

---

## 7. Metrics

**Primary — the number the decision rests on:**

> Top-1 accuracy on the **hard-confusion slice**: held-out detections whose true
> person belongs to a known confusable pair, scored against the *full* enrolled
> roster.

### 7.1 The confusable set — two opposite confusion types

Confusion here is **group-structured, not pairwise**, and it comes in two kinds
that behave in opposite ways under this plan's mechanism.

**Family structure (as supplied):**

| Family | Children |
|---|---|
| Griffin (principal) | Hannah, Emma, Ava, Evelyn — four sisters |
| Griffin (cousins) | Devon, Alycia, Marshall, **Cora** — Cora youngest, ~1y older than Hannah; the other three are not age-proximate to the sisters |
| Keener (cousins) | **Robbie** (~6 weeks from Hannah), **Thomas** (~6 months younger than Emma), **Josie** (born between Ava and Evelyn) |

**Type 1 — within-family, age-separated.** The four sisters. Same genetics, same
household, same events. Age *does* separate them at any given capture date, so
band selection is exactly the right mechanism, and the lifetime-mean anchor is
exactly what breaks it. Co-occurrence provides nothing (they are always
together).

**Type 2 — cross-family, age-matched.** Hannah↔Robbie, Emma↔Thomas, and partly
Ava↔Josie↔Evelyn. These pairs sit in the **same band for their entire lives**, so
band selection provides **zero separation** — the plan's core mechanism does not
apply. What must carry the load instead is raw embedding discriminability plus
household co-occurrence (different families, so context genuinely separates them
outside family gatherings).

The two types are complementary, and the system needs both signals for that
reason: **age separates Type 1 where context can't; context separates Type 2
where age can't.**

Sex is expected to make Type 1 cross-family pairs (Hannah/Robbie, Emma/Thomas)
tractable at most ages — but **infancy is the genuine worst case**: same age to
within weeks, and infant faces carry weak sex cues. Ages 0–2 for those pairs is
reported as its own stratum.

### 7.1.1 Evaluation strata

Metrics are reported per stratum, because a single pooled number would let Type 1
success mask Type 2 failure:

| Stratum | Members | Banding should help? |
|---|---|---|
| **S1** | Hannah, Emma, Ava, Evelyn | **Yes** — primary test of the mechanism |
| **S2** | Hannah↔Robbie, Emma↔Thomas | **No** — tests embedding + context instead |
| **S2-infant** | S2 pairs, ages 0–2 | **No** — expected worst case overall |
| **S3** | Ava, Josie, Evelyn | Partially — Josie is age-between, not age-matched |
| **S4** | Hannah, Cora | Partially — ~1y apart |
| **S5** | Raylan, Colt | **Graded** — see below |

Union: **11 people** (Hannah, Emma, Ava, Evelyn, Cora, Robbie, Thomas, Josie,
Raylan, Colt, plus any added below). Members may be added **before locking**,
never after.

**S5 is a graded prediction, and the sharpest test of band placement.** Raylan
and Colt are brothers ~12 months apart (Devon Griffin's sons, both already
labelled). Band selection should separate them *while the bands are narrower than
their age gap* — at ages 1.5 and 2.5 they fall in `(1,2)` and `(2,4)` — and stop
helping once bands widen past 12 months, since at ages 4 and 5 both sit inside
`(4,7)`. A banding benefit that decays with age exactly at that crossover is
strong confirmation the mechanism is real rather than an artifact; a benefit that
persists uniformly would be suspicious.

### 7.1.3 Registered but not evaluable in 0A

Known people with no or negligible labelled faces. Excluded from the primary
metric — recorded so the result is not later over-read as covering them, and so
the design cases are not discovered late:

| Person | Status | Design significance |
|---|---|---|
| **Devon's twin daughters** (b. Feb 2024) | No faces yet | **The pure permanently-confusable case**: identical age, same genetics, same household. *Neither* age banding nor co-occurrence separates them, by construction. Must be handled as an explicit, declared confusable pair routed to a human — not left to emerge from low margins |
| Devon's stepson | No faces yet | No genetic confusion risk; a cold-start enrollment case |
| Alycia's daughter (b. Jun 2025) | Sparse | Cold-start; `data_exhausted` band states likely |

These are also the people most affected by the **un-ingested phone photos** — the
collection has not yet absorbed phone sources, and that is where the sparse
recent coverage largely lives. Phase 0A says nothing about any of them.

### 7.1.2 The set is also derived programmatically

Hand-enumeration does not scale — the roster is ~40 and growing, and every future
sibling group creates a new Type 1 set. So the confusable set is **also** computed
by rule, and the hand-listed members serve as a validation check that the rule
finds them:

- **Type 1 candidates**: any two people in the same family whose photo coverage
  overlaps.
- **Type 2 candidates**: any two people whose birth dates fall within a tolerance
  (start: 1 year), regardless of family.
- **Empirical candidates**: any two people whose band templates exceed a
  similarity threshold in the profiling pass.

If the rule fails to surface a hand-listed pair, that is a finding about the rule
and is reported. Any *additional* pairs the rule surfaces are reported but do not
enter the primary metric for this run — they are candidates for the next one.

Devon, Alycia and Marshall are excluded from the primary set as not age-proximate
to the sisters, but the Type 1 rule will evaluate them against Cora, and that
result is reported.

Aggregate accuracy is *not* the primary metric. It would be dominated by easy
adult faces and could look excellent while the sisters remain broken — which is
the entire problem this plan exists to solve.

### 7.2 Matched-age confusion — the mechanism metric

Sibling confusion has a specific temporal shape, and it is exactly what age
banding is supposed to break:

> Emma at age 6 (2012) and Hannah at age 6 (2009) are two different people who
> look maximally alike. A lifetime-mean anchor **creates** this confusion by
> construction — Hannah's mean absorbs her age-6 faces, so Emma-at-6 matches it.
> Under banding, a 2012 face is scored against Emma's age-6 templates and
> Hannah's **age-9** templates, because Hannah was 9 in 2012. The comparison that
> causes the error is never made.

Reported as its own metric:

- **Matched-age cross-person confusion rate** — for held-out faces of person X at
  age `N`, the rate of assignment to person Y who was age `N` at a *different*
  date. Computed per confusable set, per model variant.
- **Age-matched negative pairs** — explicitly constructed (Hannah@N, Emma@N) etc.
  across all set members and all overlapping ages, with the similarity
  distribution under each variant.

If banding works at all, it works here. If M4 does not beat M1 on this metric,
outcome **D** is the honest reading regardless of what aggregate numbers say.

### 7.3 Secondary metrics, all reported

- Overall top-1 accuracy, all evaluable persons
- Top-2 margin distribution (the quantity the future routing rule depends on)
- Per-person and per-band accuracy, with `n` for each cell
- Coverage: fraction of held-out detections scorable at all
- **Full confusion matrix over the confusable set** — promoted from secondary
  diagnostic to a primary deliverable, since group structure means the *pattern*
  of errors matters as much as the rate

### 7.4 Ground-truth risk in this slice

The confusable set is where existing Tier A labels are **least reliable**: the
periods flagged as hardest (similar height, similar hair) are periods where the
human labeller was also most likely to err. The evaluation ground truth is
therefore weakest exactly where the test is sharpest.

Consequences:

- A disagreement between model and label in this slice is **not automatically a
  model error**; the confusion matrix must be read with that in mind.
- Phase 0B's curated slice should be pointed **specifically at these 8 people**,
  and at the matched-age pairs in §7.2, rather than sampled uniformly.
- If the primary result is close to the decision boundary, outcome **E** is the
  correct call — the labels cannot carry a marginal verdict.

---

## 8. Open-set protocol

Separation among known people does not establish that strangers get rejected.
Unlabeled faces cannot serve as impostors — they may be unlabeled family.

**Leave-one-person-out (LOPO).** For each enrolled person `q`: remove `q`
entirely from the template set, then score `q`'s held-out faces against the
remaining roster. Every one of those scores is a **true impostor score** — the
correct answer is always "none of these people."

Reported: false-accept rate across a threshold sweep, and the score distribution
of impostors against the distribution of genuine matches.

**Limitation, stated up front:** LOPO impostors are family members, so they are
*harder* than a random stranger — genetically similar, similar era, similar
photographic context. This makes the LOPO false-accept rate a **conservative
upper bound**, which is the useful direction for a safety metric, but it is not
the same as measuring rejection of true strangers. Genuine stranger measurement
requires manually confirmed non-family faces and belongs to Phase 0B.

---

## 9. Decision rules

Three deltas, all computed on the primary metric (§7), Tier A, primary filters:

```
Δ_band     = M4 − M2     (equal budget)   ... does age structure help?
Δ_capacity = M2 − M1                      ... does more capacity alone help?
Δ_gate     = (variant, gate on) − (same variant, gate off)
```

### 9.1 The test, fixed now — no numbers required

`Δ_band` is compared against a **band-shuffled null distribution**: rebuild M4
after randomly permuting which band each training face is assigned to (preserving
each person's band-size profile), recompute, repeat **1,000 times**. This is what
"age banding carries no information" looks like on this exact data.

**Condition 1 (statistical):** observed `Δ_band` exceeds the 95th percentile of
the shuffled null.

This is why no threshold value is needed in advance — the null distribution
calibrates itself from the data.

**Condition 2 (practical):** `Δ_band` corresponds to at least **`T` additional
hard-confusion faces resolved correctly per 1,000 evaluated**. `T` is the one
value set in §10, and it is a judgment about worth, not statistics — large `n`
can make a trivial effect statistically significant.

### 9.2 Outcomes

| | Condition | Interpretation | Action |
|---|---|---|---|
| **A** | Both conditions met | Age structure carries real signal | **Proceed with the plan as written** |
| **B** | `Δ_band` fails, `Δ_capacity` large | Multi-prototype helps; *age* doesn't | Multi-medoid templates **without** banding — much cheaper. Birth-date infra still needed for the gate (see C), not for templates |
| **C** | `Δ_gate` large, `Δ_band` small | The date **gate** carries the value, not the templates | Prioritize date intervals + hard gating (plan §5, Phase 2). Defer the template rebuild |
| **D** | All three deltas small | Model structure is not the bottleneck | **Stop and rethink.** Suspect data quality — dates, label purity, alignment — over model design |
| **E** | Tier A too small, confound exclusions gut the sample, or day-vs-event sensitivity is large | Result is not trustworthy either way | Resolve with a minimal curated slice via the **Phase 0B offline annotation tool**; do not proceed on the dirty result |

Outcomes B and C are **not failures**. They would redirect the plan toward
substantially cheaper work with most of the benefit, which is a good result for a
day's read-only analysis.

---

## 10. The one value set after profiling

`T` — the practical-significance threshold in §9.1, Condition 2.

**Procedure:** run the profiling pass, which reports the hard-confusion slice
size, the observed spread of all three deltas, and the band-shuffled null width.
`T` is then chosen **once**, recorded here with its rationale, and not revisited
after final results are computed.

Anchor for the judgment: the hard-confusion slice is expected to be small
(hundreds of faces), so `T` should be expressed in absolute resolved faces, not
percentage points.

Nothing else in this document is set after the fact.

---

## 11. What Phase 0A cannot establish

Stated so the result is not over-read:

1. **Not a deployment clearance.** Tier A labels came through the existing UI,
   which has known contamination paths. Clean validation is Phase 0B.
2. **Not a stranger-rejection measurement.** §8 gives a conservative family-only
   bound.
3. **Not a band-boundary optimization.** Edges are an input; tuning them against
   these folds would invalidate the test set.
4. **Not a statement about faces excluded by the confound filters** — orientation-
   era and fallback-dated detections are exactly the population most likely to
   behave differently.
5. **Not a joint-assignment result.** Per-detection scoring only; no same-photo
   constraints, no tracklet consistency, no co-occurrence.
6. **Says nothing about the durability, provenance, or workflow work**, which is
   justified independently of whether banding wins.

---

## 12. Deliverables

1. Manifest (§2).
2. Cohort report: Tier A/B/C counts, per-person and per-band `n`, confound
   exclusion counts, unevaluable persons.
3. **Date-provenance report** — how many sampled photos are EXIF-dated vs
   fallback. Feeds Phase 2 directly and is useful regardless of outcome.
4. Results table: 8 configurations × primary and secondary metrics, with `n`.
5. Three deltas with the band-shuffled null distribution.
6. LOPO open-set curve.
7. Sensitivity runs: `K ∈ {1,3,5}`, event-grain split, adjacent-band scoring,
   Tier B inclusion.
8. **One-page verdict** naming the outcome (A–E) and the decision that follows.

---

## 13. Lock

Locked by: _(pending)_  Date: _(pending)_

Once locked, changes to §§3–9 invalidate the run and require a fresh
pre-registration with a new manifest.

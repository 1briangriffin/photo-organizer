# Face Identity: Enrollment-First Architecture

Status: **conditional go** (revision 3), 2026-07-30. Supersedes revisions 1–2 and
the "Not yet started" items 3–5 of the face roadmap; re-frames several in-flight
repair efforts on `feature/face-cluster-quality`.
`PLAN_facial_recognition_rebase_handoff.md` is complete (all six port phases
shipped, PR #22) and is now historical; its **Non-Goals** remain binding.

Revision 3 incorporates three rounds of external review plus the human-workflow
constraint. The architecture is settled and no longer under reconsideration;
revision 3 changes only sequencing and executability.

**Authorization state:**

- **Phase 0A and Phase 1 are cleared to begin** (both read-only).
- **Phase 2 does not begin** until the Phase 2A RFC closes and decisions D3–D7
  (§11) are recorded.
- **Automatic assignment is not authorized** until the §9 criteria are written
  down, met, and demonstrated.

---

## 1. Why change direction

The current pipeline derives identity as an emergent property of a merge graph
over clusters regenerated every run:

```
scan → HDBSCAN per era window → 4 gates → 3-tier merge proposals → union-find → persons
                                                                       ↑
                                                          human reviews HERE (the end)
```

Four structural properties produced the recent bug cascade. None is a tuning
problem.

**1.1 The durable object is regenerated every run.** Cluster ids churn on
re-cluster, so human decisions attach to disposable objects. Commit `01428dd`
measured it: 523 of 535 merge-audit hits referenced a cluster that no longer
exists. The accepted-cluster immutability guard exists to resist that decay and
directly caused the fossil-era bugs — accepted clusters with corrupt era metadata
acting as universal linking hubs, because `eras_linkable()` reads only
`era_start`/`era_end` and never member capture dates.

Six repair/audit commands now exist. Each is well-built; each exists because
state cannot be re-derived.

**1.2 The single lifetime anchor is wrong for the goal and degrades as labeling
proceeds.** `CrossAgeLinker._compute_anchor_embeddings` (`linking.py:700`) and
`RefinementEngine._compute_anchors` (`refinement.py:151`) both compute one
L2-normalized mean over *all* of a person's labeled embeddings across 25 years.
For anyone spanning infancy to adulthood that mean resembles neither end. Every
additional childhood label makes it blurrier.

**1.3 The strongest ground truth is used for one thing.** `birth_date` appears in
`clustering.py` (window splitting), `db_ops.py`, and the UI — and **zero times**
in `linking.py` or `refinement.py`. Nothing prevents a 2003 face from merging
into a person born in 2010.

**1.4 The human sits at the end of a proposal-on-proposal chain.** Accepting a
merge asserts two whole clusters are one person — compound, expensive to verify,
which is how the contamination in `ab6f4d2` entered.

### 1.5 The problem is smaller than the code assumes

~40 known people today (2 principals, ~8 older generation, ~7 siblings/spouses,
~11 next generation, ~11 of their children), growing as that generation has
children, plus prior generations arriving via scanned prints. This is
**closed-set classification with a stranger tail**, not open-set clustering.
Density clustering is right for *discovering* unenrolled people; it is the wrong
primary mechanism when the roster is largely known.

---

## 2. The organizing principle: architecture and human workflow meet at the photo

This is the central insight of revision 2, and it constrains everything below.

- **The model's natural inference unit is the photo/event** — that is where the
  constraints live (one-live-occurrence-per-person prior, tracklet consistency,
  co-occurrence context).
- **The human's natural review unit is the photo** — that is where the
  recognition context lives (who else is present, whose event this is, what the
  clothes and other faces say about the year).
- **The durable mutation stays atomic**: one assertion per visible face.

The existing Label Photos page (`streamlit_app.py:1056`) already renders the
whole photo, shows every detection, marks resolved faces, and saves all decisions
in one audited run. **It is the strongest surface in the system and the plan
extends it rather than competing with it.** Corollaries:

- There is **no separate confusable-review page**. `confusable` stays a routing
  outcome, a metric, and a queue filter, but it is *presented* as a highlighted
  face inside its own photo with candidates inline and already-resolved faces
  visible.
- **Enrollment samples photos, not faces.**
- **Shadow prediction lives inside Label Photos**, progressively pre-filling as
  templates form. With no templates it is exactly today's page.
- Date assertion and depiction verdicts belong on the same surface, for the same
  reason: full-photo context is what makes a human good at them.

**Photo ≠ event.** The human's immediate unit stays one photo. The model's
constraint graph may span an event via tracklets. The UI may *expose* event
neighbors (previous/next photo, other appearances in a proposed tracklet,
already-resolved participants), but confirming one visible face must never
silently assert about every tracklet member.

---

## 3. Target architecture

```
photo / event candidate
        │
        ├─ template scoring (person × band medoids)
        ├─ date-interval eligibility gate  (hard)
        ├─ cannot-link constraints         (hard)
        ├─ co-occurrence / context         (soft, calibrated)
        ▼
   candidate score matrix  (all atomic units × all eligible people + residual)
        ▼
   joint constrained assignment
        ├─ ≤1 live occurrence per person per photo (strong prior)
        ├─ tracklet consistency
        ├─ trusted assertions pinned
        └─ residual/unknown always a valid outcome
        ▼
   predictions stored with full provenance  (run-scoped, never trusted input)
        ▼
   Label Photos
        ├─ hidden-prediction benchmark sample
        ├─ visible suggestions
        ├─ human confirmation / correction
        ├─ date + depiction decisions
        └─ optional audited propagation
        ▼
   atomic human assertions  (durable)
        │
        └─ trusted templates ──────► (feeds back into scoring only from here)
```

**Scoring and inference are separate stages** so the solver can evolve without
touching template matching.

### 3.1 Scale

Classification is `O(detections × candidate_people × bands)` — linear in roster
size, against the current `O(clusters²)` (26k clusters / 226M pairs before the
blocked-matrix rewrite). 40 people × ~12 bands × ~8 medoids ≈ 4k vectors ≈ 8 MB
float32; 100 people ≈ 20 MB.

The important property is subtler:

> **The roster grows, but the per-detection candidate set does not.**

A 2003 photo is never scored against grandchildren born after 2015; a 1975
scanned print is scored against maybe six people. As the family tree grows
forward, each individual photo's ambiguity stays roughly constant. The current
design has no equivalent, which is why one fossil era could drag 3,692 clusters
into a single merge-conflict component.

### 3.2 Three failure modes that intensify at 40+

**(a) Genetic similarity.** ArcFace confuses relatives far more than strangers,
and this collection is almost entirely relatives. Handled by margin-based
routing, not a better threshold, plus a per-person confusable set that
accumulates from low-margin decisions.

**(b) Siblings and same-age cousins.** The dominant case in this catalog is a
group, not a pair: **Hannah, Emma, Ava and Evelyn are four sisters**, with Cora
Griffin (~1y older than Hannah) and Josie Keener (born between Ava and Evelyn) as
near-age cousins. Two sisters photographed at the same age, years apart, have
embeddings more similar to each other than either has to her own adult face.

This is the case age banding exists to break, and the mechanism is worth stating
precisely: **a lifetime-mean anchor creates the confusion by construction.**
Hannah's mean absorbs her age-6 faces, so Emma-at-6 matches it. Under banding a
2012 face is scored against Emma's age-6 templates and Hannah's *age-9*
templates — because Hannah was 9 in 2012 — so the comparison that causes the
error is never made. Date arithmetic is doing the work, but through **band
selection**, not through the hard gate (all the sisters were born before any
photo that contains them, so the gate alone separates nothing).

**There is a second, opposite confusion type that banding cannot touch.** The
Keener cousins are age-paired almost one-to-one with the sisters — Robbie is ~6
weeks from Hannah, Thomas ~6 months younger than Emma, Josie between Ava and
Evelyn. Those pairs sit in the **same band for their whole lives**, so band
selection separates them not at all. The two types are complementary:

| Type | Example | Age separates? | Context separates? |
|---|---|---|---|
| Within-family, age-separated | Hannah vs Emma | **Yes** | No — always together |
| Cross-family, age-matched | Hannah vs Robbie | **No** | **Yes** — different households |

This is why both signals are required, and it sets an expectation for the Phase
0A result: banding should show a large effect on the sisters and **approximately
none** on the age-matched cousin pairs. A result that improves both equally would
be suspicious. The worst case overall is age-matched cross-family pairs *in
infancy* — same age to within weeks, and infant faces carry weak sex cues.

Two consequences for the rest of this plan:

- **Co-occurrence's value is inverted relative to age.** It is near-useless
  *within* a sibling group — the sisters are always together, so context narrows
  to "one of the Griffin girls" and no further — but it is the **primary**
  discriminator for age-matched cross-family pairs, which age cannot help at all.
  §6.3's blanket claim was too optimistic in one direction and too pessimistic in
  the other.
- **The same-photo constraint becomes load-bearing, not a nicety.** Four sisters
  in one photo are four *different* people, so `≤1 occurrence per person per
  photo` plus age-appropriate templates makes the joint assignment far better
  determined than independent per-face argmax. This is now a primary argument for
  the joint solver (§3), not a refinement of it.

**(c) Twins.** Twins are **two different people**, not an exception to
per-photo uniqueness — they are an embedding-confusion case, permanently
confusable, always routed to a human. The real duplicate-identity exceptions are
depictions, reflections, and composite images.

---

## 4. Durability foundations

### 4.1 Detection grain is not currently durable — fix before enrollment

`invalidate_detections_for_files` (`db_ops.py:181-209`) hard-`DELETE`s
memberships, person links, embeddings, and detections;
`get_files_with_human_touched_detections` (`db_ops.py:148`) exists so callers can
*exclude* those files. Today there is a hard either/or: **keep your labels, or
fix the pixels — never both.** The docstring concedes cannot-link snapshots are
left dangling.

Keying assertions to `detection_id` without fixing this would replace ephemeral
cluster ids with semi-ephemeral detection ids — the same failure on a new
substrate, discovered only after hundreds of enrollment labels exist.

**Chosen approach: immutable detections with supersession**, in preference to a
separate `face_occurrence` entity. Supersession is already this codebase's idiom
(`status='observed'`/`'no_faces'`, file retirement, proposal supersede), so it
adds no new identity layer. Required properties:

- Detection rows are never deleted once they carry human state.
- Rescans and model upgrades produce **successor** detections.
- Assertions stay attached to history; current embeddings resolve through the
  successor lineage.
- Reconciliation uses file identity + bbox IoU + crop fingerprint.
- One-to-many, many-to-one, and ambiguous rematches are **quarantined for
  review**, never auto-resolved.
- A model upgrade reuses identity labels without pretending new bounding boxes
  are automatically the same face.

### 4.2 Human assertions and machine predictions are separate stores

If automatic assignments enter the template set, the classifier self-trains: one
false assignment strengthens its own template and spreads. Two concepts:

| | Durable human assertions | Run-scoped predictions |
|---|---|---|
| Contents | identity, retraction, verdicts | person score, runner-up, margin, constraints applied, template/config/model fingerprint, outcome |
| Lifetime | permanent, superseded never deleted | recomputable, replaced each run |
| Feeds templates | **yes** | **never** |
| May become catalog state | yes | yes, as *derived* state — recomputable and replaceable |

### 4.3 Trust levels are explicit and visible

`get_photo_detections` currently collapses direct and cluster-derived identity
into one field (`db_ops.py:2938`: `r[7] if r[7] is not None else r[8]`), so the
UI says "Already labeled" without revealing the source. Once this page is the
center of enrollment and calibration, that is disqualifying. Required states:

- **Human confirmed** — an explicit label on this visible face
- **Suggested** — machine prediction, not yet acted on
- **Propagated — needs confirmation** — inherited from a cluster/tracklet action
- **Conflicting** — contradictory evidence
- **Unlabeled / residual**

### 4.4 Retraction and undo, before enrollment

Retraction plumbing exists — `link_detection_to_person` retracts other-person
links via `status='retracted'` rather than deleting (`db_ops.py:3100-3109`) — but
**no primitive returns a single face to unknown.** The available operations are
`mark_detection_not_a_face`, `mark_detection_depiction`,
`unlink_cluster_from_person`, `evict_cluster_members`, `absorb_person`. The
missing entry point is small, and must exist before enrollment creates hundreds
of labels. Undo must:

- Retract rather than delete, with a recorded `run_action`.
- Immediately invalidate the affected template set.
- Supersede or recompute predictions that depended on the retracted label.
- Resolve through detection supersession lineage.
- For a propagation action, retract only children still attributable to it,
  preserving any independently confirmed or corrected afterward.

### 4.5 Propagation is a separate, audited, provisional action

Today's Label Photos combines an atomic label with whole-cluster acceptance in
one default-on checkbox (`streamlit_app.py:1143`, applied at `:1234`). Those are
different claims: "this face is Emma" is verifiable from what is on screen;
"these 40 faces are Emma" is not. **This is the same compound-assertion flaw that
made merges unreviewable, sitting inside the best workflow in the system.**

- One click = one assertion about the visible face.
- Cluster/tracklet expansion becomes an explicit action that snapshots every
  affected detection, records its source assertion and algorithmic evidence, and
  supports group retraction.
- **Reversibility is not trustworthiness**: propagated labels are *derived and
  provisional*, excluded from trusted template construction. Each becomes a human
  assertion only on individual confirmation, or after a review that genuinely
  exposed every affected face.

### 4.6 Assertion schema

`face_assertions` needs explicit decisions on: XOR/uniqueness constraints;
retraction and supersession history (never hard deletion); idempotency and
mandatory `run_actions` provenance; actor/source vocabulary; classifier/model/
config versioning; interaction with cannot-links; person absorption and aliases;
and retirement of `face_person_links`.

**Identity and validity are orthogonal axes.** Depiction is currently a
*detection status* (`db_ops.py:640` sets `status='depiction'`) while
`get_labeled_person_embeddings` requires `status='observed'`
(`db_ops.py:2038`) — so a depiction cannot carry an identity at all. Label Photos
compounds it: typing a name suppresses the depiction verdict entirely
(`streamlit_app.py:1203`, `if selected == DEPICTION and not typed`). This
contradicts the July depicted-faces decision that a depiction "keeps embedding
and optional person label," and it is a live bug independent of this plan. The
model must express "this is a depiction" **and** "the depicted person is Emma" —
searchable as Emma, excluded from age arithmetic, tracklets, live co-occurrence,
and templates.

---

## 5. Dates and the age model

### 5.1 Capture dates are intervals with provenance

Revision 1 wrongly claimed missing EXIF yields NULL. `filesystem.py:181-182`
falls back to file mtime, so existing dates silently mix EXIF capture time with
filesystem time and **misdating is more common than absence** — the harder
failure to detect. Scanned prints therefore arrive dated to the scan.

Required:

- **Observed date + source** stored separately from **asserted date/range**.
- Precision vocabulary for both: `exact | day | month | year | range | unknown`.
- One indexed **effective face capture interval** accessor.
- A year-precision date must gate age but must **not** be materialized as a
  synthetic midnight — a whole folder sharing one timestamp would manufacture a
  false event and unsafe tracklets. Tracklet formation requires sufficient
  precision.
- RAW/output variants of one capture must not acquire inconsistent asserted
  dates.
- Precedence must be **scoped**: `capture_datetime` also drives organization
  paths, RAW/output linking, reconciliation, and sync. Decide explicitly whether
  manual dates affect general catalog organization.
- `upsert_media_metadata` uses `INSERT OR REPLACE` (`ops.py:147`) and would erase
  new asserted columns on any metadata refresh. Must change.

### 5.2 Birth dates: precision, not exactness

| Subject | Precision needed | Rationale |
|---|---|---|
| Age 0–3 | **Month** | Appearance changes monthly |
| Age 3–12 | **Year** | Bands 2–4y wide |
| Age 12–25 | Year, loose | Fast change, wide bands |
| Adult at enrollment | Year or coarser | But see below |
| Scanned ancestors | Approximate birth year | Very wide bands |

`seed.py:75-81` hard-rejects anything that is not `YYYY-MM-DD` — a blocker for
enrolling older generations. Store `birth_precision` so coarse dates widen bands
instead of failing.

**Revision 1's advice to skip birth dates for adults is withdrawn.** It is safe
only if no childhood or historical photos ever arrive, which directly contradicts
the scanned-archive plan. Every enrolled person gets at least an approximate
birth year.

**Age anchoring** (asserting approximate age on one reference detection to
back-compute a birth year at wide precision) needs its own algorithm, schema, and
provenance — it is frequently the only available input for scanned ancestors.
Same for `appearance_stable_from`. Neither is specified yet; both are Phase 2
design tasks, not sketches.

**Hard rejection is interval arithmetic**: reject only when the capture interval
*ends* before the earliest possible birth date.

### 5.3 Age bands

Developmental rate, not calendar time. Starting point, to be **set empirically by
the Phase 0 spike** rather than by assertion:

```
AGE_BANDS = [(0, 0.5), (0.5, 1), (1, 2), (2, 4), (4, 7), (7, 11),
             (11, 14), (14, 18), (18, 25), (25, 40), (40, 55),
             (55, 70), (70, 200)]
```

Revision 1 said infancy needs ~6-month bands and then specified 1-year bands;
fixed. `(40, 200)` is split — grandparents spanning 60→85 in one band recreates
the lifetime-centroid problem at the other end of life.

Coarse birth precision widens band membership. Adjacent bands are scoreable at a
penalty. **Max-over-medoids favors people with more templates**, so template
count needs normalization or a cap, calibrated in Phase 0.

---

## 6. Templates, quality, and context

### 6.1 Templates are derived, never stored as authority

Medoid selection is deterministic given the trusted assertion set plus birth
interval, so `(person, band)` templates are recomputed each run; cache for
performance only. This is the discipline the `representative_embedding` fix
(`98b6681`) arrived at empirically — applied from the start.

### 6.2 Quality gets a real phase

`save_thumbnail` accepts `landmarks` and never references it in the body
(`thumbnails.py:26`); the scan passes detection-space landmarks alongside a
full-resolution image without scaling (`detection.py:383`). Landmarks are not
persisted.

**Critical distinction:** insightface computes `normed_embedding` from its own
internal alignment (`detection.py:126`). Landmark-aligning review thumbnails
improves review UX and changes *nothing* about the vectors. Any embedding
improvement requires **re-embedding**. The plan must state:

- Which proxies are stored or derived: relative face area, blur, pose/yaw,
  occlusion, detection score, landmark geometry.
- Whether alignment is review-only or triggers re-embedding.
- Quality **exclusion** vs **weighting** rules.
- How a quality algorithm/version change invalidates derived templates.

Quality is a prerequisite for template selection: a blurry 45px face as a band
medoid poisons every decision in that band.

### 6.3 Co-occurrence, in two forms

`CO_OCCURRENCE_WEIGHT = 0.30` is currently the second-largest linker signal
(`config.py:79`, scored at `linking.py:684`). Revision 1 dropped it silently.
Reinstated, but in two distinct roles:

**(a) Human-facing context — ship with enrollment, no calibration needed.**
Showing the full photo with already-resolved faces *is* the co-occurrence signal,
delivered to the person best equipped to use it. This requires no statistical
validation, only UX validation (which candidates are offered first is still
model-influenced and must be presented as suggestion, not fact).

**(b) Scored contextual feature — post-calibration, never independently
sufficient.** The existing implementation cannot be copied: it is defined over
clusters (the substrate being made disposable), may be contaminated by legacy
clusters, uses a global prior over decades of changing households, and — if
derived from machine assignments — creates another feedback loop. Family
gatherings can also make cousins' contexts nearly identical. It must be
era/event-conditioned, applied only after hard date and cannot-link constraints,
and never the sole cause of an automatic assignment.

### 6.4 Explainability

Every routed decision retains: unary template scores, date eligibility,
co-occurrence contribution, constraints applied, and **why the joint assignment
differed from independent top-1**. Without this, difficult cousin decisions
become harder to audit than they are today, not easier.

---

## 7. Enrollment and the Label Photos surface

### 7.1 Photo sampling is expected-utility, not literal set cover

Before review the system does not know which people an unlabeled photo contains,
so it cannot know which `(person, band)` cells that photo covers. Sampling
maximizes **expected** coverage using trusted labels, predictions, legacy-cluster
hints, and residual diversity, and must account for human cost: a five-person
gathering may fill several cells but requires five decisions and may contain
small, poor-quality faces.

**Target metric: useful confirmed assertions per minute** — not photos, not
faces. Revision 1's "strictly cheaper" claim was wrong.

The current sampler (`db_ops.py:2786`) requires live cluster membership (the
`JOIN face_cluster_members` at `:2827`), so it cannot serve cold start or
unclustered residual detections. A replacement must.

**The sampler cache must change.** `_sample_photos` is cached on a catalog
version that advances only on non-UI runs, and its docstring states individual
label saves deliberately do not invalidate (`streamlit_app.py:1030-1037`).
Tolerable for a cluster-mass heuristic; wrong for active enrollment, where every
confirmation changes band coverage, template state, and expected utility.
Enrollment saves must invalidate or incrementally refresh the queue.

### 7.2 Cold start: three enrollment modes

Farthest-point diversity sampling can only diversify an existing candidate pool —
it cannot find the first face for an empty `(person, band)` cell.

| Mode | Mechanism |
|---|---|
| **Seed** | Human identifies an initial face from a known photo, timeline, or legacy-cluster suggestion |
| **Expand** | Existing templates retrieve likely same-person faces in the target band; diversity sampling chooses among them |
| **Discover** | Residual HDBSCAN proposes unknown people or uncovered appearances |

Residual discovery must therefore be available *during* bootstrap, but the full
target residual pipeline need not precede enrollment: existing HDBSCAN can serve
as a read-only **candidate generator**, provided legacy clusters are navigation
aids only and are never projected wholesale into trusted assertions. The UI must
visibly distinguish "shown because of legacy cluster proximity" from "trusted
identity evidence," and clicking a candidate asserts only that face.

### 7.3 Staged prediction rollout

A unified page does not remove the need for safety gates:

1. **`hidden`** — score and store, do not display.
2. **`suggested`** — display, require explicit human action.
3. **`derived`** — apply as recomputable machine state.
4. **`automatic`** — only after precision targets are met.

**Automation bias is a real threat to calibration.** A pre-filled dropdown that
gets accepted is weaker evidence than an independent label, and "reviewed and
confirmed" must be distinguishable from "left untouched." Therefore:

- Explicit confirmation; never save untouched defaults as assertions.
- Corrections recorded separately from confirmations.
- A randomized or locked subset where predictions are stored but **hidden until
  after** the human labels — this is the honest calibration channel.
- Prediction provenance retained for later comparison.

Without this, the "calibration data collector" mostly measures how often people
accept suggestions.

### 7.4 Date and depiction decisions inline

Both belong on the full-photo surface, for the same reason enrollment does:
context is what makes a human good at them. Asserted dates need inline
year/month/day/exact precision entry. Depiction must be expressible *together
with* an identity (§4.6).

### 7.5 Stopping criteria, at two levels

Revision 1 had no stopping rule; the first review's per-band statistical criteria
were too demanding for cells that will often hold a handful of photos.

**Global classifier readiness** — pooled locked-test precision, open-set false
accepts, calibration quality.

**Per-person/band completion** — minimum trusted examples from multiple
*independent photo events*; required quality/pose diversity; event-held-out
recall *where n permits*; no unresolved high-margin contradictions.

Band states: `sufficient` | `insufficient` | `not_applicable` (no evidence the
person appears in this band) | `data_exhausted` (all candidates reviewed,
statistical target unreachable). The last two prevent enrollment becoming an
endless hunt for photos that may not exist.

---

## 8. Phases

Phases are dependency-ordered. Two gates (0A, 2A) must return a decision before
downstream work begins.

### Phase 0A — Directional spike (read-only, no schema, no writes)

> Protocol: **`PLAN_face_phase0a_prereg.md`** — locks metrics, splits, ablations,
> confound handling, and decision rules before the run. Must be locked first.

**Gate: does the architecture look promising enough to build?** Revision 1
assumed the central hypothesis; this tests it on existing evidence and sizes the
band structure empirically. Deliberately dirty — legacy labels, mixed date
provenance — because its job is direction, not deployment clearance.

Ablation matrix, all at **equal template budget** — without an age-agnostic
multi-medoid baseline, a win for banded medoids cannot be distinguished from
"more prototypes helped":

1. Lifetime single mean (today's behavior)
2. Age-agnostic multi-medoid
3. Age-banded mean
4. Age-banded multi-medoid
5. Each of the above, with and without date gating
6. Optional quality and co-occurrence ablations

Safeguards:

- Split by **photo event**, not detection — random splits leak near-duplicates
  from a burst and inflate results badly.
- Evaluate direct human labels **separately** from cluster-derived labels;
  `get_labeled_person_embeddings` unions both (`db_ops.py:2029-2058`).
- Filter or separately report probable mtime-fallback dates (`filesystem.py:181`).
- Include an **open-set / impostor slice** — separation among known people does
  not establish that strangers are rejected safely.
- Report top-1 similarity, top-2 margin, false-accept rate, and per-person and
  per-band breakdowns — not aggregate accuracy.
- Target the known hard confusions specifically: Hannah/Emma, Ava/Evelyn,
  Robbie/Thomas Keener.

**Outcomes and what follows from each:**

| Result | Action |
|---|---|
| Banding clearly beats the equal-budget age-agnostic baseline | Proceed to Phase 1 |
| Banding shows no effect over baseline | **Stop and rethink** — the pivot's core premise fails |
| Ambiguous, or confounded by dirty labels/dates | Resolve with a *minimal* curated slice (§8, Phase 0B tooling), not full enrollment |

### Phase 0B — Clean validation gate

**Gate: is the classifier safe to expose to a human?** Runs later — after
foundations and the Label Photos work, before any prediction is displayed.
Requires independently labeled, hidden-prediction examples.

Revision 2 contained a circular dependency here: it made Phase 0 a gate on
everything while sourcing its clean slice from "the first enrollment session,"
which does not occur until enrollment. It is also not true that the current UI
could quietly serve as read-only curation — Label Photos writes accepted links
(`streamlit_app.py:1227`) and defaults whole-cluster expansion on (`:1143`).

**Resolution: a standalone offline annotation path** that records labels to a
side table or file and never touches `face_person_links`. Roughly half a day,
and it decouples validation from the production write path permanently — useful
well beyond this phase, since every future model swap needs the same clean
comparison. This also gives Phase 0A an escape hatch if its result is ambiguous.

### Phase 1 — Inventory and data-quality audit (read-only)

Trusted labels per person/band/event; date-source quality distribution;
zero-label bands; which legacy clusters can safely act as candidate generators;
reproducible counts for the migration-driving figures (§13).

### Phase 2A — Schema and state-transition RFC (design gate, no code)

**Gate: are the state semantics settled?** §4.6 lists open schema decisions;
they must be resolved in writing before any migration runs. The RFC defines:

- Assertion uniqueness, and which identity/validity verdict combinations are
  legal (§4.6).
- Supersession inheritance and ambiguous-reconciliation handling.
- Retraction semantics and propagation-child ownership.
- Semantics for `unknown`, `uncertain/deferred`, one-off stranger, depiction,
  non-person, not-a-face.
- Which state wins on dual-read conflict.
- Whether assertion writes are projected into `face_person_links` during
  coexistence.
- Per-consumer cutover and rollback rules.
- Whether manual dates affect face inference only, or general catalog
  organization (see §11, D3).

**Scoping correction.** Revision 2 framed this around four write callers. The
*read* surface is far wider: **22 distinct methods in `db_ops.py` alone**
reference `face_person_links` — including `get_photo_detections`,
`get_labeled_person_embeddings`, `get_persons_summary`, `get_stats`,
`get_photos_for_labeling`, `absorb_person`, and both anchor consumers — plus
references in `linking.py` and `streamlit_app.py`. Replacing four write paths
does not complete the transition; the dual-read compatibility layer is the
substantial part of Phase 2, not a footnote.

### Phase 2B — Durability and provenance implementation

Detection supersession and reconciliation (§4.1). Assertion vs prediction stores
(§4.2). Trust-level model (§4.3). Individual retract/unassign (§4.4). Guarded
identity-mutation service — one write path, replacing four production call sites
(three UI: `streamlit_app.py:288`, `:626`, `:1227`; one internal from
`absorb_person`, `db_ops.py:2295`). Date/birth interval model and parser (§5.1,
§5.2). Model/config/artifact fingerprinting. Dual-read compatibility layer;
dry-run legacy-evidence report.

### Phase 3 — Label Photos as the unified surface

Trust-state rendering; atomic-label/propagation split (§4.5); inline date and
depiction decisions (§7.4); sampler cache invalidation (§7.1); hidden-prediction
benchmark channel (§7.3). Interaction prototype before broad implementation.

**Correction and retraction are first-class requirements here, not backend-only.**
An already-labeled face is currently rendered and then skipped with `continue`
(`streamlit_app.py:1109-1120`), so the central workflow cannot fix its own
mistakes — the face must be hunted down elsewhere. Backend undo (§4.4) does not
satisfy the human-workflow goal by itself. Label Photos must support, on any
visible face: correct the identity, return it to unknown, and change or withdraw
a verdict.

### Phase 4 — Quality

§6.2 declared quality a prerequisite without giving it an owning phase, while
enrollment already assumes quality-aware sampling. It gets one here: proxy
computation (relative face area, blur, pose/yaw, occlusion, detection score,
landmark geometry), landmark persistence, storage and versioning, the
review-alignment vs re-embedding decision (§11, D6), exclusion vs weighting
rules, and validation. Must precede template construction, since a poor-quality
medoid poisons every decision in its band.

### Phase 5 — Enrollment

Backfill **only trusted labels** under the four-tier classification (§12.2).
Quality-aware, expected-utility photo sampling with the three cold-start modes
(§7.2). All new labels write through the assertion service. Event-disjoint
calibration and locked test sets carved out here.

**Includes minimal template construction and retrieval** — Expand mode requires
templates, which revision 2 deferred to the classifier phase, making enrollment
depend on a later phase. Split resolved: *template construction + similarity
retrieval* land here (retrieval only, no automatic assignment); *joint
constrained assignment and calibration* stay in Phase 6.

### Phase 6 — Classifier (hidden → suggested)

Deterministic band templates from human assertions only. Candidate score matrix +
joint constrained assignment (§3). Co-occurrence as a measured feature. Residual
always a valid outcome. Explainability payload (§6.4). Runs without mutating
identity; precision measured against the locked set from Phase 0B.

**Photo-only constraints initially.** Revision 2 put tracklet consistency in the
solver while tracklet purity work happened a phase later. `build_tracklets` is
currently naive union-find over mutual-nearest-neighbor edges (`tracklets.py:153`)
with no validation against labels, cannot-links, or weak single-edge bridges — it
must not become a trusted constraint before it is hardened. Tracklet-constrained
assignment is enabled only after Phase 7.

### Phase 7 — Tracklets and residual clustering

Tracklet purity: component validation against labels, cannot-links, multiple
faces from one file, and weak single-edge bridges — `build_tracklets`
(`tracklets.py:104`) does none of this today, and those checks currently happen
later at cluster grain. Specify unit scoring (quality-weighted centroid vs best
face vs consensus — revision 1 suggested all three), minimum independent edge
support, conflicting-assertion handling, and precision-gated event formation.
Residual-scoped HDBSCAN: input scoping, tracklet representation and membership
expansion, singletons, date/era treatment, constraint propagation, per-run
cluster storage. **Era windows may remain useful for residual unknowns** —
revision 1's claim they become largely irrelevant is withdrawn pending
experiment.

### Phase 8 — Cutover

Enable `derived` then `automatic` assignment only after precision targets are
met. Retire cluster authority; add the run/generation key and revised uniqueness
semantics (`face_clusters` is currently `UNIQUE(cluster_key, model_name,
model_version)`, `schema.py:377-379`) and decide whether per-run clusters are
retained immutable or deleted — if deleted, durable `run_actions` must snapshot
stable detection ids plus enough evidence to stay auditable. Retire repair
commands after a migration audit.

---

## 9. Pre-registration and acceptance criteria

"Returns a verdict," "precision targets," and "minimum trusted examples" are not
operational definitions. They must become so — but the sequencing matters, and
locking numbers before seeing the metric's scale on this catalog would be
arbitrary.

**Fix the procedure before running; calibrate the values from a profiling pass;
lock before final evaluation.** Specifically:

1. **Before Phase 0A runs** — fix the *form* of each decision rule (what is
   compared against what, on which split, using which metric), the dataset
   manifest, SQL, random seed, commit, model/config fingerprints, and split
   identities. Register what would constitute *no effect*.
2. **After a profiling pass** — set numeric thresholds from the observed metric
   distribution.
3. **Lock, then evaluate** the locked test set once.

Criteria to define:

| Gate | Criterion |
|---|---|
| Phase 0A | Improvement over the equal-budget age-agnostic multi-medoid baseline that counts as a win; what counts as no effect |
| Phase 0B | Pooled precision with a confidence bound; maximum open-set false-accept rate |
| Band completion | Minimum independent photo events; quality/pose diversity floor |
| `suggested` | Precision floor at which displaying a prediction is net-helpful |
| `derived` | Precision floor for machine state |
| `automatic` | Precision floor plus open-set false-accept ceiling |

**One qualification.** Confidence bounds belong at the *pooled global* level, not
per band. Many `(person, band)` cells will hold a handful of photos, where a
bound is not meaningful — this is why revision 2 moved per-band criteria to event
counts and diversity rather than statistics. That distinction must not erode:
global precision gets a bound, band completion gets counts and coverage.

**Scale note for the false-accept ceiling.** At ~100k photos, even a 1%
false-accept rate produces hundreds of wrong assignments — and each one is a
durable-looking identity claim. The ceiling should be set from "how many wrong
assignments are tolerable in absolute terms," not from a percentage that sounds
small.

## 10. Architecture-invariant tests

The inherited handoff tests plus re-derivability are insufficient for this
redesign. These invariants encode the failures this architecture exists to
prevent, and each should fail on a deliberately broken implementation:

1. Predictions and propagated labels **never** feed trusted templates.
2. Individual and group undo preserve later independent confirmations.
3. Detection supersession handles exact and ambiguous rematches safely;
   ambiguous ones quarantine rather than auto-resolve.
4. Untouched suggestions never become assertions.
5. Dual-read migration, backfill, and retries are idempotent.
6. Solver constraints always retain residual/unknown as a feasible outcome.
7. Benchmark splits exclude duplicate derivatives and event leakage.
8. Re-derivability: two runs over identical assertions produce identical
   assignments.

## 11. Decisions required, and when

Decisions that are genuinely the user's, ordered by the point at which they
block work. Everything not listed here is a technical choice the implementation
can make.

### Now — before Phase 0A

| | Decision | Notes |
|---|---|---|
| **D1** | Approve the pre-registration procedure (§9) | Methodological; fixing the decision-rule form before seeing results is what stops post-hoc rationalization |
| **D2** | Build the standalone offline annotation tool? | ~half a day; decouples validation from the production write path permanently and gives Phase 0A an escape hatch. **Recommended yes** — every future model swap needs the same clean comparison |

### Before the Phase 2A RFC closes

| | Decision | Notes |
|---|---|---|
| **D3** | Do manual asserted dates affect general catalog organization, or face inference only? | Real consequences — `capture_datetime` drives organization paths, RAW/output linking, reconciliation, and sync. Asserting a 1975 date on a scanned print could move files on disk |
| **D4** | Are per-run clusters retained immutable, or deleted after review? | If deleted, durable `run_actions` must snapshot stable detection ids plus enough evidence to stay auditable |
| **D5** | During coexistence, are assertion writes projected into `face_person_links`? | Determines rollback story and how long the dual-read layer must live |
| **D6** | Does landmark alignment trigger re-embedding? | Compute decision at ~100k photos. Also determines whether supersession must handle a full re-embed wave — **worth batching with any other planned rescan**, so decide before scheduling one |
| **D7** | Which legacy workflows count as "potentially trusted" for backfill? | Needs the Phase 1 inventory as input; cannot be decided before it |

### Before Phase 6 (classifier rollout)

| | Decision | Notes |
|---|---|---|
| **D8** | Numeric precision targets and the open-set false-accept ceiling | Locked after the profiling pass, per §9 |

### Before Phase 8 (cutover)

| | Decision | Notes |
|---|---|---|
| **D9** | Authorize `automatic` assignment | Only after the Phase 6 criteria are met and demonstrated |

## 12. Migration

### 12.1 Bug classes eliminated

| Shipped bug / tool | Under this architecture |
|---|---|
| Stale `representative_embedding` (`98b6681`) | **Impossible** — representatives always recomputed |
| Fossil eras as universal hubs (`56c90df`, `db25ae7`) | **Impossible** — era metadata not durable; gating uses date intervals |
| Merges built on evicted evidence (`ab6f4d2`) | **Impossible** — no merges; retraction re-derives |
| 98% of audit hits on dead cluster ids (`01428dd`) | **Impossible** — nothing durable references a cluster id |
| Cross-run contamination into one `cluster_id` (`417ea06`) | **Impossible** — cluster rows are per-run |

### 12.2 Backfill is tiered, not wholesale

"Project only clusters whose members were reviewed" is **not queryable** — the
database does not record per-member review. Classify legacy evidence instead:

| Tier | Treatment |
|---|---|
| **Trusted** | Explicit detection-level human labels → become assertions, feed templates |
| **Potentially trusted** | Narrowly whitelisted reproducible human workflows → assertions after audit |
| **Provisional legacy** | Visible for review, **excluded from templates** |
| **Quarantined** | Mechanical or untraceable cluster-derived labels → not projected |

The 58,571 single-pair mechanical tracklet merges (42% of applied tracklet
merges) are quarantine candidates by default. Backfill is a dry-run-capable,
resumable, audited command — never a large data operation inside schema startup.

### 12.3 The property currently missing: iteration

Changing a threshold today means re-clustering, which regenerates cluster ids,
which orphans accepted state and audit results. Every experiment costs human
work, so thresholds cannot be tuned — the mechanism behind the current mess. With
durable assertions and disposable clusters, re-running is free and human labels
are never at risk.

---

## 13. Evidence provenance

Catalog-derived figures in this document come from commit messages on
`feature/face-cluster-quality` recording real-catalog investigation; they were
not re-derived here, and the catalog is not in the repository. Before any number
drives migration policy — the 58,571 single-pair merges especially — record:
catalog/database snapshot identity, schema version and commit, exact SQL or
command, denominator definition, and measurement date.

---

## 14. Disposition of the existing roadmap

### Roadmap items 3–5

| Item | Disposition |
|---|---|
| **3. Age-banded person templates** | **Validated in principle, now gated on the Phase 0 spike.** Scope corrected: a shared template service plus consumer adaptations, not two mean-function swaps — the consumers differ structurally (refinement compares cluster reps to anchors, `refinement.py:88-99`; linking uses anchors as a boost term, `linking.py:536-540`), and `get_labeled_person_embeddings` returns no detection id, date, quality, or provenance. |
| **4. Quality proxies + landmark alignment** | **Elevated to prerequisite, with a real phase (§6.2)** and an explicit review-UX vs re-embedding decision. |
| **5. Benchmark harness** | **Elevated to prerequisite and made concrete (Phase 0, §7.3).** Event-disjoint splits, open-set coverage, locked test set separate from calibration. Model swaps and artifact fingerprinting follow it, unchanged. |

### In-flight repair work

| Item | Disposition |
|---|---|
| 12 `audit-evicted-merges` candidates | **Stop investing** — subsumed by tiered backfill (§12.2) |
| `tracklet_pairs >= 2` on `_MECHANICAL_MERGE_WHERE` (`db_ops.py:1558`, still unfixed) | Forward fix **moot**; the 58,571 historical merges become a quarantine rule |
| Relabel-doesn't-evict across three UI pages | **Do not fix the three paths** — obsoleted by assertions; fixed via the Phase 2 guarded service |
| One guarded identity-mutation service | **Elevated to Phase 2** — it becomes the assertion-writing API |
| Link scored-tier memory (~4.9M candidates, ~2GB) | **Moot** — that tier is replaced |
| `repair-*` / `audit-*` commands | **Retire after Phase 7**; keep until then as the only handle on existing damage |

### Still relevant

| Item | Note |
|---|---|
| Depiction/identity orthogonality | **Promoted to a standalone bug** (§4.6) — live today, worth fixing regardless |
| "Fold into person" for any two persons | **More** relevant — enrollment involves heavy name entry, so typo-duplicated persons multiply |
| Depicted-faces constraint | Preserved; the verdict becomes an orthogonal axis rather than a status |
| CR2/RAW orientation via DPP4 recipes | Unchanged known limitation; ~31k unprocessed CR2s |
| 15 same-photo flags awaiting triage | Small, still relevant |
| UI smalls: WAL connections, checkpoint cadence, dropdowns keyed by id | Independent of architecture |
| Delete legacy branches | Housekeeping, overdue |
| `MIN_WORKING_DET_SCORE=0.7`, EXIF orientation fix, `retire-superseded-raw-detections`, run-action provenance, detection-level cannot-links | Preserved as-is |

### From the rebase handoff

Port complete; document historical. **Non-Goals remain binding**: no legacy
`faces`/`persons` schema, no bypassing `run_actions` for accepted decisions, no
embeddings in `file_observations`. Its Required Tests still apply, plus one
addition: **re-derivability** — two runs over identical assertions must produce
identical assignments.

---

## 15. Open questions

1. **Undated detections.** Once asserted intervals exist, what is the default for
   a detection with no usable date — excluded, or classified without the date
   gate at a higher margin requirement?
2. **Stranger handling.** One-off strangers (weddings, school photos) need bulk
   dismissal without enrollment, and a verdict distinct from "not a face."
3. **Upper date bounds.** The gate has a natural lower bound (birth) but no upper
   one. A death date is a heavier ask than a birth date; a soft bound derived
   from last-observed date may suffice.
4. **Same-photo flag machinery reuse.** It contributes UI, full-photo review,
   file membership, and proposal lifecycle, but cannot supply constraint state
   as-is: it flags only unusually similar pairs while uniqueness applies to every
   assignment; dismissal deliberately records no relationship
   (`streamlit_app.py:704`); resolutions carry notes, not a durable typed
   exception (`db_ops.py:1493`).
5. **Does manual date assertion affect catalog organization**, or only face work?

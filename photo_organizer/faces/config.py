"""
Configuration constants for face recognition.
"""

# --- Detection ---
DETECTION_THRESHOLD = 0.5          # Minimum insightface det_score to RECORD a face
MIN_FACE_SIZE = 40                 # Minimum face width/height in pixels
# Minimum det_score for a detection to participate in clustering (and hence
# everything downstream). Recording keeps everything >= DETECTION_THRESHOLD so
# this floor is tunable without rescanning; below ~0.7 the detector produces
# significant pareidolia (bricks, pipes) that forms junk clusters.
MIN_WORKING_DET_SCORE = 0.7
MODEL_NAME = "buffalo_l"           # insightface model pack
MODEL_VERSION_TAG = "buffalo_l_v1" # Stored in DB to track which model produced embeddings

# --- GPU / Batching ---
DETECTION_SIZE = (640, 640)        # insightface detection input size

# --- Thumbnails ---
THUMBNAIL_SIZE = (224, 224)        # Aligned face crop dimensions
THUMBNAIL_QUALITY = 90             # JPEG save quality
THUMBNAIL_DIR_NAME = ".face_thumbnails"
THUMBNAIL_BBOX_EXPAND = 0.3       # Expand bounding box by 30% for context

# --- Clustering (used by the cluster/link phases) ---
DEFAULT_ERA_SIZE_YEARS = 2.5       # Standard era window width
ERA_OVERLAP_FRACTION = 0.5         # 50% overlap between adjacent eras
HDBSCAN_MIN_CLUSTER_SIZE = 3      # Minimum faces to form a cluster
HDBSCAN_MIN_SAMPLES = 2
# Coherence gate on HDBSCAN output: every member must sit within this cosine
# similarity of its cluster's centroid (in full embedding space), and a
# cluster must retain HDBSCAN_MIN_CLUSTER_SIZE coherent members to survive.
# Density clustering degenerates on sparse era windows (a window with a
# dozen faces can chain unrelated people into one blob); same-person
# same-era ArcFace similarity is typically >0.55 while cross-person is
# <0.35, so 0.45 separates them with margin.
MIN_MEMBER_SIMILARITY = 0.45

# PCA-reduce embeddings before HDBSCAN (0 disables). KD-trees lose their
# pruning power at 512 dims, degrading HDBSCAN toward slow brute force;
# ArcFace identity structure survives reduction to ~64 dims essentially
# intact. Representatives and anchors always stay in the original space.
CLUSTER_PCA_DIMS = 64

# Child-specific era boundaries (years from birth)
# Used when birth_date is known from seed config
CHILD_ERA_BOUNDARIES = [0, 2, 5, 10, 15]

# --- Cross-Age Linking ---
# buffalo_l's age-estimation head is unreliable (a single toddler cluster
# reads anywhere from ~4y to ~30y), so estimated age carries no weight in
# merge scoring. True age comes from birth_date + capture date once a person
# is named — exact arithmetic, no model guess needed.
EMBEDDING_SIMILARITY_WEIGHT = 0.45
CO_OCCURRENCE_WEIGHT = 0.30
TEMPORAL_CONTINUITY_WEIGHT = 0.10
SUPERVISED_ANCHOR_WEIGHT = 0.15
MIN_MERGE_CONFIDENCE = 0.6
# Only compare clusters whose eras overlap or sit within this gap — a person
# is linked across decades transitively through intermediate eras, not by
# comparing 2005 directly against 2020.
MAX_ERA_GAP_YEARS = 1.0
# Keep only each cluster's K best scored merge suggestions (0 = unlimited).
# Union-find at accept time needs a spanning set, not the complete pair
# graph — without a cap a well-photographed person's clusters generate
# quadratically many redundant suggestions.
LINK_TOP_K = 3
# Clusters sharing at least this fraction of member detections (relative to
# the smaller cluster) are window duplicates: the same faces clustered twice
# by overlapping era windows. Proposed at full confidence, bulk-acceptable.
DUPLICATE_MEMBER_OVERLAP = 0.5

# --- Refinement ---
AUTO_ASSIGN_THRESHOLD = 0.85       # Cosine sim threshold for auto-assigning to known person
AUTO_ASSIGN_MARGIN = 0.1           # Required gap between best and second-best match

# --- Processing ---
CHECKPOINT_INTERVAL = 500          # Commit to DB every N faces during scan

# Default scan scope: editor-exported outputs only. JPEG/TIFF decode is fast
# and covers every RAW that has a linked output. RAWs without an output are an
# explicit opt-in (--include-raw) because rawpy decode is an order of
# magnitude slower; RAWs whose output is already linked are always skipped to
# avoid duplicate detections of the same capture.
FACE_ELIGIBLE_TYPES = {'jpeg', 'tiff'}
RAW_FALLBACK_TYPES = {'jpeg', 'tiff', 'raw'}

# Max image dimension for face detection (larger images are downscaled)
MAX_DETECTION_DIMENSION = 2048

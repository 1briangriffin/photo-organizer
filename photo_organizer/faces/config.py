"""
Configuration constants for face recognition.
"""

# --- Detection ---
DETECTION_THRESHOLD = 0.5          # Minimum insightface det_score to keep a face
MIN_FACE_SIZE = 40                 # Minimum face width/height in pixels
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

# Child-specific era boundaries (years from birth)
# Used when birth_date is known from seed config
CHILD_ERA_BOUNDARIES = [0, 2, 5, 10, 15]

# --- Cross-Age Linking ---
EMBEDDING_SIMILARITY_WEIGHT = 0.35
CO_OCCURRENCE_WEIGHT = 0.25
AGE_PROGRESSION_WEIGHT = 0.20
TEMPORAL_CONTINUITY_WEIGHT = 0.10
SUPERVISED_ANCHOR_WEIGHT = 0.10
MIN_MERGE_CONFIDENCE = 0.6
# Only compare clusters whose eras overlap or sit within this gap — a person
# is linked across decades transitively through intermediate eras, not by
# comparing 2005 directly against 2020.
MAX_ERA_GAP_YEARS = 1.0

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

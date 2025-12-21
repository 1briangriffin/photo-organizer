"""
Configuration constants for the photo organizer.
"""
from pathlib import Path

# --- File Type Definitions ---
RAW_EXTS = {'.cr2', '.cr3', '.nef', '.arw', '.orf', '.rw2', '.dng'}
JPEG_EXTS = {'.jpg', '.jpeg', '.jpe', '.gif', '.png'}
VIDEO_EXTS = {'.mp4', '.mov', '.m4v', '.avi', '.mts', '.m2ts', '.3gp', '.mpg', '.mpeg', '.tod'}
PSD_EXTS = {'.psd', '.psb', '.pspimage'}
TIFF_EXTS = {'.tif', '.tiff'}
SIDECAR_EXTS = {'.xmp', '.vrd', '.dop', '.dpp', '.pp3'}

# Extension to Type Mapping
# Used to quickly classify files without complex if/else chains
EXT_TO_TYPE = {}
for ext in RAW_EXTS: EXT_TO_TYPE[ext] = 'raw'
for ext in JPEG_EXTS: EXT_TO_TYPE[ext] = 'jpeg'
for ext in VIDEO_EXTS: EXT_TO_TYPE[ext] = 'video'
for ext in PSD_EXTS: EXT_TO_TYPE[ext] = 'psd'
for ext in TIFF_EXTS: EXT_TO_TYPE[ext] = 'tiff'
for ext in SIDECAR_EXTS: EXT_TO_TYPE[ext] = 'sidecar'

# --- Metadata Parsing ---
DATE_TAGS = [
    'EXIF DateTimeOriginal',
    'EXIF DateTimeDigitized',
    'Image DateTime',
]

# Patterns for "Descriptiveness Score" (naming priority)
CAMERA_PATTERNS = [
    r'^img_\d+$', r'^dsc_\d+$', r'^dscf\d+$', r'^pxl_\d+$', 
    r'^sam_\d+$', r'^_dsc\d+$', r'^cimg\d+$'
]

# --- Hashing & Performance ---
# Files smaller than this are hashed fully. Larger ones get Sparse Hash first.
SPARSE_HASH_THRESHOLD = 5 * 1024 * 1024  # 5 MB
HASH_CHUNK_SIZE = 64 * 1024  # 64 KB chunks for reading

# --- PSD Smart Object Extraction ---
# PSDs larger than this threshold will skip smart object parsing to avoid memory exhaustion
PSD_SMART_OBJECT_MAX_SIZE = 100 * 1024 * 1024  # 100 MB

# --- Organization ---
FOLDER_PATTERN = "{year}/{year}-{month:02d}"
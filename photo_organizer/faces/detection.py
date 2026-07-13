"""
Face detection, embedding extraction, and scan pipeline.

Wraps insightface for GPU-accelerated face analysis and orchestrates the
scan -> detect -> thumbnail -> persist workflow against the durable face
schema: detections land in face_detections, embeddings in face_embeddings,
both tied to a command_runs row via FaceDBOperations.
"""
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np

from ..database.db import DBManager
from ..database.ops import DBOperations
from . import config
from .db_ops import FaceDBOperations
from .image_loader import load_image_as_bgr, load_image_as_rgb
from .thumbnails import ThumbnailGenerator


def _register_cuda_dll_dirs() -> None:
    """Make the CUDA/cuDNN DLLs from the nvidia-* pip wheels loadable.

    onnxruntime-gpu does not bundle CUDA; the `faces` extra ships it as pip
    wheels instead of requiring a system install. The CUDA 13 wheel family
    moved its DLLs to `nvidia/cu13/bin/x86_64/`, which onnxruntime's own
    preload_dlls() does not search yet, so every `nvidia/**/bin*` directory
    (and immediate children) is registered with the Windows loader before
    session creation. No-op on non-Windows or when the wheels are absent.
    """
    import glob
    import os
    import site

    if not hasattr(os, "add_dll_directory"):  # pragma: no cover - non-Windows
        return
    try:
        site_dirs = site.getsitepackages()
    except Exception:  # pragma: no cover
        return
    for sp in site_dirs:
        for bin_dir in glob.glob(os.path.join(sp, "nvidia", "**", "bin*"), recursive=True):
            for candidate in [bin_dir, *glob.glob(os.path.join(bin_dir, "*"))]:
                if os.path.isdir(candidate):
                    try:
                        os.add_dll_directory(candidate)
                    except OSError:  # pragma: no cover
                        pass
    try:
        import onnxruntime as ort
        if hasattr(ort, "preload_dlls"):
            ort.preload_dlls()
    except Exception:  # pragma: no cover
        logging.debug("onnxruntime.preload_dlls() failed; relying on PATH.")


class FaceDetector:
    """
    Wraps insightface for face detection + embedding + age/gender estimation.

    Uses lazy model loading to avoid import overhead and GPU allocation
    until detection is actually needed.
    """

    def __init__(self, use_gpu: bool = True,
                 det_size: tuple[int, int] = config.DETECTION_SIZE):
        self._app = None
        self._use_gpu = use_gpu
        self._det_size = det_size

    def _ensure_model(self):
        """Lazy-load the insightface model on first use."""
        if self._app is not None:
            return

        try:
            import insightface
        except ImportError:
            raise ImportError(
                "insightface is required for face detection. "
                "Install with: uv sync --extra faces"
            )

        if self._use_gpu:
            _register_cuda_dll_dirs()

        providers = (
            ['CUDAExecutionProvider', 'CPUExecutionProvider']
            if self._use_gpu
            else ['CPUExecutionProvider']
        )

        logging.info(f"Loading face model '{config.MODEL_NAME}' "
                     f"(GPU={'yes' if self._use_gpu else 'no'})...")

        self._app = insightface.app.FaceAnalysis(
            name=config.MODEL_NAME,
            providers=providers,
        )
        self._app.prepare(
            ctx_id=0 if self._use_gpu else -1,
            det_size=self._det_size,
        )
        self._log_active_providers()
        logging.info("Face model loaded.")

    def _log_active_providers(self):
        """Surface whether CUDA actually engaged — onnxruntime silently falls
        back to CPU when the GPU build doesn't support the installed driver
        (e.g. Blackwell GPUs need onnxruntime-gpu >= 1.21)."""
        try:
            session = next(iter(self._app.models.values())).session
            active = session.get_providers()
            logging.info(f"onnxruntime providers active: {active}")
            if self._use_gpu and 'CUDAExecutionProvider' not in active:
                logging.warning(
                    "GPU was requested but CUDAExecutionProvider is not active — "
                    "detection will run on CPU. Check the onnxruntime-gpu / CUDA "
                    "driver pairing."
                )
        except Exception:  # pragma: no cover - purely diagnostic
            logging.debug("Could not determine active onnxruntime providers.")

    def detect_faces(self, img_bgr: np.ndarray) -> list[dict]:
        """
        Detect all faces in an image.

        Args:
            img_bgr: numpy array (H, W, 3) uint8 in BGR order.

        Returns:
            List of dicts with keys:
                bbox: (x, y, w, h) - face bounding box
                embedding: np.ndarray shape (512,) - L2-normalized ArcFace embedding
                det_score: float - detection confidence
                age: int - estimated age
                gender: str - 'M' or 'F'
                landmarks: np.ndarray shape (5, 2) - facial keypoints
        """
        self._ensure_model()
        faces = self._app.get(img_bgr)

        results = []
        for face in faces:
            if face.det_score < config.DETECTION_THRESHOLD:
                continue

            # insightface returns bbox as [x1, y1, x2, y2]
            x1, y1, x2, y2 = face.bbox.astype(int)
            w, h = x2 - x1, y2 - y1

            if w < config.MIN_FACE_SIZE or h < config.MIN_FACE_SIZE:
                continue

            results.append({
                'bbox': (int(x1), int(y1), int(w), int(h)),
                'embedding': face.normed_embedding,  # 512-dim, L2-normalized
                'det_score': float(face.det_score),
                'age': int(face.age),
                'gender': 'M' if face.gender == 1 else 'F',
                'landmarks': face.kps if hasattr(face, 'kps') else None,
            })

        return results


class FaceScanPipeline:
    """
    Orchestrates the face scan workflow: pick unscanned catalog files, detect
    faces, save thumbnails, and persist detections/embeddings with run
    provenance.

    Incremental by construction — files with any face_detections row for the
    current model (including the no-faces sentinel) are skipped, so an
    interrupted scan resumes where it left off and re-running is idempotent.
    """

    def __init__(self, db_path: Path, thumbnail_dir: Path,
                 use_gpu: bool = True, include_raw: bool = False,
                 detector: Optional[FaceDetector] = None):
        self.db_manager = DBManager(db_path)
        self.thumbnail_dir = thumbnail_dir
        self.detector = detector or FaceDetector(use_gpu=use_gpu)
        self.thumbnailer = ThumbnailGenerator(thumbnail_dir)
        self.eligible_types = (
            config.RAW_FALLBACK_TYPES if include_raw else config.FACE_ELIGIBLE_TYPES
        )

    def run(self, *, run_id: int, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Process unscanned images incrementally. Returns a stats dict.

        Args:
            run_id: command_runs id recorded on every detection/embedding.
            limit: Maximum number of images to process (None = all).
        """
        try:
            from tqdm import tqdm
        except ImportError:  # pragma: no cover - tqdm is a core dependency
            tqdm = lambda x, **kw: x  # noqa: E731

        stats = {
            "images_scanned": 0,
            "images_no_faces": 0,
            "images_failed": 0,
            "faces_detected": 0,
        }

        with self.db_manager as conn:
            face_ops = FaceDBOperations(DBOperations(conn))
            unscanned = face_ops.get_unscanned_files(
                model_name=config.MODEL_NAME,
                model_version=config.MODEL_VERSION_TAG,
                eligible_types=self.eligible_types,
                limit=limit,
            )
            if not unscanned:
                logging.info("No unscanned images found.")
                return stats

            logging.info(f"Processing {len(unscanned)} image(s) for face detection...")

            for file_id, orig_path, dest_path, file_type, file_hash in tqdm(
                unscanned, desc="Scanning faces",
            ):
                img_path = Path(dest_path) if dest_path else Path(orig_path)
                if not img_path.exists():
                    img_path = Path(orig_path)
                    if not img_path.exists():
                        logging.debug(f"File not found on disk: {img_path}")
                        stats["images_failed"] += 1
                        continue

                img_bgr = load_image_as_bgr(img_path, file_type)
                if img_bgr is None:
                    stats["images_failed"] += 1
                    continue

                try:
                    faces = self.detector.detect_faces(img_bgr)
                except ImportError:
                    # Missing insightface stack fails every image the same way —
                    # abort instead of decoding the whole library for nothing.
                    raise
                except Exception as e:
                    logging.warning(f"Detection failed for {img_path}: {e}")
                    stats["images_failed"] += 1
                    continue

                if not faces:
                    face_ops.record_no_faces_scan(
                        run_id=run_id,
                        file_id=file_id,
                        model_name=config.MODEL_NAME,
                        model_version=config.MODEL_VERSION_TAG,
                        image_hash=file_hash,
                    )
                    stats["images_scanned"] += 1
                    stats["images_no_faces"] += 1
                    continue

                # Full-res RGB only when there are faces to crop.
                img_rgb_full = load_image_as_rgb(img_path, file_type)

                for detection_index, face_data in enumerate(faces):
                    detection_id = face_ops.record_detection(
                        run_id=run_id,
                        file_id=file_id,
                        detection_index=detection_index,
                        bbox=face_data['bbox'],
                        confidence=face_data['det_score'],
                        model_name=config.MODEL_NAME,
                        model_version=config.MODEL_VERSION_TAG,
                        image_hash=file_hash,
                        payload={
                            "estimated_age": face_data['age'],
                            "estimated_gender": face_data['gender'],
                        },
                    )
                    face_ops.record_embedding(
                        run_id=run_id,
                        detection_id=detection_id,
                        embedding=face_data['embedding'],
                        model_name=config.MODEL_NAME,
                        model_version=config.MODEL_VERSION_TAG,
                    )

                    if img_rgb_full is not None:
                        thumb_path = self.thumbnailer.save_thumbnail(
                            img_rgb_full, face_data['bbox'], detection_id,
                            landmarks=face_data.get('landmarks'),
                        )
                        if thumb_path:
                            face_ops.set_detection_thumbnail(detection_id, thumb_path)

                    stats["faces_detected"] += 1
                    if stats["faces_detected"] % config.CHECKPOINT_INTERVAL == 0:
                        conn.commit()
                        logging.info(
                            f"Checkpoint: {stats['faces_detected']} faces from "
                            f"{stats['images_scanned']} images"
                        )

                stats["images_scanned"] += 1

            conn.commit()
            logging.info(
                f"Face scan complete. {stats['faces_detected']} face(s) detected in "
                f"{stats['images_scanned']} image(s) "
                f"({stats['images_no_faces']} with no faces, "
                f"{stats['images_failed']} failed)."
            )
        return stats

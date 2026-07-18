"""
Seed known people and birth dates from a YAML config file.

Loads a faces_config.yaml into face_persons so clustering can use
developmental era windows and linking can use supervised anchors from the
first run. Seeding is an accepted-state mutation: every create/update is
recorded as an applied run action with the seeding run's provenance.
"""
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from ..database.ops import DBOperations
from ..pipeline.actions import ActionSpec, PHASE_FACE_PERSON_LINK_APPLY, RunActionRecorder
from .db_ops import FaceDBOperations

try:
    import yaml
except ImportError:
    yaml = None


def load_seed_config(yaml_path: Path) -> list[dict]:
    """
    Parse a YAML config file and return a list of person dicts.

    Expected format:
        known_people:
          - name: Sam
            birth_date: 2005-03-15
            notes: "oldest child"
          - name: Emma
            birth_date: 2010-08-22

    Returns:
        List of dicts with keys: name (required), birth_date (optional),
        notes (optional).

    Raises:
        ImportError: If pyyaml is not installed.
        FileNotFoundError: If the YAML file doesn't exist.
        ValueError: If the YAML structure is invalid.
    """
    if yaml is None:
        raise ImportError(
            "pyyaml is required for seed config. Install with: uv sync --extra faces"
        )

    yaml_path = Path(yaml_path)
    if not yaml_path.exists():
        raise FileNotFoundError(f"Seed config not found: {yaml_path}")

    with yaml_path.open('r', encoding='utf-8') as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict) or 'known_people' not in data:
        raise ValueError("Seed config must contain a 'known_people' list")

    people = data['known_people']
    if not isinstance(people, list):
        raise ValueError("'known_people' must be a list")

    result = []
    for i, entry in enumerate(people):
        if not isinstance(entry, dict) or 'name' not in entry:
            raise ValueError(f"Entry {i} must be a dict with at least a 'name' key")

        person = {'name': str(entry['name']).strip()}

        birth_date = entry.get('birth_date')
        if birth_date is not None:
            birth_str = str(birth_date).strip()
            try:
                datetime.strptime(birth_str, '%Y-%m-%d')
            except ValueError:
                raise ValueError(
                    f"Invalid birth_date for {person['name']}: '{birth_str}'. "
                    f"Expected YYYY-MM-DD format."
                )
            person['birth_date'] = birth_str
        else:
            person['birth_date'] = None

        person['notes'] = entry.get('notes')
        result.append(person)

    return result


def apply_seed(db_ops: DBOperations, people: list[dict], *,
               run_id: int) -> Dict[str, Any]:
    """
    Upsert known people into face_persons.

    Matches on display_name (case-insensitive). Existing persons keep their
    id; birth_date/notes are filled or refreshed. Every mutation records an
    applied 'face_person_seed' run action.

    Returns stats: {"created": n, "updated": n, "unchanged": n}.
    """
    face_ops = FaceDBOperations(db_ops)
    recorder = RunActionRecorder(db_ops, run_id)
    stats = {"created": 0, "updated": 0, "unchanged": 0}

    for sequence, person in enumerate(people, start=1):
        name = person["name"]
        birth_date = person.get("birth_date")
        notes = person.get("notes")
        payload = {"notes": notes} if notes else None

        existing = face_ops.find_person_by_name(name)
        if existing is None:
            person_id = face_ops.create_person(
                run_id=run_id, display_name=name,
                birth_date=birth_date, payload=payload,
            )
            action_type = "create"
            stats["created"] += 1
            logging.info(f"Seeded new person: {name}"
                         + (f" (born {birth_date})" if birth_date else ""))
        else:
            person_id, _, existing_bd = existing
            if birth_date is None or birth_date == existing_bd:
                # Nothing to change (notes-only refreshes are not tracked as
                # changes; payload updates ride along with birth date fixes).
                stats["unchanged"] += 1
                logging.debug(f"Person already seeded (no changes): {name}")
                continue
            face_ops.update_person(
                run_id=run_id, person_id=person_id,
                birth_date=birth_date, payload=payload,
            )
            action_type = "update"
            stats["updated"] += 1
            logging.info(f"Updated person: {name} (born {birth_date})")

        recorder.record(ActionSpec(
            action_type="face_person_seed",
            entity_type="face_person",
            entity_id=person_id,
            source_path=None,
            target_path=None,
            status="applied",
            phase=PHASE_FACE_PERSON_LINK_APPLY,
            sequence=sequence,
            idempotency_key=f"face_person_seed:{name.lower()}:{birth_date or ''}",
            method=f"seed_{action_type}",
            payload={"display_name": name, "birth_date": birth_date},
        ))

    return stats

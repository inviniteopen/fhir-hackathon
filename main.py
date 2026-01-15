from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fhir.resources import get_fhir_model_class
from fhir.resources.bundle import Bundle
from pydantic import ValidationError


DEFAULT_INPUT_DIR = Path(__file__).resolve().parent / "data" / "EPS"


@dataclass
class ParseError:
    source: Path
    entry_index: int | None
    resource_type: str | None
    message: str


@dataclass
class ParsedEntry:
    index: int
    full_url: str | None
    resource_type: str | None
    resource: Any | None
    raw: dict[str, Any]


@dataclass
class ParsedBundle:
    source: Path
    bundle: Bundle
    entries: list[ParsedEntry] = field(default_factory=list)
    errors: list[ParseError] = field(default_factory=list)


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def parse_resource(
    source: Path,
    entry_index: int,
    resource_json: dict[str, Any],
    errors: list[ParseError],
) -> Any | None:
    resource_type = resource_json.get("resourceType")
    if not resource_type:
        errors.append(
            ParseError(
                source=source,
                entry_index=entry_index,
                resource_type=None,
                message="Missing resourceType",
            )
        )
        return None

    try:
        model_cls = get_fhir_model_class(resource_type)
    except Exception as exc:  # pragma: no cover - defensive
        errors.append(
            ParseError(
                source=source,
                entry_index=entry_index,
                resource_type=resource_type,
                message=f"Unsupported resourceType: {exc}",
            )
        )
        return None

    try:
        return model_cls.parse_obj(resource_json)
    except ValidationError as exc:
        for err in exc.errors():
            loc = ".".join(str(part) for part in err.get("loc", []))
            msg = err.get("msg", "Validation error")
            errors.append(
                ParseError(
                    source=source,
                    entry_index=entry_index,
                    resource_type=resource_type,
                    message=f"{loc}: {msg}",
                )
            )
        return None


def parse_bundle_file(path: Path) -> ParsedBundle:
    data = load_json(path)
    bundle_meta = {key: value for key, value in data.items() if key != "entry"}
    bundle = Bundle.parse_obj(bundle_meta)

    parsed = ParsedBundle(source=path, bundle=bundle)
    entries = data.get("entry", [])

    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            parsed.errors.append(
                ParseError(
                    source=path,
                    entry_index=index,
                    resource_type=None,
                    message="Entry is not an object",
                )
            )
            continue

        resource_json = entry.get("resource")
        if not isinstance(resource_json, dict):
            parsed.errors.append(
                ParseError(
                    source=path,
                    entry_index=index,
                    resource_type=None,
                    message="Entry.resource is missing or not an object",
                )
            )
            continue

        resource = parse_resource(path, index, resource_json, parsed.errors)
        parsed.entries.append(
            ParsedEntry(
                index=index,
                full_url=entry.get("fullUrl"),
                resource_type=resource_json.get("resourceType"),
                resource=resource,
                raw=resource_json,
            )
        )

    return parsed


def parse_directory(directory: Path) -> list[ParsedBundle]:
    return [parse_bundle_file(path) for path in sorted(directory.glob("*.json"))]


def summarize(bundles: list[ParsedBundle]) -> str:
    total_entries = 0
    total_parsed = 0
    total_errors = 0

    for bundle in bundles:
        total_entries += len(bundle.entries)
        total_parsed += sum(1 for entry in bundle.entries if entry.resource is not None)
        total_errors += len(bundle.errors)

    return (
        "Parsed bundles: {bundles}\n"
        "Entries: {entries}\n"
        "Typed resources: {typed}\n"
        "Errors: {errors}"
    ).format(
        bundles=len(bundles),
        entries=total_entries,
        typed=total_parsed,
        errors=total_errors,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parse FHIR JSON bundles into typed fhir.resources objects."
    )
    parser.add_argument(
        "input",
        nargs="?",
        default=DEFAULT_INPUT_DIR,
        type=Path,
        help="Directory containing FHIR Bundle JSON files.",
    )
    parser.add_argument(
        "--show-errors",
        action="store_true",
        help="Print validation errors for any resources that failed to parse.",
    )
    args = parser.parse_args()

    if not args.input.exists():
        raise SystemExit(f"Input directory does not exist: {args.input}")

    bundles = parse_directory(args.input)
    print(summarize(bundles))

    if args.show_errors:
        for bundle in bundles:
            for error in bundle.errors:
                print(
                    f"{bundle.source.name} entry={error.entry_index} "
                    f"type={error.resource_type} {error.message}"
                )


if __name__ == "__main__":
    main()

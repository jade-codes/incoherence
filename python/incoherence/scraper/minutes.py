"""Parser for council meeting minutes PDFs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pymupdf


@dataclass
class ParsedMinutes:
    """Extracted text from a council meeting minutes PDF."""

    file_path: str
    title: str
    body: str
    n_pages: int


def parse_minutes_pdf(pdf_path: Path) -> ParsedMinutes:
    """Extract text from a council meeting minutes PDF."""
    doc = pymupdf.open(str(pdf_path))
    pages_text = []
    for page in doc:
        pages_text.append(page.get_text())
    doc.close()

    body = "\n\n".join(pages_text)
    title = pdf_path.stem.replace("-", " ").replace("_", " ").title()

    return ParsedMinutes(
        file_path=str(pdf_path),
        title=title,
        body=body,
        n_pages=len(pages_text),
    )


def save_parsed_minutes(minutes: ParsedMinutes, output_dir: Path) -> Path:
    """Save parsed minutes as JSON for the extraction pipeline."""
    output_dir.mkdir(parents=True, exist_ok=True)
    slug = Path(minutes.file_path).stem
    out_path = output_dir / f"minutes_{slug}.json"
    out_path.write_text(
        json.dumps(
            {
                "source_file": minutes.file_path,
                "title": minutes.title,
                "body": minutes.body,
                "n_pages": minutes.n_pages,
            },
            indent=2,
        )
    )
    return out_path

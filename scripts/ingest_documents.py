from __future__ import annotations

import asyncio
import argparse
import hashlib
import json
from pathlib import Path
from typing import Iterable

import sqlalchemy as sa

from app.db.session import get_sessionmaker
from app.services.embedding_service import embedding_service


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_KNOWLEDGE_DIR = ROOT / "data" / "knowledge"
CHUNK_SIZE_DEFAULT = 500
OVERLAP_DEFAULT = 50


def _ensure_sample_docs(knowledge_dir: Path) -> None:
    knowledge_dir.mkdir(parents=True, exist_ok=True)
    sample = knowledge_dir / "sample_autotech_report.txt"
    if not sample.exists():
        sample.write_text(
            "新能源汽车行业在2022-2024年维持高增长，但竞争加剧导致价格承压。"
            "头部企业通过供应链整合、研发投入和渠道优化提升抗风险能力。"
            "风险关注点包括现金流波动、库存周转与法律诉讼事件。"
            "投资判断应结合财务稳健性、销售结构和政策变化。",
            encoding="utf-8",
        )


def _file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_manifest(manifest_path: Path) -> dict[str, str]:
    if not manifest_path.exists():
        return {}
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_manifest(manifest_path: Path, data: dict[str, str]) -> None:
    manifest_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _chunk_text(text: str, size: int = CHUNK_SIZE_DEFAULT, overlap: int = OVERLAP_DEFAULT) -> list[str]:
    t = (text or "").strip()
    if not t:
        return []
    chunks: list[str] = []
    start = 0
    step = max(1, size - overlap)
    while start < len(t):
        chunks.append(t[start : start + size])
        start += step
    return chunks


async def _upsert_source_chunks(source: str, title: str, chunks: list[str]) -> int:
    if not chunks:
        return 0
    vectors = await embedding_service.embed(chunks)
    vec_literals = ["[" + ",".join(f"{x:.8f}" for x in vec) + "]" for vec in vectors]

    sm = get_sessionmaker()
    async with sm() as db:
        # simple incremental strategy: replace chunks for changed file
        await db.execute(sa.text("DELETE FROM documents WHERE source = :s"), {"s": source})

        # ensure content_hash column exists even before migration is applied
        await db.execute(sa.text("ALTER TABLE documents ADD COLUMN IF NOT EXISTS content_hash TEXT"))

        sql = sa.text(
            """
            INSERT INTO documents (title, content, source, embedding, content_hash)
            VALUES (:title, :content, :source, CAST(:embedding AS vector), :content_hash)
            """
        )
        payload = [
            {
                "title": title,
                "content": chunk,
                "source": source,
                "embedding": vec_literal,
                "content_hash": hashlib.sha256(chunk.encode("utf-8")).hexdigest(),
            }
            for chunk, vec_literal in zip(chunks, vec_literals)
        ]
        await db.execute(sql, payload)
        await db.commit()
        return len(payload)


async def _source_chunk_count(source: str) -> int:
    sm = get_sessionmaker()
    async with sm() as db:
        row = (await db.execute(sa.text("SELECT COUNT(*)::int AS c FROM documents WHERE source=:s"), {"s": source})).first()
        return int(row[0] if row else 0)


def _parse_args():
    p = argparse.ArgumentParser(description="Ingest knowledge documents into pgvector documents table.")
    p.add_argument("--knowledge-dir", default=str(DEFAULT_KNOWLEDGE_DIR))
    p.add_argument("--chunk-size", type=int, default=CHUNK_SIZE_DEFAULT)
    p.add_argument("--overlap", type=int, default=OVERLAP_DEFAULT)
    p.add_argument("--no-sample", action="store_true")
    return p.parse_args()


async def main() -> int:
    args = _parse_args()
    knowledge_dir = Path(args.knowledge_dir)
    manifest_path = knowledge_dir / ".ingest_manifest.json"

    if not args.no_sample:
        _ensure_sample_docs(knowledge_dir)

    if not knowledge_dir.exists():
        print(f"[ingest] knowledge dir not found: {knowledge_dir}")
        return 0

    manifest = _load_manifest(manifest_path)
    docs = sorted([p for p in knowledge_dir.glob("**/*") if p.is_file() and p.suffix.lower() in {".txt", ".md"}])

    inserted_total = 0
    updated_manifest = dict(manifest)
    for p in docs:
        rel = str(p.relative_to(ROOT)).replace("\\", "/")
        h = _file_hash(p)
        if manifest.get(rel) == h:
            existing_count = await _source_chunk_count(rel)
            if existing_count > 0:
                print(f"[ingest] skip unchanged: {rel}")
                continue
            print(f"[ingest] manifest hit but source empty, re-ingest: {rel}")
        text = p.read_text(encoding="utf-8", errors="ignore")
        chunks = _chunk_text(text, size=args.chunk_size, overlap=args.overlap)
        inserted = await _upsert_source_chunks(source=rel, title=p.stem, chunks=chunks)
        inserted_total += inserted
        updated_manifest[rel] = h
        print(f"[ingest] {rel} chunks={inserted}")

    _save_manifest(manifest_path, updated_manifest)
    print(f"[ingest] done inserted_chunks={inserted_total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))


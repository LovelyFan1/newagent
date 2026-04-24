from __future__ import annotations

import io
import os
import tempfile
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from app.api.response import ok
from app.services.session_trace_service import session_trace_service

router = APIRouter()

_SUPPORTED_EXTS = {".csv", ".txt", ".docx", ".pdf"}


def _parse_csv(raw: bytes) -> str:
    bio = io.BytesIO(raw)
    df = pd.read_csv(bio)
    return df.to_csv(index=False)


def _parse_txt(raw: bytes) -> str:
    for enc in ("utf-8", "gbk", "latin-1"):
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode("utf-8", errors="ignore")


def _parse_docx(raw: bytes) -> str:
    try:
        from docx import Document
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"缺少 python-docx 依赖: {exc}") from exc
    with tempfile.NamedTemporaryFile(delete=False, suffix=".docx") as tmp:
        tmp.write(raw)
        tmp_path = tmp.name
    try:
        doc = Document(tmp_path)
        return "\n".join(p.text for p in doc.paragraphs if p.text).strip()
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


def _parse_pdf(raw: bytes) -> str:
    try:
        import fitz
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"缺少 PyMuPDF 依赖: {exc}") from exc
    text_chunks: list[str] = []
    with fitz.open(stream=raw, filetype="pdf") as doc:
        for page in doc:
            text_chunks.append(page.get_text("text") or "")
    return "\n".join(text_chunks).strip()


@router.post("/upload")
async def upload_file(file: UploadFile = File(...), session_id: str | None = Form(default=None)):
    filename = file.filename or "uploaded_file"
    suffix = Path(filename).suffix.lower()
    if suffix not in _SUPPORTED_EXTS:
        raise HTTPException(status_code=400, detail=f"不支持的文件格式: {suffix}")
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="上传文件为空")

    if suffix == ".csv":
        content = _parse_csv(raw)
    elif suffix == ".txt":
        content = _parse_txt(raw)
    elif suffix == ".docx":
        content = _parse_docx(raw)
    elif suffix == ".pdf":
        content = _parse_pdf(raw)
    else:
        raise HTTPException(status_code=400, detail=f"暂不支持的文件格式: {suffix}")

    sid = session_trace_service.ensure_session(session_id)
    session_trace_service.add_uploaded_file(session_id=sid, filename=filename, content=content)
    return ok(
        {
            "session_id": sid,
            "filename": filename,
            "content_preview": (content or "")[:300],
            "content_length": len(content or ""),
        }
    )


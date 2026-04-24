from __future__ import annotations

import io

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from app.services.session_trace_service import session_trace_service

router = APIRouter()


def _build_pdf_bytes(session_id: str, summary: str, sections: dict, uploaded_files: list) -> bytes:
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.pdfgen import canvas
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"缺少 reportlab 依赖: {exc}") from exc

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4
    x, y = 40, height - 50
    line_h = 16

    def _draw_line(text: str) -> None:
        nonlocal y
        if y < 50:
            c.showPage()
            y = height - 50
        c.drawString(x, y, text[:120])
        y -= line_h

    _draw_line(f"Session ID: {session_id}")
    _draw_line("Report Summary:")
    for ln in (summary or "No summary").splitlines():
        _draw_line(ln)
    _draw_line("-" * 48)
    _draw_line("Uploaded Files:")
    for f in uploaded_files or []:
        _draw_line(f"- {f.get('filename', 'unknown')} ({f.get('content_length', 0)} chars)")
    _draw_line("-" * 48)
    _draw_line("Sections:")
    for k, v in (sections or {}).items():
        _draw_line(f"{k}: {str(v)[:100]}")

    c.save()
    buf.seek(0)
    return buf.read()


@router.get("/download/{session_id}")
async def download_report(session_id: str):
    report = session_trace_service.get_latest_report(session_id)
    summary = report.get("summary") or ""
    if not summary:
        raise HTTPException(status_code=404, detail="未找到对应会话报告")
    pdf_bytes = _build_pdf_bytes(
        session_id=session_id,
        summary=summary,
        sections=report.get("sections") or {},
        uploaded_files=report.get("uploaded_files") or [],
    )
    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="report_{session_id}.pdf"'},
    )


from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException


def install_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        # Keep consistent envelope for common HTTP errors.
        return JSONResponse(status_code=exc.status_code, content={"code": exc.status_code, "data": None, "message": exc.detail})

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        # Keep consistent envelope for FastAPI validation errors.
        return JSONResponse(
            status_code=422,
            content={
                "code": 422,
                "data": exc.errors(),
                "message": "Validation Error",
            },
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(_: Request, __: Exception) -> JSONResponse:
        return JSONResponse(status_code=500, content={"code": 500, "data": None, "message": "Internal Server Error"})


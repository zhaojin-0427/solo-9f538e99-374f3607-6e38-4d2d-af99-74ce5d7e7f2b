"""FastAPI entry point for the conversion-prediction sample batch service."""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator, Optional

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .db import Database
from .models import ApiError, EventBatchRequest, OpenBatchRequest

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DEFAULT_DB_PATH = os.environ.get("SAMPLE_BATCH_DB_PATH", "data/sample_batch.db")
MAX_BODY_BYTES = int(
    os.environ.get("SAMPLE_BATCH_MAX_BODY_BYTES", str(16 * 1024 * 1024))
)
# Excel opens UTF-8 CSV correctly when the BOM is present.
CSV_BOM = "﻿"


class BodySizeLimitMiddleware:
    """Reject request bodies above a byte limit with a structured 413."""

    def __init__(self, app: ASGIApp, max_bytes) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["method"] not in {
            "POST",
            "PUT",
            "PATCH",
        }:
            await self.app(scope, receive, send)
            return

        limit = self.max_bytes()
        declared = dict(scope["headers"]).get(b"content-length")
        if declared is not None:
            try:
                if int(declared) > limit:
                    await self._reject(send, limit)
                    return
            except ValueError:
                await self._reject(send, limit)
                return

        chunks: list[bytes] = []
        total = 0
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            body = message.get("body", b"")
            total += len(body)
            if total > limit:
                await self._reject(send, limit)
                return
            chunks.append(body)
            if not message.get("more_body", False):
                break

        buffered = b"".join(chunks)
        replayed = False

        async def replay_receive() -> Message:
            nonlocal replayed
            if not replayed:
                replayed = True
                return {
                    "type": "http.request",
                    "body": buffered,
                    "more_body": False,
                }
            return {"type": "http.disconnect"}

        await self.app(scope, replay_receive, send)

    @staticmethod
    async def _reject(send: Send, limit: int) -> None:
        payload = json.dumps(
            {
                "error": {
                    "code": "payload_too_large",
                    "message": f"请求体过大，请拆分后重试。上限为 {limit} 字节。",
                    "details": [],
                }
            },
            ensure_ascii=False,
        ).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": 413,
                "headers": [
                    (b"content-type", b"application/json; charset=utf-8"),
                    (b"content-length", str(len(payload)).encode("ascii")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": payload})


def create_app(db_path: Optional[str] = None) -> FastAPI:
    database = Database(db_path or DEFAULT_DB_PATH)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        database.initialize()
        try:
            yield
        finally:
            database.close()

    app = FastAPI(
        title="成交预测样本批次服务",
        version="1.0.0",
        description=(
            "按受理快照冻结观察窗口：开批固定名单与七日内分渠道触点特征，"
            "到期结批生成每用户一行的成交标签 CSV。"
        ),
        lifespan=lifespan,
    )
    app.state.db = database

    app.add_middleware(
        BodySizeLimitMiddleware,
        max_bytes=lambda: MAX_BODY_BYTES,
    )

    @app.exception_handler(ApiError)
    async def handle_api_error(_: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": exc.code,
                    "message": exc.message,
                    "details": exc.errors,
                }
            },
        )

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(
        _: Request, exc: RequestValidationError
    ) -> JSONResponse:
        details = []
        for error in exc.errors():
            location = ".".join(str(part) for part in error.get("loc", []))
            message = error.get("msg", "invalid value")
            if message.startswith("Value error, "):
                message = message[len("Value error, ") :]
            details.append(
                {
                    "field": location or "body",
                    "message": message,
                    "type": error.get("type", "value_error"),
                }
            )
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "invalid_payload",
                    "message": "请求校验失败，整批请求均未被处理。",
                    "details": details,
                }
            },
        )

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/health", tags=["system"])
    def health() -> JSONResponse:
        healthy = database.health()
        return JSONResponse(
            status_code=200 if healthy else 503,
            content={"status": "ok" if healthy else "degraded"},
        )

    # -- event ingestion ----------------------------------------------------

    @app.post("/api/events", tags=["events"])
    def submit_events(payload: EventBatchRequest) -> dict:
        return database.ingest_events(payload)

    @app.get("/api/users", tags=["events"])
    def list_users() -> dict:
        users = database.list_users()
        return {"users": users, "count": len(users)}

    @app.get("/api/users/{user_id}/events", tags=["events"])
    def user_events(user_id: str) -> dict:
        return database.get_user_events(user_id)

    # -- batch lifecycle ----------------------------------------------------

    @app.post("/api/batches", tags=["batches"], status_code=201)
    def open_batch(payload: OpenBatchRequest) -> dict:
        return database.open_batch(
            user_ids=payload.user_ids,
            observation_minutes=payload.observation_minutes,
        )

    @app.get("/api/batches", tags=["batches"])
    def list_batches() -> dict:
        batches = database.list_batches()
        return {"batches": batches, "count": len(batches)}

    @app.get("/api/batches/{batch_id}", tags=["batches"])
    def get_batch(batch_id: str) -> dict:
        return database.get_batch(batch_id)

    @app.post("/api/batches/{batch_id}/close", tags=["batches"])
    def close_batch(batch_id: str) -> dict:
        return database.close_batch(batch_id)

    @app.get("/api/batches/{batch_id}/csv", tags=["batches"])
    def download_batch_csv(batch_id: str) -> Response:
        csv_text, filename = database.download_csv(batch_id)
        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
        }
        return Response(
            content=CSV_BOM + csv_text,
            media_type="text/csv; charset=utf-8",
            headers=headers,
        )

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
    return app


app = create_app()

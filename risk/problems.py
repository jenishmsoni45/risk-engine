"""RFC 9457 problem responses, shared by the decision API and the ledger stub."""

from __future__ import annotations

from dataclasses import dataclass
from http import HTTPStatus
from typing import Any

from fastapi.responses import JSONResponse

PROBLEM_JSON = "application/problem+json"


@dataclass(frozen=True)
class FieldError:
    field: str
    message: str


def problem(
    status: int,
    detail: str,
    *,
    type_: str = "about:blank",
    title: str | None = None,
    **extra: Any,
) -> JSONResponse:
    body: dict[str, Any] = {
        "type": type_,
        "title": title or HTTPStatus(status).phrase,
        "status": status,
        "detail": detail,
        **extra,
    }
    return JSONResponse(body, status_code=status, media_type=PROBLEM_JSON)


def bad_request(detail: str, errors: list[FieldError], **extra: Any) -> JSONResponse:
    return problem(
        400,
        detail,
        errors=[{"field": e.field, "message": e.message} for e in errors],
        **extra,
    )

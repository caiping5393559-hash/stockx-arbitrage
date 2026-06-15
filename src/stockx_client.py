from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any

import requests

from .config import Settings, get_settings
from .db import log_sync, save_raw_response
from .parsing import extract_next_cursor


@dataclass
class ApiCallResult:
    endpoint: str
    params: dict[str, Any]
    pages: list[Any]
    ok: bool = True
    error: str | None = None


class StockXClient:
    def __init__(self, conn, settings: Settings | None = None) -> None:
        self.conn = conn
        self.settings = settings or get_settings()
        self.session = requests.Session()

    def _credential_modes(self) -> list[str]:
        primary = self.settings.credential_mode if self.settings.credential_mode in {"header", "query", "both"} else "both"
        ordered: list[str] = []
        for mode in [primary, "both", "header", "query"]:
            if mode not in ordered:
                ordered.append(mode)
        return ordered

    def _credentials_for_mode(self, mode: str) -> tuple[dict[str, str], dict[str, str]]:
        params: dict[str, str] = {}
        headers: dict[str, str] = {}
        use_query = mode in {"query", "both"}
        use_headers = mode in {"header", "both"}
        if use_query and self.settings.token:
            params[self.settings.token_param] = self.settings.token
        if use_query and self.settings.auth:
            params[self.settings.auth_param] = self.settings.auth
        if use_headers and self.settings.token:
            headers[self.settings.token_header] = self.settings.token
        if use_headers and self.settings.auth:
            headers[self.settings.auth_header] = self.settings.auth
        return params, headers

    def _safe_params(self, params: dict[str, Any]) -> dict[str, Any]:
        hidden_keys = {
            self.settings.token_param.lower(),
            self.settings.auth_param.lower(),
            self.settings.token_header.lower(),
            self.settings.auth_header.lower(),
            "token",
            "auth",
            "authorization",
        }
        safe: dict[str, Any] = {}
        for key, value in params.items():
            safe[key] = "***" if key.lower() in hidden_keys else value
        return safe

    def _safe_error_text(self, message: str) -> str:
        text = str(message)
        for secret in (self.settings.token, self.settings.auth):
            if secret:
                text = text.replace(secret, "***")
        for key in {
            self.settings.token_param,
            self.settings.auth_param,
            self.settings.token_header,
            self.settings.auth_header,
            "token",
            "auth",
            "authorization",
        }:
            if key:
                text = re.sub(rf"({re.escape(key)}=)[^&\s)]+", r"\1***", text, flags=re.IGNORECASE)
        return text

    def _audit_write(self, write_fn) -> None:
        for attempt in range(5):
            try:
                write_fn()
                self._safe_commit()
                return
            except Exception as exc:  # noqa: BLE001 - audit storage must not invalidate a successful API response.
                if "database is locked" not in str(exc).lower() or attempt == 4:
                    return
                time.sleep(0.5 * (attempt + 1))

    def _safe_commit(self) -> None:
        for attempt in range(5):
            try:
                self.conn.commit()
                return
            except Exception as exc:  # noqa: BLE001
                if "database is locked" not in str(exc).lower() or attempt == 4:
                    return
                time.sleep(0.5 * (attempt + 1))

    @staticmethod
    def _business_error(payload: Any) -> str | None:
        if not isinstance(payload, dict):
            return None
        code = payload.get("code")
        if code not in (None, 0, 200, "200", "success"):
            return str(payload.get("msg") or payload.get("message") or f"code={code}")
        errors = payload.get("errors")
        data = payload.get("data")
        if isinstance(data, dict):
            errors = errors or data.get("errors")
        if errors:
            return str(errors)
        return None

    def request(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        *,
        paginate: bool = True,
    ) -> ApiCallResult:
        endpoint = endpoint if endpoint.startswith("/") else f"/{endpoint}"
        url = f"{self.settings.host}{endpoint}"
        base_params = dict(params or {})
        auth_present = bool(self.settings.token or self.settings.auth)
        errors: list[str] = []

        for mode in self._credential_modes():
            auth_params, headers = self._credentials_for_mode(mode)
            pages: list[Any] = []
            cursor: str | None = None
            seen_cursors: set[str] = set()
            auth_failed = False

            while True:
                request_params = {**base_params, **auth_params}
                if cursor:
                    request_params["cursor"] = cursor
                try:
                    request_headers = {
                        "Cache-Control": "no-cache",
                        "Pragma": "no-cache",
                        **headers,
                    }
                    response = self.session.get(
                        url,
                        params=request_params,
                        headers=request_headers,
                        timeout=self.settings.timeout,
                    )
                    status_code = response.status_code
                    try:
                        payload = response.json()
                    except ValueError:
                        payload = {"text": response.text}
                    self._audit_write(
                        lambda: save_raw_response(
                            self.conn,
                            endpoint,
                            self._safe_params(request_params),
                            status_code=status_code,
                            response=payload,
                        )
                    )
                    if auth_present and status_code in {401, 403}:
                        auth_failed = True
                        errors.append(f"{mode}: {status_code}")
                        log_sync(
                            self.conn,
                            f"{endpoint} 鉴权失败，尝试切换凭证位置: {mode} -> {status_code}",
                            severity="warning",
                            event_type="auth_retry",
                            endpoint=endpoint,
                            details={"mode": mode, "status_code": status_code, "params": self._safe_params(request_params)},
                        )
                        break
                    response.raise_for_status()
                    business_error = self._business_error(payload)
                    if business_error:
                        raise ValueError(f"业务错误: {business_error}")
                    pages.append(payload)
                    next_cursor = extract_next_cursor(payload)
                    if not paginate or not next_cursor or next_cursor in seen_cursors:
                        self._safe_commit()
                        return ApiCallResult(endpoint, params or {}, pages, ok=True)
                    seen_cursors.add(next_cursor)
                    cursor = next_cursor
                except Exception as exc:  # noqa: BLE001 - every exception is logged for audit.
                    safe_error = self._safe_error_text(str(exc))
                    message = f"{endpoint} 调用失败: {safe_error}"
                    self._audit_write(
                        lambda: save_raw_response(
                            self.conn,
                            endpoint,
                            self._safe_params(request_params),
                            error_message=safe_error,
                        )
                    )
                    self._audit_write(
                        lambda: log_sync(
                            self.conn,
                            message,
                            severity="error",
                            event_type="api_error",
                            endpoint=endpoint,
                            details={"params": self._safe_params(request_params), "mode": mode},
                        )
                    )
                    self._safe_commit()
                    return ApiCallResult(endpoint, params or {}, pages, ok=False, error=safe_error)

            if auth_failed:
                continue

        error_message = "鉴权失败，已尝试 header/query/both 三种方式"
        if errors:
            error_message = f"{error_message}（{'; '.join(errors)}）"
        log_sync(
            self.conn,
            f"{endpoint} {error_message}",
            severity="error",
            event_type="auth_failed",
            endpoint=endpoint,
            details={"errors": errors, "params": self._safe_params(base_params)},
        )
        self._safe_commit()
        return ApiCallResult(endpoint, params or {}, [], ok=False, error=error_message)

    def get_product_detail_info_by_sku(self, sku: str) -> ApiCallResult:
        return self.request("/get_product_detail_info_by_sku", {"sku": sku}, paginate=False)

    def get_product_size_info_by_sku(self, sku: str) -> ApiCallResult:
        return self.request("/get_product_size_info_by_sku", {"sku": sku}, paginate=False)

    def search_product(
        self,
        *,
        keyword: str,
        page: int = 1,
        country: str = "US",
        category: str | None = None,
        currency_code: str = "USD",
    ) -> ApiCallResult:
        params: dict[str, Any] = {
            "keyword": keyword,
            "page": page,
            "country": country,
            "currency_code": currency_code,
        }
        if category:
            params["category"] = category
        return self.request("/search_product", params)

    def product_detail(self, product_uuid: str | None = None, *, currency_code: str = "USD") -> ApiCallResult:
        return self.request("/product_detail", self._product_uuid_params(product_uuid, currency_code), paginate=False)

    def product_market_info(
        self,
        product_id: str | None = None,
        *,
        country: str = "US",
        currency_code: str = "USD",
    ) -> ApiCallResult:
        return self.request(
            "/product_market_info",
            self._product_id_params(product_id, country, currency_code),
        )

    def product_size_price(
        self,
        product_id: str | None = None,
        *,
        country: str = "US",
        currency_code: str = "USD",
        need_guidance_info: int = 0,
    ) -> ApiCallResult:
        params = self._product_id_params(product_id, country, currency_code)
        params["need_guidance_info"] = need_guidance_info
        return self.request("/product_size_price", params)

    def product_size_market_info(
        self,
        product_size_uuid: str,
        *,
        country: str = "US",
        currency_code: str = "USD",
    ) -> ApiCallResult:
        return self.request(
            "/product_size_market_info",
            self._product_size_uuid_params(product_size_uuid, country, currency_code),
        )

    def product_activity_new(
        self,
        product_uuid: str | None = None,
        *,
        country: str = "US",
        currency_code: str = "USD",
        view: str = "SELLER",
    ) -> ApiCallResult:
        return self.request(
            "/product_activity_new",
            self._product_uuid_params(product_uuid, currency_code, country=country, extra={"view": view}),
        )

    def product_size_activity_new(
        self,
        product_uuid: str | None = None,
        *,
        country: str = "US",
        currency_code: str = "USD",
        view: str = "SELLER",
    ) -> ApiCallResult:
        return self.request(
            "/product_size_activity_new",
            self._product_uuid_params(product_uuid, currency_code, country=country, extra={"view": view}),
        )

    def product_ask_list(
        self,
        product_uuid: str | None = None,
        *,
        country: str = "US",
        currency_code: str = "USD",
        page: int = 1,
    ) -> ApiCallResult:
        return self.request(
            "/product_ask_list",
            self._product_uuid_params(product_uuid, currency_code, country=country, extra={"page": page}),
        )

    def product_size_ask_list(
        self,
        product_uuid: str | None = None,
        *,
        country: str = "US",
        currency_code: str = "USD",
        page: int = 1,
    ) -> ApiCallResult:
        return self.request(
            "/product_size_ask_list",
            self._product_uuid_params(product_uuid, currency_code, country=country, extra={"page": page}),
        )

    def product_bid_list(
        self,
        product_uuid: str | None = None,
        *,
        country: str = "US",
        currency_code: str = "USD",
        page: int = 1,
    ) -> ApiCallResult:
        return self.request(
            "/product_bid_list",
            self._product_uuid_params(product_uuid, currency_code, country=country, extra={"page": page}),
        )

    def product_size_bid_list(
        self,
        product_uuid: str | None = None,
        *,
        country: str = "US",
        currency_code: str = "USD",
        page: int = 1,
    ) -> ApiCallResult:
        return self.request(
            "/product_size_bid_list",
            self._product_uuid_params(product_uuid, currency_code, country=country, extra={"page": page}),
        )

    @staticmethod
    def _product_uuid_params(
        product_uuid: str | None,
        currency_code: str = "USD",
        *,
        country: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"currency_code": currency_code}
        if product_uuid:
            params["product_uuid"] = product_uuid
        if country:
            params["country"] = country
        if extra:
            params.update(extra)
        return params

    @staticmethod
    def _product_id_params(product_id: str | None, country: str = "US", currency_code: str = "USD") -> dict[str, Any]:
        params: dict[str, Any] = {"country": country, "currency_code": currency_code}
        if product_id:
            params["product_id"] = product_id
        return params

    @staticmethod
    def _product_size_uuid_params(
        product_size_uuid: str | None,
        country: str = "US",
        currency_code: str = "USD",
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"country": country, "currency_code": currency_code}
        if product_size_uuid:
            params["product_size_uuid"] = product_size_uuid
        return params

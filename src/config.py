from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - allows the app to show a useful error.
    load_dotenv = None


BASE_DIR = Path(__file__).resolve().parents[1]


def _load_env() -> None:
    if load_dotenv is not None:
        load_dotenv(BASE_DIR / ".env", override=True)


_load_env()


def _clean_env_value(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        return text[1:-1].strip()
    return text


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in raw_line:
            continue
        key, _, value = raw_line.partition("=")
        key = key.strip()
        if not key:
            continue
        values[key] = _clean_env_value(value)
    return values


def _settings_values() -> dict[str, str]:
    values = dict(os.environ)
    values.update(_read_env_file(BASE_DIR / ".env"))
    return values


def _int_from_values(values: dict[str, str], name: str, default: int) -> int:
    value = values.get(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _float_from_values(values: dict[str, str], name: str, default: float) -> float:
    value = values.get(name)
    if not value:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _bool_from_values(values: dict[str, str], name: str, default: bool) -> bool:
    value = values.get(name)
    if value is None or value == "":
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _path_from_values(values: dict[str, str], name: str, default: str) -> Path:
    raw_path = values.get(name, default)
    path = Path(raw_path)
    if not path.is_absolute():
        path = BASE_DIR / path
    return path


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _float_env(name: str, default: float) -> float:
    value = os.getenv(name)
    if not value:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _path_env(name: str, default: str) -> Path:
    raw_path = os.getenv(name, default)
    path = Path(raw_path)
    if not path.is_absolute():
        path = BASE_DIR / path
    return path


def _str_env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


@dataclass(frozen=True)
class Settings:
    app_login_enabled: bool = field(default_factory=lambda: _bool_env("APP_LOGIN_ENABLED", False))
    app_username: str = field(default_factory=lambda: _str_env("APP_USERNAME", "admin"))
    app_password: str = field(default_factory=lambda: _str_env("APP_PASSWORD"))
    cloud_storage_backend: str = field(default_factory=lambda: _str_env("CLOUD_STORAGE_BACKEND", "sqlite").lower())
    firebase_project_id: str = field(default_factory=lambda: _str_env("FIREBASE_PROJECT_ID"))
    firebase_collection_prefix: str = field(default_factory=lambda: _str_env("FIREBASE_COLLECTION_PREFIX", "stockx_goat"))
    firebase_credentials_path: str = field(default_factory=lambda: _str_env("FIREBASE_CREDENTIALS_PATH"))
    firebase_service_account_json: str = field(default_factory=lambda: _str_env("FIREBASE_SERVICE_ACCOUNT_JSON"))
    firebase_service_account_b64: str = field(default_factory=lambda: _str_env("FIREBASE_SERVICE_ACCOUNT_B64"))
    firebase_sqlite_backup_max_mb: float = field(default_factory=lambda: _float_env("FIREBASE_SQLITE_BACKUP_MAX_MB", 1500.0))
    host: str = field(default_factory=lambda: _str_env("STOCKX_HOST", "http://43.136.43.128:61030/api/stockx").rstrip("/"))
    token: str = field(default_factory=lambda: _str_env("STOCKX_TOKEN"))
    auth: str = field(default_factory=lambda: _str_env("STOCKX_AUTH"))
    credential_mode: str = field(default_factory=lambda: _str_env("STOCKX_CREDENTIAL_MODE", "both").lower())
    token_param: str = field(default_factory=lambda: _str_env("STOCKX_TOKEN_PARAM", "token"))
    auth_param: str = field(default_factory=lambda: _str_env("STOCKX_AUTH_PARAM", "auth"))
    token_header: str = field(default_factory=lambda: _str_env("STOCKX_TOKEN_HEADER", "token"))
    auth_header: str = field(default_factory=lambda: _str_env("STOCKX_AUTH_HEADER", "auth"))
    timeout: int = field(default_factory=lambda: _int_env("STOCKX_REQUEST_TIMEOUT", 20))
    db_path: Path = field(default_factory=lambda: _path_env("STOCKX_DB_PATH", "data/stockx_arbitrage.sqlite"))
    estimated_seller_fee_rate: float = field(default_factory=lambda: _float_env("ESTIMATED_SELLER_FEE_RATE", 0.03))
    buy_depth_sales_fraction: float = field(default_factory=lambda: _float_env("BUY_DEPTH_SALES_FRACTION", 0.75))
    auto_full_sync_enabled: bool = field(default_factory=lambda: _bool_env("AUTO_FULL_SYNC_ENABLED", True))
    auto_full_sync_interval_minutes: int = field(default_factory=lambda: _int_env("AUTO_FULL_SYNC_INTERVAL_MINUTES", 60))
    sync_max_workers: int = field(default_factory=lambda: max(1, min(_int_env("SYNC_MAX_WORKERS", 4), 8)))

    @property
    def credentials_ready(self) -> bool:
        return bool(self.token and self.auth)

    @property
    def app_auth_ready(self) -> bool:
        return bool(self.app_username and self.app_password)

    @property
    def firebase_enabled(self) -> bool:
        return self.cloud_storage_backend == "firebase"


def get_settings() -> Settings:
    values = _settings_values()
    return Settings(
        app_login_enabled=_bool_from_values(values, "APP_LOGIN_ENABLED", False),
        app_username=values.get("APP_USERNAME", "admin").strip(),
        app_password=values.get("APP_PASSWORD", "").strip(),
        cloud_storage_backend=values.get("CLOUD_STORAGE_BACKEND", "sqlite").strip().lower(),
        firebase_project_id=values.get("FIREBASE_PROJECT_ID", "").strip(),
        firebase_collection_prefix=values.get("FIREBASE_COLLECTION_PREFIX", "stockx_goat").strip(),
        firebase_credentials_path=values.get("FIREBASE_CREDENTIALS_PATH", "").strip(),
        firebase_service_account_json=values.get("FIREBASE_SERVICE_ACCOUNT_JSON", "").strip(),
        firebase_service_account_b64=values.get("FIREBASE_SERVICE_ACCOUNT_B64", "").strip(),
        firebase_sqlite_backup_max_mb=_float_from_values(values, "FIREBASE_SQLITE_BACKUP_MAX_MB", 1500.0),
        host=values.get("STOCKX_HOST", "http://43.136.43.128:61030/api/stockx").rstrip("/"),
        token=values.get("STOCKX_TOKEN", "").strip(),
        auth=values.get("STOCKX_AUTH", "").strip(),
        credential_mode=values.get("STOCKX_CREDENTIAL_MODE", "both").lower(),
        token_param=values.get("STOCKX_TOKEN_PARAM", "token").strip(),
        auth_param=values.get("STOCKX_AUTH_PARAM", "auth").strip(),
        token_header=values.get("STOCKX_TOKEN_HEADER", "token").strip(),
        auth_header=values.get("STOCKX_AUTH_HEADER", "auth").strip(),
        timeout=_int_from_values(values, "STOCKX_REQUEST_TIMEOUT", 20),
        db_path=_path_from_values(values, "STOCKX_DB_PATH", "data/stockx_arbitrage.sqlite"),
        estimated_seller_fee_rate=_float_from_values(values, "ESTIMATED_SELLER_FEE_RATE", 0.03),
        buy_depth_sales_fraction=_float_from_values(values, "BUY_DEPTH_SALES_FRACTION", 0.75),
        auto_full_sync_enabled=_bool_from_values(values, "AUTO_FULL_SYNC_ENABLED", True),
        auto_full_sync_interval_minutes=_int_from_values(values, "AUTO_FULL_SYNC_INTERVAL_MINUTES", 60),
        sync_max_workers=max(1, min(_int_from_values(values, "SYNC_MAX_WORKERS", 4), 8)),
    )

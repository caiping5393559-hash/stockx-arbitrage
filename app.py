from __future__ import annotations

import json
import base64
import hashlib
import hmac
import math
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time as time_module
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from datetime import datetime, time, timedelta, timezone
from html import escape
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import quote
try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python 3.10+ normally has zoneinfo.
    ZoneInfo = None

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from src.analytics import (
    adjacent_size_context,
    build_ask_depth_strategies,
    build_strategy_options,
    compute_and_store_opportunities,
    get_sales_stats,
    get_reference_price,
    latest_ask_rows,
    latest_bid_rows,
    latest_lowest_ask_by_size,
    rating_from_score,
    simulate_ask_depth,
)
from src.config import BASE_DIR, get_settings
from src.db import connect, init_db, json_dumps, json_loads, log_sync, query_rows, upsert_reference_price
from src.firebase_cloud import (
    backup_core_tables_to_firestore,
    backup_sqlite_to_firestore,
    firebase_status,
    restore_core_tables_if_needed,
    restore_packaged_stockx_seed_if_empty,
    restore_sqlite_backup_if_needed,
)
from src.importer import import_sku_file, list_imported_skus
from src.portfolio import add_trade, portfolio_summary
from src.parsing import extract_product, extract_product_uuid, extract_release_date, extract_size_variants, normalize_style_no
from src.sample_data import seed_sample_data
from src.release_dates import lookup_release_date
from src.sync import SyncSummary, _extract_depth_rows, _resolve_product_from_search, sync_style
from src.stockx_client import StockXClient


st.set_page_config(
    page_title="StockX / GOAT 套利扫描器 MVP",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .stApp,
    div[data-testid="stAppViewContainer"],
    .main,
    .main .block-container {
        background: #f5f6f8;
        color: #111827;
    }
    .main .block-container,
    .main .block-container h1,
    .main .block-container h2,
    .main .block-container h3,
    .main .block-container p,
    .main .block-container label,
    .main .block-container span,
    .main .block-container div[data-testid="stMarkdownContainer"] {
        color: #111827;
    }
    .main .block-container [data-testid="stWidgetLabel"] *,
    .main .block-container [data-testid="stRadio"] *,
    .main .block-container [data-testid="stCheckbox"] *,
    .main .block-container [data-testid="stToggle"] *,
    .main .block-container [data-baseweb="select"] *,
    .main .block-container [data-baseweb="input"] *,
    .main .block-container [data-baseweb="textarea"] * {
        color: #111827 !important;
    }
    .main .block-container small,
    .main .block-container [data-testid="stCaptionContainer"],
    .main .block-container [data-testid="stCaptionContainer"] * {
        color: #4b5563 !important;
    }
    .main input,
    .main textarea,
    .main [data-baseweb="select"] div,
    .main [data-baseweb="input"] input {
        color: #111827 !important;
    }
    header[data-testid="stHeader"] {
        background: #f5f6f8;
    }
    header[data-testid="stHeader"],
    div[data-testid="stToolbar"],
    div[data-testid="stDecoration"],
    #MainMenu,
    footer {
        display: none !important;
        height: 0 !important;
        min-height: 0 !important;
        visibility: hidden !important;
    }
    section[data-testid="stMain"] > div,
    div[data-testid="stMainBlockContainer"],
    div[data-testid="stAppViewBlockContainer"],
    .block-container {
        padding-top: 0.25rem !important;
        margin-top: 0 !important;
    }
    section[data-testid="stSidebar"] {
        background: #252632;
        color: #ffffff;
    }
    section[data-testid="stSidebar"] * {
        color: inherit;
    }
    .main .block-container {
        max-width: 1780px;
        padding-top: 0.25rem !important;
        padding-left: 2.4rem;
        padding-right: 2.4rem;
    }
    .main .block-container h1,
    .main .block-container h2,
    .main .block-container h3 {
        margin-top: 0.2rem !important;
        margin-bottom: 0.35rem !important;
        line-height: 1.15 !important;
    }
    div[data-testid="stVerticalBlock"] {
        gap: 0.45rem !important;
    }
    div[data-testid="stForm"] {
        border-radius: 8px;
        padding: 0.8rem 1rem 0.6rem !important;
    }
    div[data-testid="stForm"] div[data-testid="stVerticalBlock"] {
        gap: 0.35rem !important;
    }
    div[data-testid="stForm"] label,
    div[data-testid="stForm"] label *,
    div[data-testid="stForm"] [data-testid="stWidgetLabel"],
    div[data-testid="stForm"] [data-testid="stWidgetLabel"] *,
    div[data-testid="stForm"] [data-testid="stMarkdownContainer"],
    div[data-testid="stForm"] [data-testid="stMarkdownContainer"] *,
    div[data-testid="stForm"] [data-testid="stCaptionContainer"],
    div[data-testid="stForm"] [data-testid="stCaptionContainer"] * {
        color: #111827 !important;
        -webkit-text-fill-color: #111827 !important;
    }
    div[data-testid="stExpander"] {
        margin-top: 0.25rem !important;
        margin-bottom: 0.25rem !important;
    }
    div[data-testid="stExpander"] details,
    div[data-testid="stExpander"] summary {
        background: #ffffff !important;
        color: #111827 !important;
        border-radius: 8px !important;
    }
    div[data-testid="stExpander"] summary *,
    div[data-baseweb="tab-list"] *,
    button[role="tab"] *,
    [role="tab"] * {
        color: #111827 !important;
        -webkit-text-fill-color: #111827 !important;
    }
    div[data-testid="stMetric"] {
        background: #ffffff;
        border: 1px solid #e5e7eb;
        border-radius: 8px;
        padding: 10px 12px;
    }
    div[data-testid="stMetric"] * {
        color: #111827 !important;
    }
    html body [data-testid="stAppViewContainer"] .main .block-container button,
    html body [data-testid="stAppViewContainer"] .main .block-container button *,
    html body [data-testid="stAppViewContainer"] .main .block-container [data-testid="stButton"] *,
    html body [data-testid="stAppViewContainer"] .main .block-container [data-testid="stFormSubmitButton"] *,
    html body [data-testid="stAppViewContainer"] .main .block-container [data-testid="stDownloadButton"] * {
        color: #ffffff !important;
        -webkit-text-fill-color: #ffffff !important;
    }
    html body [data-testid="stAppViewContainer"] .main .block-container input,
    html body [data-testid="stAppViewContainer"] .main .block-container textarea,
    html body [data-testid="stAppViewContainer"] .main .block-container [data-baseweb="input"],
    html body [data-testid="stAppViewContainer"] .main .block-container [data-baseweb="input"] *,
    html body [data-testid="stAppViewContainer"] .main .block-container [data-baseweb="select"],
    html body [data-testid="stAppViewContainer"] .main .block-container [data-baseweb="select"] *,
    html body [data-testid="stAppViewContainer"] .main .block-container [data-baseweb="textarea"],
    html body [data-testid="stAppViewContainer"] .main .block-container [data-baseweb="textarea"] * {
        background-color: #ffffff !important;
        color: #111827 !important;
        -webkit-text-fill-color: #111827 !important;
    }
    html body [data-testid="stAppViewContainer"] .main .block-container [data-testid="stWidgetLabel"] *,
    html body [data-testid="stAppViewContainer"] .main .block-container [data-testid="stMarkdownContainer"] *,
    html body [data-testid="stAppViewContainer"] .main .block-container [data-testid="stCaptionContainer"] *,
    html body [data-testid="stAppViewContainer"] .main .block-container [data-testid="stCheckbox"] *,
    html body [data-testid="stAppViewContainer"] .main .block-container [data-testid="stToggle"] *,
    html body [data-testid="stAppViewContainer"] .main .block-container [data-testid="stRadio"] * {
        color: #111827 !important;
        -webkit-text-fill-color: #111827 !important;
    }
    div[data-testid="stDataFrame"] {
        border-radius: 8px;
        overflow: hidden;
    }
    .ui-page-head {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 16px;
        margin: 0.15rem 0 0.75rem;
    }
    .ui-page-title {
        margin: 0;
        color: #0f172a !important;
        font-size: 32px;
        font-weight: 850;
        line-height: 1.1;
    }
    .ui-page-sub {
        margin-top: 8px;
        color: #64748b !important;
        font-size: 14px;
        line-height: 1.5;
        max-width: 1180px;
    }
    .ui-pill {
        flex: 0 0 auto;
        border-radius: 999px;
        padding: 7px 12px;
        background: #e8f0ff;
        color: #175cd3 !important;
        font-size: 12px;
        font-weight: 800;
        border: 1px solid #c7d7fe;
    }
    .ui-card-grid {
        display: grid;
        grid-template-columns: repeat(5, minmax(140px, 1fr));
        gap: 12px;
        margin: 8px 0 12px;
    }
    .ui-stat-card {
        background: #ffffff;
        border: 1px solid #d9dee7;
        border-radius: 8px;
        padding: 12px 14px;
        min-height: 86px;
    }
    .ui-stat-label {
        color: #64748b !important;
        font-size: 13px;
        font-weight: 700;
        margin-bottom: 8px;
    }
    .ui-stat-value {
        color: #0f172a !important;
        font-size: 28px;
        font-weight: 850;
        line-height: 1.05;
        word-break: break-word;
    }
    .ui-section {
        background: #ffffff;
        border: 1px solid #d9dee7;
        border-radius: 8px;
        padding: 12px 14px;
        margin: 10px 0 12px;
    }
    .ui-toolbar {
        background: #ffffff;
        border: 1px solid #d9dee7;
        border-radius: 8px;
        padding: 12px 14px;
        margin: 10px 0 12px;
    }
    .ui-toolbar .stButton button,
    .ui-toolbar [data-testid="stButton"] button,
    .ui-toolbar [data-testid="stFormSubmitButton"] button {
        min-height: 42px !important;
    }
    .ui-compact-note {
        color: #64748b !important;
        font-size: 12px;
        line-height: 1.35;
    }
    .ui-section-title {
        color: #0f172a !important;
        font-size: 16px;
        font-weight: 820;
        margin-bottom: 4px;
    }
    .ui-muted {
        color: #64748b !important;
        font-size: 13px;
        line-height: 1.45;
    }
    .opp-card {
        border: 1px solid rgba(255,255,255,0.12);
        background: #0f141b;
        color: #f8fafc;
        border-radius: 8px;
        padding: 16px 18px;
        margin: 12px 0 14px;
        box-shadow: 0 12px 28px rgba(0,0,0,0.18);
    }
    .opp-card,
    .opp-card * {
        color: #f8fafc !important;
    }
    .opp-head {
        display: flex;
        gap: 12px;
        align-items: flex-start;
        justify-content: space-between;
        margin-bottom: 12px;
    }
    .opp-title {
        font-size: 18px;
        font-weight: 800;
        line-height: 1.25;
        color: #f5f7fb;
    }
    .opp-title-layout {
        display: flex;
        gap: 12px;
        align-items: flex-start;
    }
    .opp-title-text {
        min-width: 0;
    }
    .opp-product-image {
        width: 74px;
        height: 56px;
        object-fit: contain;
        flex: 0 0 auto;
        background: #ffffff;
        border: 1px solid rgba(255,255,255,0.18);
        border-radius: 8px;
        padding: 3px;
    }
    .opp-sub {
        margin-top: 4px;
        color: #aab3c2 !important;
        font-size: 13px;
    }
    .rating-badge {
        min-width: 42px;
        text-align: center;
        border-radius: 999px;
        padding: 6px 10px;
        font-weight: 800;
        color: #10131a !important;
        background: #70f59d;
    }
    .rating-badge.rating-a { background: #8bd8ff; }
    .rating-badge.rating-b { background: #ffd166; }
    .rating-badge.rating-c { background: #ff8f8f; }
    .opp-grid {
        display: grid;
        grid-template-columns: repeat(6, minmax(130px, 1fr));
        gap: 10px;
    }
    .opp-price-grid {
        grid-template-columns: repeat(5, minmax(150px, 1fr));
        margin-top: 10px;
    }
    .opp-action-grid {
        grid-template-columns: minmax(360px, 2fr) minmax(130px, 0.7fr) minmax(130px, 0.7fr);
        margin-top: 10px;
    }
    .opp-cell {
        background: rgba(255,255,255,0.045);
        border: 1px solid rgba(255,255,255,0.075);
        border-radius: 8px;
        padding: 10px 11px;
        min-height: 72px;
    }
    .opp-label {
        color: #8f9aac !important;
        font-size: 12px;
        margin-bottom: 5px;
    }
    .opp-value {
        color: #f8fafc;
        font-size: 17px;
        font-weight: 760;
        line-height: 1.22;
        word-break: break-word;
    }
    .opp-strategy .opp-value {
        font-size: 12px;
        font-weight: 650;
        line-height: 1.35;
        overflow-wrap: anywhere;
    }
    .opp-wide {
        grid-column: span 2;
    }
    .opp-extra-wide {
        grid-column: span 3;
    }
    .opp-plan {
        margin-top: 12px;
        border-radius: 8px;
        padding: 10px 12px;
        background: rgba(68, 136, 255, 0.12);
        color: #d8e8ff !important;
        border: 1px solid rgba(91, 154, 255, 0.22);
        font-size: 14px;
        line-height: 1.45;
        overflow-wrap: anywhere;
    }
    .opp-refresh-row {
        display: flex;
        align-items: center;
        gap: 14px;
        margin-top: 12px;
        padding-top: 12px;
        border-top: 1px solid rgba(255,255,255,0.10);
    }
    .opp-refresh-link,
    .opp-refresh-disabled {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-width: 160px;
        min-height: 38px;
        padding: 8px 14px;
        border-radius: 8px;
        background: #172033;
        border: 1px solid rgba(255,255,255,0.22);
        color: #ffffff !important;
        font-weight: 800;
        text-decoration: none !important;
    }
    .opp-refresh-link:hover {
        background: #1f2c46;
        color: #ffffff !important;
    }
    .opp-refresh-disabled {
        opacity: 0.55;
    }
    .opp-refresh-status {
        color: #cbd5e1 !important;
        font-size: 13px;
        line-height: 1.35;
    }
    div[data-testid="stAppViewContainer"] .main *:not(.opp-card):not(.opp-card *):not(button):not(button *) {
        color: #111827;
    }
    .main a:not(.opp-refresh-link) {
        color: #0f62fe !important;
    }
    .main div[data-baseweb="select"],
    .main div[data-baseweb="input"],
    .main div[data-baseweb="textarea"],
    .main textarea,
    .main input {
        background: #ffffff !important;
        color: #111827 !important;
    }
    .main div[data-baseweb="select"] *,
    .main div[data-baseweb="input"] *,
    .main div[data-baseweb="textarea"] * {
        color: #111827 !important;
    }
    .main [data-testid="stAlert"] *,
    .main [data-testid="stNotification"] *,
    .main [data-testid="stException"] * {
        color: inherit !important;
    }
    .main div[data-testid="stMarkdownContainer"] code {
        background: #eef2f7;
        color: #0f172a !important;
    }
    div.stButton > button {
        background: #111827 !important;
        color: #f8fafc !important;
        border: 1px solid rgba(255,255,255,0.25) !important;
        border-radius: 8px !important;
        font-weight: 750 !important;
    }
    div[data-testid="stButton"] button,
    div[data-testid="stFormSubmitButton"] button,
    div[data-testid="stDownloadButton"] button,
    button[kind="primary"],
    button[kind="secondary"] {
        background: #111827 !important;
        border: 1px solid #374151 !important;
        color: #ffffff !important;
        -webkit-text-fill-color: #ffffff !important;
        border-radius: 8px !important;
        font-weight: 750 !important;
    }
    div[data-testid="stButton"] button:disabled,
    div[data-testid="stFormSubmitButton"] button:disabled,
    div[data-testid="stDownloadButton"] button:disabled {
        background: #6b7280 !important;
        border-color: #6b7280 !important;
        opacity: 0.78 !important;
    }
    div[data-testid="stButton"] button *,
    div[data-testid="stFormSubmitButton"] button *,
    div[data-testid="stDownloadButton"] button *,
    button[kind="primary"] *,
    button[kind="secondary"] * {
        color: #ffffff !important;
        -webkit-text-fill-color: #ffffff !important;
    }
    div.stButton > button p {
        color: #f8fafc !important;
    }
    div.stButton > button *,
    div.stDownloadButton > button * {
        color: #f8fafc !important;
    }
    div.stButton > button:disabled {
        opacity: 0.55 !important;
    }
    .main .block-container :is(p, label, span, small, h1, h2, h3, h4, h5, h6),
    .main .block-container [data-testid="stMarkdownContainer"],
    .main .block-container [data-testid="stMarkdownContainer"] *,
    .main .block-container [data-testid="stCaptionContainer"],
    .main .block-container [data-testid="stCaptionContainer"] *,
    .main .block-container [data-testid="stWidgetLabel"],
    .main .block-container [data-testid="stWidgetLabel"] * {
        color: #111827 !important;
    }
    .opp-card,
    .opp-card *,
    .opp-card :is(p, label, span, small, h1, h2, h3, h4, h5, h6) {
        color: #f8fafc !important;
    }
    .opp-label,
    .opp-card .opp-label {
        color: #8f9aac !important;
    }
    .opp-sub,
    .opp-card .opp-sub {
        color: #aab3c2 !important;
    }
    .rating-badge,
    .opp-card .rating-badge {
        color: #10131a !important;
    }
    div.stButton > button,
    div.stButton > button *,
    div.stDownloadButton > button,
    div.stDownloadButton > button * {
        color: #f8fafc !important;
    }
    html body [data-testid="stAppViewContainer"] section.main .block-container,
    html body [data-testid="stAppViewContainer"] .main .block-container,
    html body [data-testid="stAppViewContainer"] section.main .block-container * {
        color: #111827 !important;
        -webkit-text-fill-color: #111827 !important;
    }
    html body [data-testid="stAppViewContainer"] .main .block-container * {
        color: #111827 !important;
        -webkit-text-fill-color: #111827 !important;
    }
    html body [data-testid="stAppViewContainer"] section.main .block-container button,
    html body [data-testid="stAppViewContainer"] section.main .block-container button *,
    html body [data-testid="stAppViewContainer"] .main .block-container button,
    html body [data-testid="stAppViewContainer"] .main .block-container button * {
        color: #ffffff !important;
        -webkit-text-fill-color: #ffffff !important;
    }
    html body [data-testid="stAppViewContainer"] section.main .block-container .opp-card,
    html body [data-testid="stAppViewContainer"] section.main .block-container .opp-card *,
    html body [data-testid="stAppViewContainer"] .main .block-container .opp-card,
    html body [data-testid="stAppViewContainer"] .main .block-container .opp-card * {
        color: #f8fafc !important;
        -webkit-text-fill-color: #f8fafc !important;
    }
    html body [data-testid="stAppViewContainer"] section.main .block-container .opp-label,
    html body [data-testid="stAppViewContainer"] section.main .block-container .opp-card .opp-label {
        color: #8f9aac !important;
        -webkit-text-fill-color: #8f9aac !important;
    }
    html body [data-testid="stAppViewContainer"] section.main .block-container .opp-sub,
    html body [data-testid="stAppViewContainer"] section.main .block-container .opp-card .opp-sub {
        color: #aab3c2 !important;
        -webkit-text-fill-color: #aab3c2 !important;
    }
    html body [data-testid="stAppViewContainer"] section.main .block-container .rating-badge,
    html body [data-testid="stAppViewContainer"] section.main .block-container .opp-card .rating-badge {
        color: #10131a !important;
        -webkit-text-fill-color: #10131a !important;
    }
    html body [data-testid="stAppViewContainer"] section.main .block-container input::placeholder {
        color: #6b7280 !important;
        -webkit-text-fill-color: #6b7280 !important;
        opacity: 1 !important;
    }
    html body [data-testid="stAppViewContainer"] .main .block-container
    :is([data-testid="stButton"], [data-testid="stFormSubmitButton"], [data-testid="stDownloadButton"]) button,
    html body [data-testid="stAppViewContainer"] .main .block-container
    :is([data-testid="stButton"], [data-testid="stFormSubmitButton"], [data-testid="stDownloadButton"]) button :is(p, span, div) {
        color: #ffffff !important;
        -webkit-text-fill-color: #ffffff !important;
        opacity: 1 !important;
        text-shadow: none !important;
    }
    html body [data-testid="stAppViewContainer"] .main .block-container
    :is([data-testid="stButton"], [data-testid="stFormSubmitButton"], [data-testid="stDownloadButton"]) button {
        background-color: #111827 !important;
    }
    html body [data-testid="stAppViewContainer"] .main .block-container
    :is([data-testid="stButton"], [data-testid="stFormSubmitButton"], [data-testid="stDownloadButton"]) button *,
    html body [data-testid="stAppViewContainer"] .main .block-container
    :is([data-testid="stButton"], [data-testid="stFormSubmitButton"], [data-testid="stDownloadButton"]) button p,
    html body [data-testid="stAppViewContainer"] .main .block-container
    :is([data-testid="stButton"], [data-testid="stFormSubmitButton"], [data-testid="stDownloadButton"]) button span,
    html body [data-testid="stAppViewContainer"] .main .block-container
    :is([data-testid="stButton"], [data-testid="stFormSubmitButton"], [data-testid="stDownloadButton"]) button div {
        color: #ffffff !important;
        -webkit-text-fill-color: #ffffff !important;
        opacity: 1 !important;
        visibility: visible !important;
        text-shadow: none !important;
    }
    html body [data-testid="stAppViewContainer"] .main .block-container
    :is([data-testid="stButton"], [data-testid="stFormSubmitButton"], [data-testid="stDownloadButton"]) button:disabled,
    html body [data-testid="stAppViewContainer"] .main .block-container
    :is([data-testid="stButton"], [data-testid="stFormSubmitButton"], [data-testid="stDownloadButton"]) button:disabled * {
        background-color: #6b7280 !important;
        color: #ffffff !important;
        -webkit-text-fill-color: #ffffff !important;
        opacity: 1 !important;
    }
    html body [data-testid="stAppViewContainer"] .main .block-container
    :is([data-testid="stRadio"], [data-testid="stCheckbox"], [data-testid="stToggle"]) *,
    html body [data-testid="stAppViewContainer"] .main .block-container
    :is([role="radiogroup"], [role="tablist"], [role="tab"]) * {
        color: #111827 !important;
        -webkit-text-fill-color: #111827 !important;
        opacity: 1 !important;
    }
    html body [data-testid="stAppViewContainer"] .main .block-container [role="tab"][aria-selected="true"] *,
    html body [data-testid="stAppViewContainer"] .main .block-container [data-baseweb="tab-highlight"] {
        color: #ef4444 !important;
        -webkit-text-fill-color: #ef4444 !important;
    }
    @media (max-width: 1300px) {
        .ui-card-grid { grid-template-columns: repeat(3, minmax(140px, 1fr)); }
        .opp-grid { grid-template-columns: repeat(3, minmax(150px, 1fr)); }
        .opp-price-grid { grid-template-columns: repeat(2, minmax(180px, 1fr)); }
        .opp-action-grid { grid-template-columns: 1fr; }
    }
    @media (max-width: 800px) {
        .main .block-container { padding-left: 1rem; padding-right: 1rem; }
        .ui-page-head { flex-direction: column; }
        .ui-card-grid { grid-template-columns: 1fr; }
        .opp-grid { grid-template-columns: 1fr; }
        .opp-wide { grid-column: span 1; }
        .opp-head { flex-direction: column; }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

ENV_PATH = BASE_DIR / ".env"
AUTO_FULL_REFRESH_FLAG = BASE_DIR / "data" / "auto_full_refresh.flag"
AUTO_HOURLY_SYNC_MARKER = BASE_DIR / "data" / "auto_hourly_full_sync.json"
CORE_BACKUP_BOOTSTRAP_MARKER = BASE_DIR / "data" / "core_backup_bootstrap.json"
OPPORTUNITY_SEARCH_HISTORY_PATH = BASE_DIR / "data" / "opportunity_search_history.json"
GOAT_RESCORE_REQUEST_PATH = BASE_DIR / "data" / "goat_rescore_request.json"
GOAT_STOCKX_WORKER_MARKER_PATH = BASE_DIR / "data" / "goat_stockx_worker.json"
GOAT_STOCKX_WORKER_SCRIPT = BASE_DIR / "scripts" / "goat_stockx_worker.py"
GOAT_DEFAULT_CONSIGNMENT_PATH = Path(
    r"C:\Users\caipi\xwechat_files\qq543399463_ebfa\temp\RWTemp\2026-06\5c4b0f3d59e6e60c2168578703c4722e\untitled_report-query_2-c842066d6501-2026-06-10-00-44-22.csv"
)
GOAT_UPLOAD_DIR = BASE_DIR / "data" / "goat_uploads"
SKU_UPLOAD_DIR = BASE_DIR / "data" / "sku_uploads"
GOAT_BUY_COST_ADDER_USD = 6.0
PAUSED_STOCKX_TASK_PATH = BASE_DIR / "data" / "paused_stockx_task.json"
AUTO_HOURLY_SYNC_POLL_SECONDS = 60
AUTO_HOURLY_SYNC_MIN_INTERVAL_SECONDS = 15 * 60
SYNC_SCORE_BATCH_SIZE = 4
STYLE_SYNC_HARD_TIMEOUT_SECONDS = 180
SYNC_CHECKPOINT_STYLE_INTERVAL = 8
SYNC_CHECKPOINT_MIN_SECONDS = 120
SYNC_STARTUP_STALL_SECONDS = 5 * 60
JOB_LOCK_PATH = BASE_DIR / "data" / "sync_job.lock"
SYNC_STATE_PATH = BASE_DIR / "data" / "sync_state.json"
JOB_LOCK_STALE_SECONDS = 15 * 60
OPPORTUNITY_DEFAULT_SORT_LABEL = "预计卖完天数（少到多）"
OPPORTUNITY_SEARCH_DEFAULTS = {
    "opp_filter_style": "",
    "opp_filter_size": "",
    "opp_filter_min_buy": "",
    "opp_filter_max_buy": "300",
    "opp_filter_release_op": "不限",
    "release_days_filter_value": "",
    "opp_filter_sell_op": "不限",
    "sell_days_filter_value": "",
    "opp_sort_label": OPPORTUNITY_DEFAULT_SORT_LABEL,
}
ENV_KEYS = [
    "APP_LOGIN_ENABLED",
    "APP_USERNAME",
    "APP_PASSWORD",
    "CLOUD_STORAGE_BACKEND",
    "FIREBASE_PROJECT_ID",
    "FIREBASE_COLLECTION_PREFIX",
    "FIREBASE_SERVICE_ACCOUNT_B64",
    "FIREBASE_SQLITE_BACKUP_MAX_MB",
    "FIREBASE_CREDENTIALS_PATH",
    "FIREBASE_SERVICE_ACCOUNT_JSON",
    "STOCKX_HOST",
    "STOCKX_TOKEN",
    "STOCKX_AUTH",
    "STOCKX_CREDENTIAL_MODE",
    "STOCKX_TOKEN_PARAM",
    "STOCKX_AUTH_PARAM",
    "STOCKX_TOKEN_HEADER",
    "STOCKX_AUTH_HEADER",
    "STOCKX_REQUEST_TIMEOUT",
    "STOCKX_DB_PATH",
    "ESTIMATED_SELLER_FEE_RATE",
    "BUY_DEPTH_SALES_FRACTION",
    "AUTO_FULL_SYNC_ENABLED",
    "AUTO_FULL_SYNC_INTERVAL_MINUTES",
    "SYNC_MAX_WORKERS",
]

SYNC_JOB_LOCK = threading.Lock()
SYNC_JOB_STATE: dict[str, Any] = {
    "job_id": None,
    "status": "idle",
    "message": "",
    "progress": 0.0,
    "completed": 0,
    "total": 0,
    "current_style": None,
    "current_size": None,
    "current_endpoint": None,
    "current_phase": None,
    "score_completed": 0,
    "score_total": 0,
    "recent_events": [],
    "summaries": [],
    "error": None,
    "recomputed": 0,
    "started_at": None,
    "finished_at": None,
    "row_refresh_result": None,
    "row_refresh_style": None,
    "row_refresh_size": None,
}

def _initial_goat_job_state() -> dict[str, Any]:
    return {
        "job_id": None,
        "status": "idle",
        "message": "",
        "progress": 0.0,
        "completed": 0,
        "total": 0,
        "phase": None,
        "current_pid": None,
        "current_style": None,
        "current_size": None,
        "source_name": None,
        "imported": 0,
        "computed": 0,
        "error": None,
        "started_at": None,
        "finished_at": None,
    }


@st.cache_resource(show_spinner=False)
def _goat_job_resource() -> dict[str, Any]:
    return {"lock": threading.Lock(), "state": _initial_goat_job_state()}


@st.cache_resource(show_spinner=False)
def _api_sync_lock_resource():
    return threading.Lock()


def _acquire_job_file_lock(job_id: str, kind: str, payload: dict[str, Any] | None = None) -> bool:
    JOB_LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    if JOB_LOCK_PATH.exists():
        try:
            age = datetime.utcnow().timestamp() - JOB_LOCK_PATH.stat().st_mtime
            if age > JOB_LOCK_STALE_SECONDS:
                JOB_LOCK_PATH.unlink()
            else:
                return False
        except OSError:
            return False
    try:
        with JOB_LOCK_PATH.open("x", encoding="utf-8") as handle:
            lock_data = {"job_id": job_id, "kind": kind, "pid": os.getpid(), "started_at": datetime.utcnow().isoformat()}
            if payload:
                lock_data.update(payload)
            handle.write(json.dumps(lock_data, ensure_ascii=False, default=str))
        return True
    except FileExistsError:
        return False
    except OSError:
        return False


def _release_job_file_lock(job_id: str) -> None:
    try:
        if not JOB_LOCK_PATH.exists():
            return
        data = json_loads(JOB_LOCK_PATH.read_text(encoding="utf-8"), {}) or {}
        if data.get("job_id") == job_id:
            JOB_LOCK_PATH.unlink()
    except OSError:
        pass


def _read_json_path(path: Path) -> dict[str, Any]:
    try:
        return json_loads(path.read_text(encoding="utf-8"), {}) or {}
    except OSError:
        return {}


def _write_json_path(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _read_text_tail(path: Path, max_chars: int = 4000) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    if len(text) <= max_chars:
        return text
    return text[-max_chars:]


def _write_sync_state_file(state: dict[str, Any]) -> None:
    try:
        SYNC_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        temp_path = SYNC_STATE_PATH.with_name(
            f"{SYNC_STATE_PATH.name}.{os.getpid()}.{threading.get_ident()}.tmp"
        )
        payload = json.dumps(state, ensure_ascii=False, indent=2, default=str)
        for attempt in range(6):
            try:
                temp_path.write_text(payload, encoding="utf-8")
                os.replace(temp_path, SYNC_STATE_PATH)
                return
            except PermissionError:
                time_module.sleep(0.05 * (attempt + 1))
            except OSError:
                time_module.sleep(0.05 * (attempt + 1))
        try:
            if temp_path.exists():
                temp_path.unlink()
        except OSError:
            pass
    except Exception:
        pass


def _sync_state_from_lock() -> dict[str, Any]:
    lock = _read_lock_file(JOB_LOCK_PATH)
    if lock.get("kind") != "sync":
        return {}
    total = int(lock.get("total") or 0)
    return {
        "job_id": lock.get("job_id"),
        "status": "running",
        "message": "任务已启动，等待后端写入详细进度",
        "progress": 0.0,
        "completed": int(lock.get("completed") or 0),
        "total": total,
        "current_style": lock.get("current_style"),
        "current_size": None,
        "current_endpoint": lock.get("current_endpoint") or "-",
        "current_phase": "StockX API",
        "score_completed": 0,
        "score_total": 0,
        "recent_events": [],
        "summaries": [],
        "error": None,
        "recomputed": 0,
        "started_at": lock.get("started_at"),
        "finished_at": None,
        "updated_at": datetime.utcnow().isoformat(timespec="seconds"),
    }


def _recover_sync_state_from_logs(state: dict[str, Any]) -> dict[str, Any]:
    if state.get("status") != "running" or not state.get("started_at"):
        return state
    try:
        conn = connect()
        try:
            completed = (
                conn.execute(
                    """
                    SELECT COUNT(DISTINCT style_no)
                    FROM sync_logs
                    WHERE created_at >= ?
                      AND style_no IS NOT NULL
                      AND TRIM(style_no) != ''
                    """,
                    [state.get("started_at")],
                ).fetchone()[0]
                or 0
            )
            latest = conn.execute(
                """
                SELECT style_no, size, endpoint, event_type, message
                FROM sync_logs
                WHERE created_at >= ?
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                [state.get("started_at")],
            ).fetchone()
        finally:
            conn.close()
        total = int(state.get("total") or 0)
        if completed and total and completed > total:
            total = completed
        if completed and not total:
            total = completed
        recovered = dict(state)
        recovered["completed"] = int(completed or state.get("completed") or 0)
        recovered["total"] = total
        if total:
            recovered["progress"] = min(1.0, max(0.0, recovered["completed"] / total))
        if latest:
            recovered["current_style"] = latest["style_no"] or recovered.get("current_style")
            recovered["current_size"] = latest["size"] or recovered.get("current_size")
            recovered["current_endpoint"] = latest["endpoint"] or latest["event_type"] or recovered.get("current_endpoint")
            recovered["message"] = latest["message"] or recovered.get("message")
        recovered["updated_at"] = datetime.utcnow().isoformat(timespec="seconds")
        if recovered != state:
            _write_sync_state_file(recovered)
        return recovered
    except Exception:
        return state


def _is_sync_startup_stalled(state: dict[str, Any]) -> bool:
    if state.get("status") != "running":
        return False
    try:
        total = int(state.get("total") or 0)
        completed = int(state.get("completed") or 0)
    except (TypeError, ValueError):
        return False
    if total <= 0 or completed != 0 or state.get("current_style"):
        return False
    started_ts = _timestamp_from_marker(state.get("started_at"))
    if not started_ts:
        return False
    return (datetime.utcnow().timestamp() - started_ts) >= SYNC_STARTUP_STALL_SECONDS


def _mark_sync_startup_stalled(state: dict[str, Any]) -> dict[str, Any]:
    if not _is_sync_startup_stalled(state):
        return state
    stalled = dict(state)
    stalled.update(
        {
            "status": "error",
            "error": "任务启动超过5分钟仍为0，已释放旧锁，允许后台worker接管。",
            "message": "任务启动超过5分钟仍为0，已释放旧锁，允许后台worker接管。",
            "current_phase": "启动卡死",
            "finished_at": datetime.utcnow().isoformat(timespec="seconds"),
            "updated_at": datetime.utcnow().isoformat(timespec="seconds"),
        }
    )
    job_id = stalled.get("job_id")
    try:
        lock = _read_lock_file(JOB_LOCK_PATH)
        if not job_id or lock.get("job_id") == job_id:
            JOB_LOCK_PATH.unlink(missing_ok=True)
    except Exception:
        pass
    _write_sync_state_file(stalled)
    return stalled


def _current_sync_run_counts(conn, state: dict[str, Any], imported_scope_sql: str) -> dict[str, int]:
    started_at = state.get("started_at")
    if state.get("status") != "running" or not started_at:
        return {}
    try:
        completed = (
            conn.execute(
                """
                SELECT COUNT(DISTINCT style_no)
                FROM sync_logs
                WHERE created_at >= ?
                  AND event_type = 'sync_complete'
                  AND style_no IS NOT NULL
                  AND TRIM(style_no) != ''
                """,
                [started_at],
            ).fetchone()[0]
            or 0
        )
        completed_with_product = (
            conn.execute(
                f"""
                SELECT COUNT(DISTINCT l.style_no)
                FROM sync_logs l
                JOIN products p ON p.style_no = l.style_no
                WHERE l.created_at >= ?
                  AND l.event_type = 'sync_complete'
                  AND l.style_no IN ({imported_scope_sql})
                """,
                [started_at],
            ).fetchone()[0]
            or 0
        )
        completed_with_score = (
            conn.execute(
                f"""
                SELECT COUNT(DISTINCT l.style_no)
                FROM sync_logs l
                JOIN opportunity_scores o ON o.style_no = l.style_no
                WHERE l.created_at >= ?
                  AND l.event_type = 'sync_complete'
                  AND l.style_no IN ({imported_scope_sql})
                """,
                [started_at],
            ).fetchone()[0]
            or 0
        )
        return {
            "completed": int(completed),
            "with_product": int(completed_with_product),
            "with_score": int(completed_with_score),
        }
    except Exception:
        return {}


def _stop_process_tree(pid: Any) -> bool:
    try:
        pid_int = int(pid)
    except (TypeError, ValueError):
        return False
    if pid_int <= 0 or pid_int == os.getpid():
        return False
    try:
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        result = subprocess.run(
            ["taskkill", "/PID", str(pid_int), "/F", "/T"],
            capture_output=True,
            text=True,
            timeout=15,
            creationflags=creationflags,
        )
        return result.returncode == 0
    except Exception:
        return False


def _read_lock_file(path: Path) -> dict[str, Any]:
    return _read_json_path(path)


def _pause_stockx_task_for_goat(reason: str) -> dict[str, Any]:
    # StockX sync jobs run inside the Streamlit process. Killing the lock PID can
    # terminate the web app itself, so GOAT tasks no longer force-pause them.
    return {"paused": False, "reason": f"stockx task left running: {reason}"}


def _stop_goat_worker_for_new_upload() -> dict[str, Any]:
    lock = _read_lock_file(BASE_DIR / "data" / "goat_stockx_worker.lock")
    marker = _read_goat_stockx_worker_marker()
    if marker.get("status") != "running":
        return {"stopped": False, "reason": "goat not running"}
    stopped = _stop_process_tree(lock.get("pid"))
    try:
        lock_path = BASE_DIR / "data" / "goat_stockx_worker.lock"
        if lock_path.exists():
            lock_path.unlink()
    except OSError:
        pass
    marker.update(
        {
            "status": "paused_by_new_upload",
            "message": "旧GOAT补数任务已被新上传清单暂停。",
            "paused_at": datetime.utcnow().isoformat(timespec="seconds"),
        }
    )
    _write_json_path(GOAT_STOCKX_WORKER_MARKER_PATH, marker)
    return {"stopped": stopped, "job_id": marker.get("job_id")}


def _read_auto_hourly_marker() -> dict[str, Any]:
    try:
        return json_loads(AUTO_HOURLY_SYNC_MARKER.read_text(encoding="utf-8"), {}) or {}
    except OSError:
        return {}


def _read_goat_stockx_worker_marker() -> dict[str, Any]:
    try:
        return json_loads(GOAT_STOCKX_WORKER_MARKER_PATH.read_text(encoding="utf-8"), {}) or {}
    except OSError:
        return {}


def _write_auto_hourly_marker(data: dict[str, Any]) -> None:
    AUTO_HOURLY_SYNC_MARKER.parent.mkdir(parents=True, exist_ok=True)
    AUTO_HOURLY_SYNC_MARKER.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _ensure_opportunity_search_defaults(sort_options: dict[str, tuple[str, bool]]) -> None:
    for key, value in OPPORTUNITY_SEARCH_DEFAULTS.items():
        st.session_state.setdefault(key, value)
    if not st.session_state.get("_opp_home_defaults_v2_applied"):
        if not st.session_state.get("opp_filter_style") and not st.session_state.get("opp_filter_size"):
            st.session_state["opp_filter_max_buy"] = "300"
            st.session_state["opp_sort_label"] = OPPORTUNITY_DEFAULT_SORT_LABEL
        st.session_state["_opp_home_defaults_v2_applied"] = True
    if st.session_state.get("opp_sort_label") not in sort_options:
        st.session_state["opp_sort_label"] = OPPORTUNITY_DEFAULT_SORT_LABEL
    for key, options in {
        "opp_filter_release_op": ["不限", "大于等于", "小于等于"],
        "opp_filter_sell_op": ["不限", "大于等于", "小于等于"],
    }.items():
        if st.session_state.get(key) not in options:
            st.session_state[key] = "不限"


def _reset_opportunity_search_state() -> None:
    for key, value in OPPORTUNITY_SEARCH_DEFAULTS.items():
        st.session_state[key] = value


def _read_opportunity_search_history() -> list[dict[str, Any]]:
    try:
        data = json_loads(OPPORTUNITY_SEARCH_HISTORY_PATH.read_text(encoding="utf-8"), [])
    except OSError:
        return []
    if not isinstance(data, list):
        return []
    return [item for item in data if isinstance(item, dict)]


def _write_opportunity_search_history(history: list[dict[str, Any]]) -> None:
    OPPORTUNITY_SEARCH_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    OPPORTUNITY_SEARCH_HISTORY_PATH.write_text(
        json.dumps(history[:12], ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def _opportunity_search_snapshot() -> dict[str, Any]:
    return {key: st.session_state.get(key, default) for key, default in OPPORTUNITY_SEARCH_DEFAULTS.items()}


def _opportunity_history_label(snapshot: dict[str, Any]) -> str:
    parts: list[str] = []
    style = str(snapshot.get("opp_filter_style") or "").strip()
    size = str(snapshot.get("opp_filter_size") or "").strip()
    if style:
        parts.append(style)
    if size:
        parts.append(f"US {size}")
    min_buy = str(snapshot.get("opp_filter_min_buy") or "").strip()
    max_buy = str(snapshot.get("opp_filter_max_buy") or "").strip()
    if min_buy:
        parts.append(f"买价≥{min_buy}")
    if max_buy:
        parts.append(f"买价≤{max_buy}")
    release_op = str(snapshot.get("opp_filter_release_op") or "不限")
    release_days = str(snapshot.get("release_days_filter_value") or "").strip()
    if release_op != "不限" and release_days:
        parts.append(f"发售{release_op}{release_days}天")
    sell_op = str(snapshot.get("opp_filter_sell_op") or "不限")
    sell_days = str(snapshot.get("sell_days_filter_value") or "").strip()
    if sell_op != "不限" and sell_days:
        parts.append(f"卖完{sell_op}{sell_days}天")
    sort_label = str(snapshot.get("opp_sort_label") or OPPORTUNITY_DEFAULT_SORT_LABEL)
    parts.append(sort_label.replace("（", "(").replace("）", ")"))
    return " / ".join(parts[:8])


def _save_opportunity_search_history(snapshot: dict[str, Any]) -> None:
    normalized = {key: snapshot.get(key, default) for key, default in OPPORTUNITY_SEARCH_DEFAULTS.items()}
    history = _read_opportunity_search_history()
    key = json.dumps(normalized, ensure_ascii=False, sort_keys=True)
    deduped = [
        item for item in history
        if json.dumps({k: item.get(k, v) for k, v in OPPORTUNITY_SEARCH_DEFAULTS.items()}, ensure_ascii=False, sort_keys=True) != key
    ]
    normalized["saved_at"] = datetime.utcnow().isoformat(timespec="seconds")
    _write_opportunity_search_history([normalized, *deduped])


def _apply_opportunity_search_snapshot(snapshot: dict[str, Any]) -> None:
    for key, default in OPPORTUNITY_SEARCH_DEFAULTS.items():
        st.session_state[key] = snapshot.get(key, default)


GOAT_HEADER_ALIASES = {
    "pid": {
        "pid",
        "productid",
        "product_id",
        "productidentifier",
        "productvariantid",
        "variantid",
        "inventoryid",
        "itemid",
        "listingid",
    },
    "style_no": {
        "sku",
        "style",
        "styleno",
        "style_no",
        "style number",
        "stylenumber",
        "productsku",
        "product_sku",
        "merchant sku",
        "merchantsku",
        "itemsku",
        "货号",
        "款号",
        "商品货号",
    },
    "size": {
        "size",
        "usize",
        "us_size",
        "goatsize",
        "goat_size",
        "shoe size",
        "shoesize",
        "尺码",
        "码数",
        "us尺码",
    },
    "price": {
        "price",
        "goatprice",
        "goat_price",
        "unitprice",
        "unit_price",
        "listprice",
        "list_price",
        "saleprice",
        "sale_price",
        "askprice",
        "ask_price",
        "amount",
        "价格",
        "goat价格",
        "采购价",
    },
    "price_cents": {
        "pricecents",
        "price_cents",
        "amountcents",
        "amount_cents",
    },
    "title": {
        "title",
        "name",
        "productname",
        "product_name",
        "producttemplatename",
        "product_template_name",
        "商品名",
        "名称",
    },
    "warehouse_id": {"warehouseid", "warehouse_id", "仓库id"},
    "warehouse_name": {"warehousename", "warehouse_name", "warehouse", "仓库", "仓库名"},
    "product_template_id": {"producttemplateid", "product_template_id", "templateid", "template_id"},
    "sale_status": {"salestatus", "sale_status", "status", "状态"},
}


def _goat_header_key(value: Any) -> str:
    return re.sub(r"[\s_\-./()（）:：#]+", "", str(value or "").strip().lower())


def _goat_aliases(field: str) -> set[str]:
    return {_goat_header_key(alias) for alias in GOAT_HEADER_ALIASES.get(field, set())}


def _goat_header_score(values: list[Any]) -> int:
    aliases = {_goat_header_key(alias) for names in GOAT_HEADER_ALIASES.values() for alias in names}
    score = 0
    for value in values:
        key = _goat_header_key(value)
        if key in aliases:
            score += 2
        elif any(alias and alias in key for alias in aliases if len(alias) >= 4):
            score += 1
    return score


def _make_unique_columns(values: list[Any]) -> list[str]:
    counts: dict[str, int] = {}
    columns: list[str] = []
    for idx, value in enumerate(values):
        base = str(value or "").strip() or f"column_{idx + 1}"
        count = counts.get(base, 0)
        counts[base] = count + 1
        columns.append(base if count == 0 else f"{base}_{count + 1}")
    return columns


def _goat_frame_from_raw(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return raw
    best_idx = 0
    best_score = -1
    for idx in range(min(len(raw), 25)):
        values = [None if pd.isna(v) else v for v in raw.iloc[idx].tolist()]
        score = _goat_header_score(values)
        if score > best_score:
            best_idx = idx
            best_score = score
    if best_score >= 2:
        frame = raw.iloc[best_idx + 1 :].copy()
        frame.columns = _make_unique_columns([None if pd.isna(v) else v for v in raw.iloc[best_idx].tolist()])
        return frame.dropna(how="all").reset_index(drop=True)
    frame = raw.copy()
    frame.columns = _make_unique_columns(frame.columns.tolist())
    return frame.dropna(how="all").reset_index(drop=True)


def _read_goat_consignment_frame(file_name: str, content: bytes) -> pd.DataFrame:
    suffix = Path(file_name).suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        sheets = pd.read_excel(BytesIO(content), sheet_name=None, header=None)
        frames = [_goat_frame_from_raw(sheet) for sheet in sheets.values()]
        frames = [frame for frame in frames if not frame.empty]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    raw = pd.read_csv(BytesIO(content), header=None)
    return _goat_frame_from_raw(raw)


def _goat_find_column(columns: list[str], field: str) -> str | None:
    aliases = _goat_aliases(field)
    normalized = {column: _goat_header_key(column) for column in columns}
    for column, key in normalized.items():
        if key in aliases:
            return column
    for column, key in normalized.items():
        if any(alias and alias in key for alias in aliases if len(alias) >= 4):
            return column
    return None


def _goat_number_from_value(value: Any) -> float | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    text = re.sub(r"[^0-9.\-]", "", text)
    if text in {"", ".", "-", "-."}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _goat_content_score(series: pd.Series, field: str, used: set[str]) -> float:
    if str(series.name) in used:
        return -1
    values = [None if pd.isna(v) else v for v in series.head(200).tolist()]
    nonempty = [v for v in values if v not in (None, "")]
    if not nonempty:
        return -1
    if field == "pid":
        hits = 0
        for value in nonempty:
            text = re.sub(r"\D", "", str(value))
            if 5 <= len(text) <= 12:
                hits += 1
        return hits / max(len(nonempty), 1)
    if field == "style_no":
        hits = 0
        for value in nonempty:
            style = normalize_style_no(value)
            compact = re.sub(r"[^A-Z0-9]", "", str(style or "").upper())
            if style and 5 <= len(compact) <= 14 and re.search(r"[A-Z]", compact) and re.search(r"\d", compact):
                hits += 1
        return hits / max(len(nonempty), 1)
    if field == "size":
        hits = 0
        for value in nonempty:
            size = normalize_us_size(value)
            number = _goat_number_from_value(size)
            if size and number is not None and 1 <= number <= 25:
                hits += 1
        return hits / max(len(nonempty), 1)
    if field in {"price", "price_cents"}:
        nums = [_goat_number_from_value(v) for v in nonempty]
        nums = [n for n in nums if n is not None]
        if not nums:
            return -1
        if field == "price_cents":
            hits = sum(500 <= n <= 500000 for n in nums)
        else:
            hits = sum(5 <= n <= 5000 for n in nums)
        return hits / max(len(nonempty), 1)
    return -1


def _goat_detect_column_by_content(frame: pd.DataFrame, field: str, used: set[str]) -> str | None:
    best_column = None
    best_score = 0.0
    for column in frame.columns:
        score = _goat_content_score(frame[column], field, used)
        if score > best_score:
            best_column = str(column)
            best_score = score
    threshold = 0.55 if field in {"pid", "style_no"} else 0.45
    return best_column if best_column and best_score >= threshold else None


def _goat_detect_columns(frame: pd.DataFrame) -> dict[str, str]:
    columns = [str(column) for column in frame.columns]
    mapping: dict[str, str] = {}
    used: set[str] = set()
    for field in ["pid", "style_no", "size", "price_cents", "price", "title", "warehouse_id", "warehouse_name", "product_template_id", "sale_status"]:
        column = _goat_find_column(columns, field)
        if column and column not in used:
            mapping[field] = column
            used.add(column)
    for field in ["pid", "style_no", "size", "price"]:
        if field not in mapping:
            column = _goat_detect_column_by_content(frame, field, used)
            if column:
                mapping[field] = column
                used.add(column)
    return mapping


def _goat_row_value(row: dict[str, Any], mapping: dict[str, str], field: str, *fallbacks: str) -> Any:
    column = mapping.get(field)
    if column and row.get(column) not in (None, ""):
        return row.get(column)
    for key in fallbacks:
        if row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _goat_price_from_row(row: dict[str, Any], mapping: dict[str, str] | None = None) -> float | None:
    mapping = mapping or {}
    cents_value = _goat_row_value(row, mapping, "price_cents", "PRICE_CENTS")
    if cents_value not in (None, ""):
        cents = _goat_number_from_value(cents_value)
        return cents / 100.0 if cents is not None else None
    price_value = _goat_row_value(row, mapping, "price", "PRICE", "GOAT_PRICE", "price", "goat_price")
    if price_value not in (None, ""):
        return _goat_number_from_value(price_value)
    for key in ("PRICE", "GOAT_PRICE", "price", "goat_price"):
        if row.get(key) not in (None, ""):
            return _goat_number_from_value(row[key])
    return None


def _goat_raw_sku_exactly_matches_style(raw_sku: Any, style_no: str) -> bool:
    raw_text = str(raw_sku or "").strip().upper()
    if not raw_text or not style_no:
        return False
    compact_raw = re.sub(r"[^A-Z0-9]", "", raw_text)
    compact_style = re.sub(r"[^A-Z0-9]", "", style_no.upper())
    return compact_raw == compact_style


def _goat_import_record_rank(record: dict[str, Any]) -> tuple[float, int, str]:
    clean_penalty = 0 if record.get("_clean_sku") else 1
    return float(record.get("goat_price") or 999999), clean_penalty, str(record.get("pid") or "")


def _goat_product_id_from_row(raw: dict[str, Any], mapping: dict[str, str] | None = None) -> str:
    mapping = mapping or {}
    value = _goat_row_value(raw, mapping, "pid", "PRODUCT_ID", "PID", "product_id", "pid")
    return str(value).strip() if value not in (None, "") else ""


def _archive_current_goat_consignment(conn, *, new_source_name: str, note: str = "") -> int | None:
    item_count = int(conn.execute("SELECT COUNT(*) FROM goat_consignment_items").fetchone()[0] or 0)
    if item_count <= 0:
        return None
    score_count = int(conn.execute("SELECT COUNT(*) FROM goat_consignment_scores").fetchone()[0] or 0)
    batches = [
        str(row["import_batch"])
        for row in query_rows(
            conn,
            """
            SELECT DISTINCT import_batch
            FROM goat_consignment_items
            ORDER BY import_batch
            """,
        )
    ]
    archive_batch = f"archive-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    archived_at = datetime.utcnow().isoformat(timespec="seconds")
    cursor = conn.execute(
        """
        INSERT INTO goat_consignment_import_history (
            archive_batch, archived_at, new_source_name, previous_batches,
            item_count, score_count, note
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (archive_batch, archived_at, new_source_name, json_dumps(batches), item_count, score_count, note),
    )
    archive_id = int(cursor.lastrowid)
    conn.execute(
        """
        INSERT INTO goat_consignment_history_items (
            archive_id, original_item_id, import_batch, warehouse_id, warehouse_name,
            pid, product_template_id, style_no, size, title, sale_status,
            goat_price, buy_cost, raw_row_json, imported_at
        )
        SELECT
            ?, id, import_batch, warehouse_id, warehouse_name,
            pid, product_template_id, style_no, size, title, sale_status,
            goat_price, buy_cost, raw_row_json, imported_at
        FROM goat_consignment_items
        """,
        (archive_id,),
    )
    conn.execute(
        """
        INSERT INTO goat_consignment_history_scores (
            archive_id, original_score_id, original_item_id, score, rating,
            style_no, size, matched_stockx_size, pid, title, goat_price, buy_cost,
            stockx_lowest_ask, ask_snapshot_time, sales_7d, sales_30d,
            avg_7d, avg_30d, estimated_sell_price, estimated_profit,
            estimated_profit_rate, estimated_days_to_sell, risk_notes,
            components_json, computed_at
        )
        SELECT
            ?, id, item_id, score, rating,
            style_no, size, matched_stockx_size, pid, title, goat_price, buy_cost,
            stockx_lowest_ask, ask_snapshot_time, sales_7d, sales_30d,
            avg_7d, avg_30d, estimated_sell_price, estimated_profit,
            estimated_profit_rate, estimated_days_to_sell, risk_notes,
            components_json, computed_at
        FROM goat_consignment_scores
        """,
        (archive_id,),
    )
    log_sync(
        conn,
        f"GOAT当前清单已归档：{item_count} 行，评分 {score_count} 行",
        event_type="goat_consignment_archived",
        details={"archive_id": archive_id, "archive_batch": archive_batch, "new_source_name": new_source_name},
    )
    return archive_id


def _import_goat_consignment_rows(conn, frame: pd.DataFrame, *, source_name: str, replace_current: bool = False) -> int:
    if replace_current:
        _archive_current_goat_consignment(conn, new_source_name=source_name, note="new upload replaced current list")
        conn.execute("DELETE FROM goat_consignment_scores")
        conn.execute("DELETE FROM goat_consignment_items")
    import_batch = f"{source_name}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    now = datetime.utcnow().isoformat(timespec="seconds")
    records_by_style_size: dict[tuple[str, str], dict[str, Any]] = {}
    column_mapping = _goat_detect_columns(frame)
    required_missing = [field for field in ("pid", "style_no", "size", "price") if field not in column_mapping]
    if required_missing:
        log_sync(
            conn,
            "GOAT导入表头识别不完整",
            event_type="goat_import_header_warning",
            details={
                "missing": required_missing,
                "columns": [str(column) for column in frame.columns],
                "mapping": column_mapping,
            },
        )
    for _, series in frame.iterrows():
        raw = {str(k): (None if pd.isna(v) else v) for k, v in series.to_dict().items()}
        pid = _goat_product_id_from_row(raw, column_mapping)
        raw_style = _goat_row_value(raw, column_mapping, "style_no", "SKU", "STYLE_NO", "style_no")
        raw_size = _goat_row_value(raw, column_mapping, "size", "SIZE", "US_SIZE", "size")
        style_no = normalize_style_no(raw_style)
        size = normalize_us_size(raw_size)
        goat_price = _goat_price_from_row(raw, column_mapping)
        if not pid or not style_no or not size or goat_price is None:
            continue
        buy_cost = round(float(goat_price) + GOAT_BUY_COST_ADDER_USD, 2)
        record = {
            "import_batch": import_batch,
            "warehouse_id": str(_goat_row_value(raw, column_mapping, "warehouse_id", "WAREHOUSE_ID") or ""),
            "warehouse_name": str(_goat_row_value(raw, column_mapping, "warehouse_name", "WAREHOUSE_NAME") or ""),
            "pid": pid,
            "product_template_id": str(_goat_row_value(raw, column_mapping, "product_template_id", "PRODUCT_TEMPLATE_ID") or ""),
            "style_no": style_no,
            "size": size,
            "title": str(_goat_row_value(raw, column_mapping, "title", "PRODUCT_TEMPLATE_NAME", "TITLE") or ""),
            "sale_status": str(_goat_row_value(raw, column_mapping, "sale_status", "SALE_STATUS") or ""),
            "goat_price": round(float(goat_price), 2),
            "buy_cost": buy_cost,
            "raw_row_json": json_dumps(raw),
            "imported_at": now,
            "_clean_sku": _goat_raw_sku_exactly_matches_style(
                raw_style,
                style_no,
            ),
        }
        key = (style_no, size)
        current = records_by_style_size.get(key)
        if current is None or _goat_import_record_rank(record) < _goat_import_record_rank(current):
            records_by_style_size[key] = record

    log_sync(
        conn,
        f"GOAT导入列识别：导入 {len(records_by_style_size)} 行",
        event_type="goat_import_header_mapping",
        details={
            "mapping": column_mapping,
            "columns": [str(column) for column in frame.columns],
            "imported": len(records_by_style_size),
            "source_name": source_name,
        },
    )

    for record in records_by_style_size.values():
        conn.execute(
            """
            INSERT OR REPLACE INTO goat_consignment_items (
                import_batch, warehouse_id, warehouse_name, pid, product_template_id,
                style_no, size, title, sale_status, goat_price, buy_cost,
                raw_row_json, imported_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["import_batch"],
                record["warehouse_id"],
                record["warehouse_name"],
                record["pid"],
                record["product_template_id"],
                record["style_no"],
                record["size"],
                record["title"],
                record["sale_status"],
                record["goat_price"],
                record["buy_cost"],
                record["raw_row_json"],
                record["imported_at"],
            ),
        )
    return len(records_by_style_size)


def _latest_stockx_lowest_ask_for_size(conn, style_no: str, size: str, title_hint: str | None = None) -> dict[str, Any] | None:
    style_candidates = _goat_style_candidates(style_no)
    style_placeholders = ",".join("?" for _ in style_candidates)
    preferred_suffixes = _goat_preferred_stockx_size_suffixes(conn, style_no, title_hint)
    candidates = _goat_size_candidates(size, preferred_suffixes=preferred_suffixes)
    for candidate in candidates:
        rows = query_rows(
            conn,
            f"""
            SELECT ask_price, SUM(ask_quantity) AS ask_quantity, MAX(snapshot_time) AS snapshot_time
            FROM ask_depth
            WHERE style_no IN ({style_placeholders})
              AND COALESCE(size, '') = COALESCE(?, '')
              AND snapshot_time = (
                SELECT MAX(snapshot_time)
                FROM ask_depth
                WHERE style_no IN ({style_placeholders})
                  AND COALESCE(size, '') = COALESCE(?, '')
              )
            GROUP BY ask_price
            ORDER BY ask_price ASC
            LIMIT 1
            """,
            tuple([*style_candidates, candidate, *style_candidates, candidate]),
        )
        if rows:
            row = dict(rows[0])
            row["matched_size"] = candidate
            return row
        market_rows = query_rows(
            conn,
            f"""
            SELECT lowest_ask AS ask_price, 1 AS ask_quantity, MAX(snapshot_time) AS snapshot_time
            FROM market_snapshots
            WHERE style_no IN ({style_placeholders})
              AND COALESCE(size, '') = COALESCE(?, '')
              AND lowest_ask IS NOT NULL
            GROUP BY lowest_ask
            ORDER BY snapshot_time DESC, lowest_ask ASC
            LIMIT 1
            """,
            tuple([*style_candidates, candidate]),
        )
        if market_rows:
            row = dict(market_rows[0])
            row["matched_size"] = candidate
            return row
    return None


def _goat_sales_stats(conn, style_no: str, size: str, title_hint: str | None = None) -> dict[str, Any]:
    preferred_suffixes = _goat_preferred_stockx_size_suffixes(conn, style_no, title_hint)
    candidates = _goat_size_candidates(size, preferred_suffixes=preferred_suffixes)
    style_candidates = _goat_style_candidates(style_no)
    for style_candidate in style_candidates:
        for candidate in candidates:
            stats = get_sales_stats(conn, style_candidate, candidate)
            if stats.sales_7d or stats.sales_30d or stats.avg_7d or stats.avg_30d:
                return {"matched_size": candidate, "stats": stats}
    fallback_size = candidates[0] if candidates else normalize_us_size(size)
    fallback_style = style_candidates[0] if style_candidates else str(style_no)
    return {"matched_size": fallback_size, "stats": get_sales_stats(conn, fallback_style, fallback_size)}


def _goat_size_candidates(size: str, *, preferred_suffixes: list[str] | None = None) -> list[str]:
    raw = str(size or "").strip()
    cleaned = normalize_us_size(size)
    if not cleaned:
        return []
    compact = cleaned.upper().replace(" ", "")
    base = re.sub(r"^[A-Z]+", "", compact)
    base = re.sub(r"[A-Z]+$", "", base)
    base_candidates = [cleaned, compact, raw, raw.upper().replace(" ", "")]
    candidates = list(base_candidates)
    if re.fullmatch(r"\d{1,2}(?:\.5)?", base):
        decimal_base = f"{float(base):.1f}" if "." not in base else None
        plain_candidates = [base, f"US {base}", f"US{base}"]
        if decimal_base:
            plain_candidates.extend([decimal_base, f"US {decimal_base}", f"US{decimal_base}"])

        def suffix_candidates(suffix: str) -> list[str]:
            suffix = suffix.upper()
            values = [
                f"{base}{suffix}",
                f"{suffix}{base}",
                f"{suffix} {base}",
                f"US {base}{suffix}",
                f"US {suffix}{base}",
                f"US {suffix} {base}",
            ]
            if decimal_base:
                values.extend(
                    [
                        f"{decimal_base}{suffix}",
                        f"{suffix}{decimal_base}",
                        f"{suffix} {decimal_base}",
                        f"US {decimal_base}{suffix}",
                        f"US {suffix}{decimal_base}",
                        f"US {suffix} {decimal_base}",
                    ]
                )
            return values

        preferred = [suffix.upper() for suffix in (preferred_suffixes or []) if suffix]
        if preferred:
            ordered_candidates: list[str] = []
            for suffix in preferred:
                ordered_candidates.extend(suffix_candidates(suffix))
            candidates = [*ordered_candidates, *plain_candidates, *base_candidates]
            fallback_suffixes = [suffix for suffix in ["M", "W", "Y", "C"] if suffix not in preferred]
            for suffix in fallback_suffixes:
                candidates.extend(suffix_candidates(suffix))
        else:
            candidates = [*plain_candidates, *base_candidates, *suffix_candidates("M")]
            for suffix in ["W", "Y", "C"]:
                candidates.extend(suffix_candidates(suffix))
    deduped: list[str] = []
    for candidate in candidates:
        if candidate and candidate not in deduped:
            deduped.append(candidate)
    return deduped


def _goat_style_candidates(style_no: str) -> list[str]:
    raw = str(style_no or "").strip().upper()
    normalized = normalize_style_no(raw) or raw
    candidates = [normalized]
    if "-" in normalized:
        candidates.append(normalized.replace("-", " "))
    if " " in normalized:
        candidates.append(normalized.replace(" ", "-"))
    compact = normalized.replace("-", "").replace(" ", "")
    if compact and compact != normalized:
        candidates.append(compact)
    deduped: list[str] = []
    for candidate in candidates:
        if candidate and candidate not in deduped:
            deduped.append(candidate)
    return deduped


def _goat_size_suffixes_from_titles(*titles: Any) -> list[str]:
    text = " ".join(str(title or "") for title in titles).upper()
    if not text.strip():
        return []
    if re.search(r"\b(WOMEN|WOMEN'S|WOMENS|WMNS|WMN|LADIES)\b|\(WOMEN'?S\)|\(W\)", text):
        return ["W"]
    if re.search(r"\b(GS|GRADE SCHOOL|BIG KIDS?|YOUTH)\b|\(GS\)", text):
        return ["Y"]
    if re.search(r"\b(PS|PRESCHOOL|PRE-SCHOOL|LITTLE KIDS?)\b|\(PS\)", text):
        return ["C", "Y"]
    if re.search(r"\b(TD|TODDLER|INFANT|CRIB)\b|\(TD\)", text):
        return ["C"]
    return []


def _goat_preferred_stockx_size_suffixes(conn, style_no: str, title_hint: str | None = None) -> list[str]:
    suffixes = _goat_size_suffixes_from_titles(title_hint)
    if suffixes:
        return suffixes
    style_candidates = _goat_style_candidates(style_no)
    if not style_candidates:
        return []
    placeholders = ",".join("?" for _ in style_candidates)
    rows = query_rows(
        conn,
        f"""
        SELECT title FROM products WHERE style_no IN ({placeholders})
        UNION ALL
        SELECT title FROM goat_consignment_items WHERE style_no IN ({placeholders})
        LIMIT 8
        """,
        tuple([*style_candidates, *style_candidates]),
    )
    return _goat_size_suffixes_from_titles(*(row["title"] for row in rows))


def _append_goat_size_filter(where: list[str], params: list[Any], size_filter: str, *, alias: str = "g") -> None:
    candidates = _goat_size_candidates(size_filter)
    if not candidates:
        return
    placeholders = ",".join("?" for _ in candidates)
    where.append(f"({alias}.size IN ({placeholders}) OR {alias}.matched_stockx_size IN ({placeholders}))")
    params.extend(candidates)
    params.extend(candidates)


def _goat_days_asc_sql(alias: str = "g") -> str:
    field = f"{alias}.estimated_days_to_sell"
    return (
        f"CASE WHEN {field} IS NULL "
        f"OR TRIM(CAST({field} AS TEXT)) IN ('', 'None', 'none', '-', 'NaN', 'nan') "
        f"OR CAST({field} AS REAL) < 0 "
        f"THEN 1 ELSE 0 END, CAST({field} AS REAL) ASC"
    )


def _numeric_asc_sql(field: str) -> str:
    return f"CASE WHEN {_numeric_invalid_sql(field)} THEN 1 ELSE 0 END, CAST({field} AS REAL) ASC"


def _numeric_invalid_sql(field: str) -> str:
    return (
        f"{field} IS NULL "
        f"OR TRIM(CAST({field} AS TEXT)) IN ('', 'None', 'none', '-', 'NaN', 'nan')"
    )


def _profit_desc_sql(field: str) -> str:
    return f"CASE WHEN {_numeric_invalid_sql(field)} THEN 1 ELSE 0 END, CAST({field} AS REAL) DESC"


def _numeric_order_sql(field: str, descending: bool) -> str:
    direction = "DESC" if descending else "ASC"
    return f"CASE WHEN {_numeric_invalid_sql(field)} THEN 1 ELSE 0 END, CAST({field} AS REAL) {direction}"


def _goat_days_order_sql(field: str, descending: bool) -> str:
    direction = "DESC" if descending else "ASC"
    invalid = (
        f"{field} IS NULL "
        f"OR TRIM(CAST({field} AS TEXT)) IN ('', 'None', 'none', '-', 'NaN', 'nan') "
        f"OR CAST({field} AS REAL) < 0"
    )
    return f"CASE WHEN {invalid} THEN 1 ELSE 0 END, CAST({field} AS REAL) {direction}"


def _text_order_sql(field: str, descending: bool) -> str:
    direction = "DESC" if descending else "ASC"
    return f"CASE WHEN {field} IS NULL OR TRIM(CAST({field} AS TEXT)) = '' THEN 1 ELSE 0 END, CAST({field} AS TEXT) COLLATE NOCASE {direction}"


def _date_order_sql(field: str, descending: bool) -> str:
    direction = "DESC" if descending else "ASC"
    return f"CASE WHEN {field} IS NULL OR TRIM(CAST({field} AS TEXT)) = '' THEN 1 ELSE 0 END, DATE({field}) {direction}"


def _goat_sort_sql(sort_field: str, descending: bool) -> str:
    numeric_fields = {
        "分数": "score",
        "预估利润": "total_profit",
        "单双利润": "estimated_profit",
        "StockX最低Ask": "stockx_lowest_ask",
        "默认出售": "estimated_sell_price",
        "GOAT价格": "goat_price",
        "采购成本": "buy_cost",
        "利润率": "profit_rate",
        "7天均价": "avg_7d",
        "30天均价": "avg_30d",
        "7天销量": "sales_7d",
        "30天销量": "sales_30d",
    }
    text_fields = {
        "评级": "rating",
        "货号": "style_no",
        "PID": "pid",
        "GOAT尺码": "size",
        "StockX尺码": "matched_stockx_size",
    }
    if sort_field == "发售日期":
        return f"{_date_order_sql('release_date', descending)}, {_profit_desc_sql('total_profit')}"
    if sort_field == "Ask快照":
        return f"{_date_order_sql('ask_snapshot_time', descending)}, {_profit_desc_sql('total_profit')}"
    if sort_field == "售罄天数":
        return f"{_goat_days_order_sql('estimated_days_to_sell', descending)}, {_profit_desc_sql('total_profit')}"
    if sort_field in numeric_fields:
        return f"{_numeric_order_sql(numeric_fields[sort_field], descending)}, {_profit_desc_sql('total_profit')}"
    if sort_field in text_fields:
        return f"{_text_order_sql(text_fields[sort_field], descending)}, {_profit_desc_sql('total_profit')}"
    return f"{_profit_desc_sql('total_profit')}, {_numeric_asc_sql('estimated_days_to_sell')}, score DESC"


def _goat_days_sort_value(value: Any) -> float:
    parsed = optional_float(value)
    if parsed is None or parsed != parsed or parsed < 0:
        return math.inf
    return parsed


def _profit_sort_value(value: Any) -> float:
    parsed = optional_float(value)
    if parsed is None or parsed != parsed:
        return -math.inf
    return parsed


def _is_sellout_days_label(value: Any) -> bool:
    text = str(value or "")
    return any(
        token in text
        for token in (
            "售罄",
            "预计售",
            "预估售",
            "卖完",
            "消化",
            "estimated_days_to_sell",
            "鍞",
            "鍗栧畬",
            "棰勮鍗栧畬",
        )
    )


def _is_profit_label(value: Any) -> bool:
    text = str(value or "")
    return any(token in text for token in ("利润", "profit", "estimated_profit", "鍒╂鼎", "棰勪及鍒╂鼎"))


def _sort_goat_rows_with_null_days_last(rows: list[Any], *, profit_desc: bool = True) -> list[Any]:
    def row_value(row: Any, key: str) -> Any:
        if isinstance(row, dict):
            return row.get(key)
        return row[key]

    return sorted(
        rows,
        key=lambda row: (
            _goat_days_sort_value(row_value(row, "estimated_days_to_sell")),
            -_profit_sort_value(row_value(row, "estimated_profit")) if profit_desc else 0.0,
        ),
    )


def _sort_goat_rows_with_null_profit_last(rows: list[Any]) -> list[Any]:
    def row_value(row: Any, key: str) -> Any:
        if isinstance(row, dict):
            return row.get(key)
        try:
            return row[key]
        except Exception:
            return None

    return sorted(
        rows,
        key=lambda row: (
            -_profit_sort_value(row_value(row, "total_profit") if row_value(row, "total_profit") is not None else row_value(row, "estimated_profit")),
            _goat_days_sort_value(row_value(row, "estimated_days_to_sell")),
        ),
    )


def _goat_local_sales_row_count(conn, style_no: str, size: str, title_hint: str | None = None) -> int:
    preferred_suffixes = _goat_preferred_stockx_size_suffixes(conn, style_no, title_hint)
    candidates = _goat_size_candidates(size, preferred_suffixes=preferred_suffixes)
    style_candidates = _goat_style_candidates(style_no)
    if not candidates or not style_candidates:
        return 0
    size_placeholders = ",".join(["?"] * len(candidates))
    style_placeholders = ",".join(["?"] * len(style_candidates))
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM sales_history
        WHERE style_no IN ({style_placeholders})
          AND COALESCE(size, '') IN ({size_placeholders})
        """,
        tuple([*style_candidates, *candidates]),
    ).fetchone()
    return int(row["count"] if row and row["count"] is not None else 0)


def _goat_recent_sync_error(conn, style_no: str) -> str | None:
    rows = query_rows(
        conn,
        """
        SELECT message, details_json
        FROM sync_logs
        WHERE style_no = ?
          AND severity = 'error'
        ORDER BY created_at DESC, id DESC
        LIMIT 3
        """,
        (style_no,),
    )
    for row in rows:
        message = str(row["message"] or "")
        details = json_loads(row["details_json"], {}) or {}
        error = str(details.get("error") or message or "").strip()
        lowered = error.lower()
        if "database is locked" in lowered:
            return "数据库写入被占用，本次未完成写入"
        if "timed out" in lowered or "timeout" in lowered:
            return "StockX接口超时"
        if "max retries exceeded" in lowered or "failed to establish a new connection" in lowered:
            return "StockX接口连接失败"
        if "500 server error" in lowered or "internal server error" in lowered:
            return "StockX接口500错误"
        if error:
            return error[:160]
    return None


def _goat_missing_stockx_reason(
    conn,
    style_no: str,
    size: str,
    refresh_summary: SyncSummary | None,
    title_hint: str | None = None,
) -> str:
    normalized_style = normalize_style_no(style_no) or str(style_no or "").strip().upper()
    style_candidates = _goat_style_candidates(normalized_style)
    preferred_suffixes = _goat_preferred_stockx_size_suffixes(conn, style_no, title_hint)
    candidates = _goat_size_candidates(size, preferred_suffixes=preferred_suffixes)
    summary_errors = [str(err) for err in (refresh_summary.errors or [])] if refresh_summary else []
    joined_errors = "；".join(summary_errors)
    lowered_errors = joined_errors.lower()

    if "database is locked" in lowered_errors:
        return "数据库写入被占用，本次未完成写入"
    if "timed out" in lowered_errors or "timeout" in lowered_errors:
        return "StockX接口超时"
    if "max retries exceeded" in lowered_errors or "failed to establish a new connection" in lowered_errors:
        return "StockX接口连接失败"
    if "500 server error" in lowered_errors or "internal server error" in lowered_errors:
        return "StockX接口500错误"

    product = query_rows(
        conn,
        f"SELECT product_id FROM products WHERE style_no IN ({','.join('?' for _ in style_candidates)}) LIMIT 1",
        tuple(style_candidates),
    )
    if not product or not (product[0]["product_id"]):
        recent_error = None
        for style_candidate in style_candidates:
            recent_error = _goat_recent_sync_error(conn, style_candidate)
            if recent_error:
                break
        if recent_error:
            return recent_error
        if "sku lookup did not resolve stockx uuid" in lowered_errors or "stockx_uuid" in lowered_errors:
            return "StockX查不到对应商品"
        return "StockX查不到对应商品"

    size_rows = query_rows(
        conn,
        f"SELECT size FROM product_sizes WHERE style_no IN ({','.join('?' for _ in style_candidates)})",
        tuple(style_candidates),
    )
    available_sizes = {normalize_us_size(row["size"]) for row in size_rows if row["size"]}
    matched_size = next((candidate for candidate in candidates if candidate in available_sizes), None)
    if available_sizes and not matched_size:
        sample = "、".join(list(sorted(available_sizes))[:8])
        return f"StockX有商品，但没有匹配US尺码；可用尺码示例：{sample}"

    if candidates:
        size_placeholders = ",".join("?" for _ in candidates)
        style_placeholders = ",".join("?" for _ in style_candidates)
        ask_rows = query_rows(
            conn,
            f"""
            SELECT COUNT(*) AS count
            FROM ask_depth
            WHERE style_no IN ({style_placeholders})
              AND COALESCE(size, '') IN ({size_placeholders})
            """,
            tuple([*style_candidates, *candidates]),
        )
        market_rows = query_rows(
            conn,
            f"""
            SELECT COUNT(*) AS count
            FROM market_snapshots
            WHERE style_no IN ({style_placeholders})
              AND COALESCE(size, '') IN ({size_placeholders})
              AND lowest_ask IS NOT NULL
            """,
            tuple([*style_candidates, *candidates]),
        )
        if int((ask_rows[0]["count"] if ask_rows else 0) or 0) <= 0 and int((market_rows[0]["count"] if market_rows else 0) or 0) <= 0:
            return "StockX有商品/尺码，但该尺码当前没有卖家Ask"

    recent_error = None
    for style_candidate in style_candidates:
        recent_error = _goat_recent_sync_error(conn, style_candidate)
        if recent_error:
            break
    if recent_error:
        return recent_error
    return "StockX已查询，但没有返回可用最低Ask"


def _refresh_stockx_size_snapshot_for_goat(
    conn,
    style_no: str,
    size: str,
    *,
    refresh_cache: set[str],
    progress_callback=None,
) -> SyncSummary | None:
    normalized_style = normalize_style_no(style_no) or str(style_no).strip().upper()
    normalized_size = normalize_us_size(size)
    if not normalized_style or not normalized_size:
        return None
    cache_key = f"{normalized_style}|{normalized_size}"
    if cache_key in refresh_cache:
        return None
    refresh_cache.add(cache_key)

    if progress_callback:
        progress_callback(
            {
                "phase": "实时补StockX",
                "style_no": normalized_style,
                "size": normalized_size,
                "message": f"本地缺Ask或销量，正在实时查询 {normalized_style} US {normalized_size}",
            }
        )
    last_error: str | None = None
    for attempt in range(1, 5):
        try:
            conn.commit()
            summary = sync_style(
                conn,
                normalized_style,
                include_sales=True,
                include_depth=True,
                include_size_endpoints=True,
                target_size=normalized_size,
                progress_callback=progress_callback,
            )
            locked_errors = [
                err for err in (summary.errors or [])
                if "database is locked" in str(err).lower()
            ]
            if not locked_errors:
                return summary
            last_error = "; ".join(locked_errors)
        except Exception as exc:  # noqa: BLE001 - one GOAT row must not kill the whole batch.
            last_error = str(exc)
            if "database is locked" not in last_error.lower():
                break
        if progress_callback:
            progress_callback(
                {
                    "phase": "GOAT实时补StockX",
                    "style_no": normalized_style,
                    "size": normalized_size,
                    "message": f"数据库写入被占用，第 {attempt}/4 次重试 {normalized_style} US {normalized_size}",
                }
            )
        time_module.sleep(min(8, attempt * 2))

    try:
        log_sync(
            conn,
            f"GOAT实时补StockX失败 {normalized_style} US {normalized_size}",
            severity="error",
            event_type="goat_live_refresh_error",
            style_no=normalized_style,
            details={"size": normalized_size, "error": last_error or "unknown"},
        )
        conn.commit()
    except Exception:
        pass
    return None

    try:
        return sync_style(
            conn,
            normalized_style,
            include_sales=True,
            include_depth=True,
            include_size_endpoints=True,
            target_size=normalized_size,
            progress_callback=progress_callback,
        )
    except Exception as exc:  # noqa: BLE001 - one GOAT row must not kill the whole batch.
        log_sync(
            conn,
            f"GOAT实时补StockX失败 {normalized_style} US {normalized_size}",
            severity="error",
            event_type="goat_live_refresh_error",
            style_no=normalized_style,
            details={"size": normalized_size, "error": str(exc)},
        )
        return None


def _estimate_goat_sellout_days(stats, sell_price: float | None) -> float | None:
    if sell_price is None:
        return None
    velocity = max(
        stats.sales_7d / 7 if stats.sales_7d else 0.0,
        stats.sales_14d / 14 if stats.sales_14d else 0.0,
        stats.sales_30d / 30 if stats.sales_30d else 0.0,
    )
    if velocity <= 0:
        return None
    days = 1 / velocity
    reference = stats.avg_7d or stats.median or stats.avg_30d or stats.last_sale_amount
    if reference and sell_price > reference:
        premium = sell_price / reference - 1
        days *= 1 + min(8.0, premium * 8.0)
    return round(days, 1)


def _score_goat_consignment_item(
    conn,
    item: dict[str, Any],
    *,
    fee_rate: float | None = None,
    live_refresh_missing: bool = True,
    refresh_cache: set[str] | None = None,
    progress_callback=None,
) -> dict[str, Any]:
    style_no = str(item["style_no"])
    size = normalize_us_size(item["size"])
    title_hint = str(item.get("title") or "")
    ask = _latest_stockx_lowest_ask_for_size(conn, style_no, size, title_hint=title_hint)
    sales_info = _goat_sales_stats(conn, style_no, size, title_hint=title_hint)
    refresh_summary: SyncSummary | None = None
    if live_refresh_missing and refresh_cache is not None:
        has_sales_snapshot = _goat_local_sales_row_count(conn, style_no, size, title_hint=title_hint) > 0
        if ask is None or not has_sales_snapshot:
            refresh_summary = _refresh_stockx_size_snapshot_for_goat(
                conn,
                style_no,
                size,
                refresh_cache=refresh_cache,
                progress_callback=progress_callback,
            )
            ask = _latest_stockx_lowest_ask_for_size(conn, style_no, size, title_hint=title_hint)
            sales_info = _goat_sales_stats(conn, style_no, size, title_hint=title_hint)
    stats = sales_info["stats"]
    matched_size = (ask or {}).get("matched_size") or sales_info.get("matched_size") or size
    stockx_ask = float(ask["ask_price"]) if ask and ask.get("ask_price") is not None else None
    buy_cost = round(float(item.get("goat_price") or 0.0) + GOAT_BUY_COST_ADDER_USD, 2)
    item["buy_cost"] = buy_cost
    net_fee_rate = max(0.0, float(get_settings().estimated_seller_fee_rate if fee_rate is None else fee_rate))
    sell_price = stockx_ask
    net_proceeds = round(sell_price * (1 - net_fee_rate), 2) if sell_price is not None else None
    profit = round(net_proceeds - buy_cost, 2) if net_proceeds is not None else None
    profit_rate = round(profit / buy_cost, 4) if profit is not None and buy_cost else None
    days = _estimate_goat_sellout_days(stats, sell_price)

    speed_score = 0.0
    if days is not None:
        if days <= 3:
            speed_score = 50
        elif days <= 7:
            speed_score = 42
        elif days <= 14:
            speed_score = 30
        elif days <= 30:
            speed_score = 16
        else:
            speed_score = 6
    profit_score = 0.0
    if profit is not None:
        if profit >= 80:
            profit_score = 50
        elif profit >= 50:
            profit_score = 42
        elif profit >= 30:
            profit_score = 32
        elif profit >= 15:
            profit_score = 20
        elif profit > 0:
            profit_score = 10
    score = round(min(100, speed_score + profit_score), 2)
    notes: list[str] = []
    if stockx_ask is None:
        notes.append("缺少StockX最低Ask")
    if stats.sales_7d == 0:
        notes.append("7天无成交")
    if stats.sales_30d == 0:
        notes.append("30天无成交")
    if profit is not None and profit <= 0:
        notes.append("按StockX最低价出售利润为负")
    notes = []
    if stockx_ask is None:
        notes.append(_goat_missing_stockx_reason(conn, style_no, size, refresh_summary, title_hint=title_hint))
    if stats.sales_7d == 0:
        notes.append("7天无成交")
    if stats.sales_30d == 0:
        notes.append("30天无成交")
    if profit is not None and profit <= 0:
        notes.append("按StockX最低Ask出售利润为负")
    risk_notes_text = "；".join(notes) if notes else "可继续看利润和流速"
    rating = rating_from_score(score)
    return {
        "score": score,
        "rating": rating,
        "matched_stockx_size": matched_size,
        "stockx_lowest_ask": stockx_ask,
        "ask_snapshot_time": (ask or {}).get("snapshot_time"),
        "sales_7d": stats.sales_7d,
        "sales_30d": stats.sales_30d,
        "avg_7d": stats.avg_7d,
        "avg_30d": stats.avg_30d,
        "estimated_sell_price": sell_price,
        "estimated_profit": profit,
        "estimated_profit_rate": profit_rate,
        "estimated_days_to_sell": days,
        "risk_notes": "；".join(notes) if notes else "可继续看利润和流速",
        "components": {
            "speed_score": speed_score,
            "profit_score": profit_score,
            "missing_reason": notes[0] if stockx_ask is None and notes else None,
            "sell_price_basis": "stockx_lowest_ask",
            "sell_net_proceeds": net_proceeds,
            "payment_fee_rate": net_fee_rate,
            "buy_cost_formula": f"goat_price + {GOAT_BUY_COST_ADDER_USD:g}",
            "profit_formula": "stockx_lowest_ask * (1 - payment_fee_rate) - buy_cost",
        },
    }


def _refresh_stockx_style_snapshot_for_goat(
    conn,
    style_no: str,
    *,
    refresh_cache: set[str],
    progress_callback=None,
) -> SyncSummary | None:
    normalized_style = normalize_style_no(style_no) or str(style_no).strip().upper()
    if not normalized_style or normalized_style in refresh_cache:
        return None
    refresh_cache.add(normalized_style)
    if progress_callback:
        progress_callback(
            {
                "phase": "GOAT?StockX",
                "style_no": normalized_style,
                "message": f"???GOAT??????StockX?{normalized_style}",
            }
        )
    last_error: str | None = None
    for attempt in range(1, 4):
        try:
            conn.commit()
            summary = sync_style(
                conn,
                normalized_style,
                include_sales=True,
                include_depth=True,
                include_size_endpoints=False,
                progress_callback=progress_callback,
            )
            locked_errors = [
                err for err in (summary.errors or [])
                if "database is locked" in str(err).lower()
            ]
            if not locked_errors:
                return summary
            last_error = "; ".join(locked_errors)
        except Exception as exc:  # noqa: BLE001 - one GOAT style must not stop the batch.
            last_error = str(exc)
            if "database is locked" not in last_error.lower():
                break
        time_module.sleep(min(6, attempt * 2))
    try:
        log_sync(
            conn,
            f"GOAT?StockX?? {normalized_style}",
            severity="error",
            event_type="goat_live_refresh_error",
            style_no=normalized_style,
            details={"error": last_error or "unknown"},
        )
        conn.commit()
    except Exception:
        pass
    return None


def _goat_style_needs_refresh(conn, style_no: str, items: list[dict[str, Any]]) -> bool:
    for item in items:
        size = str(item.get("size") or "")
        title_hint = str(item.get("title") or "")
        if _latest_stockx_lowest_ask_for_size(conn, style_no, size, title_hint=title_hint) is None:
            return True
        if _goat_local_sales_row_count(conn, style_no, size, title_hint=title_hint) <= 0:
            return True
    return False


def _store_goat_consignment_score(conn, item: dict[str, Any], scored: dict[str, Any], computed_at: str) -> None:
    conn.execute(
        """
        INSERT INTO goat_consignment_scores (
            item_id, score, rating, style_no, size, matched_stockx_size, pid, title,
            goat_price, buy_cost, stockx_lowest_ask, ask_snapshot_time,
            sales_7d, sales_30d, avg_7d, avg_30d,
            estimated_sell_price, estimated_profit, estimated_profit_rate,
            estimated_days_to_sell, risk_notes, components_json, computed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(item_id) DO UPDATE SET
            score=excluded.score,
            rating=excluded.rating,
            matched_stockx_size=excluded.matched_stockx_size,
            goat_price=excluded.goat_price,
            buy_cost=excluded.buy_cost,
            stockx_lowest_ask=excluded.stockx_lowest_ask,
            ask_snapshot_time=excluded.ask_snapshot_time,
            sales_7d=excluded.sales_7d,
            sales_30d=excluded.sales_30d,
            avg_7d=excluded.avg_7d,
            avg_30d=excluded.avg_30d,
            estimated_sell_price=excluded.estimated_sell_price,
            estimated_profit=excluded.estimated_profit,
            estimated_profit_rate=excluded.estimated_profit_rate,
            estimated_days_to_sell=excluded.estimated_days_to_sell,
            risk_notes=excluded.risk_notes,
            components_json=excluded.components_json,
            computed_at=excluded.computed_at
        """,
        (
            item["id"],
            scored["score"],
            scored["rating"],
            item["style_no"],
            item["size"],
            scored["matched_stockx_size"],
            item["pid"],
            item.get("title"),
            item["goat_price"],
            item["buy_cost"],
            scored["stockx_lowest_ask"],
            scored["ask_snapshot_time"],
            scored["sales_7d"],
            scored["sales_30d"],
            scored["avg_7d"],
            scored["avg_30d"],
            scored["estimated_sell_price"],
            scored["estimated_profit"],
            scored["estimated_profit_rate"],
            scored["estimated_days_to_sell"],
            scored["risk_notes"],
            json_dumps(scored["components"]),
            computed_at,
        ),
    )


def _compute_goat_consignment_scores(conn, *, progress_callback=None, live_refresh_missing: bool = True) -> int:
    rows = [dict(row) for row in query_rows(conn, "SELECT * FROM goat_consignment_items ORDER BY imported_at DESC, id DESC")]
    computed_at = datetime.utcnow().isoformat(timespec="seconds")
    count = 0
    total = len(rows)
    style_groups: dict[str, list[dict[str, Any]]] = {}
    for item in rows:
        style_no = normalize_style_no(item.get("style_no")) or str(item.get("style_no") or "").strip().upper()
        if not style_no:
            continue
        style_groups.setdefault(style_no, []).append(item)

    style_refresh_cache: set[str] = set()
    size_refresh_cache: set[str] = set()
    total_styles = len(style_groups)
    for style_index, (style_no, items) in enumerate(style_groups.items(), start=1):
        if live_refresh_missing and _goat_style_needs_refresh(conn, style_no, items):
            if progress_callback:
                progress_callback(
                    {
                        "phase": "GOAT?StockX",
                        "completed": count,
                        "total": total,
                        "style_no": style_no,
                        "message": f"??? {style_index}/{total_styles}: {style_no}",
                    }
                )
            _refresh_stockx_style_snapshot_for_goat(
                conn,
                style_no,
                refresh_cache=style_refresh_cache,
                progress_callback=progress_callback,
            )

        for item in items:
            if progress_callback:
                progress_callback(
                    {
                        "phase": "GOAT??",
                        "completed": count,
                        "total": total,
                        "pid": item.get("pid"),
                        "style_no": item.get("style_no"),
                        "size": item.get("size"),
                        "message": f"???? {count + 1}/{total}: {item.get('style_no')} US {item.get('size')}",
                    }
                )
            scored = _score_goat_consignment_item(
                conn,
                item,
                live_refresh_missing=live_refresh_missing,
                refresh_cache=size_refresh_cache,
                progress_callback=progress_callback,
            )
            _store_goat_consignment_score(conn, item, scored, computed_at)
            count += 1
            conn.commit()

        conn.commit()
        if progress_callback and (style_index == 1 or style_index % 5 == 0 or style_index == total_styles):
            progress_callback(
                {
                    "phase": "GOAT??",
                    "completed": count,
                    "total": total,
                    "style_no": style_no,
                    "message": f"??? {style_index}/{total_styles} ?GOAT?????? {count}/{total} ?",
                }
            )
    return count

def _goat_job_snapshot() -> dict[str, Any]:
    resource = _goat_job_resource()
    with resource["lock"]:
        return dict(resource["state"])


def _update_goat_job_state(**changes: Any) -> None:
    resource = _goat_job_resource()
    with resource["lock"]:
        resource["state"].update(changes)


def _goat_progress_callback(event: dict[str, Any]) -> None:
    total = event.get("total")
    completed = event.get("completed")
    current = _goat_job_snapshot()
    updates: dict[str, Any] = {
        "phase": event.get("phase") or current.get("phase"),
        "current_pid": event.get("pid") or current.get("current_pid"),
        "current_style": event.get("style_no") or current.get("current_style"),
        "current_size": event.get("size") or current.get("current_size"),
        "message": event.get("message") or current.get("message", ""),
    }
    if isinstance(total, int):
        updates["total"] = total
    if isinstance(completed, int):
        updates["completed"] = completed
    if isinstance(total, int) and total > 0 and isinstance(completed, int):
        updates["progress"] = max(0.0, min(0.98, completed / total))
    _update_goat_job_state(**updates)


def _run_goat_consignment_job(
    job_id: str,
    db_path_str: str,
    *,
    file_path_str: str | None,
    source_name: str,
    import_file: bool,
    replace_current: bool,
    live_refresh_missing: bool,
) -> None:
    conn = connect(Path(db_path_str))
    init_db(conn)
    try:
        imported = 0
        if import_file:
            if not file_path_str:
                raise ValueError("缺少GOAT清单文件")
            file_path = Path(file_path_str)
            _update_goat_job_state(
                phase="导入GOAT清单",
                message=f"正在读取 {file_path.name}",
                progress=0.02,
                completed=0,
                total=0,
                current_pid=None,
                current_style=None,
                current_size=None,
            )
            frame = _read_goat_consignment_frame(file_path.name, file_path.read_bytes())
            _update_goat_job_state(
                message=f"正在导入 {len(frame)} 行GOAT清单",
                total=int(len(frame)),
                progress=0.05,
            )
            imported = _import_goat_consignment_rows(
                conn,
                frame,
                source_name=source_name,
                replace_current=replace_current,
            )
            conn.commit()
            _update_goat_job_state(
                imported=imported,
                message=f"已导入 {imported} 行，开始评分",
                phase="GOAT评分",
                progress=0.08,
            )

        computed = _compute_goat_consignment_scores(
            conn,
            progress_callback=_goat_progress_callback,
            live_refresh_missing=live_refresh_missing,
        )
        conn.commit()
        _update_goat_job_state(
            status="done",
            phase="完成",
            message=f"GOAT寄存评分完成：导入 {imported} 行，评分 {computed} 行",
            progress=1.0,
            computed=computed,
            error=None,
            finished_at=datetime.utcnow().isoformat(timespec="seconds"),
        )
        schedule_cloud_backup("goat_consignment_done")
    except Exception as exc:  # noqa: BLE001
        try:
            log_sync(
                conn,
                "GOAT寄存评分失败",
                severity="error",
                event_type="goat_consignment_error",
                details={"job_id": job_id, "error": str(exc)},
            )
            conn.commit()
        except Exception:
            pass
        _update_goat_job_state(
            status="error",
            phase="失败",
            message=f"GOAT寄存评分失败：{exc}",
            error=str(exc),
            finished_at=datetime.utcnow().isoformat(timespec="seconds"),
        )
    finally:
        conn.close()


def start_goat_consignment_job(
    *,
    db_path_str: str,
    file_path_str: str | None = None,
    source_name: str = "goat_slover",
    import_file: bool = False,
    replace_current: bool = False,
    live_refresh_missing: bool = True,
) -> str | None:
    if _goat_job_snapshot().get("status") == "running":
        return None
    job_id = uuid.uuid4().hex[:8]
    _update_goat_job_state(
        job_id=job_id,
        status="running",
        message="GOAT寄存任务已启动",
        progress=0.0,
        completed=0,
        total=0,
        phase="准备",
        current_pid=None,
        current_style=None,
        current_size=None,
        source_name=source_name,
        imported=0,
        computed=0,
        error=None,
        started_at=datetime.utcnow().isoformat(timespec="seconds"),
        finished_at=None,
    )
    thread = threading.Thread(
        target=_run_goat_consignment_job,
        args=(job_id, db_path_str),
        kwargs={
            "file_path_str": file_path_str,
            "source_name": source_name,
            "import_file": import_file,
            "replace_current": replace_current,
            "live_refresh_missing": live_refresh_missing,
        },
        name=f"goat-consignment-{job_id}",
        daemon=True,
    )
    thread.start()
    return job_id


def _goat_request_loop() -> None:
    while True:
        try:
            if GOAT_RESCORE_REQUEST_PATH.exists() and _goat_job_snapshot().get("status") != "running":
                request = json_loads(GOAT_RESCORE_REQUEST_PATH.read_text(encoding="utf-8"), {}) or {}
                try:
                    GOAT_RESCORE_REQUEST_PATH.unlink()
                except OSError:
                    pass
                settings = get_settings()
                start_goat_consignment_job(
                    db_path_str=str(settings.db_path),
                    source_name=str(request.get("source_name") or "goat_background_rescore"),
                    import_file=False,
                    replace_current=False,
                    live_refresh_missing=True,
                )
        except Exception as exc:  # noqa: BLE001
            try:
                GOAT_RESCORE_REQUEST_PATH.parent.mkdir(parents=True, exist_ok=True)
                GOAT_RESCORE_REQUEST_PATH.with_suffix(".error.json").write_text(
                    json.dumps({"error": str(exc), "at": datetime.utcnow().isoformat(timespec="seconds")}, ensure_ascii=False),
                    encoding="utf-8",
                )
            except Exception:
                pass
        time_module.sleep(10)


@st.cache_resource(show_spinner=False)
def ensure_goat_request_scheduler() -> dict[str, Any]:
    thread = threading.Thread(target=_goat_request_loop, name="goat-request-loop", daemon=True)
    thread.start()
    return {"started_at": datetime.utcnow().isoformat(timespec="seconds"), "thread_name": thread.name}


def ensure_goat_stockx_worker_process() -> dict[str, Any]:
    if not GOAT_STOCKX_WORKER_SCRIPT.exists():
        return {"started": False, "reason": "missing worker script"}
    python_exe = Path(sys.executable)
    if not python_exe.exists():
        return {"started": False, "reason": "missing python executable"}
    try:
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        subprocess.Popen(
            [str(python_exe), str(GOAT_STOCKX_WORKER_SCRIPT)],
            cwd=str(BASE_DIR),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=creationflags,
        )
        return {"started": True, "started_at": datetime.utcnow().isoformat(timespec="seconds")}
    except Exception as exc:  # noqa: BLE001 - frontend must stay usable if the worker cannot start.
        return {"started": False, "reason": str(exc)}


def start_goat_stockx_worker_request(source_name: str = "goat_rescore", *, live_refresh_missing: bool = True) -> dict[str, Any]:
    GOAT_RESCORE_REQUEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    marker = _read_goat_stockx_worker_marker()
    is_running = marker.get("status") == "running"
    GOAT_RESCORE_REQUEST_PATH.write_text(
        json.dumps(
            {
                "source_name": source_name,
                "live_refresh_missing": live_refresh_missing,
                "requested_at": datetime.utcnow().isoformat(timespec="seconds"),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    if is_running:
        return {
            "started": True,
            "queued": True,
            "reason": "当前任务运行中，已排队等待执行",
            "current_job_id": marker.get("job_id"),
        }
    return ensure_goat_stockx_worker_process()


def _timestamp_from_marker(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        pass
    try:
        text = str(value).strip().replace("Z", "+00:00")
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return None


def _zoneinfo_or_offset(name: str, offset_hours: int):
    if ZoneInfo is not None:
        try:
            return ZoneInfo(name)
        except Exception:
            pass
    return timezone(timedelta(hours=offset_hours))


def _los_angeles_timezone():
    return _zoneinfo_or_offset("America/Los_Angeles", -7)


def _beijing_timezone():
    return _zoneinfo_or_offset("Asia/Shanghai", 8)


def _format_elapsed(seconds: float | int | None) -> str:
    if seconds is None:
        return "-"
    try:
        total = max(0, int(float(seconds)))
    except (TypeError, ValueError):
        return "-"
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}小时{minutes}分钟"
    if minutes:
        return f"{minutes}分钟{secs}秒"
    return f"{secs}秒"


def _auto_hourly_status_snapshot(settings) -> dict[str, Any]:
    marker = _read_auto_hourly_marker()
    interval_minutes = int(getattr(settings, "auto_full_sync_interval_minutes", 60) or 60)
    enabled = bool(getattr(settings, "auto_full_sync_enabled", False))
    completed = int(marker.get("completed") or 0)
    total = int(marker.get("total") or marker.get("last_style_count") or 0)
    last_status = str(marker.get("last_status") or "idle")
    active_job_id = marker.get("active_job_id")
    lock_exists = JOB_LOCK_PATH.exists()
    started_ts = _timestamp_from_marker(marker.get("last_started_ts")) or _timestamp_from_marker(marker.get("last_started_at"))
    finished_ts = _timestamp_from_marker(marker.get("last_finished_ts")) or _timestamp_from_marker(marker.get("last_finished_at"))
    valid_finished_ts = finished_ts if finished_ts and (not started_ts or finished_ts >= started_ts) else None
    now_ts = datetime.utcnow().timestamp()
    last_touch_ts = (
        _timestamp_from_marker(marker.get("last_progress_ts"))
        or _timestamp_from_marker(marker.get("last_progress_at"))
        or _timestamp_from_marker(marker.get("last_checkpoint_at"))
        or started_ts
    )
    stale_no_progress = (
        last_status == "running"
        and bool(last_touch_ts)
        and now_ts - float(last_touch_ts) >= 10 * 60
    )

    stale_running = last_status == "running" and (
        not lock_exists
        or (total > 0 and completed >= total)
        or stale_no_progress
    )
    running = last_status == "running" and bool(active_job_id) and lock_exists and not stale_running
    if running:
        status_label = "正在跑"
        tone = "info"
        elapsed = _format_elapsed(now_ts - started_ts if started_ts else None)
        message = f"本轮进行中：{completed}/{total or '-'}，当前 {marker.get('current_style') or '-'}，已用 {elapsed}。"
    elif stale_running:
        status_label = "上一轮收尾异常"
        tone = "warning"
        elapsed_end = finished_ts if finished_ts and started_ts and finished_ts >= started_ts else now_ts
        elapsed = _format_elapsed(elapsed_end - started_ts if started_ts else None)
        error = marker.get("last_error")
        suffix = f"；错误：{error}" if error else ""
        message = f"上一轮处理数 {completed}/{total or '-'}，但状态没有正常收尾，已按非运行处理，用时约 {elapsed}{suffix}。没有正常完成时间时不会自动开下一轮。"
    elif last_status == "done":
        status_label = "上一轮已完成"
        tone = "success"
        elapsed = _format_elapsed(finished_ts - started_ts if started_ts and finished_ts else None)
        message = f"上一轮完成：{completed}/{total or '-'}，完成时间 {_format_datetime_minute(marker.get('last_finished_at'))}，用时 {elapsed}。"
    elif last_status == "error":
        status_label = "上一轮失败"
        tone = "warning"
        message = f"上一轮失败：{marker.get('last_error') or marker.get('last_message') or '-'}；时间 {_format_datetime_minute(marker.get('last_finished_at'))}。"
    elif not enabled:
        status_label = "已关闭"
        tone = "info"
        message = "自动全量同步已关闭。"
    else:
        status_label = "等待下一轮"
        tone = "info"
        last_time = _format_datetime_minute(marker.get("last_finished_at")) if valid_finished_ts else "-"
        if valid_finished_ts:
            message = f"自动全量开启；上一轮完成后等待 {interval_minutes} 分钟再检查，最近正常完成 {last_time}。"
        else:
            message = "自动全量开启；但还没有正常完成时间，不会按开始时间重复开新一轮。"

    return {
        "enabled": enabled,
        "status_label": status_label,
        "tone": tone,
        "message": message,
        "interval_minutes": interval_minutes,
        "last_started_at": marker.get("last_started_at"),
        "last_finished_at": marker.get("last_finished_at") if valid_finished_ts else None,
        "completed": completed,
        "total": total,
        "running": running,
        "stale_running": stale_running,
        "has_valid_finish": bool(valid_finished_ts),
    }


def _frontend_auto_refresh(enabled: bool, *, interval_seconds: int = 8, key: str = "progress") -> None:
    # Intentionally do not reload the browser page here.
    # Background workers write progress to marker files; Streamlit fragments handle
    # lightweight progress updates without interrupting table/card browsing.
    return


@st.fragment(run_every="8s")
def _render_auto_hourly_status(settings) -> dict[str, Any]:
    status = _auto_hourly_status_snapshot(settings)
    detail = (
        f"规则：不会重叠跑；只按“上一轮正常完成时间 + {status['interval_minutes']} 分钟”触发。"
        f"如果一轮本身超过这个时间，会等它先跑完，再重新计时。"
        f" 开始：{_format_datetime_minute(status.get('last_started_at'))}；"
        f"结束：{_format_datetime_minute(status.get('last_finished_at'))}。"
    )
    text = f"{status['status_label']}：{status['message']}\n\n{detail}"
    if status["tone"] == "success":
        st.success(text)
    elif status["tone"] == "warning":
        st.warning(text)
    else:
        st.info(text)
    return status


def get_conn():
    conn = connect()
    init_db(conn)
    return conn


def schedule_cloud_backup(reason: str) -> None:
    settings = get_settings()
    if not settings.firebase_enabled:
        return

    db_path = Path(settings.db_path)

    def worker() -> None:
        try:
            sqlite_result = backup_sqlite_to_firestore(db_path, reason=reason)
            if not sqlite_result.get("ok"):
                backup_core_tables_to_firestore(db_path, reason=reason)
            else:
                backup_core_tables_to_firestore(db_path, reason=reason)
        except Exception as exc:  # noqa: BLE001
            try:
                with connect(db_path) as conn:
                    init_db(conn)
                    log_sync(
                        conn,
                        f"Firebase SQLite 备份失败：{exc}",
                        severity="error",
                        event_type="firebase_backup_error",
                        details={"reason": reason, "error": str(exc)},
                    )
                    conn.commit()
            except Exception:
                pass

    threading.Thread(target=worker, daemon=True).start()


def schedule_startup_core_backup_if_needed(settings) -> None:
    if not settings.firebase_enabled:
        return
    try:
        marker = json_loads(CORE_BACKUP_BOOTSTRAP_MARKER.read_text(encoding="utf-8"), {}) or {}
    except OSError:
        marker = {}
    last_ts = _timestamp_from_marker(marker.get("last_started_ts"))
    if last_ts and datetime.utcnow().timestamp() - last_ts < 3600:
        return
    try:
        CORE_BACKUP_BOOTSTRAP_MARKER.parent.mkdir(parents=True, exist_ok=True)
        CORE_BACKUP_BOOTSTRAP_MARKER.write_text(
            json.dumps(
                {
                    "last_started_at": datetime.utcnow().isoformat(timespec="seconds"),
                    "last_started_ts": datetime.utcnow().timestamp(),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    except OSError:
        pass
    schedule_cloud_backup("startup_core_backup")


def restore_opportunity_scores_from_latest_history_if_empty(conn) -> int:
    current_scores = int(conn.execute("SELECT COUNT(*) FROM opportunity_scores").fetchone()[0] or 0)
    if current_scores > 0:
        return 0
    snapshot = conn.execute(
        """
        SELECT id, score_count
        FROM opportunity_import_snapshots
        WHERE score_count > 0
        ORDER BY archived_at DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    if not snapshot:
        return 0
    snapshot_id = int(snapshot["id"])
    rows = query_rows(
        conn,
        """
        SELECT
            product_id, style_no, title, brand, size, score, rating,
            recommended_buy_qty, max_buy_price, weighted_avg_cost,
            next_lowest_ask, target_sell_price_low, target_sell_price_high,
            estimated_profit, estimated_profit_per_pair, estimated_days_to_sell,
            release_date, release_days, risk_notes, components_json, computed_at
        FROM opportunity_score_history
        WHERE snapshot_id = ?
        """,
        (snapshot_id,),
    )
    restored = 0
    for row in rows:
        data = dict(row)
        conn.execute(
            """
            INSERT INTO opportunity_scores (
                product_id, style_no, title, brand, size, score, rating,
                recommended_buy_qty, max_buy_price, weighted_avg_cost,
                next_lowest_ask, target_sell_price_low, target_sell_price_high,
                estimated_profit, estimated_profit_per_pair, estimated_days_to_sell,
                risk_notes, components_json, computed_at,
                release_date, release_days
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(style_no, size) DO UPDATE SET
                product_id=excluded.product_id,
                title=excluded.title,
                brand=excluded.brand,
                score=excluded.score,
                rating=excluded.rating,
                recommended_buy_qty=excluded.recommended_buy_qty,
                max_buy_price=excluded.max_buy_price,
                weighted_avg_cost=excluded.weighted_avg_cost,
                next_lowest_ask=excluded.next_lowest_ask,
                target_sell_price_low=excluded.target_sell_price_low,
                target_sell_price_high=excluded.target_sell_price_high,
                estimated_profit=excluded.estimated_profit,
                estimated_profit_per_pair=excluded.estimated_profit_per_pair,
                estimated_days_to_sell=excluded.estimated_days_to_sell,
                risk_notes=excluded.risk_notes,
                components_json=excluded.components_json,
                computed_at=excluded.computed_at,
                release_date=excluded.release_date,
                release_days=excluded.release_days
            """,
            (
                data.get("product_id"),
                data.get("style_no"),
                data.get("title"),
                data.get("brand"),
                data.get("size"),
                data.get("score"),
                data.get("rating"),
                data.get("recommended_buy_qty") or 0,
                data.get("max_buy_price"),
                data.get("weighted_avg_cost"),
                data.get("next_lowest_ask"),
                data.get("target_sell_price_low"),
                data.get("target_sell_price_high"),
                data.get("estimated_profit"),
                data.get("estimated_profit_per_pair"),
                data.get("estimated_days_to_sell"),
                data.get("risk_notes"),
                data.get("components_json") or "{}",
                data.get("computed_at") or utc_now(),
                data.get("release_date"),
                data.get("release_days"),
            ),
        )
        restored += 1
    if restored:
        log_sync(
            conn,
            f"已从历史快照 #{snapshot_id} 恢复 {restored} 行机会评分",
            event_type="opportunity_scores_restored_from_history",
            details={"snapshot_id": snapshot_id, "restored": restored},
        )
    return restored


def sync_style_isolated(
    style_no: str,
    *,
    title_hint: str | None = None,
    include_sales: bool,
    include_depth: bool,
    include_size_endpoints: bool,
    target_size: str | None = None,
    reset_snapshot: bool = False,
    progress_callback=None,
) -> SyncSummary:
    worker_conn = connect()
    try:
        return sync_style(
            worker_conn,
            style_no,
            title_hint=title_hint,
            include_sales=include_sales,
            include_depth=include_depth,
            include_size_endpoints=include_size_endpoints,
            target_size=target_size,
            reset_snapshot=reset_snapshot,
            progress_callback=progress_callback,
        )
    finally:
        worker_conn.close()


def _sync_state_snapshot() -> dict[str, Any]:
    with SYNC_JOB_LOCK:
        memory_state = dict(SYNC_JOB_STATE)
    if memory_state.get("status") == "running":
        return _mark_sync_startup_stalled(_recover_sync_state_from_logs(memory_state))
    file_state = _read_json_path(SYNC_STATE_PATH)
    if file_state:
        if file_state.get("status") == "running":
            lock_state = _sync_state_from_lock()
            if lock_state and (
                not file_state.get("job_id") or file_state.get("job_id") == lock_state.get("job_id")
            ):
                merged = dict(lock_state)
                merged.update(file_state)
                return _mark_sync_startup_stalled(_recover_sync_state_from_logs(merged))
        elif file_state.get("status") in {"done", "error"}:
            return file_state
    lock_state = _sync_state_from_lock()
    if lock_state:
        return _mark_sync_startup_stalled(_recover_sync_state_from_logs(lock_state))
    return memory_state


def _update_sync_state(**changes: Any) -> None:
    with SYNC_JOB_LOCK:
        SYNC_JOB_STATE.update(changes)
        SYNC_JOB_STATE["updated_at"] = datetime.utcnow().isoformat(timespec="seconds")
        state = dict(SYNC_JOB_STATE)
    _write_sync_state_file(state)


def _append_sync_event(event: dict[str, Any]) -> None:
    timestamp = datetime.utcnow().isoformat(timespec="seconds")
    clean_event = {
        "time": timestamp,
        "phase": event.get("phase") or "",
        "style_no": event.get("style_no") or "",
        "size": event.get("size") or "",
        "endpoint": event.get("endpoint") or "",
        "status": event.get("status") or "",
        "message": event.get("message") or "",
    }
    with SYNC_JOB_LOCK:
        recent = list(SYNC_JOB_STATE.get("recent_events") or [])
        recent.append(clean_event)
        SYNC_JOB_STATE["recent_events"] = recent[-30:]
        SYNC_JOB_STATE["updated_at"] = datetime.utcnow().isoformat(timespec="seconds")
        state = dict(SYNC_JOB_STATE)
    _write_sync_state_file(state)


def _sync_progress_callback(event: dict[str, Any]) -> None:
    _update_sync_state(
        current_phase=event.get("phase") or SYNC_JOB_STATE.get("current_phase"),
        current_style=event.get("style_no") or SYNC_JOB_STATE.get("current_style"),
        current_size=event.get("size"),
        current_endpoint=event.get("endpoint") or SYNC_JOB_STATE.get("current_endpoint"),
        score_completed=event.get("score_completed", SYNC_JOB_STATE.get("score_completed", 0)),
        score_total=event.get("score_total", SYNC_JOB_STATE.get("score_total", 0)),
        message=event.get("message") or SYNC_JOB_STATE.get("message", ""),
    )
    status = str(event.get("status") or "")
    if status in {"ok", "error"} or event.get("phase") == "评分":
        _append_sync_event(event)


def data_cache_version() -> int:
    try:
        return int(st.session_state.get("data_cache_version", 0))
    except Exception:
        return 0


def bump_data_cache_version() -> int:
    version = data_cache_version() + 1
    st.session_state["data_cache_version"] = version
    return version


def _run_live_api_probe(settings) -> list[dict[str, Any]]:
    cached = st.session_state.get("live_api_probe_results")
    if cached is not None:
        return cached

    conn = connect(Path(settings.db_path))
    init_db(conn)
    client = StockXClient(conn, settings)
    samples = [("HQ6998-200", "12"), ("IF4396-103", "9.5")]
    results: list[dict[str, Any]] = []
    try:
        for style_no, want_size in samples:
            probe: dict[str, Any] = {"style_no": style_no, "size": want_size}
            try:
                target = None
                variants: list[dict[str, Any]] = []
                product_uuid: str | None = None
                detail_source = "get_product_detail_info_by_sku"

                detail_lookup = client.get_product_detail_info_by_sku(style_no)
                probe["detail_ok"] = detail_lookup.ok
                probe["detail_error"] = detail_lookup.error
                if detail_lookup.ok and detail_lookup.pages:
                    detail_data = extract_product(detail_lookup.pages[-1])
                    product_uuid = extract_product_uuid(detail_lookup.pages[-1]) or extract_product_uuid(detail_data)
                    variants = extract_size_variants(detail_lookup.pages[-1])

                if not variants or not product_uuid:
                    detail_source = "search_product"
                    for country in ("US", "HK"):
                        search = client.search_product(keyword=style_no, page=1, country=country)
                        probe[f"search_{country}_ok"] = search.ok
                        probe[f"search_{country}_error"] = search.error
                        if not search.ok or not search.pages:
                            continue
                        search_data = search.pages[-1]
                        search_product, search_uuid = _resolve_product_from_search(search_data, style_no)
                        if search_uuid and not product_uuid:
                            product_uuid = search_uuid
                        if search_product and not variants:
                            variants = extract_size_variants(search_product)
                        if product_uuid and not variants:
                            detail_lookup2 = client.product_detail(product_uuid=product_uuid)
                            probe["product_detail_ok"] = detail_lookup2.ok
                            probe["product_detail_error"] = detail_lookup2.error
                            if detail_lookup2.ok and detail_lookup2.pages:
                                variants = extract_size_variants(detail_lookup2.pages[-1])
                                if not product_uuid:
                                    product_uuid = extract_product_uuid(detail_lookup2.pages[-1])
                        if variants or product_uuid:
                            break

                probe["variant_count"] = len(variants)
                for variant in variants:
                    if normalize_us_size(variant.get("size")) == normalize_us_size(want_size):
                        target = variant
                        break
                probe["product_size_uuid"] = target.get("product_size_uuid") if target else None
                probe["lookup_source"] = detail_source
                if not target or not target.get("product_size_uuid"):
                    probe["error"] = "未找到对应尺码"
                    results.append(probe)
                    print(f"[LIVE_API_PROBE] {style_no} US {want_size} variant not found")
                    continue

                ask_lookup = client.product_size_ask_list(str(target["product_size_uuid"]))
                probe["ask_ok"] = ask_lookup.ok
                probe["ask_error"] = ask_lookup.error
                ask_rows: list[dict[str, Any]] = []
                for page in ask_lookup.pages:
                    ask_rows.extend(_extract_depth_rows(page, side="ask", forced_size=want_size))
                if not ask_rows:
                    market_lookup = client.product_size_market_info(str(target["product_size_uuid"]))
                    probe["market_ok"] = market_lookup.ok
                    probe["market_error"] = market_lookup.error
                    if market_lookup.ok and market_lookup.pages:
                        for page in market_lookup.pages:
                            ask_rows.extend(_extract_depth_rows(page, side="ask", forced_size=want_size))
                prices = sorted(float(row["price"]) for row in ask_rows if row.get("price") is not None)
                probe["live_lowest_ask"] = prices[0] if prices else None
                probe["live_top_levels"] = ask_rows[:5]
                results.append(probe)
                print(f"[LIVE_API_PROBE] {style_no} US {want_size} -> lowest={probe['live_lowest_ask']} rows={len(ask_rows)}")
            except Exception as exc:  # noqa: BLE001 - probe should never break the app.
                probe["error"] = str(exc)
                results.append(probe)
                print(f"[LIVE_API_PROBE] {style_no} US {want_size} crashed: {exc}")
    finally:
        conn.close()

    st.session_state["live_api_probe_results"] = results
    return results


def _refresh_cache_after_sync_if_needed() -> None:
    state = _sync_state_snapshot()
    job_id = state.get("job_id")
    if not job_id:
        return

    marker_key = "last_sync_refresh_job_id"
    if state.get("status") == "done" and st.session_state.get(marker_key) != job_id:
        bump_data_cache_version()
        st.session_state[marker_key] = job_id
        st.session_state["sync_notice"] = state.get("message") or "同步完成"
    elif state.get("status") == "error" and st.session_state.get(marker_key) != job_id:
        st.session_state[marker_key] = job_id
        st.session_state["sync_notice"] = state.get("message") or state.get("error") or "同步失败"


def _mark_completed_auto_hourly_job_if_needed(marker: dict[str, Any]) -> dict[str, Any]:
    active_job_id = marker.get("active_job_id")
    if not active_job_id:
        return marker
    state = _sync_state_snapshot()
    if state.get("job_id") != active_job_id or state.get("status") not in {"done", "error"}:
        return marker
    updated = dict(marker)
    updated["active_job_id"] = None
    updated["last_finished_at"] = datetime.utcnow().isoformat(timespec="seconds")
    updated["last_finished_ts"] = datetime.utcnow().timestamp()
    updated["last_status"] = state.get("status")
    updated["last_message"] = state.get("message") or state.get("error") or ""
    _write_auto_hourly_marker(updated)
    return updated


def _auto_hourly_full_sync_due(marker: dict[str, Any], interval_seconds: int) -> bool:
    started_ts = _timestamp_from_marker(marker.get("last_started_ts")) or _timestamp_from_marker(marker.get("last_started_at"))
    finished_ts = _timestamp_from_marker(marker.get("last_finished_ts")) or _timestamp_from_marker(marker.get("last_finished_at"))
    last_status = str(marker.get("last_status") or "")
    valid_finished_ts = finished_ts if finished_ts and (not started_ts or finished_ts >= started_ts) else None

    if last_status == "running":
        return False
    if valid_finished_ts is None:
        return started_ts is None
    return (datetime.utcnow().timestamp() - valid_finished_ts) >= interval_seconds


def _load_all_imported_styles_for_auto_sync(db_path: Path) -> list[str]:
    conn = connect(db_path)
    init_db(conn)
    try:
        _, _, styles, _ = latest_stockx_import_scope(conn)
        return [str(style).strip().upper() for style in styles if style]
    finally:
        conn.close()


def _run_stockx_full_sync_thread(source: str) -> None:
    try:
        if os.environ.get("RENDER") or os.environ.get("RENDER_SERVICE_ID"):
            os.environ["STOCKX_INLINE_STYLE_WORKER"] = "1"
        if _stockx_worker_should_run_incomplete_only(source):
            os.environ["STOCKX_SYNC_ONLY_INCOMPLETE"] = "1"
        else:
            os.environ.pop("STOCKX_SYNC_ONLY_INCOMPLETE", None)
        from scripts.auto_full_sync_worker import _run_full_sync

        _run_full_sync()
    except Exception as exc:  # noqa: BLE001
        marker = _read_auto_hourly_marker()
        error_message = f"StockX后台线程失败：{exc}"
        marker.update(
            {
                "last_status": "error",
                "last_error": error_message,
                "last_traceback": traceback.format_exc(limit=8),
                "last_finished_at": datetime.utcnow().isoformat(timespec="seconds"),
                "last_finished_ts": datetime.utcnow().timestamp(),
                "last_message": error_message,
                "last_worker_source": source,
            }
        )
        _write_auto_hourly_marker(marker)
        _write_sync_state_file(
            {
                "job_id": marker.get("active_job_id") or "",
                "status": "error",
                "message": error_message,
                "error": error_message,
                "traceback": marker.get("last_traceback"),
                "progress": 0.0,
                "completed": int(marker.get("completed") or 0),
                "total": int(marker.get("total") or 0),
                "current_phase": "worker线程失败",
                "current_style": marker.get("current_style"),
                "current_endpoint": None,
                "finished_at": datetime.utcnow().isoformat(timespec="seconds"),
            }
        )


def _stockx_worker_should_run_incomplete_only(source: str) -> bool:
    source_text = str(source or "").lower()
    return any(token in source_text for token in ("resume", "partial", "zero_score", "incomplete"))


def start_stockx_full_sync_worker_process(source: str = "manual_resume") -> dict[str, Any]:
    python_exe = Path(sys.executable)
    if not python_exe.exists():
        return {"started": False, "reason": "missing python executable"}
    if not (BASE_DIR / "scripts" / "auto_full_sync_worker.py").exists():
        return {"started": False, "reason": "missing auto_full_sync_worker.py"}
    marker = _read_auto_hourly_marker()
    now_ts = datetime.utcnow().timestamp()
    if str(marker.get("last_status") or "") == "running" and JOB_LOCK_PATH.exists():
        last_touch_ts = (
            _timestamp_from_marker(marker.get("last_progress_ts"))
            or _timestamp_from_marker(marker.get("last_progress_at"))
            or _timestamp_from_marker(marker.get("last_checkpoint_at"))
            or _timestamp_from_marker(marker.get("last_started_ts"))
            or _timestamp_from_marker(marker.get("last_started_at"))
        )
        if last_touch_ts and now_ts - float(last_touch_ts) >= 10 * 60:
            try:
                JOB_LOCK_PATH.unlink(missing_ok=True)
            except Exception:
                return {"started": False, "reason": "旧任务超过10分钟无进展，但旧锁释放失败"}
            marker.update(
                {
                    "last_status": "stale",
                    "last_message": "旧StockX后台任务超过10分钟无进展，已释放旧锁并准备重启。",
                    "last_error": "旧任务超过10分钟无进展",
                }
            )
            _write_auto_hourly_marker(marker)
        else:
            return {"started": True, "queued": True, "reason": "当前StockX任务已在运行"}
    elif JOB_LOCK_PATH.exists():
        state = _sync_state_snapshot()
        state_status = str(state.get("status") or "")
        state_touch_ts = (
            _timestamp_from_marker(state.get("updated_at"))
            or _timestamp_from_marker(state.get("last_progress_at"))
            or _timestamp_from_marker(state.get("started_at"))
        )
        lock_age = None
        try:
            lock_age = now_ts - JOB_LOCK_PATH.stat().st_mtime
        except OSError:
            lock_age = None
        state_is_fresh_running = (
            state_status == "running"
            and state_touch_ts is not None
            and now_ts - float(state_touch_ts) < 10 * 60
        )
        if state_is_fresh_running:
            return {"started": True, "queued": True, "reason": "当前StockX任务已在运行"}
        try:
            JOB_LOCK_PATH.unlink(missing_ok=True)
            marker.update(
                {
                    "last_status": "stale",
                    "last_message": "发现StockX旧锁但没有有效运行状态，已释放旧锁并准备重启。",
                    "last_error": "发现孤儿sync_job.lock",
                    "last_lock_age_seconds": lock_age,
                }
            )
            _write_auto_hourly_marker(marker)
        except Exception as exc:  # noqa: BLE001
            return {"started": False, "reason": f"旧锁释放失败：{exc}"}
    now = datetime.utcnow()
    existing_completed = int(marker.get("completed") or 0)
    existing_recomputed = int(marker.get("recomputed") or 0)
    marker.update(
        {
            "enabled": True,
            "last_status": "starting",
            "last_started_at": now.isoformat(timespec="seconds"),
            "last_started_ts": now.timestamp(),
            "last_finished_at": None,
            "last_finished_ts": None,
            "last_error": None,
            "last_traceback": None,
            "completed": existing_completed,
            "recomputed": existing_recomputed,
            "run_scope": "incomplete" if _stockx_worker_should_run_incomplete_only(source) else "all",
            "current_style": None,
            "last_message": f"StockX后台worker启动中：{source}",
        }
    )
    _write_auto_hourly_marker(marker)
    if os.environ.get("RENDER") or os.environ.get("RENDER_SERVICE_ID"):
        try:
            thread = threading.Thread(
                target=_run_stockx_full_sync_thread,
                args=(source,),
                name=f"stockx-full-sync-{source}",
                daemon=True,
            )
            thread.start()
            return {"started": True, "started_at": now.isoformat(timespec="seconds"), "mode": "thread"}
        except Exception as exc:  # noqa: BLE001
            marker.update(
                {
                    "last_status": "error",
                    "last_error": str(exc),
                    "last_finished_at": datetime.utcnow().isoformat(timespec="seconds"),
                    "last_finished_ts": datetime.utcnow().timestamp(),
                    "last_message": f"StockX后台线程启动失败：{exc}",
                }
            )
            _write_auto_hourly_marker(marker)
            return {"started": False, "reason": str(exc)}
    try:
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        log_dir = BASE_DIR / "data" / "worker_logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"stockx_full_sync_{now.strftime('%Y%m%d%H%M%S')}_{job_id if 'job_id' in locals() else source}.log"
        log_handle = log_path.open("a", encoding="utf-8", errors="replace")
        worker_env = os.environ.copy()
        worker_env["PYTHONUNBUFFERED"] = "1"
        if _stockx_worker_should_run_incomplete_only(source):
            worker_env["STOCKX_SYNC_ONLY_INCOMPLETE"] = "1"
        else:
            worker_env.pop("STOCKX_SYNC_ONLY_INCOMPLETE", None)
        proc = subprocess.Popen(
            [
                str(python_exe),
                "-c",
                "from scripts.auto_full_sync_worker import _run_full_sync; _run_full_sync()",
            ],
            cwd=str(BASE_DIR),
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            creationflags=creationflags,
            env=worker_env,
        )
        time_module.sleep(2)
        exit_code = proc.poll()
        if exit_code is not None:
            try:
                log_handle.close()
            except Exception:
                pass
            log_tail = _read_text_tail(log_path)
            error_message = f"StockX后台worker启动后立即退出，退出码 {exit_code}"
            marker.update(
                {
                    "last_status": "error",
                    "last_error": error_message,
                    "last_traceback": log_tail,
                    "last_finished_at": datetime.utcnow().isoformat(timespec="seconds"),
                    "last_finished_ts": datetime.utcnow().timestamp(),
                    "last_message": error_message,
                    "completed": int(marker.get("completed") or 0),
                    "recomputed": int(marker.get("recomputed") or 0),
                }
            )
            _write_auto_hourly_marker(marker)
            _write_sync_state_file(
                {
                    "job_id": marker.get("active_job_id") or "",
                    "status": "error",
                    "message": error_message,
                    "error": error_message,
                    "traceback": log_tail,
                    "progress": 0.0,
                    "completed": 0,
                    "total": 0,
                    "current_phase": "worker启动失败",
                    "current_style": None,
                    "current_endpoint": None,
                    "finished_at": datetime.utcnow().isoformat(timespec="seconds"),
                }
            )
            return {"started": False, "reason": error_message, "log_tail": log_tail}
        log_handle.close()
        return {"started": True, "started_at": now.isoformat(timespec="seconds")}
    except Exception as exc:  # noqa: BLE001
        marker.update(
            {
                "last_status": "error",
                "last_error": str(exc),
                "last_finished_at": datetime.utcnow().isoformat(timespec="seconds"),
                "last_message": f"StockX后台worker启动失败：{exc}",
            }
        )
        _write_auto_hourly_marker(marker)
        return {"started": False, "reason": str(exc)}


def _auto_hourly_full_sync_loop() -> None:
    while True:
        try:
            settings = get_settings()
            interval_seconds = max(
                AUTO_HOURLY_SYNC_MIN_INTERVAL_SECONDS,
                int(settings.auto_full_sync_interval_minutes or 60) * 60,
            )
            marker = _mark_completed_auto_hourly_job_if_needed(_read_auto_hourly_marker())
            if settings.auto_full_sync_enabled and settings.credentials_ready:
                state = _sync_state_snapshot()
                imported_styles = _load_all_imported_styles_for_auto_sync(settings.db_path)
                if imported_styles:
                    conn = connect(settings.db_path)
                    init_db(conn)
                    try:
                        active_import_id, _, _, imported_scope_sql = latest_stockx_import_scope(conn)
                        score_count = (
                            conn.execute(
                                f"SELECT COUNT(*) FROM opportunity_scores WHERE style_no IN ({imported_scope_sql})"
                            ).fetchone()[0]
                            or 0
                        )
                        active_import_style_set = set(imported_styles)
                        incomplete_count = len(
                            [style for style in load_incomplete_stockx_skus(conn) if style in active_import_style_set]
                        )
                    finally:
                        conn.close()
                    partial_resume_key = f"{active_import_id}:{score_count}:{incomplete_count}"
                    last_partial_key = str(marker.get("last_partial_resume_key") or "")
                    last_partial_ts = _timestamp_from_marker(marker.get("last_partial_resume_ts"))
                    partial_retry_due = (
                        not last_partial_ts
                        or datetime.utcnow().timestamp() - last_partial_ts >= JOB_LOCK_STALE_SECONDS
                    )
                    state_blocks_resume = state.get("status") == "running"
                    resume_is_due = (
                        score_count > 0
                        and incomplete_count > 0
                        and (partial_resume_key != last_partial_key or partial_retry_due)
                    )
                    if state_blocks_resume and resume_is_due and partial_retry_due:
                        stale_state = dict(state)
                        stale_state.update(
                            {
                                "status": "error",
                                "error": "后台任务超过10分钟无评分进展，已释放旧运行状态并准备重启。",
                                "message": "后台任务超过10分钟无评分进展，已释放旧运行状态并准备重启。",
                                "finished_at": datetime.utcnow().isoformat(timespec="seconds"),
                                "updated_at": datetime.utcnow().isoformat(timespec="seconds"),
                            }
                        )
                        _write_sync_state_file(stale_state)
                        try:
                            JOB_LOCK_PATH.unlink(missing_ok=True)
                        except Exception:
                            pass
                        state_blocks_resume = False
                    if (
                        resume_is_due
                        and not state_blocks_resume
                    ):
                        if partial_resume_key == last_partial_key and partial_retry_due and JOB_LOCK_PATH.exists():
                            try:
                                JOB_LOCK_PATH.unlink(missing_ok=True)
                            except Exception:
                                pass
                        worker = start_stockx_full_sync_worker_process("scheduler_partial_auto_resume")
                        now = datetime.utcnow()
                        updated = _read_auto_hourly_marker()
                        updated.update(
                            {
                                "enabled": True,
                                "interval_minutes": int(settings.auto_full_sync_interval_minutes or 60),
                                "last_checked_at": now.isoformat(timespec="seconds"),
                                "last_checked_ts": now.timestamp(),
                                "last_style_count": len(imported_styles),
                            }
                        )
                        if worker.get("started"):
                            updated.update(
                                {
                                    "last_partial_resume_key": partial_resume_key,
                                    "last_partial_resume_at": now.isoformat(timespec="seconds"),
                                    "last_partial_resume_ts": now.timestamp(),
                                }
                            )
                            updated["last_message"] = (
                                f"检测到当前批次还有 {incomplete_count} 个未完成货号，"
                                f"已自动启动后台worker继续补跑：{len(imported_styles)} 个货号"
                            )
                        else:
                            updated.pop("last_partial_resume_key", None)
                            updated.pop("last_partial_resume_at", None)
                            updated.pop("last_partial_resume_ts", None)
                            updated["last_message"] = f"检测到未完成货号，但worker未启动：{worker.get('reason') or '-'}"
                        _write_auto_hourly_marker(updated)
                        time_module.sleep(AUTO_HOURLY_SYNC_POLL_SECONDS)
                        continue
                if (
                    state.get("status") != "running"
                    and _auto_hourly_full_sync_due(marker, interval_seconds)
                ):
                    if imported_styles:
                        worker = start_stockx_full_sync_worker_process("auto_hourly")
                        now = datetime.utcnow()
                        updated = _read_auto_hourly_marker()
                        updated.update(
                            {
                                "enabled": True,
                                "interval_minutes": int(settings.auto_full_sync_interval_minutes or 60),
                                "last_checked_at": now.isoformat(timespec="seconds"),
                                "last_checked_ts": now.timestamp(),
                                "last_style_count": len(imported_styles),
                            }
                        )
                        if worker.get("started"):
                            updated["last_message"] = f"今日机会全量刷新StockX API worker已启动：{len(imported_styles)} 个货号"
                        else:
                            updated["last_message"] = f"自动全量同步到点，但worker未启动：{worker.get('reason') or '-'}"
                        _write_auto_hourly_marker(updated)
            else:
                marker = dict(marker)
                marker.update(
                    {
                        "enabled": bool(settings.auto_full_sync_enabled),
                        "last_message": "自动全量同步未开启或凭证未填写",
                    }
                )
                _write_auto_hourly_marker(marker)
        except Exception as exc:  # noqa: BLE001 - background scheduler must not kill the app.
            marker = _read_auto_hourly_marker()
            marker.update(
                {
                    "last_error_at": datetime.utcnow().isoformat(timespec="seconds"),
                    "last_error": str(exc),
                }
            )
            try:
                _write_auto_hourly_marker(marker)
            except Exception:
                pass
        time_module.sleep(AUTO_HOURLY_SYNC_POLL_SECONDS)


@st.cache_resource(show_spinner=False)
def ensure_auto_hourly_full_sync_scheduler() -> dict[str, Any]:
    thread = threading.Thread(target=_auto_hourly_full_sync_loop, name="auto-hourly-full-sync", daemon=True)
    thread.start()
    return {"started_at": datetime.utcnow().isoformat(timespec="seconds"), "thread_name": thread.name}



def _run_sync_job(
    job_id: str,
    db_path_str: str,
    selected_skus: list[str],
    *,
    include_sales: bool,
    include_depth: bool,
    include_size_endpoints: bool,
    parallel_sync: bool,
    max_workers: int,
    fee_rate: float,
    sales_fraction: float,
    target_size: str | None = None,
    reset_snapshot: bool = False,
) -> None:
    main_conn = connect(Path(db_path_str))
    init_db(main_conn)
    try:
        title_rows = query_rows(
            main_conn,
            """
            SELECT style_no, MAX(title_hint) AS title_hint
            FROM sku_items
            GROUP BY style_no
            """,
        )
        title_map = {str(row["style_no"]): str(row["title_hint"]) if row["title_hint"] else None for row in title_rows}
        total = len(selected_skus)
        _update_sync_state(
            job_id=job_id,
            status="running",
            message=f"正在同步 {total} 个货号",
            progress=0.0,
            completed=0,
            total=total,
            current_style=None,
            current_size=None,
            current_endpoint=None,
            current_phase="同步接口",
            score_completed=0,
            score_total=0,
            recent_events=[],
            summaries=[],
            error=None,
            recomputed=0,
            started_at=datetime.utcnow().isoformat(timespec="seconds"),
            finished_at=None,
        )

        summaries: list[SyncSummary | None] = [None] * total
        completed = 0
        recomputed = 0
        last_checkpoint_completed = 0
        last_checkpoint_ts = 0.0

        def score_count() -> int:
            try:
                return int(main_conn.execute("SELECT COUNT(*) FROM opportunity_scores").fetchone()[0] or 0)
            except Exception:
                return 0

        def checkpoint_scores(force: bool = False) -> None:
            nonlocal last_checkpoint_completed, last_checkpoint_ts
            now_ts = time_module.monotonic()
            should_checkpoint = (
                force
                or last_checkpoint_completed == 0
                or completed - last_checkpoint_completed >= SYNC_CHECKPOINT_STYLE_INTERVAL
                or now_ts - last_checkpoint_ts >= SYNC_CHECKPOINT_MIN_SECONDS
            )
            if not should_checkpoint:
                return
            scores = score_count()
            if scores <= 0:
                return
            _update_sync_state(
                completed=completed,
                total=total,
                current_phase="云端检查点",
                current_endpoint="firebase_checkpoint",
                progress=completed / total if total else 1.0,
                message=f"已保存云端检查点：{completed}/{total}，评分 {scores} 条",
                recomputed=recomputed,
            )
            try:
                backup_core_tables_to_firestore(db_path_str, reason=f"manual_sync_checkpoint:{completed}/{total}")
                last_checkpoint_completed = completed
                last_checkpoint_ts = now_ts
            except Exception as exc:  # noqa: BLE001
                _append_sync_event(
                    {
                        "phase": "云端检查点",
                        "status": "error",
                        "message": f"检查点保存失败：{exc}",
                    }
                )

        def recompute_completed_batch(batch_styles: list[str]) -> bool:
            nonlocal recomputed
            if not batch_styles:
                return True
            pending_styles = batch_styles[:]
            try:
                batch_recomputed = compute_and_store_opportunities(
                    main_conn,
                    fee_rate=fee_rate,
                    sales_fraction=sales_fraction,
                    progress_callback=_sync_progress_callback,
                    style_nos=pending_styles,
                )
            except sqlite3.OperationalError as exc:
                if "locked" in str(exc).lower():
                    main_conn.rollback()
                    return False
                raise
            recomputed += batch_recomputed
            main_conn.commit()
            batch_styles.clear()
            _update_sync_state(
                completed=completed,
                total=total,
                current_phase="增量评分",
                current_style=pending_styles[-1],
                current_size=None,
                current_endpoint="score_opportunity",
                progress=completed / total if total else 1.0,
                message=f"已同步 {completed}/{total}，已增量重算 {recomputed} 个尺码",
                recomputed=recomputed,
            )
            checkpoint_scores()
            return True

        if parallel_sync and total > 1:
            worker_count = min(max_workers, total)
            completed_styles: list[str] = []
            next_index = 0
            future_map: dict[Any, dict[str, Any]] = {}
            executor = ThreadPoolExecutor(max_workers=worker_count)

            def submit_next() -> None:
                nonlocal next_index
                if next_index >= total:
                    return
                index = next_index
                next_index += 1
                style_no = selected_skus[index]
                future = executor.submit(
                    sync_style_isolated,
                    style_no,
                    title_hint=title_map.get(style_no),
                    include_sales=include_sales,
                    include_depth=include_depth,
                    include_size_endpoints=include_size_endpoints,
                    target_size=target_size if total == 1 else None,
                    reset_snapshot=reset_snapshot,
                    progress_callback=_sync_progress_callback,
                )
                future_map[future] = {"index": index, "style_no": style_no, "started": time_module.monotonic()}
                _update_sync_state(
                    completed=completed,
                    total=total,
                    current_style=style_no,
                    current_phase="同步接口",
                    current_endpoint="准备请求",
                    progress=completed / total if total else 1.0,
                    message=f"已派发 {next_index}/{total}，并发 {worker_count}",
                    recomputed=recomputed,
                )

            try:
                for _ in range(worker_count):
                    submit_next()

                while future_map:
                    done, _ = wait(set(future_map), timeout=5, return_when=FIRST_COMPLETED)
                    timed_out = [
                        future
                        for future, meta in list(future_map.items())
                        if time_module.monotonic() - float(meta["started"]) >= STYLE_SYNC_HARD_TIMEOUT_SECONDS
                    ]
                    for future in timed_out:
                        meta = future_map.pop(future, None)
                        if not meta:
                            continue
                        index = int(meta["index"])
                        style_no = str(meta["style_no"])
                        future.cancel()
                        summaries[index] = SyncSummary(
                            style_no=style_no,
                            product_id=None,
                            sizes=[],
                            errors=[f"单货号超过 {STYLE_SYNC_HARD_TIMEOUT_SECONDS} 秒，已跳过继续跑后面货号"],
                        )
                        completed += 1
                        _append_sync_event(
                            {
                                "phase": "同步接口",
                                "style_no": style_no,
                                "status": "timeout",
                                "message": f"超过 {STYLE_SYNC_HARD_TIMEOUT_SECONDS} 秒未完成，已跳过",
                            }
                        )
                        _update_sync_state(
                            completed=completed,
                            total=total,
                            current_style=style_no,
                            current_phase="同步接口",
                            current_endpoint="timeout_skip",
                            progress=completed / total if total else 1.0,
                            message=f"{style_no} 超时跳过；继续同步 {completed}/{total}",
                            recomputed=recomputed,
                        )
                        submit_next()

                    for future in done:
                        meta = future_map.pop(future, None)
                        if not meta:
                            continue
                        index = int(meta["index"])
                        style_no = str(meta["style_no"])
                        completed_styles.append(style_no)
                        try:
                            summaries[index] = future.result()
                        except Exception as exc:  # noqa: BLE001
                            summaries[index] = SyncSummary(style_no=style_no, product_id=None, sizes=[], errors=[str(exc)])
                            _append_sync_event(
                                {"phase": "同步接口", "style_no": style_no, "status": "error", "message": str(exc)}
                            )
                        completed += 1
                        _update_sync_state(
                            completed=completed,
                            total=total,
                            current_style=style_no,
                            progress=completed / total if total else 1.0,
                            message=f"已同步 {completed}/{total}，等待本批评分",
                            recomputed=recomputed,
                        )
                        if len(completed_styles) >= SYNC_SCORE_BATCH_SIZE:
                            recompute_completed_batch(completed_styles)
                        submit_next()
                recompute_completed_batch(completed_styles)
            finally:
                executor.shutdown(wait=False, cancel_futures=True)
        else:
            completed_styles: list[str] = []
            for index, style_no in enumerate(selected_skus, start=1):
                _update_sync_state(
                    current_phase="同步接口",
                    current_style=style_no,
                    current_size=None,
                    current_endpoint="准备请求",
                    message=f"正在同步 {index}/{total}: {style_no}",
                )
                try:
                    summaries[index - 1] = sync_style_isolated(
                        style_no,
                        title_hint=title_map.get(style_no),
                        include_sales=include_sales,
                        include_depth=include_depth,
                        include_size_endpoints=include_size_endpoints,
                        target_size=target_size if total == 1 else None,
                        reset_snapshot=reset_snapshot,
                        progress_callback=_sync_progress_callback,
                    )
                except Exception as exc:  # noqa: BLE001
                    summaries[index - 1] = SyncSummary(style_no=style_no, product_id=None, sizes=[], errors=[str(exc)])
                    _append_sync_event(
                        {"phase": "同步接口", "style_no": style_no, "status": "error", "message": str(exc)}
                    )
                completed = index
                completed_styles.append(style_no)
                _update_sync_state(
                    completed=index,
                    total=total,
                    current_style=style_no,
                    progress=index / total if total else 1.0,
                    message=f"已同步 {index}/{total}，等待本批评分",
                    recomputed=recomputed,
                )
                if len(completed_styles) >= SYNC_SCORE_BATCH_SIZE or index == total:
                    recompute_completed_batch(completed_styles)

        _update_sync_state(
            status="done",
            progress=1.0,
            completed=total,
            current_style=None,
            current_size=None,
            current_endpoint=None,
            current_phase="完成",
            message=f"同步完成，并重新计算 {recomputed} 个尺码机会",
            summaries=[summary.__dict__ for summary in summaries if summary is not None],
            recomputed=recomputed,
            finished_at=datetime.utcnow().isoformat(timespec="seconds"),
        )
        checkpoint_scores(force=True)
        schedule_cloud_backup("stockx_sync_done")
    except Exception as exc:  # noqa: BLE001
        log_sync(
            main_conn,
            "后台同步失败",
            severity="error",
            event_type="sync_error",
            details={"job_id": job_id, "error": str(exc)},
        )
        main_conn.commit()
        _update_sync_state(
            status="error",
            error=str(exc),
            message=f"同步失败：{exc}",
            current_phase="失败",
            finished_at=datetime.utcnow().isoformat(timespec="seconds"),
        )
    finally:
        main_conn.close()
        _release_job_file_lock(job_id)


def start_sync_job(
    selected_skus: list[str],
    *,
    db_path_str: str,
    include_sales: bool,
    include_depth: bool,
    include_size_endpoints: bool,
    parallel_sync: bool,
    max_workers: int,
    fee_rate: float,
    sales_fraction: float,
    row_refresh_style: str | None = None,
    row_refresh_size: str | None = None,
    reset_snapshot: bool = False,
) -> str | None:
    if _sync_state_snapshot().get("status") == "running":
        return None
    cleaned = [str(item).strip().upper() for item in selected_skus if str(item).strip()]
    if not cleaned:
        return None

    job_id = uuid.uuid4().hex[:8]
    total = len(cleaned)
    if not _acquire_job_file_lock(job_id, "sync", {"total": total}):
        return None
    _update_sync_state(
        job_id=job_id,
        status="running",
        message=f"正在启动同步：{total} 个货号",
        progress=0.0,
        completed=0,
        total=total,
        current_style=None,
        current_size=None,
        current_endpoint="准备启动",
        current_phase="同步接口",
        score_completed=0,
        score_total=0,
        recent_events=[],
        summaries=[],
        error=None,
        recomputed=0,
        row_refresh_style=row_refresh_style,
        row_refresh_size=row_refresh_size,
        started_at=datetime.utcnow().isoformat(timespec="seconds"),
        finished_at=None,
    )
    thread = threading.Thread(
        target=_run_sync_job,
        args=(job_id, db_path_str, cleaned),
        kwargs={
            "include_sales": include_sales,
            "include_depth": include_depth,
            "include_size_endpoints": include_size_endpoints,
            "parallel_sync": parallel_sync,
            "max_workers": max_workers,
            "fee_rate": fee_rate,
            "sales_fraction": sales_fraction,
            "target_size": row_refresh_size,
            "reset_snapshot": reset_snapshot,
        },
        daemon=True,
    )
    thread.start()
    return job_id

def _path_display(value: Path | str) -> str:
    path = Path(value)
    try:
        return str(path.relative_to(BASE_DIR))
    except ValueError:
        return str(path)


def _save_uploaded_file_copy(uploaded: Any, directory: Path, *, prefix: str) -> tuple[bytes, Path]:
    content = uploaded.getvalue()
    directory.mkdir(parents=True, exist_ok=True)
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", Path(str(uploaded.name or "upload")).name).strip("._")
    if not safe_name:
        safe_name = "upload"
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    target = directory / f"{timestamp}_{prefix}_{safe_name}"
    if target.exists():
        target = directory / f"{timestamp}_{prefix}_{uuid.uuid4().hex[:6]}_{safe_name}"
    target.write_bytes(content)
    return content, target


def archive_current_opportunity_snapshot(conn, *, note: str = "new_import") -> dict[str, Any]:
    import_id, row_count, styles, scope_sql = latest_stockx_import_scope(conn)
    if not import_id:
        return {"archived": False, "reason": "no_import"}
    import_row = conn.execute(
        "SELECT source_name, file_name FROM sku_imports WHERE id = ?",
        (import_id,),
    ).fetchone()
    if not import_row:
        return {"archived": False, "reason": "missing_import"}
    score_count = int(conn.execute(f"SELECT COUNT(*) FROM opportunity_scores WHERE style_no IN ({scope_sql})").fetchone()[0] or 0)
    if score_count <= 0:
        return {"archived": False, "reason": "no_scores", "import_id": import_id}
    archived_at = datetime.utcnow().isoformat(timespec="seconds")
    cur = conn.execute(
        """
        INSERT INTO opportunity_import_snapshots (
            import_id, source_name, file_name, row_count, style_count, score_count, archived_at, note
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            import_id,
            import_row["source_name"],
            import_row["file_name"],
            int(row_count),
            len(styles),
            score_count,
            archived_at,
            note,
        ),
    )
    snapshot_id = int(cur.lastrowid)
    conn.execute(
        f"""
        INSERT INTO opportunity_score_history (
            snapshot_id, original_score_id, product_id, style_no, title, brand, size, score, rating,
            recommended_buy_qty, max_buy_price, weighted_avg_cost, next_lowest_ask,
            target_sell_price_low, target_sell_price_high, estimated_profit, estimated_profit_per_pair,
            estimated_days_to_sell, release_date, release_days, risk_notes, components_json,
            computed_at, archived_at
        )
        SELECT
            ?, id, product_id, style_no, title, brand, size, score, rating,
            recommended_buy_qty, max_buy_price, weighted_avg_cost, next_lowest_ask,
            target_sell_price_low, target_sell_price_high, estimated_profit, estimated_profit_per_pair,
            estimated_days_to_sell, release_date, release_days, risk_notes, components_json,
            computed_at, ?
        FROM opportunity_scores
        WHERE style_no IN ({scope_sql})
        """,
        (snapshot_id, archived_at),
    )
    return {
        "archived": True,
        "snapshot_id": snapshot_id,
        "import_id": import_id,
        "score_count": score_count,
        "style_count": len(styles),
    }


def render_sku_upload_panel(conn, *, key_prefix: str, default_source: str = "manual") -> None:
    upload_cols = st.columns([1.8, 1.05, 0.95])
    uploaded = upload_cols[0].file_uploader(
        "上传货号清单（Excel / CSV / ZIP）",
        type=["xlsx", "xls", "csv", "zip"],
        key=f"{key_prefix}_sku_upload",
    )
    default_label = "手动上传" if default_source == "manual" else default_source
    source_name = upload_cols[1].text_input(
        "来源 / 批次名（可不填）",
        value=default_label,
        help="只是给这次上传起一个名字，方便以后在历史结果里找回。例如：StockX Top1000、GOAT热销榜、手动上传。",
        key=f"{key_prefix}_sku_source",
    )
    if uploaded is not None and upload_cols[2].button("导入货号清单", type="primary", use_container_width=True, key=f"{key_prefix}_sku_import"):
        try:
            archived = archive_current_opportunity_snapshot(conn, note=f"before_import:{uploaded.name}")
            content, saved_path = _save_uploaded_file_copy(uploaded, SKU_UPLOAD_DIR, prefix="sku")
            stored_source = "manual" if source_name in ("", "手动上传") else source_name
            result = import_sku_file(conn, file_name=uploaded.name, content=content, source_name=stored_source)
            conn.commit()
            bump_data_cache_version()
            schedule_cloud_backup("sku_import")
            archived_text = (
                f"；上一个清单已归档快照 {archived.get('score_count')} 行"
                if archived.get("archived")
                else ""
            )
            st.session_state["sync_notice"] = (
                f"导入完成：识别 {result.rows_imported}/{result.rows_seen} 行；"
                f"sheet：{', '.join(result.sheet_names)}；源文件已保存到 {_path_display(saved_path)}{archived_text}。"
            )
            st.rerun()
        except Exception as exc:
            conn.rollback()
            log_sync(
                conn,
                f"货号清单导入失败：{exc}",
                event_type="sku_import_error",
                severity="error",
                details={"file_name": getattr(uploaded, "name", ""), "source_name": source_name or "manual"},
            )
            conn.commit()
            st.error(f"导入失败：{exc}")
            st.info("请确认表格里有货号/款号/SKU/style/style number/product sku 等字段，或其中一列主要内容就是货号。")

    recent_uploads = sorted(SKU_UPLOAD_DIR.glob("*"), key=lambda p: p.stat().st_mtime, reverse=True)[:5] if SKU_UPLOAD_DIR.exists() else []
    if recent_uploads:
        st.caption("最近保存的上传源文件：" + "；".join(_path_display(path) for path in recent_uploads))


def _save_env_file(values: dict[str, Any]) -> None:
    existing_lines = ENV_PATH.read_text(encoding="utf-8").splitlines() if ENV_PATH.exists() else []
    updated = {key: "" if values.get(key) is None else str(values.get(key)).strip() for key in ENV_KEYS}
    output: list[str] = []
    seen: set[str] = set()

    for line in existing_lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            output.append(line)
            continue
        key, _, _ = line.partition("=")
        key = key.strip()
        if key in updated:
            output.append(f"{key}={updated[key]}")
            seen.add(key)
        else:
            output.append(line)

    for key in ENV_KEYS:
        if key not in seen:
            output.append(f"{key}={updated[key]}")

    text = "\n".join(output).rstrip() + "\n"
    ENV_PATH.write_text(text, encoding="utf-8")


def db_signature(db_path: Path | str) -> int:
    path = Path(db_path)
    try:
        return hash((data_cache_version(), path.stat().st_mtime_ns))
    except OSError:
        return data_cache_version()


@st.cache_data(show_spinner=False)
def load_rows_cached(
    db_path_str: str,
    signature: tuple[int, int],
    sql: str,
    params: tuple[Any, ...] = (),
) -> list[dict[str, Any]]:
    conn = connect(Path(db_path_str))
    try:
        return [dict(row) for row in query_rows(conn, sql, params)]
    finally:
        conn.close()


@st.cache_data(show_spinner=False)
def load_skus_cached(db_path_str: str, signature: tuple[int, int]) -> list[str]:
    rows = load_rows_cached(
        db_path_str,
        signature,
        """
        SELECT DISTINCT style_no
        FROM sku_items
        WHERE style_no IS NOT NULL AND TRIM(style_no) != ''
        ORDER BY style_no
        """,
    )
    return [str(row["style_no"]) for row in rows]


def latest_stockx_import_scope(conn) -> tuple[int | None, int, list[str], str]:
    row = conn.execute(
        """
        SELECT id, file_name
        FROM sku_imports
        WHERE source_name IN ('stockx_top1000', 'manual')
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return None, 0, [], "SELECT DISTINCT style_no FROM sku_items WHERE 1=0"
    import_id = int(row["id"])
    row_count = conn.execute("SELECT COUNT(*) FROM sku_items WHERE import_id = ?", (import_id,)).fetchone()[0] or 0
    style_rows = query_rows(
        conn,
        """
        SELECT DISTINCT style_no
        FROM sku_items
        WHERE import_id = ?
          AND style_no IS NOT NULL
          AND TRIM(style_no) != ''
        ORDER BY style_no
        """,
        (import_id,),
    )
    styles = [str(item["style_no"]) for item in style_rows]
    scope_sql = (
        "SELECT DISTINCT style_no FROM sku_items "
        f"WHERE import_id = {import_id} AND style_no IS NOT NULL AND TRIM(style_no) != ''"
    )
    return import_id, int(row_count), styles, scope_sql


def render_opportunity_snapshot_history(conn, settings) -> None:
    with st.expander("历史源文件结果快照（只读，不参与自动刷新）", expanded=False):
        snapshots = query_rows(
            conn,
            """
            SELECT id, import_id, source_name, file_name, row_count, style_count, score_count, archived_at, note
            FROM opportunity_import_snapshots
            ORDER BY id DESC
            LIMIT 50
            """,
        )
        cols = st.columns([1, 3])
        if cols[0].button("保存当前结果为历史快照", use_container_width=True, key="save_current_opp_snapshot"):
            archived = archive_current_opportunity_snapshot(conn, note="manual_snapshot")
            conn.commit()
            bump_data_cache_version()
            schedule_cloud_backup("opportunity_snapshot")
            if archived.get("archived"):
                st.success(f"已保存当前快照：{archived.get('score_count')} 行。")
            else:
                st.info(f"当前没有可归档结果：{archived.get('reason')}")
            st.rerun()
        cols[1].caption("历史快照固定为当时保存的结果；不会被每小时接口刷新修改。")
        if not snapshots:
            st.caption("暂无历史快照。上传新清单前会自动归档当前结果，也可以点上面的按钮手动保存。")
            return
        labels = [
            f"#{row['id']}｜{row['file_name'] or '-'}｜{row['score_count']} 行｜{_format_datetime_minute(row['archived_at'])}"
            for row in snapshots
        ]
        selected = st.selectbox("选择历史源文件结果", labels, key="opportunity_snapshot_select")
        snapshot_id = int(snapshots[labels.index(selected)]["id"])
        rows = query_rows(
            conn,
            """
            SELECT
                rating AS 评级,
                ROUND(score, 1) AS 分数,
                style_no AS 货号,
                size AS 尺码,
                title AS 商品名,
                recommended_buy_qty AS 买入双数,
                max_buy_price AS 最高买价,
                weighted_avg_cost AS 加权均价,
                target_sell_price_low AS 建议卖价低位,
                target_sell_price_high AS 建议卖价高位,
                estimated_profit AS 总利润,
                estimated_profit_per_pair AS 每双利润,
                estimated_days_to_sell AS 预计卖完天数,
                release_date AS 发售日期,
                release_days AS 发售天数,
                risk_notes AS 风险说明,
                computed_at AS 当时计算时间
            FROM opportunity_score_history
            WHERE snapshot_id = ?
            ORDER BY
                CASE WHEN estimated_profit IS NULL THEN 1 ELSE 0 END,
                estimated_profit DESC,
                score DESC
            LIMIT 2000
            """,
            (snapshot_id,),
        )
        frame = pd.DataFrame([dict(row) for row in rows])
        if frame.empty:
            st.info("这个历史快照没有结果行。")
            return
        dcols = st.columns([1.2, 3])
        dcols[0].download_button(
            "导出历史快照 CSV",
            data=_csv_download_bytes(frame),
            file_name=_export_file_name(f"opportunity_snapshot_{snapshot_id}"),
            mime="text/csv",
            use_container_width=True,
        )
        dcols[1].caption(f"当前预览 {len(frame)} 行；历史快照不会继续跑接口。")
        st.dataframe(frame, use_container_width=True, height=420, hide_index=True)


def load_incomplete_stockx_skus(conn) -> list[str]:
    active_import = conn.execute(
        """
        SELECT id, imported_at
        FROM sku_imports
        WHERE source_name IN ('stockx_top1000', 'manual')
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    active_import_id = int(active_import["id"]) if active_import else None
    rows = query_rows(
        conn,
        """
        WITH imported AS (
            SELECT
                si.style_no,
                MIN(COALESCE(si.rank, 999999)) AS import_rank
            FROM sku_items si
            WHERE (? IS NULL OR si.import_id = ?)
              AND si.style_no IS NOT NULL
              AND TRIM(si.style_no) != ''
            GROUP BY si.style_no
        )
        SELECT i.style_no
        FROM imported i
        WHERE (
               NOT EXISTS (SELECT 1 FROM products p WHERE p.style_no = i.style_no)
            OR NOT EXISTS (SELECT 1 FROM product_sizes ps WHERE ps.style_no = i.style_no)
            OR NOT EXISTS (SELECT 1 FROM ask_depth a WHERE a.style_no = i.style_no)
            OR NOT EXISTS (SELECT 1 FROM sales_history s WHERE s.style_no = i.style_no)
            OR NOT EXISTS (SELECT 1 FROM opportunity_scores o WHERE o.style_no = i.style_no)
          )
        ORDER BY i.import_rank, i.style_no
        """,
        (active_import_id, active_import_id),
    )
    return [str(row["style_no"]) for row in rows]


@st.cache_data(show_spinner=False)
def load_import_summary_cached(db_path_str: str, signature: int) -> list[dict[str, Any]]:
    conn = connect(Path(db_path_str))
    try:
        rows = query_rows(
            conn,
            """
            SELECT
                style_no,
                MIN(rank) AS rank,
                MAX(title_hint) AS title_hint,
                COUNT(*) AS import_count
            FROM sku_items
            GROUP BY style_no
            ORDER BY COALESCE(rank, 999999), style_no
            """,
        )
        return [dict(row) for row in rows]
    finally:
        conn.close()


@st.cache_data(show_spinner=False)
def load_imported_coverage_cached(db_path_str: str, signature: int, import_id: int | None = None) -> list[dict[str, Any]]:
    conn = connect(Path(db_path_str))
    try:
        where_sql = "WHERE s.import_id = ?" if import_id is not None else ""
        params: tuple[Any, ...] = (import_id,) if import_id is not None else tuple()
        rows = query_rows(
            conn,
            f"""
            SELECT
                s.style_no,
                MIN(s.rank) AS rank,
                MAX(s.title_hint) AS title_hint,
                COUNT(*) AS import_rows,
                MAX(CASE WHEN p.style_no IS NOT NULL THEN 1 ELSE 0 END) AS has_product_data,
                COUNT(DISTINCT o.size) AS scored_sizes,
                MAX(o.score) AS best_score,
                MAX(o.computed_at) AS last_computed_at
            FROM sku_items s
            LEFT JOIN products p
                ON p.style_no = s.style_no
            LEFT JOIN opportunity_scores o
                ON o.style_no = s.style_no
            {where_sql}
            GROUP BY s.style_no
            ORDER BY COALESCE(MIN(s.rank), 999999), s.style_no
            """,
            params,
        )
        result: list[dict[str, Any]] = []
        for row in rows:
            data = dict(row)
            best_score = data.get("best_score")
            data["best_rating"] = rating_from_score(float(best_score)) if best_score is not None else "-"
            if int(data.get("scored_sizes") or 0) > 0:
                data["status"] = "已出机会"
            elif int(data.get("has_product_data") or 0) > 0:
                data["status"] = "已拿到商品，待评分"
            else:
                data["status"] = "未拿到接口数据"
            result.append(data)
        return result
    finally:
        conn.close()


@st.cache_data(show_spinner=False)
def load_portfolio_summary_cached(db_path_str: str, signature: int) -> list[dict[str, Any]]:
    conn = connect(Path(db_path_str))
    try:
        return portfolio_summary(conn)
    finally:
        conn.close()


@st.cache_data(show_spinner=False)
def load_portfolio_trades_cached(db_path_str: str, signature: int) -> list[dict[str, Any]]:
    conn = connect(Path(db_path_str))
    try:
        rows = query_rows(
            conn,
            """
            SELECT style_no, size, side, quantity, price, trade_time, notes, created_at
            FROM portfolio_trades
            ORDER BY trade_time DESC, id DESC
            """,
        )
        return [dict(row) for row in rows]
    finally:
        conn.close()


@st.cache_data(show_spinner=False)
def load_detail_snapshot_cached(
    db_path_str: str,
    signature: int,
    style_no: str,
    size: str | None,
    fee_rate: float,
    sales_fraction: float,
) -> dict[str, Any]:
    conn = connect(Path(db_path_str))
    try:
        sales = get_sales_stats(conn, style_no, size)
        asks = latest_ask_rows(conn, style_no, size)
        bids = latest_bid_rows(conn, style_no, size)
        reference_price = get_reference_price(conn, style_no, size=size)
        neighbor_context = adjacent_size_context(size or "", latest_lowest_ask_by_size(conn, style_no)) if size else None
        ask_snapshot_time = max((str(item.get("snapshot_time")) for item in asks if item.get("snapshot_time")), default=None)
        strategy_options = build_strategy_options(
            asks,
            sales,
            fee_rate=fee_rate,
            sales_fraction=sales_fraction,
            reference_price=reference_price,
            neighbor_context=neighbor_context,
            ask_snapshot_time=ask_snapshot_time,
            max_options=3,
        )
        simulation = simulate_ask_depth(
            asks,
            sales,
            fee_rate=fee_rate,
            sales_fraction=sales_fraction,
            reference_price=reference_price,
            neighbor_context=neighbor_context,
            ask_snapshot_time=ask_snapshot_time,
        )
        score_rows = query_rows(
            conn,
            """
            SELECT risk_notes, components_json
            FROM opportunity_scores
            WHERE style_no = ? AND size = ?
            """,
            (style_no, size),
        )
        raw_rows = query_rows(
            conn,
            """
            SELECT endpoint, params_json, response_json, error_message, fetched_at
            FROM raw_api_responses
            WHERE params_json LIKE ?
            ORDER BY fetched_at DESC
            LIMIT 50
            """,
            (f"%{style_no}%",),
        )
        return {
            "sales": sales.__dict__,
            "asks": asks,
            "bids": bids,
            "reference_price": reference_price,
            "strategy_options": strategy_options,
            "simulation": simulation,
            "score_row": dict(score_rows[0]) if score_rows else None,
            "raw_rows": [dict(row) for row in raw_rows],
        }
    finally:
        conn.close()


def money(value: Any) -> str:
    if value is None or value == "":
        return "-"
    try:
        num = float(value)
        if num != num or num in (float("inf"), float("-inf")):
            return "-"
        return f"USD ${num:,.2f}"
    except (TypeError, ValueError):
        return str(value)


def normalize_us_size(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    upper = text.upper()
    if upper == "US":
        return ""
    if upper.startswith("US "):
        return text[3:].strip()
    if upper.startswith("US-"):
        return text[3:].strip()
    if upper.startswith("US") and len(text) > 2 and text[2:].strip()[:1].isdigit():
        return text[2:].strip()
    try:
        number_value = float(text)
        if number_value.is_integer():
            return str(int(number_value))
    except ValueError:
        pass
    return text


def looks_like_us_size(value: Any) -> bool:
    text = normalize_us_size(value).upper().replace(" ", "")
    if not text:
        return False
    return bool(re.fullmatch(r"(?:[1-9]|1[0-9]|2[0-2])(?:\.5)?(?:W|Y|C|M)?|(?:W|Y|C|M)(?:[1-9]|1[0-9]|2[0-2])(?:\.5)?|(?:XS|S|M|L|XL|XXL|XXXL|OS)", text))


def split_style_size_input(style_text: Any, size_text: Any = "") -> tuple[str, str]:
    explicit_size = normalize_us_size(size_text)
    raw_style = str(style_text or "").strip()
    if explicit_size:
        return normalize_style_no(raw_style) or raw_style.upper(), explicit_size
    if not raw_style:
        return "", ""

    parts = raw_style.split()
    if len(parts) >= 3 and parts[-2].upper() == "US" and looks_like_us_size(parts[-1]):
        style_part = " ".join(parts[:-2])
        return normalize_style_no(style_part) or style_part.upper(), normalize_us_size(parts[-1])
    if len(parts) >= 2 and looks_like_us_size(parts[-1]):
        style_part = " ".join(parts[:-1])
        return normalize_style_no(style_part) or style_part.upper(), normalize_us_size(parts[-1])
    return normalize_style_no(raw_style) or raw_style.upper(), ""


def us_size(value: Any) -> str:
    text = normalize_us_size(value)
    return f"US {text}" if text else "-"


def number(value: Any) -> str:
    if value is None or value == "":
        return "-"
    try:
        num = float(value)
        if num != num or num in (float("inf"), float("-inf")):
            return "-"
        if abs(num - round(num)) < 1e-9:
            return f"{int(round(num)):,}"
        return f"{num:,.1f}"
    except (TypeError, ValueError):
        return str(value)


def optional_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text.replace("$", "").replace(",", "").replace("USD", "").strip())
    except ValueError:
        return None


def optional_int(value: Any) -> int | None:
    parsed = optional_float(value)
    if parsed is None:
        return None
    return int(parsed)


def optional_date_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        pass
    match = re.search(r"\d{4}-\d{1,2}-\d{1,2}", text)
    if not match:
        return None
    year, month, day = [int(part) for part in match.group(0).split("-")]
    try:
        return datetime(year, month, day).date().isoformat()
    except ValueError:
        return None


def dataframe(rows: list[dict[str, Any]] | pd.DataFrame, *, height: int = 420) -> None:
    frame = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(rows)
    if frame.empty:
        st.info("暂无数据")
        return
    if "size" in frame.columns:
        frame = frame.copy()
        frame["size"] = frame["size"].apply(us_size)
    st.dataframe(frame, use_container_width=True, height=height)


def _render_strategy_option(option: dict[str, Any], *, expanded: bool, title_suffix: str = "") -> None:
    title = option.get("strategy_name") or "策略"
    if title_suffix:
        title = f"{title} {title_suffix}"
    consigned_qty = int(option.get("consigned_qty") or 0)
    seller_qty = int(option.get("seller_qty") or 0)
    total_qty = int(option.get("total_buy_qty") or option.get("analysis_qty") or option.get("recommended_buy_qty") or 0)
    availability_delay_days = float(option.get("availability_delay_days") or 0.0)
    consigned_ratio = float(option.get("consigned_ratio") or (consigned_qty / total_qty if total_qty else 0.0))
    buy_plan_text = _format_buy_plan_text(option.get("buy_plan") or [])
    if buy_plan_text == "-":
        buy_plan_text = str(option.get("buy_plan_text") or option.get("buyout_levels") or "-")
    buy_mix_text = option.get("buy_mix_text") or f"平台寄存 {consigned_qty} / 卖家挂售 {seller_qty} / 到账 {availability_delay_days:.1f} 天"

    with st.expander(title, expanded=expanded):
        top = st.columns(4)
        top[0].metric("推荐买入", number(option.get("recommended_buy_qty")))
        top[1].metric("最低买价", money(option.get("min_buy_price")))
        top[2].metric("最高买价", money(option.get("max_buy_price")))
        top[3].metric("平均每双利润", money(option.get("estimated_profit_per_pair")))

        middle = st.columns(4)
        middle[0].metric("总买数", number(total_qty))
        middle[1].metric("总成本", money(option.get("buy_total_cost")))
        middle[2].metric("总利润", money(option.get("estimated_profit")))
        middle[3].metric("预计卖完", number(option.get("estimated_days_to_sell")))

        mix = st.columns(4)
        mix[0].metric("平台寄存", number(consigned_qty))
        mix[1].metric("卖家挂售", number(seller_qty))
        mix[2].metric("平台寄存占比", f"{consigned_ratio * 100:.0f}%")
        mix[3].metric("到账周期", f"{availability_delay_days:.1f} 天")

        st.caption(f"买断层：{buy_plan_text}")
        st.caption(
            f"买完后下一口：{money(option.get('next_lowest_ask'))} / "
            f"数量 {number(option.get('next_lowest_ask_qty'))}"
        )
        st.caption(buy_mix_text)

        left, right = st.columns(2)
        with left:
            st.caption("买断明细")
            buy_frame = pd.DataFrame(option.get("buy_plan") or [])
            if not buy_frame.empty:
                buy_frame = buy_frame.rename(
                    columns={
                        "price": "买入价",
                        "quantity": "数量",
                        "consigned_qty": "寄存",
                        "seller_qty": "挂售",
                        "availability_delay_days": "到账天数",
                        "cumulative_qty": "累计数量",
                        "cumulative_cost": "累计成本",
                        "cumulative_consigned_qty": "累计寄存",
                        "cumulative_seller_qty": "累计挂售",
                    }
                )
                for col in ["买入价", "累计成本"]:
                    if col in buy_frame.columns:
                        buy_frame[col] = buy_frame[col].apply(money)
                if "到账天数" in buy_frame.columns:
                    buy_frame["到账天数"] = buy_frame["到账天数"].apply(number)
                dataframe(buy_frame, height=200)
            else:
                st.info("暂无买断明细")
        with right:
            st.caption("卖价建议")
            sell_frame = pd.DataFrame(option.get("sell_targets") or [])
            if not sell_frame.empty:
                sell_frame = sell_frame.rename(
                    columns={
                        "sell_strategy": "卖价方案",
                        "target_price_low": "卖价低位",
                        "target_price_high": "卖价高位",
                        "estimated_profit": "总利润",
                        "estimated_profit_per_pair": "均摊利润",
                        "estimated_days_to_sell": "预计消化天数",
                        "qualifies": "可执行",
                        "pricing_note": "说明",
                        "reason": "风险",
                    }
                )
                for col in ["卖价低位", "卖价高位", "总利润", "均摊利润"]:
                    if col in sell_frame.columns:
                        sell_frame[col] = sell_frame[col].apply(money)
                if "预计消化天数" in sell_frame.columns:
                    sell_frame["预计消化天数"] = sell_frame["预计消化天数"].apply(_format_goat_sellout_days)
                dataframe(sell_frame, height=200)
            else:
                st.info("暂无卖价建议")

        with st.expander("计算说明", expanded=False):
            st.write(option.get("reason") or "-")
            st.write(buy_plan_text)
def render_sync_monitor(state: dict[str, Any]) -> None:
    status = state.get("status")
    if status not in {"running", "error"}:
        return

    status_text = "运行中" if status == "running" else "失败"
    total = int(state.get("total") or 0)
    completed = int(state.get("completed") or 0)
    score_total = int(state.get("score_total") or 0)
    score_completed = int(state.get("score_completed") or 0)
    phase = state.get("current_phase") or "-"
    message = state.get("message") or "-"

    st.markdown("#### 后台进度")
    cols = st.columns([0.9, 1.1, 1.2, 0.9, 1.3])
    cols[0].metric("状态", status_text)
    cols[1].metric("货号进度", f"{completed}/{total}" if total else f"{completed}/-")
    cols[2].metric("当前货号", state.get("current_style") or "-")
    cols[3].metric("US尺码", us_size(state.get("current_size")))
    cols[4].metric("接口/动作", state.get("current_endpoint") or "-")

    if total:
        st.progress(min(1.0, max(0.0, completed / total)), text=f"StockX接口补跑：{completed}/{total}")
    else:
        st.progress(0.02, text="StockX接口补跑：已启动，正在读取总数")

    if score_total:
        st.progress(
            min(1.0, max(0.0, score_completed / score_total)),
            text=f"评分进度：{score_completed}/{score_total}",
        )

    if status == "error":
        st.error(f"{phase}：{message}")
    else:
        st.info(f"{phase}：{message}")

    recent_events = list(reversed(state.get("recent_events") or []))
    if recent_events:
        with st.expander("最近进展", expanded=False):
            event_frame = pd.DataFrame(recent_events)
            if "size" in event_frame.columns:
                event_frame["size"] = event_frame["size"].apply(us_size)
            event_frame = event_frame.rename(
                columns={
                    "time": "时间",
                    "phase": "阶段",
                    "style_no": "货号",
                    "size": "US尺码",
                    "endpoint": "接口/动作",
                    "status": "结果",
                    "message": "说明",
                }
            )
            dataframe(event_frame, height=240)


@st.fragment(run_every="8s")
def render_live_sync_monitor() -> None:
    render_sync_monitor(_sync_state_snapshot())


def _run_recompute_job(
    job_id: str,
    db_path_str: str,
    *,
    fee_rate: float,
    sales_fraction: float,
) -> None:
    main_conn = connect(Path(db_path_str))
    init_db(main_conn)
    try:
        _update_sync_state(
            job_id=job_id,
            status="running",
            message="正在按现有数据重算评分",
            progress=0.0,
            completed=0,
            total=0,
            current_style=None,
            current_size=None,
            current_endpoint="score_opportunity",
            current_phase="重算评分",
            score_completed=0,
            score_total=0,
            recent_events=[],
            summaries=[],
            error=None,
            recomputed=0,
            started_at=datetime.utcnow().isoformat(timespec="seconds"),
            finished_at=None,
        )
        recomputed = compute_and_store_opportunities(
            main_conn,
            fee_rate=fee_rate,
            sales_fraction=sales_fraction,
            progress_callback=_sync_progress_callback,
        )
        main_conn.commit()
        _update_sync_state(
            status="done",
            progress=1.0,
            current_style=None,
            current_size=None,
            current_endpoint=None,
            current_phase="完成",
            message=f"已重算 {recomputed} 个机会",
            recomputed=recomputed,
            finished_at=datetime.utcnow().isoformat(timespec="seconds"),
        )
    except Exception as exc:  # noqa: BLE001
        log_sync(
            main_conn,
            "后台重算失败",
            severity="error",
            event_type="recompute_error",
            details={"job_id": job_id, "error": str(exc)},
        )
        main_conn.commit()
        _update_sync_state(
            status="error",
            error=str(exc),
            message=f"重算失败：{exc}",
            current_phase="失败",
            finished_at=datetime.utcnow().isoformat(timespec="seconds"),
        )
    finally:
        main_conn.close()
        _release_job_file_lock(job_id)


def start_recompute_job(
    *,
    db_path_str: str,
    fee_rate: float,
    sales_fraction: float,
) -> str | None:
    if _sync_state_snapshot().get("status") == "running":
        return None

    job_id = uuid.uuid4().hex[:8]
    if not _acquire_job_file_lock(job_id, "recompute"):
        return None
    _update_sync_state(
        job_id=job_id,
        status="running",
        message="正在启动本地重算",
        progress=0.0,
        completed=0,
        total=0,
        current_style=None,
        current_size=None,
        current_endpoint="score_opportunity",
        current_phase="重算评分",
        score_completed=0,
        score_total=0,
        recent_events=[],
        summaries=[],
        error=None,
        recomputed=0,
        started_at=datetime.utcnow().isoformat(timespec="seconds"),
        finished_at=None,
    )
    thread = threading.Thread(
        target=_run_recompute_job,
        args=(job_id, db_path_str),
        kwargs={
            "fee_rate": fee_rate,
            "sales_fraction": sales_fraction,
        },
        daemon=True,
    )
    thread.start()
    return job_id


def _lookup_release_date_from_local_cache(
    conn,
    *,
    style_no: str,
    title: str | None,
    raw_json: str | None,
) -> tuple[str | None, str | None]:
    raw_product = json_loads(raw_json, {})
    if isinstance(raw_product, (dict, list)):
        release_date = extract_release_date(raw_product)
        if release_date:
            return release_date, "products.raw_json"

    lookup_terms: list[str] = []
    for term in (
        str(style_no or "").strip(),
        normalize_style_no(style_no) or "",
        str(style_no or "").replace("-", " ").strip(),
        str(title or "").strip(),
    ):
        if term and term not in lookup_terms:
            lookup_terms.append(term)

    for term in lookup_terms:
        rows = query_rows(
            conn,
            """
            SELECT endpoint, response_json
            FROM raw_api_responses
            WHERE response_json IS NOT NULL
              AND params_json LIKE ?
            ORDER BY id DESC
            LIMIT 24
            """,
            (f"%{term}%",),
        )
        for row in rows:
            payload = json_loads(row["response_json"], {})
            release_date = extract_release_date(payload)
            if release_date:
                return release_date, f"raw_api:{row['endpoint']}"

    return None, None


def _run_release_date_job(
    job_id: str,
    db_path_str: str,
    *,
    timeout: int,
    candidate_limit: int,
    allow_search: bool,
    fee_rate: float,
    sales_fraction: float,
) -> None:
    main_conn = connect(Path(db_path_str))
    init_db(main_conn)
    try:
        rows = [dict(row) for row in query_rows(
            main_conn,
            """
            SELECT product_id, style_no, title, brand, release_date, raw_json
            FROM products
            WHERE release_date IS NULL OR TRIM(release_date) = ''
            ORDER BY style_no
            """,
        )]
        total = len(rows)
        _update_sync_state(
            job_id=job_id,
            status="running",
            message=f"正在补齐 {total} 个发售日期",
            progress=0.0,
            completed=0,
            total=total,
            current_style=None,
            current_size=None,
            current_endpoint="release_date_lookup",
            current_phase="补齐发售日期",
            score_completed=0,
            score_total=0,
            recent_events=[],
            summaries=[],
            error=None,
            recomputed=0,
            started_at=datetime.utcnow().isoformat(timespec="seconds"),
            finished_at=None,
        )
        filled = 0
        for index, row in enumerate(rows, start=1):
            style_no = str(row["style_no"])
            title = str(row["title"] or "")
            brand = row.get("brand")
            _update_sync_state(
                completed=index - 1,
                total=total,
                current_style=style_no,
                current_size=None,
                current_endpoint="release_date_lookup",
                current_phase="补齐发售日期",
                message=f"正在补齐 {index}/{total}: {style_no}",
                progress=(index - 1) / total if total else 1.0,
            )
            release_date, release_source = _lookup_release_date_from_local_cache(
                main_conn,
                style_no=style_no,
                title=title,
                raw_json=row.get("raw_json"),
            )
            lookup_result = None
            if not release_date and allow_search:
                lookup_result = lookup_release_date(
                    style_no=style_no,
                    title=title,
                    brand=brand,
                    timeout=timeout,
                    candidate_limit=candidate_limit,
                    allow_search=allow_search,
                )
                if lookup_result:
                    release_date = lookup_result.release_date
                    release_source = getattr(lookup_result, "source_name", "external")
            if release_date:
                main_conn.execute(
                    """
                    UPDATE products
                    SET release_date = ?, updated_at = ?
                    WHERE style_no = ?
                    """,
                    (release_date, datetime.utcnow().isoformat(timespec="seconds"), style_no),
                )
                if lookup_result:
                    main_conn.execute(
                        """
                        INSERT INTO release_date_sources (
                            product_id, style_no, title, release_date,
                            source_name, source_url, confidence, raw_text, fetched_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            row.get("product_id"),
                            style_no,
                            title or None,
                            release_date,
                            getattr(lookup_result, "source_name", release_source or "raw_json"),
                            getattr(lookup_result, "source_url", None),
                            getattr(lookup_result, "confidence", 1.0 if release_source else None),
                            getattr(lookup_result, "raw_text", None),
                            datetime.utcnow().isoformat(timespec="seconds"),
                        ),
                        )
                filled += 1
                log_sync(
                    main_conn,
                    "补齐发售日期",
                    severity="info",
                    event_type="release_date_backfill",
                    style_no=style_no,
                    product_id=row.get("product_id"),
                    details={"release_date": release_date, "source": getattr(lookup_result, "source_name", release_source or "raw_json")},
                )
            else:
                log_sync(
                    main_conn,
                    "未补到发售日期",
                    severity="warning",
                    event_type="release_date_backfill",
                    style_no=style_no,
                    product_id=row.get("product_id"),
                    details={"title": title, "brand": brand},
                )
            main_conn.commit()
            _update_sync_state(
                completed=index,
                total=total,
                current_style=style_no,
                current_size=None,
                current_endpoint="release_date_lookup",
                current_phase="补齐发售日期",
                message=f"已补齐 {filled}/{total} 个发售日期",
                progress=index / total if total else 1.0,
            )

        recomputed = compute_and_store_opportunities(
            main_conn,
            fee_rate=fee_rate,
            sales_fraction=sales_fraction,
            progress_callback=_sync_progress_callback,
        )
        main_conn.commit()
        _update_sync_state(
            status="done",
            progress=1.0,
            completed=total,
            current_style=None,
            current_size=None,
            current_endpoint=None,
            current_phase="完成",
            message=f"发售日期已补齐 {filled} 个，并重算 {recomputed} 个机会",
            recomputed=recomputed,
            finished_at=datetime.utcnow().isoformat(timespec="seconds"),
        )
    except Exception as exc:  # noqa: BLE001
        log_sync(
            main_conn,
            "补齐发售日期失败",
            severity="error",
            event_type="release_date_backfill_error",
            details={"job_id": job_id, "error": str(exc)},
        )
        main_conn.commit()
        _update_sync_state(
            status="error",
            error=str(exc),
            message=f"补齐发售日期失败：{exc}",
            current_phase="失败",
            finished_at=datetime.utcnow().isoformat(timespec="seconds"),
        )
    finally:
        main_conn.close()
        _release_job_file_lock(job_id)


def start_release_date_job(
    *,
    db_path_str: str,
    timeout: int,
    candidate_limit: int,
    allow_search: bool,
    fee_rate: float,
    sales_fraction: float,
) -> str | None:
    if _sync_state_snapshot().get("status") == "running":
        return None

    job_id = uuid.uuid4().hex[:8]
    if not _acquire_job_file_lock(job_id, "release_date"):
        return None
    _update_sync_state(
        job_id=job_id,
        status="running",
        message="正在启动发售日期补齐",
        progress=0.0,
        completed=0,
        total=0,
        current_style=None,
        current_size=None,
        current_endpoint="release_date_lookup",
        current_phase="补齐发售日期",
        score_completed=0,
        score_total=0,
        recent_events=[],
        summaries=[],
        error=None,
        recomputed=0,
        row_refresh_style=None,
        row_refresh_size=None,
        started_at=datetime.utcnow().isoformat(timespec="seconds"),
        finished_at=None,
    )
    thread = threading.Thread(
        target=_run_release_date_job,
        args=(job_id, db_path_str),
        kwargs={
            "timeout": timeout,
            "candidate_limit": candidate_limit,
            "allow_search": allow_search,
            "fee_rate": fee_rate,
            "sales_fraction": sales_fraction,
        },
        daemon=True,
    )
    thread.start()
    return job_id


def page_settings(settings) -> None:
    st.title("设置")
    st.write("可以直接填写凭证并保存到项目根目录的 `.env`；Token/Auth 不写入代码。")

    credential_mode_labels = {
        "both": "两种都带",
        "query": "只放查询参数",
        "header": "只放请求头",
    }
    credential_mode_values = {value: key for key, value in credential_mode_labels.items()}

    saved_notice = st.session_state.pop("settings_saved", None)
    if saved_notice:
        st.success(saved_notice)

    config_rows = [
        {"key": "云端登录保护", "value": "开启" if settings.app_login_enabled else "关闭"},
        {"key": "云端登录账号", "value": settings.app_username if settings.app_login_enabled else "-"},
        {"key": "云端数据后端", "value": settings.cloud_storage_backend},
        {"key": "Firebase 项目", "value": settings.firebase_project_id or "-"},
        {"key": "Firebase 集合前缀", "value": settings.firebase_collection_prefix or "-"},
        {"key": "接口地址", "value": str(settings.host)},
        {"key": "凭证传递方式", "value": credential_mode_labels.get(str(settings.credential_mode), str(settings.credential_mode))},
        {"key": "请求超时（秒）", "value": str(settings.timeout)},
        {"key": "数据库路径", "value": _path_display(settings.db_path)},
        {"key": "销售支付通道费率", "value": str(settings.estimated_seller_fee_rate)},
        {"key": "扫货销量折算系数", "value": str(settings.buy_depth_sales_fraction)},
        {"key": "自动全量同步", "value": "开启" if settings.auto_full_sync_enabled else "关闭"},
        {"key": "自动全量完成后等待（分钟）", "value": str(settings.auto_full_sync_interval_minutes)},
        {"key": "同步并发数", "value": str(settings.sync_max_workers)},
        {"key": "凭证状态", "value": "已填写" if settings.credentials_ready else "未填写"},
    ]
    dataframe(config_rows, height=240)
    if settings.firebase_enabled:
        status = firebase_status()
        if status.get("ok"):
            st.success(f"Firebase 已连接：{status.get('project_id')} / {status.get('collection_prefix')}")
        else:
            st.error(f"Firebase 未连通：{status.get('message')}")

    with st.form("env_editor"):
        st.subheader("直接编辑并保存")
        left, right = st.columns(2)
        with left:
            stockx_host = st.text_input("接口地址（STOCKX_HOST）", value=settings.host)
            credential_mode_options = list(credential_mode_values.keys())
            default_credential_mode = credential_mode_labels.get(str(settings.credential_mode), credential_mode_options[0])
            credential_mode = st.selectbox(
                "凭证传递方式（STOCKX_CREDENTIAL_MODE）",
                credential_mode_options,
                index=credential_mode_options.index(default_credential_mode),
            )
            token = st.text_input("访问令牌（STOCKX_TOKEN）", value=settings.token, type="password")
            auth = st.text_input("认证串（STOCKX_AUTH）", value=settings.auth, type="password")
            timeout = st.number_input("请求超时（秒）", min_value=1, max_value=120, value=int(settings.timeout), step=1)
        with right:
            token_param = st.text_input("令牌参数名（STOCKX_TOKEN_PARAM）", value=settings.token_param)
            auth_param = st.text_input("认证参数名（STOCKX_AUTH_PARAM）", value=settings.auth_param)
            token_header = st.text_input("令牌请求头（STOCKX_TOKEN_HEADER）", value=settings.token_header)
            auth_header = st.text_input("认证请求头（STOCKX_AUTH_HEADER）", value=settings.auth_header)
            db_path = st.text_input("数据库路径（STOCKX_DB_PATH）", value=_path_display(settings.db_path))
            seller_fee_rate = st.number_input(
                "销售支付通道费率（ESTIMATED_SELLER_FEE_RATE）",
                min_value=0.0,
                max_value=1.0,
                value=float(settings.estimated_seller_fee_rate),
                step=0.01,
            )
            sales_fraction = st.number_input(
                "扫货销量折算系数（BUY_DEPTH_SALES_FRACTION）",
                min_value=0.0,
                max_value=1.0,
                value=float(settings.buy_depth_sales_fraction),
                step=0.01,
            )
            auto_full_sync_enabled = st.checkbox(
                "自动全量同步（AUTO_FULL_SYNC_ENABLED）",
                value=bool(settings.auto_full_sync_enabled),
            )
            auto_full_sync_interval = st.number_input(
                "自动全量完成后等待分钟（AUTO_FULL_SYNC_INTERVAL_MINUTES）",
                min_value=15,
                max_value=24 * 60,
                value=int(settings.auto_full_sync_interval_minutes or 60),
                step=15,
            )
            sync_max_workers = st.number_input(
                "同步并发数（SYNC_MAX_WORKERS）",
                min_value=1,
                max_value=8,
                value=int(settings.sync_max_workers or 4),
                step=1,
            )
            app_login_enabled = st.checkbox(
                "云端登录保护（APP_LOGIN_ENABLED）",
                value=bool(settings.app_login_enabled),
            )
            app_username = st.text_input("云端登录账号（APP_USERNAME）", value=settings.app_username)
            app_password = st.text_input("云端登录密码（APP_PASSWORD）", value=settings.app_password, type="password")
            cloud_storage_backend = st.selectbox(
                "云端数据后端（CLOUD_STORAGE_BACKEND）",
                ["sqlite", "firebase"],
                index=0 if settings.cloud_storage_backend != "firebase" else 1,
            )
            firebase_project_id = st.text_input("Firebase 项目ID（FIREBASE_PROJECT_ID）", value=settings.firebase_project_id)
            firebase_collection_prefix = st.text_input(
                "Firebase 集合前缀（FIREBASE_COLLECTION_PREFIX）",
                value=settings.firebase_collection_prefix,
            )
            firebase_service_account_b64 = st.text_input(
                "Firebase服务账号Base64（FIREBASE_SERVICE_ACCOUNT_B64）",
                value=settings.firebase_service_account_b64,
                type="password",
            )
            firebase_sqlite_backup_max_mb = st.number_input(
                "SQLite云端备份上限MB（FIREBASE_SQLITE_BACKUP_MAX_MB）",
                min_value=50.0,
                max_value=5000.0,
                value=float(settings.firebase_sqlite_backup_max_mb),
                step=50.0,
            )
        submitted = st.form_submit_button("保存到 .env", use_container_width=True)

    if submitted:
        credential_mode_value = credential_mode_values.get(credential_mode, str(settings.credential_mode))
        _save_env_file(
            {
                "STOCKX_HOST": stockx_host.strip(),
                "APP_LOGIN_ENABLED": str(bool(app_login_enabled)).lower(),
                "APP_USERNAME": app_username.strip(),
                "APP_PASSWORD": app_password,
                "CLOUD_STORAGE_BACKEND": cloud_storage_backend,
                "FIREBASE_PROJECT_ID": firebase_project_id.strip(),
                "FIREBASE_COLLECTION_PREFIX": firebase_collection_prefix.strip(),
                "FIREBASE_SERVICE_ACCOUNT_B64": firebase_service_account_b64.strip(),
                "FIREBASE_SQLITE_BACKUP_MAX_MB": firebase_sqlite_backup_max_mb,
                "FIREBASE_CREDENTIALS_PATH": settings.firebase_credentials_path,
                "FIREBASE_SERVICE_ACCOUNT_JSON": settings.firebase_service_account_json,
                "STOCKX_TOKEN": token,
                "STOCKX_AUTH": auth,
                "STOCKX_CREDENTIAL_MODE": credential_mode_value,
                "STOCKX_TOKEN_PARAM": token_param.strip(),
                "STOCKX_AUTH_PARAM": auth_param.strip(),
                "STOCKX_TOKEN_HEADER": token_header.strip(),
                "STOCKX_AUTH_HEADER": auth_header.strip(),
                "STOCKX_REQUEST_TIMEOUT": int(timeout),
                "STOCKX_DB_PATH": db_path.strip(),
                "ESTIMATED_SELLER_FEE_RATE": seller_fee_rate,
                "BUY_DEPTH_SALES_FRACTION": sales_fraction,
                "AUTO_FULL_SYNC_ENABLED": str(bool(auto_full_sync_enabled)).lower(),
                "AUTO_FULL_SYNC_INTERVAL_MINUTES": int(auto_full_sync_interval),
                "SYNC_MAX_WORKERS": int(sync_max_workers),
            }
        )
        st.session_state["settings_saved"] = f"已保存到 {ENV_PATH}"
        st.rerun()

    example_path = BASE_DIR / ".env.example"
    if example_path.exists():
        with st.expander(".env.example"):
            st.code(example_path.read_text(encoding="utf-8"), language="dotenv")



def _format_days(value: Any) -> str:
    text = number(value)
    return "-" if text == "-" else f"{text} 天"


def _format_goat_sellout_days(value: Any) -> str:
    parsed = optional_float(value)
    if parsed is None or parsed != parsed or parsed < 0:
        return "无限"
    return number(parsed)


def _format_goat_sellout_columns(frame: pd.DataFrame) -> pd.DataFrame:
    for col in list(frame.columns):
        label = str(col)
        if _is_sellout_days_label(label):
            frame[col] = frame[col].apply(_format_goat_sellout_days)
    return frame


def _format_datetime_minute(value: Any) -> str:
    if value in (None, ""):
        return "-"
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is not None:
            los_angeles = parsed.astimezone(_los_angeles_timezone())
            beijing = parsed.astimezone(_beijing_timezone())
            return f"{los_angeles.strftime('%Y-%m-%d %H:%M')} 洛杉矶 / {beijing.strftime('%Y-%m-%d %H:%M')} 北京"
        return parsed.strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return str(value)[:16]


def _format_date_only(value: Any) -> str:
    if value in (None, ""):
        return "-"
    text = str(value).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).date().isoformat()
    except ValueError:
        return str(value)[:10] if text else "-"


def _csv_download_bytes(frame: pd.DataFrame) -> bytes:
    if frame.empty:
        return b""
    return frame.to_csv(index=False).encode("utf-8-sig")


def _export_file_name(prefix: str) -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{stamp}.csv"


def _opportunity_export_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for row in rows:
        strategy = _strategy_summary_from_row(row)
        reference = _reference_summary_from_row(row)
        record = dict(strategy)
        for key, value in reference.items():
            if key in {"货号", "尺码"}:
                continue
            record[f"引用·{key}"] = value
        if row.get("image_url"):
            record["图片"] = row.get("image_url")
        records.append(record)
    return pd.DataFrame(records)


def _query_param_value(name: str) -> str:
    value = st.query_params.get(name)
    if isinstance(value, list):
        return str(value[0] if value else "").strip()
    return str(value or "").strip()


def _ensure_goat_hidden_styles_table(conn) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS goat_hidden_styles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            style_no TEXT NOT NULL UNIQUE,
            hidden_at TEXT NOT NULL,
            note TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_goat_hidden_styles_style
            ON goat_hidden_styles(style_no);
        """
    )


def _goat_hidden_styles(conn) -> list[dict[str, Any]]:
    _ensure_goat_hidden_styles_table(conn)
    return [dict(row) for row in query_rows(conn, "SELECT style_no, hidden_at, note FROM goat_hidden_styles ORDER BY hidden_at DESC")]


def _hide_goat_style(conn, style_no: str, *, note: str = "manual") -> bool:
    cleaned = normalize_style_no(style_no)
    if not cleaned:
        return False
    _ensure_goat_hidden_styles_table(conn)
    conn.execute(
        """
        INSERT INTO goat_hidden_styles (style_no, hidden_at, note)
        VALUES (?, ?, ?)
        ON CONFLICT(style_no) DO UPDATE SET
            hidden_at=excluded.hidden_at,
            note=excluded.note
        """,
        (cleaned, datetime.utcnow().isoformat(timespec="seconds"), note),
    )
    conn.commit()
    bump_data_cache_version()
    return True


def _restore_goat_style(conn, style_no: str) -> bool:
    cleaned = normalize_style_no(style_no)
    if not cleaned:
        return False
    _ensure_goat_hidden_styles_table(conn)
    cur = conn.execute("DELETE FROM goat_hidden_styles WHERE style_no = ?", (cleaned,))
    conn.commit()
    bump_data_cache_version()
    return cur.rowcount > 0


def _consume_goat_hidden_style_query(conn) -> None:
    hide_style = normalize_style_no(_query_param_value("hide_goat_style"))
    restore_style = normalize_style_no(_query_param_value("restore_goat_style"))
    if hide_style:
        _hide_goat_style(conn, hide_style, note="GOAT寄存选品手动隐藏")
        st.session_state["goat_notice"] = f"已隐藏货号 {hide_style}；该货号所有尺码不会再进入 GOAT 寄存选品列表。"
        st.query_params.clear()
        st.rerun()
    if restore_style:
        restored = _restore_goat_style(conn, restore_style)
        st.session_state["goat_notice"] = f"已恢复货号 {restore_style}。" if restored else f"{restore_style} 不在隐藏清单里。"
        st.query_params.clear()
        st.rerun()


def _render_goat_hidden_styles_panel(conn) -> None:
    hidden_rows = _goat_hidden_styles(conn)
    with st.expander(f"已隐藏货号清单（{len(hidden_rows)}）", expanded=False):
        if not hidden_rows:
            st.caption("暂无隐藏货号。")
            return
        controls = st.columns([1.2, 0.8, 0.8, 3])
        options = [row["style_no"] for row in hidden_rows]
        selected = controls[0].selectbox("恢复货号", options, key="goat_restore_hidden_style")
        if controls[1].button("恢复选中", use_container_width=True):
            _restore_goat_style(conn, selected)
            st.session_state["goat_notice"] = f"已恢复货号 {selected}。"
            st.rerun()
        if controls[2].button("全部恢复", use_container_width=True):
            conn.execute("DELETE FROM goat_hidden_styles")
            conn.commit()
            bump_data_cache_version()
            st.session_state["goat_notice"] = "已清空隐藏货号清单。"
            st.rerun()
        hidden_frame = pd.DataFrame(hidden_rows).rename(
            columns={
                "style_no": "货号",
                "hidden_at": "隐藏时间",
                "note": "说明",
            }
        )
        if "隐藏时间" in hidden_frame.columns:
            hidden_frame["隐藏时间"] = hidden_frame["隐藏时间"].apply(_format_datetime_minute)
        st.dataframe(hidden_frame, use_container_width=True, hide_index=True, height=260)


def _render_goat_hover_table(frame: pd.DataFrame, *, height: int = 680) -> None:
    if frame.empty:
        st.info("暂无结果。")
        return

    hidden_cols = {"商品名", "图片"}
    columns = [col for col in frame.columns if col not in hidden_cols]
    header_html = "".join(
        f'<th data-col="{idx}" title="点击排序">{escape(str(col))}<span class="sort-indicator"></span></th>'
        for idx, col in enumerate(columns)
    )
    row_html: list[str] = []
    for _, row in frame.iterrows():
        cells: list[str] = []
        product_title = str(row.get("商品名") or "").strip()
        image_url = str(row.get("图片") or "").strip()
        for col in columns:
            raw_value = row.get(col)
            value = "-" if pd.isna(raw_value) else str(raw_value)
            sort_value = escape(value, quote=True)
            if col == "操作":
                style_for_action = normalize_style_no(row.get("货号") or "")
                href = f"?hide_goat_style={quote(style_for_action)}" if style_for_action else "#"
                cell = (
                    f'<td class="goat-action-cell" data-sort="">'
                    f'<a class="goat-hide-link" href="{escape(href, quote=True)}" target="_top" title="隐藏该货号所有尺码">隐藏</a>'
                    f"</td>"
                )
            elif col == "货号":
                title_text = product_title or value
                image_html = (
                    f'<img src="{escape(image_url, quote=True)}" alt="{escape(title_text, quote=True)}" loading="lazy">'
                    if image_url
                    else '<div class="goat-hover-empty">暂无图片</div>'
                )
                cell = f"""
                <td class="goat-style-cell" data-sort="{sort_value}">
                  <span class="goat-style-link">{escape(value)}</span>
                  <div class="goat-hover-card">
                    {image_html}
                    <div class="goat-hover-title">{escape(title_text)}</div>
                    <div class="goat-hover-style">{escape(value)}</div>
                  </div>
                </td>
                """
            else:
                cell = f'<td data-sort="{sort_value}">{escape(value)}</td>'
            cells.append(cell)
        row_html.append(f"<tr>{''.join(cells)}</tr>")

    html = f"""
    <style>
      body {{
        margin: 0;
        background: #f5f6f8;
        color: #111827;
        font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      }}
      .goat-table-wrap {{
        height: {height}px;
        overflow: auto;
        border: 1px solid #d7dce3;
        border-radius: 10px;
        background: #ffffff;
      }}
      table.goat-table {{
        width: max-content;
        min-width: 0;
        border-collapse: separate;
        border-spacing: 0;
        font-size: 13px;
      }}
      .goat-table th {{
        position: sticky;
        top: 0;
        z-index: 3;
        background: #eef1f5;
        color: #334155;
        font-weight: 760;
        text-align: left;
        padding: 7px 6px;
        border-bottom: 1px solid #d7dce3;
        white-space: nowrap;
        cursor: pointer;
        user-select: none;
        line-height: 1.15;
      }}
      .goat-table th:hover {{
        background: #e2e8f0;
        color: #0f172a;
      }}
      .sort-indicator {{
        display: inline-block;
        min-width: 10px;
        margin-left: 3px;
        color: #ef4444;
        font-size: 11px;
      }}
      .goat-table td {{
        padding: 7px 6px;
        border-bottom: 1px solid #e5e7eb;
        color: #111827;
        white-space: nowrap;
        background: #ffffff;
        line-height: 1.15;
      }}
      .goat-table tr:hover td {{
        background: #f8fbff;
      }}
      .goat-action-cell {{
        text-align: center;
      }}
      .goat-hide-link {{
        display: inline-block;
        color: #b91c1c;
        font-weight: 760;
        text-decoration: none;
        padding: 1px 5px;
        border: 1px solid #fecaca;
        border-radius: 6px;
        background: #fff1f2;
        font-size: 12px;
      }}
      .goat-hide-link:hover {{
        color: #7f1d1d;
        background: #ffe4e6;
        border-color: #fca5a5;
      }}
      .goat-style-cell {{
        position: relative;
      }}
      .goat-style-link {{
        color: #0f62fe;
        font-weight: 780;
        cursor: default;
        border-bottom: 1px dashed rgba(15, 98, 254, 0.45);
      }}
      .goat-hover-card {{
        display: none;
        position: absolute;
        left: 8px;
        top: 34px;
        z-index: 20;
        width: 260px;
        padding: 10px;
        border: 1px solid #cbd5e1;
        border-radius: 10px;
        background: #ffffff;
        box-shadow: 0 18px 42px rgba(15, 23, 42, 0.18);
        white-space: normal;
      }}
      .goat-style-cell:hover .goat-hover-card {{
        display: block;
      }}
      .goat-hover-card img {{
        display: block;
        width: 100%;
        height: 150px;
        object-fit: contain;
        background: #f8fafc;
        border-radius: 8px;
        margin-bottom: 9px;
      }}
      .goat-hover-empty {{
        height: 86px;
        display: flex;
        align-items: center;
        justify-content: center;
        color: #64748b;
        background: #f1f5f9;
        border-radius: 8px;
        margin-bottom: 9px;
      }}
      .goat-hover-title {{
        color: #0f172a;
        font-size: 14px;
        font-weight: 780;
        line-height: 1.3;
      }}
      .goat-hover-style {{
        margin-top: 5px;
        color: #64748b;
        font-size: 12px;
      }}
    </style>
    <div class="goat-table-wrap">
      <table class="goat-table">
        <thead><tr>{header_html}</tr></thead>
        <tbody>{''.join(row_html)}</tbody>
      </table>
    </div>
    <script>
      (() => {{
        const table = document.querySelector("table.goat-table");
        if (!table) return;
        const tbody = table.querySelector("tbody");
        const invalidTexts = new Set(["", "-", "无限", "None", "none", "NaN", "nan"]);

        function parseCell(text) {{
          const raw = (text || "").trim();
          if (invalidTexts.has(raw)) {{
            return {{ invalid: true, value: null, numeric: true }};
          }}
          const numericText = raw
            .replace(/USD/g, "")
            .replace(/\\$/g, "")
            .replace(/,/g, "")
            .replace(/%/g, "")
            .trim();
          if (/^-?\\d+(\\.\\d+)?$/.test(numericText)) {{
            return {{ invalid: false, value: parseFloat(numericText), numeric: true }};
          }}
          if (/^\\d{{4}}-\\d{{2}}-\\d{{2}}/.test(raw)) {{
            const time = Date.parse(raw.replace(" ", "T"));
            if (!Number.isNaN(time)) {{
              return {{ invalid: false, value: time, numeric: true }};
            }}
          }}
          return {{ invalid: false, value: raw.toLowerCase(), numeric: false }};
        }}

        function compareValues(a, b, direction) {{
          if (a.invalid && b.invalid) return 0;
          if (a.invalid) return 1;
          if (b.invalid) return -1;
          let result = 0;
          if (a.numeric && b.numeric) {{
            result = a.value - b.value;
          }} else {{
            result = String(a.value).localeCompare(String(b.value), "zh-Hans", {{ numeric: true }});
          }}
          return direction === "asc" ? result : -result;
        }}

        table.querySelectorAll("th").forEach((th, index) => {{
          th.addEventListener("click", () => {{
            const direction = th.dataset.sortDirection === "asc" ? "desc" : "asc";
            table.querySelectorAll("th").forEach((header) => {{
              header.dataset.sortDirection = "";
              const indicator = header.querySelector(".sort-indicator");
              if (indicator) indicator.textContent = "";
            }});
            th.dataset.sortDirection = direction;
            const indicator = th.querySelector(".sort-indicator");
            if (indicator) indicator.textContent = direction === "asc" ? "▲" : "▼";

            const rows = Array.from(tbody.querySelectorAll("tr"));
            rows.sort((rowA, rowB) => {{
              const cellA = rowA.children[index];
              const cellB = rowB.children[index];
              const valueA = parseCell(cellA ? (cellA.dataset.sort || cellA.innerText) : "");
              const valueB = parseCell(cellB ? (cellB.dataset.sort || cellB.innerText) : "");
              return compareValues(valueA, valueB, direction);
            }});
            rows.forEach((row) => tbody.appendChild(row));
          }});
        }});
      }})();
    </script>
    """
    components.html(html, height=height + 20, scrolling=False)


def _format_buy_plan_text(buy_plan: list[dict[str, Any]] | None) -> str:
    parts: list[str] = []
    for level in buy_plan or []:
        price = level.get("price")
        qty = level.get("quantity")
        if price is None or qty in (None, ""):
            continue
        qty_int = int(float(qty))
        consigned_qty = int(float(level.get("consigned_qty") or 0))
        seller_qty = int(float(level.get("seller_qty") or 0))
        known_qty = consigned_qty + seller_qty
        mix_parts: list[str] = []
        if known_qty > 0:
            mix_parts.append(f"寄存 {consigned_qty}")
            mix_parts.append(f"挂售 {seller_qty}")
            if known_qty < qty_int:
                mix_parts.append(f"未识别 {qty_int - known_qty}")
        mix_text = f"（{' / '.join(mix_parts)}）" if mix_parts else ""
        parts.append(f"USD ${float(price):,.2f} x {qty_int}{mix_text}")
    return " / ".join(parts) if parts else "-"


def _format_neighbor_asks(neighbor_context: dict[str, Any] | None) -> str:
    parts: list[str] = []
    for item in (neighbor_context or {}).get("neighbors") or []:
        size = item.get("size")
        price = item.get("lowest_ask")
        if size in (None, "") or price in (None, ""):
            continue
        parts.append(f"US {normalize_us_size(size)} {money(price)}")
    return " / ".join(parts) if parts else "-"


def _option_from_score_row(row: dict[str, Any]) -> dict[str, Any]:
    components = json_loads(row.get("components_json"), {}) or {}
    option = dict(components.get("ask_simulation") or {})
    buy_plan = option.get("buy_plan") or []
    plan_text = _format_buy_plan_text(buy_plan)
    if plan_text == "-":
        plan_text = str(option.get("buyout_levels") or option.get("buy_plan_text") or "-")
    if buy_plan:
        prices = [float(level.get("price")) for level in buy_plan if level.get("price") is not None]
        if prices:
            option["lowest_buy_price"] = min(prices)
            option["min_buy_price"] = min(prices)
            option["max_buy_price"] = max(prices)
    option["buyout_levels"] = plan_text
    option["buy_plan_text"] = plan_text
    qty = int(option.get("recommended_buy_qty") or row.get("recommended_buy_qty") or 0)
    total_profit = float(option.get("estimated_profit") or row.get("estimated_profit") or 0.0)
    if not option.get("estimated_profit_per_pair") and qty:
        option["estimated_profit_per_pair"] = round(total_profit / qty, 2)
    option.setdefault("recommended_buy_qty", qty)
    option.setdefault("total_buy_qty", int(option.get("analysis_qty") or qty))
    option.setdefault("max_buy_price", row.get("max_buy_price"))
    option.setdefault("target_sell_price_low", row.get("target_sell_price_low"))
    option.setdefault("target_sell_price_high", row.get("target_sell_price_high"))
    option.setdefault("estimated_profit", total_profit)
    option.setdefault("estimated_days_to_sell", row.get("estimated_days_to_sell"))
    option.setdefault("strategy_name", option.get("sell_strategy") or "默认策略")
    return option


def _strategy_summary_from_row(row: dict[str, Any]) -> dict[str, Any]:
    option = _option_from_score_row(row)
    buy_plan = option.get("buy_plan") or []
    min_buy = option.get("lowest_buy_price") or option.get("min_buy_price")
    max_buy = option.get("max_buy_price") or option.get("buyout_level_price")
    if buy_plan:
        prices = [float(level.get("price")) for level in buy_plan if level.get("price") is not None]
        if prices:
            min_buy = min(prices)
            max_buy = max(prices)
    qty = int(option.get("recommended_buy_qty") or row.get("recommended_buy_qty") or 0)
    total_profit = float(option.get("estimated_profit") or row.get("estimated_profit") or 0.0)
    profit_per_pair = option.get("estimated_profit_per_pair")
    if (profit_per_pair is None or profit_per_pair == "") and qty:
        profit_per_pair = round(total_profit / qty, 2)
    plan_text = _format_buy_plan_text(buy_plan)
    if plan_text == "-":
        plan_text = option.get("buyout_levels") or option.get("buy_plan_text") or "-"
    consigned_qty = int(option.get("consigned_qty") or 0)
    seller_qty = int(option.get("seller_qty") or 0)
    ask_snapshot_time = _format_datetime_minute(row.get("ask_snapshot_time") or option.get("ask_snapshot_time"))
    neighbor_asks = _format_neighbor_asks(option.get("neighbor_context"))
    return {
        "评级": row.get("rating"),
        "分数": row.get("score"),
        "货号": row.get("style_no"),
        "商品名": row.get("title"),
        "尺码": us_size(row.get("size")),
        "发售日期": row.get("release_date") or "-",
        "发售天数": _format_days(row.get("release_days")),
        "默认买断策略": plan_text,
        "默认买断档": plan_text,
        "买入双数": number(qty),
        "最低买价": money(min_buy),
        "最高买价": money(max_buy),
        "加权均价": money(option.get("weighted_avg_cost")),
        "总成本": money(option.get("total_buy_cost") or option.get("buy_total_cost")),
        "建议卖价": f"{money(option.get('target_sell_price_low'))} - {money(option.get('target_sell_price_high'))}",
        "总利润": money(total_profit),
        "每双利润": money(profit_per_pair),
        "预计卖完": _format_goat_sellout_days(option.get("estimated_days_to_sell")),
        "平台寄存数量": number(consigned_qty),
        "卖家挂售数量": number(seller_qty),
        "平台寄存/卖家挂售": f"{number(consigned_qty)} / {number(seller_qty)}",
        "下一口价/数量": f"{money(option.get('next_lowest_ask'))} / {number(option.get('next_lowest_ask_qty'))}",
        "Ask快照时间": ask_snapshot_time,
        "相邻尺码最低Ask": neighbor_asks,
    }


def _reference_summary_from_row(row: dict[str, Any]) -> dict[str, Any]:
    components = json_loads(row.get("components_json"), {}) or {}
    ask_sim = components.get("ask_simulation") or {}
    return {
        "货号": row.get("style_no"),
        "尺码": us_size(row.get("size")),
        "7天销量": number(row.get("sales_7d")),
        "30天销量": number(row.get("sales_30d")),
        "7天成交均价": money(row.get("sales_avg_7d")),
        "30天成交均价": money(row.get("sales_avg_30d")),
        "最近成交": _format_datetime_minute(row.get("last_sale_at")),
        "距最近成交": _format_days(row.get("last_sale_days")),
        "发售日期": row.get("release_date") or "-",
        "发售天数": _format_days(row.get("release_days")),
        "Ask快照时间": _format_datetime_minute(row.get("ask_snapshot_time") or ask_sim.get("ask_snapshot_time")),
        "相邻尺码最低Ask": _format_neighbor_asks(ask_sim.get("neighbor_context")),
        "风险说明": row.get("risk_notes") or "-",
        "最近计算": row.get("computed_at") or "-",
    }


def _style_bundle_summaries(rows: list[dict[str, Any]], *, limit: int | None = 80) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("style_no") or ""), []).append(row)

    summaries: list[dict[str, Any]] = []
    for style_no, style_rows in grouped.items():
        if not style_no:
            continue
        total_qty = 0
        total_cost = 0.0
        total_profit = 0.0
        max_days = 0.0
        sizes: list[str] = []
        min_buy_values: list[float] = []
        max_buy_values: list[float] = []
        snapshots: list[str] = []
        title = style_rows[0].get("title") or "-"
        for row in style_rows:
            option = _option_from_score_row(row)
            qty = int(option.get("recommended_buy_qty") or row.get("recommended_buy_qty") or 0)
            if qty <= 0:
                continue
            cost = float(option.get("total_buy_cost") or option.get("buy_total_cost") or 0.0)
            profit = float(option.get("estimated_profit") or row.get("estimated_profit") or 0.0)
            days_value = optional_float(option.get("estimated_days_to_sell") or row.get("estimated_days_to_sell"))
            days = days_value if days_value is not None else math.inf
            buy_plan = option.get("buy_plan") or []
            prices = [float(level.get("price")) for level in buy_plan if level.get("price") is not None]
            if prices:
                min_buy_values.append(min(prices))
                max_buy_values.append(max(prices))
            snapshot = row.get("ask_snapshot_time") or option.get("ask_snapshot_time")
            if snapshot:
                snapshots.append(str(snapshot))
            total_qty += qty
            total_cost += cost
            total_profit += profit
            max_days = max(max_days, days)
            sizes.append(us_size(row.get("size")))
        if total_qty <= 0:
            continue
        summaries.append(
            {
                "货号": style_no,
                "商品名": title,
                "可组合尺码": " / ".join(sizes[:12]),
                "组合买入双数": total_qty,
                "组合成本": money(total_cost),
                "组合总利润": money(total_profit),
                "组合每双利润": money(total_profit / total_qty if total_qty else None),
                "最慢卖完": _format_days(max_days),
                "买价区间": f"{money(min(min_buy_values) if min_buy_values else None)} - {money(max(max_buy_values) if max_buy_values else None)}",
                "最新Ask快照": max(snapshots) if snapshots else "-",
            }
        )
    summaries.sort(key=lambda item: optional_float(str(item["组合总利润"]).replace("USD", "")) or 0.0, reverse=True)
    return summaries[:limit] if limit else summaries


def _opportunity_select_sql() -> str:
    return """
        SELECT
            rating,
            ROUND(score, 1) AS score,
            style_no,
            title,
            (
                SELECT MAX(p.image_url)
                FROM products p
                WHERE p.style_no = opportunity_scores.style_no
            ) AS image_url,
            size,
            recommended_buy_qty,
            max_buy_price,
            weighted_avg_cost,
            next_lowest_ask,
            target_sell_price_low,
            target_sell_price_high,
            ROUND(estimated_profit, 2) AS estimated_profit,
            ROUND(estimated_profit_per_pair, 2) AS estimated_profit_per_pair,
            ROUND(estimated_days_to_sell, 1) AS estimated_days_to_sell,
            sales_7d,
            sales_30d,
            (
                SELECT ROUND(AVG(s.amount), 2)
                FROM sales_history s
                WHERE s.style_no = opportunity_scores.style_no
                  AND COALESCE(s.size, '') = COALESCE(opportunity_scores.size, '')
                  AND s.amount IS NOT NULL
                  AND s.created_at IS NOT NULL
                  AND julianday(REPLACE(SUBSTR(s.created_at, 1, 19), 'T', ' ')) >= julianday('now', '-7 days')
            ) AS sales_avg_7d,
            (
                SELECT ROUND(AVG(s.amount), 2)
                FROM sales_history s
                WHERE s.style_no = opportunity_scores.style_no
                  AND COALESCE(s.size, '') = COALESCE(opportunity_scores.size, '')
                  AND s.amount IS NOT NULL
                  AND s.created_at IS NOT NULL
                  AND julianday(REPLACE(SUBSTR(s.created_at, 1, 19), 'T', ' ')) >= julianday('now', '-30 days')
            ) AS sales_avg_30d,
            last_sale_at,
            last_sale_days,
            risk_notes,
            release_date,
            release_days,
            components_json,
            computed_at,
            (
                SELECT MAX(a.snapshot_time)
                FROM ask_depth a
                WHERE a.style_no = opportunity_scores.style_no
                  AND COALESCE(a.size, '') = COALESCE(opportunity_scores.size, '')
            ) AS ask_snapshot_time
        FROM opportunity_scores
    """


def _rating_class(rating: Any) -> str:
    text = str(rating or "").upper()
    if text == "A":
        return "rating-a"
    if text.startswith("B"):
        return "rating-b"
    if text in {"C", "D"}:
        return "rating-c"
    return "rating-s"


def _ui_page_header(title: str, subtitle: str = "", pill: str = "") -> None:
    pill_html = f'<div class="ui-pill">{escape(pill)}</div>' if pill else ""
    subtitle_html = f'<div class="ui-page-sub">{escape(subtitle)}</div>' if subtitle else ""
    st.markdown(
        f"""
        <div class="ui-page-head">
          <div>
            <div class="ui-page-title">{escape(title)}</div>
            {subtitle_html}
          </div>
          {pill_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _ui_status_cards(items: list[tuple[str, Any]], *, max_cards: int = 6) -> None:
    if not items:
        return
    html = []
    for label, value in items[:max_cards]:
        html.append(
            '<div class="ui-stat-card">'
            f'<div class="ui-stat-label">{escape(str(label))}</div>'
            f'<div class="ui-stat-value">{escape(str(value))}</div>'
            "</div>"
        )
    st.markdown(f'<div class="ui-card-grid">{"".join(html)}</div>', unsafe_allow_html=True)


def _ui_section_label(title: str, note: str = "") -> None:
    note_html = f'<div class="ui-muted">{escape(note)}</div>' if note else ""
    st.markdown(
        f'<div class="ui-section-title">{escape(title)}</div>{note_html}',
        unsafe_allow_html=True,
    )


def _html_cell(label: str, value: Any, *, wide: bool = False, extra_wide: bool = False, compact: bool = False) -> str:
    extra = " opp-extra-wide" if extra_wide else (" opp-wide" if wide else "")
    if compact:
        extra += " opp-strategy"
    value_html = escape(str(value)).replace("\n", "<br>")
    return (
        f'<div class="opp-cell{extra}">'
        f'<div class="opp-label">{escape(str(label))}</div>'
        f'<div class="opp-value">{value_html}</div>'
        "</div>"
    )


def _row_refresh_status(row: dict[str, Any]) -> str:
    style_no = str(row.get("style_no") or "").strip()
    size = normalize_us_size(row.get("size"))
    state = _sync_state_snapshot()
    row_style = state.get("row_refresh_style")
    row_size = normalize_us_size(state.get("row_refresh_size"))
    is_this_row = row_style == style_no and (not row_size or row_size == size)
    if state.get("status") == "running" and is_this_row:
        endpoint = state.get("current_endpoint") or "API"
        current_size = us_size(state.get("current_size")) if state.get("current_size") else us_size(row_size)
        return f"正在直接请求 API：{endpoint} / {current_size}"
    if is_this_row and state.get("status") == "done":
        summaries = state.get("summaries") or []
        summary = next((item for item in summaries if item.get("style_no") == style_no), None)
        if summary:
            ask_rows = int(summary.get("ask_rows") or 0)
            errors = summary.get("errors") or []
            if ask_rows > 0:
                return f"刷新完成，API 最新 Ask 写入 {ask_rows} 行"
            if errors:
                return f"刷新完成但 Ask 未更新：{errors[0]}"
            return "刷新完成，但接口没有返回 Ask 深度"
        return "刷新完成，页面数据已更新"
    if is_this_row and state.get("status") == "error":
        return f"刷新失败：{state.get('message') or state.get('error') or '-'}"
    return f"Ask快照：{_format_datetime_minute(row.get('ask_snapshot_time'))}"


def _consume_row_refresh_query(settings) -> None:
    raw_style = st.query_params.get("refresh_style")
    raw_size = st.query_params.get("refresh_size")
    if isinstance(raw_style, list):
        raw_style = raw_style[0] if raw_style else ""
    if isinstance(raw_size, list):
        raw_size = raw_size[0] if raw_size else ""
    style_no = normalize_style_no(raw_style) or str(raw_style or "").strip().upper()
    row_size = normalize_us_size(raw_size)
    if not style_no:
        return
    job_id = start_sync_job(
        [style_no],
        db_path_str=str(settings.db_path),
        include_sales=True,
        include_depth=True,
        include_size_endpoints=True,
        parallel_sync=False,
        max_workers=1,
        fee_rate=settings.estimated_seller_fee_rate,
        sales_fraction=settings.buy_depth_sales_fraction,
        row_refresh_style=style_no,
        row_refresh_size=row_size,
        reset_snapshot=True,
    )
    st.session_state["sync_notice"] = (
        f"正在直接请求 API 刷新 {style_no} / US {row_size or '-'}，完成后会写入新的 Ask 快照并重算。"
        if job_id else "当前已有刷新/同步任务在运行，等完成后再点。"
    )
    try:
        st.query_params.clear()
    except Exception:
        pass
    st.rerun()


def _render_opportunity_card(row: dict[str, Any], settings=None, *, refresh_disabled: bool = False) -> None:
    summary = _strategy_summary_from_row(row)
    reference = _reference_summary_from_row(row)
    rating = str(summary.get("评级") or "-")
    title = str(summary.get("商品名") or "-")
    style_no = str(summary.get("货号") or "-")
    size = str(summary.get("尺码") or "-")
    score = str(summary.get("分数") or "-")
    risk = str(reference.get("风险说明") or "-")
    plan = str(summary.get("默认买断策略") or summary.get("默认买断档") or "-")
    image_url = str(row.get("image_url") or "").strip()
    image_html = (
        f'<img class="opp-product-image" src="{escape(image_url, quote=True)}" alt="{escape(title, quote=True)}" loading="lazy">'
        if image_url
        else ""
    )
    raw_size = normalize_us_size(row.get("size"))
    refresh_status = _row_refresh_status(row)
    if refresh_disabled or not style_no or style_no == "-":
        refresh_control = '<span class="opp-refresh-disabled">实时刷新这一行</span>'
    else:
        refresh_href = f"?refresh_style={quote(style_no)}&refresh_size={quote(raw_size)}"
        refresh_control = f'<a class="opp-refresh-link" href="{refresh_href}">实时刷新这一行</a>'

    cells = [
        _html_cell("总利润", summary.get("总利润")),
        _html_cell("每双利润", summary.get("每双利润")),
        _html_cell("平台寄存数量", summary.get("平台寄存数量")),
        _html_cell("卖家挂售数量", summary.get("卖家挂售数量")),
        _html_cell("引用·Ask快照时间", summary.get("Ask快照时间"), wide=True),
        _html_cell("引用·最近成交", f"{reference.get('最近成交')} / {reference.get('距最近成交')}", wide=True),
        _html_cell("引用·发售日期 / 发售天数", f"{reference.get('发售日期')} / {reference.get('发售天数')}", wide=True),
    ]
    action_cells = [
        _html_cell("完整购买策略", plan, compact=True),
        _html_cell("购买双数", summary.get("买入双数")),
        _html_cell("预计卖完", summary.get("预计卖完")),
    ]
    price_cells = [
        _html_cell("加权均价", summary.get("加权均价")),
        _html_cell("建议卖价", summary.get("建议卖价")),
        _html_cell(
            "引用·7天 / 30天销量",
            (
                f"{reference.get('7天销量')} / {reference.get('30天销量')}\n"
                f"7均 {str(reference.get('7天成交均价')).replace('USD ', '')}\n"
                f"30均 {str(reference.get('30天成交均价')).replace('USD ', '')}"
            ),
        ),
        _html_cell("下一个ASK / 数量", summary.get("下一口价/数量")),
        _html_cell("相邻尺码最低Ask", summary.get("相邻尺码最低Ask")),
    ]

    html = f"""
    <div class="opp-card">
      <div class="opp-head">
        <div class="opp-title-layout">
          {image_html}
          <div class="opp-title-text">
            <div class="opp-title">{escape(style_no)} · {escape(size)} · {escape(title)}</div>
            <div class="opp-sub">分数 {escape(score)} · {escape(risk)}</div>
          </div>
        </div>
        <div class="rating-badge {_rating_class(rating)}">{escape(rating)}</div>
      </div>
      <div class="opp-grid opp-action-grid">
        {''.join(action_cells)}
      </div>
      <div class="opp-grid opp-price-grid">
        {''.join(price_cells)}
      </div>
      <div class="opp-grid">
        {''.join(cells)}
      </div>
      <div class="opp-refresh-row">
        {refresh_control}
        <div class="opp-refresh-status">{escape(refresh_status)}</div>
      </div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)


def page_opportunity_board(conn, settings) -> None:
    _ui_page_header(
        "今日机会",
        "当前页只处理 StockX 导入货号。当前批次会保留源文件和历史结果；只有当前源文件会参与自动刷新。",
        "StockX机会",
    )
    notice = st.session_state.pop("sync_notice", None)
    if notice:
        state = _sync_state_snapshot()
        if state.get("status") == "error":
            st.error(notice)
        else:
            st.success(notice)

    _consume_row_refresh_query(settings)

    signature = db_signature(settings.db_path)
    active_import_id, imported_row_count, imported_styles, imported_scope_sql = latest_stockx_import_scope(conn)
    imported_count = len(imported_styles)
    scored_count = conn.execute(f"SELECT COUNT(*) FROM opportunity_scores WHERE style_no IN ({imported_scope_sql})").fetchone()[0] or 0
    scored_styles = conn.execute(f"SELECT COUNT(DISTINCT style_no) FROM opportunity_scores WHERE style_no IN ({imported_scope_sql})").fetchone()[0] or 0
    product_styles = conn.execute(f"SELECT COUNT(DISTINCT style_no) FROM products WHERE style_no IN ({imported_scope_sql})").fetchone()[0] or 0
    missing_release_count = conn.execute(
        f"SELECT COUNT(*) FROM products WHERE style_no IN ({imported_scope_sql}) AND (release_date IS NULL OR TRIM(release_date) = '')"
    ).fetchone()[0] or 0
    active_import_style_set = set(imported_styles)
    incomplete_styles = [style for style in load_incomplete_stockx_skus(conn) if style in active_import_style_set]
    sync_state = _sync_state_snapshot()
    auto_status = _auto_hourly_status_snapshot(settings)
    job_running = sync_state.get("status") == "running" or bool(auto_status.get("running"))
    display_scored_styles: Any = scored_styles
    display_scored_count: Any = scored_count
    if job_running and product_styles > 0 and scored_count == 0:
        display_scored_styles = "本轮评分中"
        display_scored_count = "本轮评分中"
    _ui_status_cards(
        [
            ("当前批次", f"#{active_import_id}" if active_import_id else "-"),
            ("导入行 / 货号", f"{imported_row_count} / {imported_count}"),
            ("已接入商品", product_styles),
            ("已评分货号", display_scored_styles),
            ("已评分尺码", display_scored_count),
            ("待补跑", len(incomplete_styles)),
        ]
    )
    retry_now_ts = datetime.utcnow().timestamp()
    auto_retry_seconds = 5 * 60
    zero_retry_key = f"_opp_zero_score_recovery_ts_{active_import_id}"
    partial_retry_key = f"_opp_partial_resume_ts_{active_import_id}"
    zero_retry_due = (
        retry_now_ts - float(st.session_state.get(zero_retry_key, 0) or 0)
    ) >= auto_retry_seconds
    partial_retry_due = (
        retry_now_ts - float(st.session_state.get(partial_retry_key, 0) or 0)
    ) >= auto_retry_seconds
    if (
        active_import_id
        and product_styles > 0
        and scored_count == 0
        and not job_running
        and zero_retry_due
    ):
        st.session_state[zero_retry_key] = retry_now_ts
        if incomplete_styles:
            worker = start_stockx_full_sync_worker_process("zero_score_auto_resume")
            if worker.get("started"):
                st.session_state["sync_notice"] = (
                    f"检测到当前批次评分为0，已自动启动独立后台worker补跑 {len(incomplete_styles)} 个未完成货号；"
                    "本轮会分批写云端检查点。"
                )
                st.rerun()
            else:
                st.session_state["sync_notice"] = f"检测到评分为0，但后台worker未启动：{worker.get('reason') or '-'}"
        else:
            job_id = start_recompute_job(
                db_path_str=str(settings.db_path),
                fee_rate=settings.estimated_seller_fee_rate,
                sales_fraction=settings.buy_depth_sales_fraction,
            )
            if job_id:
                st.session_state["sync_notice"] = "检测到当前批次评分为0，已自动启动本地评分恢复。"
                st.rerun()
    if (
        active_import_id
        and incomplete_styles
        and scored_count > 0
        and not job_running
        and partial_retry_due
    ):
        st.session_state[partial_retry_key] = retry_now_ts
        worker = start_stockx_full_sync_worker_process("partial_auto_resume")
        if worker.get("started"):
            st.session_state["sync_notice"] = (
                f"检测到当前批次还有 {len(incomplete_styles)} 个未完成货号，"
                "已自动启动独立后台worker继续补跑；已有评分不会清空。"
            )
            st.rerun()
        else:
            st.session_state["sync_notice"] = f"检测到未完成货号，但后台worker未启动：{worker.get('reason') or '-'}"
    _frontend_auto_refresh(job_running, interval_seconds=8, key="opportunity_progress")

    setup_tabs = st.tabs(["当前源文件 / 上传", "任务 / 数据维护", "历史结果"])
    with setup_tabs[0]:
        _ui_section_label(
            "上传当前源文件",
            "支持 CSV / Excel / ZIP。新上传会归档旧源文件结果；当前源文件才会继续自动刷新。",
        )
        render_sku_upload_panel(conn, key_prefix="opportunity", default_source="manual")
    with setup_tabs[1]:
        auto_status = _render_auto_hourly_status(settings)
        run_counts = _current_sync_run_counts(conn, sync_state, imported_scope_sql)
        if run_counts:
            st.info(
                "本轮进度说明："
                f"已处理 {run_counts['completed']} 个货号；"
                f"其中 {run_counts['with_product']} 个已经有商品主数据；"
                f"{run_counts['with_score']} 个已经生成评分。"
                "上面的“已接入”只统计商品主数据总数，不等于本轮已处理数。"
            )
        st.caption(f"缺发售日期 {missing_release_count} 个；继续补跑只处理未完整接入或未评分货号。")
        refresh_cols = st.columns([1.45, 1.35, 1, 1.7])
        if refresh_cols[0].button("继续补跑未完成货号", type="primary", use_container_width=True, disabled=job_running or not incomplete_styles):
            job_id = start_sync_job(
                incomplete_styles,
                db_path_str=str(settings.db_path),
                include_sales=True,
                include_depth=True,
                include_size_endpoints=True,
                parallel_sync=True,
                max_workers=settings.sync_max_workers,
                fee_rate=settings.estimated_seller_fee_rate,
                sales_fraction=settings.buy_depth_sales_fraction,
                reset_snapshot=False,
            )
            st.session_state["sync_notice"] = (
                f"已开始继续补跑 {len(incomplete_styles)} 个未完成货号；已有快照不会被清空。"
                if job_id else "当前已有任务在运行。"
            )
            st.rerun()
        if refresh_cols[1].button("全量刷新StockX API并重算", use_container_width=True, disabled=job_running):
            if imported_styles:
                job_id = start_sync_job(
                    imported_styles,
                    db_path_str=str(settings.db_path),
                    include_sales=True,
                    include_depth=True,
                    include_size_endpoints=True,
                    parallel_sync=True,
                    max_workers=settings.sync_max_workers,
                    fee_rate=settings.estimated_seller_fee_rate,
                    sales_fraction=settings.buy_depth_sales_fraction,
                    reset_snapshot=True,
                )
                st.session_state["sync_notice"] = (
                    f"已开始全量刷新 StockX API：{len(imported_styles)} 个货号；旧Ask/Bid/成交/市场快照已按货号清理。"
                    if job_id else "当前已有任务在运行。"
                )
                st.rerun()
            else:
                st.warning("还没有导入 StockX 货号。")
        if refresh_cols[2].button("仅重算评分（本地快照）", use_container_width=True, disabled=job_running):
            job_id = start_recompute_job(
                db_path_str=str(settings.db_path),
                fee_rate=settings.estimated_seller_fee_rate,
                sales_fraction=settings.buy_depth_sales_fraction,
            )
            st.session_state["sync_notice"] = "已开始按本地快照重算评分；不会请求StockX接口。" if job_id else "当前已有任务在运行。"
            st.rerun()
        refresh_cols[3].caption("继续补跑只处理缺商品、缺尺码、缺Ask、缺成交或缺评分的货号；全量刷新会清旧快照后重跑全部。")
        render_live_sync_monitor()
    with setup_tabs[2]:
        render_opportunity_snapshot_history(conn, settings)

    if AUTO_FULL_REFRESH_FLAG.exists() and sync_state.get("status") != "running" and imported_styles:
        try:
            AUTO_FULL_REFRESH_FLAG.unlink()
        except OSError:
            pass
        job_id = start_sync_job(
            imported_styles,
            db_path_str=str(settings.db_path),
            include_sales=True,
            include_depth=True,
            include_size_endpoints=True,
            parallel_sync=True,
            max_workers=settings.sync_max_workers,
            fee_rate=settings.estimated_seller_fee_rate,
            sales_fraction=settings.buy_depth_sales_fraction,
            reset_snapshot=True,
        )
        st.session_state["auto_release_after_full_sync"] = bool(job_id)
        st.session_state["sync_notice"] = (
            f"已自动开始全量刷新 StockX API：{len(imported_styles)} 个货号；已先清旧快照，完成后会补发售日期并重算。"
            if job_id else "当前已有任务在运行。"
        )
        st.rerun()

    if st.session_state.get("auto_release_after_full_sync") and sync_state.get("status") == "done":
        st.session_state["auto_release_after_full_sync"] = False
        job_id = start_release_date_job(
            db_path_str=str(settings.db_path),
            timeout=int(settings.timeout),
            candidate_limit=5,
            allow_search=True,
            fee_rate=settings.estimated_seller_fee_rate,
            sales_fraction=settings.buy_depth_sales_fraction,
        )
        st.session_state["sync_notice"] = "已自动开始补齐发售日期并重算。" if job_id else "当前已有任务在运行。"
        st.rerun()

    _ui_section_label("筛选和结果", "首页默认显示最高买价 300 美金以内，并按预计卖完天数从少到多排序。")

    sort_options = {
        "预计卖完天数（少到多）": ("estimated_days_to_sell", False),
        "每双利润（高到低）": ("estimated_profit_per_pair", True),
        "总利润（高到低）": ("estimated_profit", True),
        "7天销量（高到低）": ("sales_7d", True),
        "30天销量（高到低）": ("sales_30d", True),
        "分数（高到低）": ("score", True),
    }
    if st.session_state.pop("_opp_reset_requested", False):
        _reset_opportunity_search_state()
    _ensure_opportunity_search_defaults(sort_options)

    history = _read_opportunity_search_history()
    mode_cols = st.columns([1.25, 1.35, 2.3])
    with mode_cols[0]:
        rating_scope = st.selectbox(
            "等级范围",
            ["只看可买等级", "显示全部等级"],
            index=1 if bool(st.session_state.get("opp_show_all_scope", False)) else 0,
            key="opp_rating_scope_select",
        )
        show_all = rating_scope == "显示全部等级"
        st.session_state["opp_show_all_scope"] = show_all
    with mode_cols[1]:
        release_scope = st.selectbox(
            "发售时间",
            ["包含发售不足90天", "排除发售不足90天"],
            index=0 if bool(st.session_state.get("opp_include_young_scope", True)) else 1,
            key="opp_release_scope_select",
        )
        include_young = release_scope == "包含发售不足90天"
        st.session_state["opp_include_young_scope"] = include_young
    mode_cols[2].caption("输入货号/尺码时会自动放开等级过滤。清空条件会恢复首页默认：最高买价 300 美金以下，按预计卖完天数少到多。")

    history_cols = st.columns([2.4, 0.9, 3.2])
    if history:
        history_options = {
            f"{index + 1}. {_opportunity_history_label(item)}": item
            for index, item in enumerate(history[:8])
        }
        selected_history = history_cols[0].selectbox("历史查询", list(history_options.keys()), key="opp_history_select")
        if history_cols[1].button("套用历史", use_container_width=True):
            _apply_opportunity_search_snapshot(history_options[selected_history])
            st.rerun()
    else:
        history_cols[0].caption("历史查询：暂无")
    history_cols[2].caption("清空后恢复首页默认：最高买价 300 美金以下，按预计卖完天数少到多。")

    filter_cols = st.columns([1.25, 0.75, 0.9, 0.9, 0.8, 0.75, 0.8, 0.75])
    filter_style_text = filter_cols[0].text_input("货号", placeholder="例如 HQ6998-200", key="opp_filter_style").strip()
    filter_size_text = normalize_us_size(
        filter_cols[1].text_input("US尺码", placeholder="例如 11", key="opp_filter_size").strip()
    )
    min_buy_price = optional_float(filter_cols[2].text_input("最低最高买价", placeholder="例如 120", key="opp_filter_min_buy"))
    max_buy_price = optional_float(
        filter_cols[3].text_input("最高买价不超过", placeholder="例如 180", key="opp_filter_max_buy")
    )
    release_days_op = filter_cols[4].selectbox("发售天数", ["不限", "大于等于", "小于等于"], index=0, key="opp_filter_release_op")
    release_days_value = optional_int(
        filter_cols[5].text_input("发售天数值", placeholder="例如 90", key="release_days_filter_value")
    )
    sell_days_op = filter_cols[6].selectbox("卖完天数", ["不限", "大于等于", "小于等于"], index=0, key="opp_filter_sell_op")
    sell_days_value = optional_float(
        filter_cols[7].text_input("卖完天数值", placeholder="例如 21", key="sell_days_filter_value")
    )
    sort_cols = st.columns([2.4, 0.9, 0.9])
    sort_label = sort_cols[0].selectbox("排序依据", list(sort_options.keys()), index=0, key="opp_sort_label")
    search_submitted = sort_cols[1].button("确定查询", type="primary", use_container_width=True, key="opp_search_submit")
    clear_submitted = sort_cols[2].button("清空条件", use_container_width=True, key="opp_search_clear")

    if clear_submitted:
        st.session_state["_opp_reset_requested"] = True
        st.rerun()
    if search_submitted:
        _save_opportunity_search_history(_opportunity_search_snapshot())

    sort_expr = sort_options[sort_label][0]
    direction = "DESC" if sort_options[sort_label][1] else "ASC"

    where = [f"style_no IN ({imported_scope_sql})"]
    params: list[Any] = []
    normalized_filter_style, parsed_filter_size = split_style_size_input(filter_style_text, filter_size_text)
    filter_size_text = parsed_filter_size or filter_size_text
    has_direct_lookup = bool(normalized_filter_style or filter_size_text)
    if not show_all and not has_direct_lookup:
        where.append("rating IN ('S', 'A', 'B+')")
        where.append("recommended_buy_qty > 0")
        where.append("COALESCE(estimated_profit, 0) > 0")
        where.append("COALESCE(estimated_profit_per_pair, 0) > 0")
        where.append("components_json LIKE '%\"has_full_ask_depth\": true%'")
    if not include_young:
        where.append("(release_days IS NULL OR release_days >= 90)")
    if normalized_filter_style:
        where.append("style_no LIKE ?")
        params.append(f"%{normalized_filter_style}%")
    if filter_size_text:
        where.append("size = ?")
        params.append(filter_size_text)
    if min_buy_price is not None:
        where.append("max_buy_price >= ?")
        params.append(min_buy_price)
    if max_buy_price is not None:
        where.append("max_buy_price <= ?")
        params.append(max_buy_price)
    if release_days_op != "不限" and release_days_value is not None:
        if release_days_op == "大于等于":
            where.append("release_days >= ?")
        else:
            where.append("release_days <= ?")
        params.append(release_days_value)
    if sell_days_op != "不限" and sell_days_value is not None:
        if sell_days_op == "大于等于":
            where.append("estimated_days_to_sell >= ?")
        else:
            where.append("estimated_days_to_sell <= ?")
        params.append(sell_days_value)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    order_sql = f"""
        ORDER BY
            CASE WHEN {_numeric_invalid_sql(sort_expr)} THEN 1 ELSE 0 END,
            {sort_expr} {direction},
            score DESC,
            {_profit_desc_sql('estimated_profit')}
    """
    export_rows = load_rows_cached(
        str(settings.db_path),
        signature,
        f"""
        {_opportunity_select_sql()}
        {where_sql}
        {order_sql}
        """,
        tuple(params),
    )
    rows = load_rows_cached(
        str(settings.db_path),
        signature,
        f"""
        {_opportunity_select_sql()}
        {where_sql}
        {order_sql}
        LIMIT 500
        """,
        tuple(params),
    )
    coverage_rows = load_imported_coverage_cached(str(settings.db_path), signature, active_import_id)
    if coverage_rows and (not export_rows or job_running):
        with st.expander(f"当前批次已接入货号覆盖（{len(coverage_rows)}）", expanded=not export_rows):
            coverage_frame = pd.DataFrame(coverage_rows).rename(
                columns={
                    "style_no": "货号",
                    "rank": "导入排名",
                    "title_hint": "商品名线索",
                    "import_rows": "导入行数",
                    "has_product_data": "已接入商品数据",
                    "scored_sizes": "已出机会尺码",
                    "best_rating": "最高评级",
                    "best_score": "最高分",
                    "status": "状态",
                    "last_computed_at": "最近计算",
                }
            )
            st.dataframe(coverage_frame, use_container_width=True, height=520, hide_index=True)
            st.caption("这里显示的是当前上传源文件的货号覆盖情况；已接入商品数据不会因为继续补跑而重头清空。")
    if not export_rows:
        st.selectbox("显示方式", ["卡片视图", "紧凑表格", "引用数据"], disabled=True, key="opp_display_mode_empty")
        st.info("当前筛选暂无机会评分。上方已显示当前批次已接入的货号覆盖；可以点「继续补跑未完成货号」补缺口，或点「仅重算评分」用已有快照生成机会。")
    else:
        export_frame = _opportunity_export_frame(export_rows)
        export_cols = st.columns([1.2, 4])
        export_cols[0].download_button(
            "导出筛选结果 CSV",
            data=_csv_download_bytes(export_frame),
            file_name=_export_file_name("today_opportunities_filtered"),
            mime="text/csv",
            use_container_width=True,
        )
        export_style_count = export_frame["货号"].nunique() if "货号" in export_frame.columns else 0
        display_style_count = len({str(row.get("style_no") or "") for row in rows if row.get("style_no")})
        export_cols[1].caption(
            f"导出范围：完整筛选结果 {len(export_frame)} 行 / {export_style_count} 个货号；"
            f"页面预览 {len(rows)} 行 / {display_style_count} 个货号。"
        )
        if has_direct_lookup:
            for row in rows[: min(20, len(rows))]:
                _render_opportunity_card(row, settings=settings, refresh_disabled=job_running)
        else:
            strategy_frame = pd.DataFrame([_strategy_summary_from_row(row) for row in rows])
            reference_frame = pd.DataFrame([_reference_summary_from_row(row) for row in rows])
            display_cols = st.columns([1.15, 4])
            display_mode = display_cols[0].selectbox("显示方式", ["卡片视图", "紧凑表格", "引用数据"], key="opp_display_mode")
            display_cols[1].caption("卡片视图看购买策略；紧凑表格适合批量筛选；引用数据只看接口/成交参考。")
            if display_mode == "卡片视图":
                max_visible = min(100, len(rows))
                if max_visible <= 5:
                    visible_count = max_visible
                else:
                    visible_count = st.slider(
                        "显示数量",
                        min_value=5,
                        max_value=max_visible,
                        value=min(20, max_visible),
                        step=5,
                    )
                for row in rows[:visible_count]:
                    _render_opportunity_card(row, settings=settings, refresh_disabled=job_running)
            elif display_mode == "紧凑表格":
                strategy_frame_compact = strategy_frame.rename(
                    columns={
                        "发售日期": "引用·发售日期",
                        "发售天数": "引用·发售天数",
                    }
                )
                reference_frame_compact = reference_frame.rename(
                    columns={
                    "7天销量": "引用·7天销量",
                    "30天销量": "引用·30天销量",
                    "最近成交": "引用·最近成交",
                        "距最近成交": "引用·距最近成交",
                        "发售日期": "引用·发售日期",
                        "发售天数": "引用·发售天数",
                    }
                )
                merged_frame = pd.concat(
                    [
                        strategy_frame_compact,
                        reference_frame_compact[
                            [
                                "引用·7天销量",
                                "引用·30天销量",
                                "引用·最近成交",
                                "风险说明",
                            ]
                        ],
                    ],
                    axis=1,
                )
                compact_cols = [
                    "评级",
                    "分数",
                    "货号",
                    "尺码",
                    "引用·发售天数",
                    "商品名",
                    "默认买断策略",
                    "买入双数",
                    "最低买价",
                    "最高买价",
                    "建议卖价",
                    "Ask快照时间",
                    "相邻尺码最低Ask",
                    "总利润",
                    "每双利润",
                    "预计卖完",
                "引用·7天销量",
                "引用·30天销量",
                "平台寄存数量",
                    "卖家挂售数量",
                ]
                compact_frame = merged_frame[[col for col in compact_cols if col in merged_frame.columns]].copy()
                st.dataframe(compact_frame, use_container_width=True, height=640, hide_index=True)
            else:
                st.dataframe(reference_frame, use_container_width=True, height=640, hide_index=True)

        st.subheader("查看一个尺码的购买策略")
        lookup_cols = st.columns([1.4, 1, 0.8])
        default_row = rows[0]
        strategy_style_input = lookup_cols[0].text_input(
            "货号",
            value=str(st.session_state.get("opportunity_strategy_style_no") or default_row["style_no"]),
            placeholder="例如 HQ6998-200",
            key="opportunity_strategy_style_input",
        )
        strategy_size_input = lookup_cols[1].text_input(
            "US 尺码",
            value=str(st.session_state.get("opportunity_strategy_size") or normalize_us_size(default_row["size"])),
            placeholder="例如 11 或 12.5",
            key="opportunity_strategy_size_input",
        )
        if lookup_cols[2].button("查看策略", use_container_width=True):
            cleaned_style, cleaned_size = split_style_size_input(strategy_style_input, strategy_size_input)
            st.session_state["opportunity_strategy_style_no"] = cleaned_style
            st.session_state["opportunity_strategy_size"] = cleaned_size

        query_style, query_size = split_style_size_input(
            st.session_state.get("opportunity_strategy_style_no") or strategy_style_input,
            st.session_state.get("opportunity_strategy_size") or strategy_size_input,
        )
        chosen_rows = load_rows_cached(
            str(settings.db_path),
            signature,
            f"""
            {_opportunity_select_sql()}
            WHERE style_no = ? AND size = ?
            LIMIT 1
            """,
            (query_style, query_size),
        )
        if chosen_rows:
            chosen_row = chosen_rows[0]
            detail_snapshot = load_detail_snapshot_cached(
                str(settings.db_path),
                signature,
                str(chosen_row["style_no"]),
                normalize_us_size(chosen_row["size"]),
                settings.estimated_seller_fee_rate,
                settings.buy_depth_sales_fraction,
            )
            options = detail_snapshot.get("strategy_options") or [_option_from_score_row(chosen_row)]
            st.subheader(f"默认策略：{chosen_row['style_no']} / {us_size(chosen_row['size'])}")
            for index, option in enumerate(options[:3]):
                suffix = "（推荐）" if index == 0 else f"（备选 {index}）"
                _render_strategy_option(option, expanded=index == 0, title_suffix=suffix)
        else:
            st.info(f"没有找到 {query_style} / {us_size(query_size)} 的评分结果。先补齐接口并重算，或只输入已有评分的尺码。")

    with st.expander(f"导入货号覆盖情况（{imported_count}）", expanded=False):
        imported_rows = load_imported_coverage_cached(str(settings.db_path), signature, active_import_id)
        if imported_rows:
            coverage_frame = pd.DataFrame(imported_rows)
            coverage_frame = coverage_frame.rename(
                columns={
                    "style_no": "货号",
                    "rank": "导入排名",
                    "title_hint": "商品名线索",
                    "import_rows": "导入行数",
                    "has_product_data": "已接入商品数据",
                    "scored_sizes": "已出机会尺码",
                    "best_rating": "最高评级",
                    "best_score": "最高分",
                    "status": "状态",
                    "last_computed_at": "最近计算",
                }
            )
            st.dataframe(coverage_frame, use_container_width=True, height=620)
        else:
            st.info("暂无导入货号。")


def page_opportunities(conn, settings) -> None:
    tabs = st.tabs(["机会列表", "单尺码详情", "同货号组合", "任务 / 数据维护"])
    with tabs[0]:
        page_opportunity_board(conn, settings)
    with tabs[1]:
        page_detail(conn, settings)
    with tabs[2]:
        page_style_bundles(conn, settings)
    with tabs[3]:
        page_data_maintenance(conn, settings)


def page_style_bundles(conn, settings) -> None:
    st.markdown("### 同货号组合")
    signature = db_signature(settings.db_path)
    rows = load_rows_cached(
        str(settings.db_path),
        signature,
        f"""
        {_opportunity_select_sql()}
        WHERE COALESCE(recommended_buy_qty, 0) > 0
          AND NOT ({_numeric_invalid_sql('estimated_profit')})
          AND CAST(estimated_profit AS REAL) > 0
        ORDER BY {_profit_desc_sql('estimated_profit')}, score DESC
        LIMIT 2000
        """,
        tuple(),
    )
    bundle_frame = pd.DataFrame(_style_bundle_summaries(rows, limit=None))
    if bundle_frame.empty:
        st.info("暂无可组合的同货号套利结果。先同步接口并重算评分。")
        return

    filter_cols = st.columns([1, 1, 4])
    style_text = normalize_style_no(filter_cols[0].text_input("货号筛选", placeholder="例如 HQ6998-200").strip())
    min_profit = optional_float(filter_cols[1].text_input("组合总利润不低于", placeholder="例如 200"))
    display_frame = bundle_frame.copy()
    if style_text:
        display_frame = display_frame[display_frame["货号"].astype(str).str.contains(style_text, case=False, na=False)]
    if min_profit is not None:
        display_frame = display_frame[
            display_frame["组合总利润"].apply(lambda value: optional_float(str(value).replace("USD", "")) or 0.0) >= min_profit
        ]

    st.dataframe(display_frame, use_container_width=True, height=680, hide_index=True)


def page_data_maintenance(conn, settings) -> None:
    st.markdown("### 数据维护 / 实时查价")
    notice = st.session_state.pop("sync_notice", None)
    if notice:
        state = _sync_state_snapshot()
        if state.get("status") == "error":
            st.error(notice)
        else:
            st.success(notice)

    signature = db_signature(settings.db_path)
    imported_styles = load_skus_cached(str(settings.db_path), signature)
    sync_state = _sync_state_snapshot()
    job_running = sync_state.get("status") == "running"

    render_live_sync_monitor()

    imported_count = len(imported_styles)
    scored_styles = conn.execute("SELECT COUNT(DISTINCT style_no) FROM opportunity_scores").fetchone()[0] or 0
    product_styles = conn.execute("SELECT COUNT(DISTINCT style_no) FROM products").fetchone()[0] or 0
    missing_release_count = conn.execute(
        "SELECT COUNT(*) FROM products WHERE release_date IS NULL OR TRIM(release_date) = ''"
    ).fetchone()[0] or 0
    pending_styles = max(imported_count - scored_styles, 0)
    st.caption(
        f"导入 {imported_count} 个货号；已接入 {product_styles} 个；已有评分 {scored_styles} 个；"
        f"待补完整评分 {pending_styles} 个；缺发售日期 {missing_release_count} 个。"
    )

    probe_results = st.session_state.get("live_api_probe_results")
    release_backfill_allow_search = st.toggle("联网补齐发售日期", value=False)
    st.caption("默认只用本地缓存和已保存接口原文；勾选后才会联网补齐。")

    action_cols = st.columns(4)
    if action_cols[0].button("手动实时查价样本", use_container_width=True, disabled=job_running):
        st.session_state.pop("live_api_probe_results", None)
        probe_results = _run_live_api_probe(settings)
    if action_cols[1].button("????????", use_container_width=True):
        GOAT_RESCORE_REQUEST_PATH.parent.mkdir(parents=True, exist_ok=True)
        GOAT_RESCORE_REQUEST_PATH.write_text(
            json.dumps(
                {
                    "source_name": "goat_rescore",
                    "requested_at": datetime.utcnow().isoformat(timespec="seconds"),
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        st.session_state["goat_notice"] = "???GOAT?????StockX???????GOAT??????StockX?"
        st.rerun()
    if action_cols[2].button("补发售日期", use_container_width=True, disabled=job_running):
        job_id = start_release_date_job(
            db_path_str=str(settings.db_path),
            timeout=int(settings.timeout),
            candidate_limit=5,
            allow_search=release_backfill_allow_search,
            fee_rate=settings.estimated_seller_fee_rate,
            sales_fraction=settings.buy_depth_sales_fraction,
        )
        st.session_state["sync_notice"] = "已开始补齐发售日期，完成后会自动重算。" if job_id else "当前已有任务在运行。"
        st.rerun()
    if action_cols[3].button("全量刷新StockX并重算", use_container_width=True, disabled=job_running):
        if imported_styles:
            job_id = start_sync_job(
                imported_styles,
                db_path_str=str(settings.db_path),
                include_sales=True,
                include_depth=True,
                include_size_endpoints=True,
                parallel_sync=True,
                max_workers=settings.sync_max_workers,
                fee_rate=settings.estimated_seller_fee_rate,
                sales_fraction=settings.buy_depth_sales_fraction,
                reset_snapshot=True,
            )
            st.session_state["sync_notice"] = (
                f"已开始全量刷新 {len(imported_styles)} 个 StockX 货号；旧快照会先清理，完成后自动重算。"
                if job_id else "当前已有任务在运行。"
            )
            st.rerun()
        else:
            st.warning("还没有导入货号。")

    if probe_results:
        probe_frame = pd.DataFrame(
            [
                {
                    "货号": item.get("style_no"),
                    "US 尺码": item.get("size"),
                    "实时最低价": money(item.get("live_lowest_ask")),
                    "尺码查询": "OK" if item.get("product_size_uuid") else "失败",
                    "ASK 查询": "OK" if item.get("ask_ok") else "失败",
                    "说明": item.get("ask_error")
                    or item.get("detail_error")
                    or item.get("search_US_error")
                    or item.get("search_HK_error")
                    or item.get("error")
                    or "-",
                }
                for item in probe_results
            ]
        )
        st.dataframe(probe_frame, use_container_width=True, hide_index=True)


def page_detail(conn, settings) -> None:
    st.title("单尺码详情")
    st.caption("只填货号可以看该货号下所有 US 尺码；填货号 + 尺码会显示具体购买策略。")

    last_style_no = normalize_style_no(st.session_state.get("detail_style_no", "")) or str(
        st.session_state.get("detail_style_no", "")
    ).strip().upper()
    last_size = str(st.session_state.get("detail_size", "")).strip()

    with st.form("detail_lookup_form", clear_on_submit=False):
        cols = st.columns([2, 1])
        style_no_input = cols[0].text_input("货号（STYLE NO）", value=last_style_no, placeholder="例如 HQ6998-200")
        size_input = cols[1].text_input("US 尺码（可留空）", value=last_size, placeholder="例如 11 或 12.5")
        submitted = st.form_submit_button("确认查询", use_container_width=True)

    if submitted:
        style_no, size = split_style_size_input(style_no_input, size_input)
        st.session_state["detail_style_no"] = style_no
        st.session_state["detail_size"] = size
        if not style_no:
            st.error("货号不能为空。")
            return
    else:
        style_no = last_style_no
        size = normalize_us_size(last_size)
        if not style_no:
            st.info("先输入货号，再点确认查询。")
            return

    st.success(f"当前查询：货号 {style_no} / 尺码 {us_size(size) if size else '全部 US 尺码'}")

    def fmt_dt(value: Any) -> str:
        if value in (None, ""):
            return "-"
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return str(value)
        return parsed.strftime("%Y-%m-%d %H:%M")

    signature = db_signature(settings.db_path)
    if not size:
        all_sizes = load_rows_cached(
            str(settings.db_path),
            signature,
            """
            SELECT
                rating,
                ROUND(score, 1) AS score,
                style_no,
                title,
                size,
                recommended_buy_qty,
                max_buy_price,
                weighted_avg_cost,
                target_sell_price_low,
                target_sell_price_high,
                ROUND(estimated_profit, 2) AS estimated_profit,
                ROUND(estimated_profit_per_pair, 2) AS estimated_profit_per_pair,
                ROUND(estimated_days_to_sell, 1) AS estimated_days_to_sell,
                sales_7d,
                sales_30d,
                last_sale_at,
                last_sale_days,
                risk_notes,
                release_date,
                release_days,
                components_json,
                computed_at
            FROM opportunity_scores
            WHERE style_no = ?
            ORDER BY
                {_profit_desc_sql('estimated_profit_per_pair')},
                score DESC
            """,
            (style_no,),
        )
        if not all_sizes:
            st.warning("这个货号还没有本地评分数据。请先在「SKU 导入 / 同步」补齐接口并重算。")
            return
        st.subheader("全部 US 尺码结果")
        st.dataframe(pd.DataFrame([_strategy_summary_from_row(row) for row in all_sizes]), use_container_width=True, height=420)
        st.info("要看某一个尺码的完整购买策略，请在上方输入 US 尺码后点击“确认查询”。")
        return

    snapshot = load_detail_snapshot_cached(
        str(settings.db_path),
        signature,
        style_no,
        size,
        settings.estimated_seller_fee_rate,
        settings.buy_depth_sales_fraction,
    )
    sales = snapshot["sales"]
    asks = snapshot["asks"]
    bids = snapshot["bids"]
    reference_price = snapshot.get("reference_price")
    strategy_options = snapshot.get("strategy_options") or []

    summary_cols = st.columns(7)
    summary_cols[0].metric("当前最低 Ask", money(asks[0]["ask_price"] if asks else None))
    summary_cols[1].metric("7天销量", sales["sales_7d"])
    summary_cols[2].metric("14天销量", sales["sales_14d"])
    summary_cols[3].metric("30天销量", sales["sales_30d"])
    summary_cols[4].metric("成交中位数", money(sales["median"]))
    summary_cols[5].metric("75 / 90 分位", f"{money(sales['p75'])} / {money(sales['p90'])}")
    summary_cols[6].metric("GOAT/参考价", money(reference_price))
    st.caption(f"最近成交：{fmt_dt(sales.get('last_sale_at'))}；距今：{_format_days(sales.get('last_sale_days'))}")

    with st.expander("GOAT / 手动参考价", expanded=False):
        current_reference = float(reference_price or 0.0)
        with st.form("reference_price_form", clear_on_submit=False):
            ref_cols = st.columns([1, 1, 2])
            ref_price = ref_cols[0].number_input("参考价（USD）", min_value=0.0, value=current_reference, step=1.0, format="%.2f")
            ref_source = ref_cols[1].selectbox("来源", ["manual", "GOAT"], index=0)
            ref_note = ref_cols[2].text_input("备注", value="GOAT reference")
            saved = st.form_submit_button("保存参考价", use_container_width=True)
        if saved:
            upsert_reference_price(
                conn,
                style_no=style_no,
                size=size or None,
                source_name=ref_source,
                price=float(ref_price),
                currency="USD",
                note=ref_note or None,
                raw_json={"style_no": style_no, "size": size, "source": ref_source, "note": ref_note},
            )
            conn.commit()
            bump_data_cache_version()
            st.success("参考价已保存。")
            st.rerun()

    st.subheader("三种操作策略")
    if strategy_options:
        for index, option in enumerate(strategy_options[:3]):
            suffix = "（推荐）" if index == 0 else f"（备选 {index}）"
            _render_strategy_option(option, expanded=index == 0, title_suffix=suffix)
    else:
        st.info("暂无可执行策略，通常是缺少 Ask 深度或成交数据不足。")

    with st.expander("接口引用数据 / 原始 JSON", expanded=False):
        left, right = st.columns(2)
        with left:
            st.subheader("Ask 价格 + 数量分布")
            ask_frame = pd.DataFrame(asks)
            if not ask_frame.empty:
                ask_frame = ask_frame.sort_values("ask_price", ascending=True).reset_index(drop=True)
                lowest_ask = float(ask_frame["ask_price"].iloc[0])
                ask_slice = ask_frame[ask_frame["ask_price"] <= lowest_ask + 50].copy()
                ask_slice["累计数量"] = ask_slice["ask_quantity"].cumsum()
                ask_slice["累计成本"] = (ask_slice["ask_price"] * ask_slice["ask_quantity"]).cumsum()
                ask_slice["累计均价"] = ask_slice["累计成本"] / ask_slice["累计数量"]
                ask_display = ask_slice[
                    ["ask_price", "ask_quantity", "service_level", "is_consigned", "累计数量", "累计成本", "累计均价", "snapshot_time"]
                ].copy()
                for col in ["ask_price", "累计成本", "累计均价"]:
                    ask_display[col] = ask_display[col].apply(money)
                ask_display = ask_display.rename(
                    columns={
                        "ask_price": "Ask 价格（USD）",
                        "ask_quantity": "数量",
                        "service_level": "服务等级",
                        "is_consigned": "平台寄存",
                        "snapshot_time": "抓取时间",
                    }
                )
                st.dataframe(ask_display, use_container_width=True, height=320)
                if len(ask_frame) > len(ask_slice):
                    st.caption(f"仅展示最低价 +50 美元内的 {len(ask_slice)} 个档位，共 {len(ask_frame)} 个档位。")
            else:
                st.info("暂无 Ask 深度。")
        with right:
            st.subheader("Bid 价格 + 数量分布")
            bid_frame = pd.DataFrame(bids)
            if not bid_frame.empty:
                bid_display = bid_frame[["bid_price", "bid_quantity", "snapshot_time"]].copy()
                bid_display["bid_price"] = bid_display["bid_price"].apply(money)
                bid_display = bid_display.rename(
                    columns={"bid_price": "Bid 价格（USD）", "bid_quantity": "数量", "snapshot_time": "抓取时间"}
                )
                st.dataframe(bid_display, use_container_width=True, height=300)
            else:
                st.info("暂无 Bid 深度。")

        st.subheader("原始接口 JSON")
        raw_rows = snapshot["raw_rows"]
        if not raw_rows:
            st.info("没有匹配到该货号的原始接口记录。")
        else:
            raw_labels = [f"{row['fetched_at']} {row['endpoint']}" for row in raw_rows]
            chosen = st.selectbox("原始记录", raw_labels)
            raw_row = raw_rows[raw_labels.index(chosen)]
            if raw_row["error_message"]:
                st.error(raw_row["error_message"])
            st.json(json_loads(raw_row["response_json"], {}))

    st.subheader("风险说明")
    score_row = snapshot["score_row"]
    if score_row:
        st.write(score_row["risk_notes"])
        with st.expander("评分拆解"):
            st.json(json_loads(score_row["components_json"], {}))
    else:
        st.write("尚未生成评分。")


def page_import_sync(conn, settings) -> None:
    st.title("SKU 导入 / 同步")
    st.caption("支持 CSV / XLSX，多 sheet 会自动读取；STYLE NO 列会作为正确货号优先使用，尺码统一按 US。")
    render_live_sync_monitor()

    render_sku_upload_panel(conn, key_prefix="sku_import", default_source="manual")

    signature = db_signature(settings.db_path)
    imported_skus = load_skus_cached(str(settings.db_path), signature)
    st.subheader(f"已导入货号（{len(imported_skus)}）")
    if imported_skus:
        sync_mode = st.radio("同步范围", ["全部导入货号", "只同步前 N 个", "手动输入货号"], horizontal=True)
        if sync_mode == "全部导入货号":
            selected_skus = imported_skus
        elif sync_mode == "只同步前 N 个":
            limit = st.number_input("N", min_value=1, max_value=max(1, len(imported_skus)), value=min(20, len(imported_skus)), step=1)
            selected_skus = imported_skus[: int(limit)]
        else:
            manual = st.text_area("手动输入货号，一行一个", value="")
            selected_skus = [normalize_style_no(item) or item.strip().upper() for item in manual.splitlines() if item.strip()]

        st.info(f"本次将同步 {len(selected_skus)} 个货号。")
        opts = st.columns(3)
        include_sales = opts[0].checkbox("抓取成交历史", value=True)
        include_depth = opts[1].checkbox("抓取 Ask / Bid 深度", value=True)
        include_size_endpoints = opts[2].checkbox("逐尺码接口补抓", value=True)
        if st.button("开始同步选中货号", type="primary", use_container_width=True, disabled=_sync_state_snapshot().get("status") == "running"):
            job_id = start_sync_job(
                selected_skus,
                db_path_str=str(settings.db_path),
                include_sales=include_sales,
                include_depth=include_depth,
                include_size_endpoints=include_size_endpoints,
                parallel_sync=True,
                max_workers=settings.sync_max_workers,
                fee_rate=settings.estimated_seller_fee_rate,
                sales_fraction=settings.buy_depth_sales_fraction,
                reset_snapshot=True,
            )
            st.session_state["sync_notice"] = "已开始刷新StockX API，页面会自动刷新进度。" if job_id else "当前已有任务在运行。"
            st.rerun()

        summary = load_import_summary_cached(str(settings.db_path), signature)
        if summary:
            st.dataframe(pd.DataFrame(summary), use_container_width=True, height=320)
    else:
        st.info("还没有导入任何货号。")


def _page_goat_consignment_selection_legacy(conn, settings) -> None:
    st.markdown("### GOAT寄存选品")
    st.caption("导入 GOAT 洛杉矶 S 仓寄存库存；购买成本 = GOAT价格 + 6 美元；StockX 出售回款按出售价扣 3%。")

    upload_cols = st.columns([1.3, 1, 1])
    uploaded = upload_cols[0].file_uploader("上传GOAT寄存CSV/XLSX", type=["csv", "xlsx"], key="goat_consignment_upload")
    source_name = upload_cols[1].text_input("批次名称", value="goat_slover")
    if uploaded is not None and upload_cols[2].button("导入并评分", type="primary", use_container_width=True):
        frame = _read_goat_consignment_frame(uploaded.name, uploaded.getvalue())
        imported = _import_goat_consignment_rows(conn, frame, source_name=source_name)
        computed = _compute_goat_consignment_scores(conn)
        conn.commit()
        bump_data_cache_version()
        st.success(f"导入 {imported} 行，已评分 {computed} 行。")
        st.rerun()

    if GOAT_DEFAULT_CONSIGNMENT_PATH.exists():
        if st.button("导入当前微信收到的S仓CSV并评分", use_container_width=True):
            content = GOAT_DEFAULT_CONSIGNMENT_PATH.read_bytes()
            frame = _read_goat_consignment_frame(GOAT_DEFAULT_CONSIGNMENT_PATH.name, content)
            imported = _import_goat_consignment_rows(conn, frame, source_name="goat_slover_wechat")
            computed = _compute_goat_consignment_scores(conn)
            conn.commit()
            bump_data_cache_version()
            st.success(f"导入 {imported} 行，已评分 {computed} 行。")
            st.rerun()

    action_cols = st.columns([1, 1, 3])
    if action_cols[0].button("仅重新评分", use_container_width=True):
        computed = _compute_goat_consignment_scores(conn)
        conn.commit()
        bump_data_cache_version()
        st.success(f"已重新评分 {computed} 行。")
        st.rerun()
    if action_cols[1].button("清空GOAT清单", use_container_width=True):
        conn.execute("DELETE FROM goat_consignment_scores")
        conn.execute("DELETE FROM goat_consignment_items")
        conn.commit()
        bump_data_cache_version()
        st.warning("已清空GOAT寄存清单。")
        st.rerun()

    total_items = conn.execute("SELECT COUNT(*) FROM goat_consignment_items").fetchone()[0] or 0
    scored_items = conn.execute("SELECT COUNT(*) FROM goat_consignment_scores").fetchone()[0] or 0
    st.caption(f"当前GOAT寄存清单 {total_items} 行；已评分 {scored_items} 行。")

    filter_cols = st.columns([1, 0.7, 0.8, 0.8, 0.8, 1.2])
    style_filter = normalize_style_no(filter_cols[0].text_input("货号筛选", placeholder="例如 DD1391-100"))
    size_filter = normalize_us_size(filter_cols[1].text_input("US尺码", placeholder="例如 8.5"))
    max_cost = optional_float(filter_cols[2].text_input("成本不超过", placeholder="例如 100"))
    min_profit = optional_float(filter_cols[3].text_input("利润不低于", placeholder="例如 10"))
    max_days = optional_float(filter_cols[4].text_input("售罄不超过", placeholder="例如 14"))
    sort_label = filter_cols[5].selectbox(
        "排序依据",
        ["综合分（高到低）", "预计售罄（少到多）", "预估利润（高到低）", "7天销量（高到低）", "成本（低到高）"],
    )

    where: list[str] = []
    params: list[Any] = []
    if style_filter:
        where.append("g.style_no LIKE ?")
        params.append(f"%{style_filter}%")
    if size_filter:
        _append_goat_size_filter(where, params, size_filter)
    if max_cost is not None:
        where.append("g.buy_cost <= ?")
        params.append(max_cost)
    if min_profit is not None:
        where.append("COALESCE(g.estimated_profit, -999999) >= ?")
        params.append(min_profit)
    if max_days is not None:
        where.append(
            "g.estimated_days_to_sell IS NOT NULL "
            "AND CAST(g.estimated_days_to_sell AS REAL) >= 0 "
            "AND CAST(g.estimated_days_to_sell AS REAL) <= ?"
        )
        params.append(max_days)
    sort_sql = {
        "综合分（高到低）": f"g.score DESC, {_goat_days_asc_sql()}",
        "预计售罄（少到多）": _goat_days_asc_sql(),
        "预估利润（高到低）": f"{_profit_desc_sql('g.estimated_profit')}, g.score DESC",
        "7天销量（高到低）": "g.sales_7d DESC, g.score DESC",
        "成本（低到高）": "g.buy_cost ASC, g.score DESC",
    }[sort_label]
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    rows = query_rows(
        conn,
        f"""
        SELECT
            g.rating,
            ROUND(g.score, 1) AS score,
            g.pid,
            g.style_no,
            g.size AS goat_size,
            g.matched_stockx_size,
            g.title,
            g.goat_price,
            g.buy_cost,
            g.stockx_lowest_ask,
            g.avg_7d,
            g.avg_30d,
            g.sales_7d,
            g.sales_30d,
            g.estimated_sell_price,
            g.estimated_profit,
            ROUND(g.estimated_profit_rate * 100, 1) AS estimated_profit_rate_pct,
            g.estimated_days_to_sell,
            g.ask_snapshot_time,
            g.risk_notes,
            g.computed_at
        FROM goat_consignment_scores g
        {where_sql}
        ORDER BY {sort_sql}
        LIMIT 1000
        """,
        tuple(params),
    )
    if not rows:
        st.info("暂无GOAT寄存评分数据。先导入S仓CSV并评分。")
        return

    frame = pd.DataFrame([dict(row) for row in rows])
    frame = frame.rename(
        columns={
            "rating": "评级",
            "score": "分数",
            "pid": "PID",
            "style_no": "货号",
            "goat_size": "GOAT尺码",
            "matched_stockx_size": "匹配StockX尺码",
            "title": "商品名",
            "goat_price": "GOAT价格",
            "buy_cost": "采购成本(+6)",
            "stockx_lowest_ask": "引用·StockX最低Ask",
            "avg_7d": "引用·7日成交均价",
            "avg_30d": "引用·30日成交均价",
            "sales_7d": "引用·7天销量",
            "sales_30d": "引用·30天销量",
            "estimated_sell_price": "默认出售价",
            "estimated_profit": "预估利润",
            "estimated_profit_rate_pct": "预估利润率%",
            "estimated_days_to_sell": "预估售罄天数",
            "ask_snapshot_time": "引用·Ask快照时间",
            "risk_notes": "风险说明",
            "computed_at": "评分时间",
        }
    )
    money_cols = ["GOAT价格", "采购成本(+6)", "引用·StockX最低Ask", "引用·7日成交均价", "引用·30日成交均价", "默认出售价", "预估利润"]
    for col in money_cols:
        if col in frame.columns:
            frame[col] = frame[col].apply(money)
    for col in ["预估售罄天数"]:
        if col in frame.columns:
            frame[col] = frame[col].apply(_format_goat_sellout_days)
    for col in ["引用·Ask快照时间", "评分时间"]:
        if col in frame.columns:
            frame[col] = frame[col].apply(_format_datetime_minute)

    frame = _format_goat_sellout_columns(frame)
    st.dataframe(frame, use_container_width=True, height=720, hide_index=True)


def _legacy_page_goat_consignment_selection_unused(conn, settings) -> None:
    st.markdown("### GOAT寄存选品")
    st.caption("导入 GOAT 洛杉矶 S 仓寄存库存；购买成本 = GOAT价格 + 6 美元；StockX 出售回款按出售价扣 3%。")

    state = _goat_job_snapshot()
    worker_state = _read_goat_stockx_worker_marker()
    if worker_state.get("job_id"):
        worker_status = worker_state.get("status")
        worker_progress = float(worker_state.get("progress") or 0.0)
        st.progress(min(1.0, max(0.0, worker_progress)))
        worker_current = ""
        if worker_state.get("current_style") or worker_state.get("current_size"):
            worker_current = f"；当前：{worker_state.get('current_style') or '-'} US {worker_state.get('current_size') or '-'}"
        st.caption(f"{worker_state.get('phase') or 'GOAT补StockX'}：{worker_state.get('message') or ''}{worker_current}")
        if worker_status == "running":
            st.info("GOAT清单正在独立补StockX接口；前端只显示进度，不负责长时间运算。")
        elif worker_status == "error":
            st.error(worker_state.get("message") or worker_state.get("error") or "GOAT清单补StockX失败")
        elif worker_status == "done":
            st.success(worker_state.get("message") or "GOAT清单补StockX完成")
    job_running = state.get("status") == "running"
    if state.get("job_id"):
        progress_value = float(state.get("progress") or 0.0)
        st.progress(min(1.0, max(0.0, progress_value)))
        status_text = state.get("message") or ""
        current_text = ""
        if state.get("current_style") or state.get("current_size"):
            current_text = f"；当前：{state.get('current_style') or '-'} US {state.get('current_size') or '-'}"
        st.caption(f"{state.get('phase') or '状态'}：{status_text}{current_text}")
        if job_running:
            st.info("GOAT后台任务运行中。页面不会自动刷新；需要看最新进度时手动刷新浏览器。")
        elif state.get("status") == "done":
            if st.session_state.get("goat_last_seen_job") != state.get("job_id"):
                bump_data_cache_version()
                st.session_state["goat_last_seen_job"] = state.get("job_id")
            st.success(state.get("message") or "GOAT寄存评分完成")
        elif state.get("status") == "error":
            st.error(state.get("message") or state.get("error") or "GOAT寄存评分失败")

    if st.session_state.get("goat_notice"):
        st.info(st.session_state["goat_notice"])

    upload_cols = st.columns([1.4, 1, 1])
    uploaded = upload_cols[0].file_uploader("上传GOAT寄存CSV/XLSX", type=["csv", "xlsx"], key="goat_consignment_upload")
    source_name = upload_cols[1].text_input("批次名称", value="goat_slover")
    if uploaded is not None and upload_cols[2].button("导入并后台评分", type="primary", use_container_width=True, disabled=job_running):
        GOAT_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", uploaded.name)
        saved_path = GOAT_UPLOAD_DIR / f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{safe_name}"
        saved_path.write_bytes(uploaded.getvalue())
        job_id = start_goat_consignment_job(
            db_path_str=str(settings.db_path),
            file_path_str=str(saved_path),
            source_name=source_name or "goat_slover",
            import_file=True,
            replace_current=True,
            live_refresh_missing=True,
        )
        st.session_state["goat_notice"] = f"GOAT后台任务已启动：{job_id}" if job_id else "GOAT后台任务没有启动：当前已有任务在运行。"
        st.rerun()

    action_cols = st.columns([1, 1, 1, 2])
    if GOAT_DEFAULT_CONSIGNMENT_PATH.exists():
        if action_cols[0].button("导入微信S仓表并评分", use_container_width=True, disabled=job_running):
            job_id = start_goat_consignment_job(
                db_path_str=str(settings.db_path),
                file_path_str=str(GOAT_DEFAULT_CONSIGNMENT_PATH),
                source_name="goat_slover_wechat",
                import_file=True,
                replace_current=True,
                live_refresh_missing=True,
            )
            st.session_state["goat_notice"] = f"GOAT后台任务已启动：{job_id}" if job_id else "GOAT后台任务没有启动：当前已有任务在运行。"
            st.rerun()
    if action_cols[1].button("补缺失快照并重评", use_container_width=True, disabled=job_running):
        job_id = start_goat_consignment_job(
            db_path_str=str(settings.db_path),
            source_name="goat_rescore",
            import_file=False,
            replace_current=False,
            live_refresh_missing=True,
        )
        st.session_state["goat_notice"] = f"GOAT后台任务已启动：{job_id}" if job_id else "GOAT后台任务没有启动：当前已有任务在运行。"
        st.rerun()
    if action_cols[2].button("清空GOAT清单", use_container_width=True, disabled=job_running):
        conn.execute("DELETE FROM goat_consignment_scores")
        conn.execute("DELETE FROM goat_consignment_items")
        conn.commit()
        bump_data_cache_version()
        st.warning("已清空GOAT寄存清单。")
        st.rerun()

    total_items = conn.execute("SELECT COUNT(*) FROM goat_consignment_items").fetchone()[0] or 0
    scored_items = conn.execute("SELECT COUNT(*) FROM goat_consignment_scores").fetchone()[0] or 0
    st.caption(f"当前GOAT寄存清单 {total_items} 行；已评分 {scored_items} 行。")

    sort_options = ["预估利润（高到低）", "预估售罄（少到多）", "综合分（高到低）", "7天销量（高到低）", "成本（低到高）"]
    if st.session_state.get("goat_sort_label") not in sort_options:
        st.session_state["goat_sort_label"] = sort_options[0]

    with st.form("goat_consignment_filter_form"):
        filter_cols = st.columns([1, 0.7, 0.8, 0.8, 0.8, 1.2])
        style_input = filter_cols[0].text_input("货号", value=st.session_state.get("goat_filter_style", ""), placeholder="例如 DD1391-100")
        size_input = filter_cols[1].text_input("US尺码", value=st.session_state.get("goat_filter_size", ""), placeholder="例如 8.5")
        max_cost_input = filter_cols[2].text_input("成本不超过", value=st.session_state.get("goat_filter_max_cost", ""), placeholder="例如 100")
        min_profit_input = filter_cols[3].text_input("利润不低于", value=st.session_state.get("goat_filter_min_profit", ""), placeholder="例如 10")
        max_days_input = filter_cols[4].text_input("售罄不超过", value=st.session_state.get("goat_filter_max_days", ""), placeholder="例如 14")
        sort_label_input = filter_cols[5].selectbox(
            "排序依据",
            sort_options,
            index=sort_options.index(st.session_state.get("goat_sort_label", sort_options[0])),
        )
        submit_cols = st.columns([1, 1, 4])
        submitted = submit_cols[0].form_submit_button("确定查询", use_container_width=True)
        cleared = submit_cols[1].form_submit_button("清空条件", use_container_width=True)
    if cleared:
        for key in ["goat_filter_style", "goat_filter_size", "goat_filter_max_cost", "goat_filter_min_profit", "goat_filter_max_days"]:
            st.session_state[key] = ""
        st.session_state["goat_sort_label"] = sort_options[0]
        st.rerun()
    if submitted:
        st.session_state["goat_filter_style"] = style_input
        st.session_state["goat_filter_size"] = size_input
        st.session_state["goat_filter_max_cost"] = max_cost_input
        st.session_state["goat_filter_min_profit"] = min_profit_input
        st.session_state["goat_filter_max_days"] = max_days_input
        st.session_state["goat_sort_label"] = sort_label_input

    style_filter = normalize_style_no(st.session_state.get("goat_filter_style", ""))
    size_filter = normalize_us_size(st.session_state.get("goat_filter_size", ""))
    max_cost = optional_float(st.session_state.get("goat_filter_max_cost", ""))
    min_profit = optional_float(st.session_state.get("goat_filter_min_profit", ""))
    max_days = optional_float(st.session_state.get("goat_filter_max_days", ""))
    sort_label = st.session_state.get("goat_sort_label", sort_options[0])

    where: list[str] = []
    params: list[Any] = []
    if style_filter:
        where.append("g.style_no LIKE ?")
        params.append(f"%{style_filter}%")
    if size_filter:
        _append_goat_size_filter(where, params, size_filter)
    if max_cost is not None:
        where.append("g.buy_cost <= ?")
        params.append(max_cost)
    if min_profit is not None:
        where.append("COALESCE(g.estimated_profit, -999999) >= ?")
        params.append(min_profit)
    if max_days is not None:
        where.append(
            "g.estimated_days_to_sell IS NOT NULL "
            "AND CAST(g.estimated_days_to_sell AS REAL) >= 0 "
            "AND CAST(g.estimated_days_to_sell AS REAL) <= ?"
        )
        params.append(max_days)
    sort_sql = {
        "预估利润（高到低）": f"{_profit_desc_sql('g.estimated_profit')}, {_goat_days_asc_sql()}, g.score DESC",
        "预估售罄（少到多）": f"{_goat_days_asc_sql()}, {_profit_desc_sql('g.estimated_profit')}",
        "综合分（高到低）": f"g.score DESC, {_goat_days_asc_sql()}",
        "7天销量（高到低）": f"g.sales_7d DESC, {_profit_desc_sql('g.estimated_profit')}",
        "成本（低到高）": f"g.buy_cost ASC, {_profit_desc_sql('g.estimated_profit')}",
    }.get(sort_label, f"{_profit_desc_sql('g.estimated_profit')}, {_goat_days_asc_sql()}, g.score DESC")
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    rows = query_rows(
        conn,
        f"""
        SELECT
            g.rating,
            ROUND(g.score, 1) AS score,
            g.pid,
            g.style_no,
            g.size AS goat_size,
            g.matched_stockx_size,
            g.title,
            g.estimated_profit,
            g.estimated_days_to_sell,
            g.stockx_lowest_ask,
            g.estimated_sell_price,
            g.goat_price,
            g.buy_cost,
            ROUND(g.estimated_profit_rate * 100, 1) AS estimated_profit_rate_pct,
            g.avg_7d,
            g.avg_30d,
            g.sales_7d,
            g.sales_30d,
            g.ask_snapshot_time,
            g.risk_notes,
            g.computed_at
        FROM goat_consignment_scores g
        {where_sql}
        ORDER BY {sort_sql}
        LIMIT 1000
        """,
        tuple(params),
    )
    if not rows:
        st.info("暂无GOAT寄存评分数据。先导入S仓CSV并评分；如果指定货号/尺码查不到，先清空筛选确认是否已导入。")
        return

    frame = pd.DataFrame([dict(row) for row in rows])
    frame = frame.rename(
        columns={
            "rating": "评级",
            "score": "分数",
            "pid": "PID",
            "style_no": "货号",
            "goat_size": "GOAT尺码",
            "matched_stockx_size": "StockX尺码",
            "title": "商品名",
            "estimated_profit": "预估利润",
            "estimated_days_to_sell": "售罄天数",
            "stockx_lowest_ask": "引用·StockX最低Ask",
            "estimated_sell_price": "默认出售价",
            "goat_price": "GOAT价格",
            "buy_cost": "采购成本(+6)",
            "estimated_profit_rate_pct": "利润率%",
            "avg_7d": "引用·7日成交均价",
            "avg_30d": "引用·30日成交均价",
            "sales_7d": "引用·7天销量",
            "sales_30d": "引用·30天销量",
            "ask_snapshot_time": "引用·Ask快照时间",
            "risk_notes": "说明",
            "computed_at": "评分时间",
        }
    )
    preferred_cols = [
        "评级",
        "分数",
        "PID",
        "货号",
        "GOAT尺码",
        "StockX尺码",
        "预估利润",
        "售罄天数",
        "引用·StockX最低Ask",
        "默认出售价",
        "GOAT价格",
        "采购成本(+6)",
        "利润率%",
        "引用·7日成交均价",
        "引用·30日成交均价",
        "引用·7天销量",
        "引用·30天销量",
        "引用·Ask快照时间",
        "评分时间",
        "说明",
        "商品名",
    ]
    frame = frame[[col for col in preferred_cols if col in frame.columns]]
    money_cols = ["预估利润", "引用·StockX最低Ask", "默认出售价", "GOAT价格", "采购成本(+6)", "引用·7日成交均价", "引用·30日成交均价"]
    for col in money_cols:
        if col in frame.columns:
            frame[col] = frame[col].apply(money)
    if "售罄天数" in frame.columns:
        frame["售罄天数"] = frame["售罄天数"].apply(_format_goat_sellout_days)
    for col in ["引用·Ask快照时间", "评分时间"]:
        if col in frame.columns:
            frame[col] = frame[col].apply(_format_datetime_minute)

    frame = _format_goat_sellout_columns(frame)
    st.dataframe(frame, use_container_width=True, height=720, hide_index=True)


def page_goat_consignment_selection(conn, settings) -> None:
    _ui_page_header(
        "GOAT寄存选品",
        "当前页只处理 GOAT寄存清单。采购成本 = GOAT价格 + 6 美元；StockX 出售回款按出售价扣 3%。补数任务独立运行，不和今日机会绑定。",
        "GOAT寄存",
    )
    _ensure_goat_hidden_styles_table(conn)
    _consume_goat_hidden_style_query(conn)

    worker_state = _read_goat_stockx_worker_marker()
    current_import = query_rows(
        conn,
        """
        SELECT COUNT(*) AS items, MAX(imported_at) AS max_imported_at
        FROM goat_consignment_items
        """,
    )[0]
    current_item_count = int(current_import["items"] or 0)
    current_imported_ts = _timestamp_from_marker(current_import["max_imported_at"])
    worker_started_ts = _timestamp_from_marker(worker_state.get("started_at"))
    worker_total = int(worker_state.get("total") or worker_state.get("total_items") or 0)
    worker_matches_current = (
        bool(worker_state.get("job_id"))
        and worker_total == current_item_count
        and (current_imported_ts is None or worker_started_ts is None or worker_started_ts >= current_imported_ts)
    )
    worker_status = worker_state.get("status")
    if not worker_matches_current and worker_state.get("job_id"):
        worker_status = "stale"
        st.caption("上方旧任务进度已忽略：它不属于当前导入的GOAT清单。")
    if worker_state.get("job_id") and worker_matches_current:
        progress = min(1.0, max(0.0, float(worker_state.get("progress") or 0.0)))
        st.progress(progress)
        current = ""
        if worker_state.get("current_style") or worker_state.get("current_size"):
            current = f" 当前：{worker_state.get('current_style') or '-'} US {worker_state.get('current_size') or '-'}"
        st.caption(f"{worker_state.get('phase') or 'StockX补数'}：{worker_state.get('message') or ''}{current}")
        if worker_status == "running":
            st.info("StockX补数正在独立后台运行；页面只读进度和结果。")
        elif worker_status == "done":
            st.success(worker_state.get("message") or "StockX补数完成")
        elif worker_status == "error":
            st.error(worker_state.get("message") or worker_state.get("error") or "StockX补数失败")

    job_running = worker_status == "running"
    _frontend_auto_refresh(job_running and worker_matches_current, interval_seconds=8, key="goat_stockx_progress")

    _ui_section_label("当前清单和补数任务", "上传新清单会替换当前清单，并启动独立 StockX 补数任务；旧清单进入历史，不继续刷新。")
    upload_cols = st.columns([1.25, 0.9, 1.2])
    uploaded = upload_cols[0].file_uploader("上传GOAT寄存CSV/XLSX", type=["csv", "xlsx"], key="goat_consignment_upload_clean")
    source_name = upload_cols[1].text_input("批次名称", value="goat_slover")
    if uploaded is not None and upload_cols[2].button("导入并启动StockX补数", type="primary", use_container_width=True):
        paused_stockx = _pause_stockx_task_for_goat("new_goat_upload")
        stopped_goat = _stop_goat_worker_for_new_upload() if job_running else {"stopped": False}
        GOAT_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", uploaded.name)
        saved_path = GOAT_UPLOAD_DIR / f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{safe_name}"
        saved_path.write_bytes(uploaded.getvalue())
        frame = _read_goat_consignment_frame(saved_path.name, saved_path.read_bytes())
        imported = _import_goat_consignment_rows(conn, frame, source_name=source_name or "goat_slover", replace_current=True)
        conn.commit()
        worker = start_goat_stockx_worker_request(source_name or "goat_slover")
        prefix = []
        if paused_stockx.get("paused"):
            prefix.append("已暂停今日机会StockX全量")
        if stopped_goat.get("stopped"):
            prefix.append("已停止旧GOAT任务")
        prefix_text = "；".join(prefix)
        if prefix_text:
            prefix_text += "；"
        st.session_state["goat_notice"] = f"{prefix_text}已导入 {imported} 行，StockX补数已启动。" if worker.get("started") else f"{prefix_text}已导入 {imported} 行，但补数未启动：{worker.get('reason') or '已有任务在运行'}"
        st.rerun()

    action_cols = st.columns([1, 1, 1, 1])
    if GOAT_DEFAULT_CONSIGNMENT_PATH.exists() and action_cols[0].button("导入微信S仓表并补数", use_container_width=True):
        paused_stockx = _pause_stockx_task_for_goat("new_goat_default_import")
        stopped_goat = _stop_goat_worker_for_new_upload() if job_running else {"stopped": False}
        frame = _read_goat_consignment_frame(GOAT_DEFAULT_CONSIGNMENT_PATH.name, GOAT_DEFAULT_CONSIGNMENT_PATH.read_bytes())
        imported = _import_goat_consignment_rows(conn, frame, source_name="goat_slover_wechat", replace_current=True)
        conn.commit()
        worker = start_goat_stockx_worker_request("goat_slover_wechat")
        prefix = []
        if paused_stockx.get("paused"):
            prefix.append("已暂停今日机会StockX全量")
        if stopped_goat.get("stopped"):
            prefix.append("已停止旧GOAT任务")
        prefix_text = "；".join(prefix)
        if prefix_text:
            prefix_text += "；"
        st.session_state["goat_notice"] = f"{prefix_text}已导入 {imported} 行，StockX补数已启动。" if worker.get("started") else f"{prefix_text}已导入 {imported} 行，但补数未启动：{worker.get('reason') or '已有任务在运行'}"
        st.rerun()
    if action_cols[1].button("补缺StockX并重评", use_container_width=True, disabled=job_running):
        worker = start_goat_stockx_worker_request("goat_rescore")
        st.session_state["goat_notice"] = "StockX补缺任务已启动。" if worker.get("started") else f"StockX补缺任务未启动：{worker.get('reason') or '已有任务在运行'}"
        st.rerun()
    if action_cols[2].button("只重算评分（本地快照）", use_container_width=True):
        worker = start_goat_stockx_worker_request("goat_local_rescore", live_refresh_missing=False)
        if worker.get("queued"):
            st.session_state["goat_notice"] = "当前补数任务还在运行，本地快照重评已排队；当前任务结束后会自动执行。"
        else:
            st.session_state["goat_notice"] = "本地快照重评任务已启动，不会请求StockX接口。" if worker.get("started") else f"本地快照重评未启动：{worker.get('reason') or '已有任务在运行'}"
        st.rerun()
    if action_cols[3].button("清空GOAT清单", use_container_width=True, disabled=job_running):
        conn.execute("DELETE FROM goat_consignment_scores")
        conn.execute("DELETE FROM goat_consignment_items")
        conn.commit()
        bump_data_cache_version()
        st.warning("已清空GOAT清单。")
        st.rerun()

    if st.session_state.get("goat_notice"):
        st.info(st.session_state["goat_notice"])
    _render_goat_hidden_styles_panel(conn)

    counts = query_rows(
        conn,
        """
        SELECT
          (SELECT COUNT(*) FROM goat_consignment_items) AS items,
          (SELECT COUNT(*) FROM goat_consignment_scores) AS scored,
          (SELECT COUNT(*) FROM goat_consignment_scores WHERE stockx_lowest_ask IS NOT NULL) AS with_ask,
          (SELECT COUNT(*) FROM goat_consignment_scores WHERE stockx_lowest_ask IS NULL) AS missing_ask,
          (SELECT COUNT(*) FROM goat_hidden_styles) AS hidden_styles
        """,
    )[0]
    _ui_status_cards(
        [
            ("GOAT行数", int(counts["items"] or 0)),
            ("已评分", int(counts["scored"] or 0)),
            ("本次处理", int(worker_state.get("completed") or 0) if worker_matches_current else 0),
            ("有StockX Ask", int(counts["with_ask"] or 0)),
            ("缺StockX Ask", int(counts["missing_ask"] or 0)),
            ("隐藏货号", int(counts["hidden_styles"] or 0)),
        ]
    )

    today_text = datetime.now().date().isoformat()
    if not st.session_state.get("goat_filter_release_from"):
        st.session_state["goat_filter_release_from"] = "2020-01-01"
    if not st.session_state.get("goat_filter_release_to"):
        st.session_state["goat_filter_release_to"] = today_text
    _ui_section_label("筛选和结果", "筛选只影响下方列表和导出，不会改动后台补数任务。")
    goat_sort_fields = [
        "预估利润",
        "售罄天数",
        "分数",
        "评级",
        "货号",
        "PID",
        "GOAT尺码",
        "StockX尺码",
        "发售日期",
        "StockX最低Ask",
        "默认出售",
        "GOAT价格",
        "采购成本",
        "利润率",
        "7天均价",
        "30天均价",
        "7天销量",
        "30天销量",
        "Ask快照",
    ]
    if st.session_state.get("goat_sort_field") not in goat_sort_fields:
        st.session_state["goat_sort_field"] = "预估利润"
    if st.session_state.get("goat_sort_direction") not in ["降序", "升序"]:
        st.session_state["goat_sort_direction"] = "降序"
    with st.form("goat_filter_clean"):
        c = st.columns([1, 0.65, 0.75, 0.75, 0.75, 0.9, 0.6])
        style_input = c[0].text_input("货号", value=st.session_state.get("goat_filter_style", ""), placeholder="例如 DD1391-100")
        size_input = c[1].text_input("US尺码", value=st.session_state.get("goat_filter_size", ""), placeholder="例如 8.5")
        max_cost_input = c[2].text_input("成本不超过", value=st.session_state.get("goat_filter_max_cost", ""), placeholder="例如 100")
        min_profit_input = c[3].text_input("利润不低于", value=st.session_state.get("goat_filter_min_profit", ""), placeholder="例如 10")
        max_days_input = c[4].text_input("售罄不超过", value=st.session_state.get("goat_filter_max_days", ""), placeholder="例如 14")
        sort_field_input = c[5].selectbox("排序列", goat_sort_fields, index=goat_sort_fields.index(st.session_state["goat_sort_field"]))
        sort_direction_input = c[6].selectbox("方向", ["降序", "升序"], index=["降序", "升序"].index(st.session_state["goat_sort_direction"]))
        d = st.columns([0.9, 0.9, 4.2])
        release_from_input = d[0].text_input("发售从", value=st.session_state.get("goat_filter_release_from", ""), placeholder="例如 2024-01-01")
        release_to_input = d[1].text_input("发售到", value=st.session_state.get("goat_filter_release_to", ""), placeholder="例如 2026-12-31")
        b = st.columns([1, 1, 4])
        submitted = b[0].form_submit_button("确定查询", use_container_width=True)
        cleared = b[1].form_submit_button("清空条件", use_container_width=True)
    if cleared:
        for key in [
            "goat_filter_style",
            "goat_filter_size",
            "goat_filter_max_cost",
            "goat_filter_min_profit",
            "goat_filter_max_days",
        ]:
            st.session_state[key] = ""
        st.session_state["goat_filter_release_from"] = "2020-01-01"
        st.session_state["goat_filter_release_to"] = today_text
        st.session_state["goat_sort_field"] = "预估利润"
        st.session_state["goat_sort_direction"] = "降序"
        st.rerun()
    if submitted:
        st.session_state["goat_filter_style"] = style_input
        st.session_state["goat_filter_size"] = size_input
        st.session_state["goat_filter_max_cost"] = max_cost_input
        st.session_state["goat_filter_min_profit"] = min_profit_input
        st.session_state["goat_filter_max_days"] = max_days_input
        st.session_state["goat_filter_release_from"] = release_from_input
        st.session_state["goat_filter_release_to"] = release_to_input
        st.session_state["goat_sort_field"] = sort_field_input
        st.session_state["goat_sort_direction"] = sort_direction_input

    style_filter = normalize_style_no(st.session_state.get("goat_filter_style", ""))
    size_filter = normalize_us_size(st.session_state.get("goat_filter_size", ""))
    max_cost = optional_float(st.session_state.get("goat_filter_max_cost", ""))
    min_profit = optional_float(st.session_state.get("goat_filter_min_profit", ""))
    max_days = optional_float(st.session_state.get("goat_filter_max_days", ""))
    release_from = optional_date_text(st.session_state.get("goat_filter_release_from", ""))
    release_to = optional_date_text(st.session_state.get("goat_filter_release_to", ""))
    sort_sql = _goat_sort_sql(
        str(st.session_state.get("goat_sort_field") or "预估利润"),
        str(st.session_state.get("goat_sort_direction") or "降序") == "降序",
    )

    where: list[str] = ["g.style_no NOT IN (SELECT style_no FROM goat_hidden_styles)"]
    params: list[Any] = []
    if style_filter:
        where.append("g.style_no LIKE ?")
        params.append(f"%{style_filter}%")
    if size_filter:
        _append_goat_size_filter(where, params, size_filter)
    if max_cost is not None:
        where.append("g.buy_cost <= ?")
        params.append(max_cost)
    if min_profit is not None:
        where.append("COALESCE(g.estimated_profit, -999999) >= ?")
        params.append(min_profit)
    if max_days is not None:
        where.append(
            "g.estimated_days_to_sell IS NOT NULL "
            "AND CAST(g.estimated_days_to_sell AS REAL) >= 0 "
            "AND CAST(g.estimated_days_to_sell AS REAL) <= ?"
        )
        params.append(max_days)
    if release_from:
        where.append("DATE(product_ref.release_date) >= DATE(?)")
        params.append(release_from)
    if release_to:
        where.append("DATE(product_ref.release_date) <= DATE(?)")
        params.append(release_to)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    rows = query_rows(
        conn,
        f"""
        WITH product_ref AS (
            SELECT
                UPPER(REPLACE(style_no, ' ', '-')) AS style_key,
                MAX(title) AS product_title,
                MAX(image_url) AS image_url,
                MAX(NULLIF(release_date, '')) AS release_date
            FROM products
            GROUP BY UPPER(REPLACE(style_no, ' ', '-'))
        ),
        ranked AS (
            SELECT
                g.rating AS rating,
                ROUND(g.score, 1) AS score,
                g.pid AS pid,
                g.style_no AS style_no,
                COALESCE(product_ref.product_title, g.title) AS product_title,
                product_ref.image_url AS image_url,
                g.size AS size,
                g.matched_stockx_size AS matched_stockx_size,
                ROUND(g.estimated_profit, 2) AS estimated_profit,
                ROUND(g.estimated_profit, 2) AS total_profit,
                CASE
                    WHEN g.estimated_days_to_sell IS NULL
                      OR CAST(g.estimated_days_to_sell AS REAL) < 0
                    THEN NULL
                    ELSE g.estimated_days_to_sell
                END AS estimated_days_to_sell,
                g.stockx_lowest_ask AS stockx_lowest_ask,
                g.estimated_sell_price AS estimated_sell_price,
                g.goat_price AS goat_price,
                g.buy_cost AS buy_cost,
                ROUND(g.estimated_profit_rate * 100, 1) AS profit_rate,
                g.avg_7d AS avg_7d,
                g.avg_30d AS avg_30d,
                g.sales_7d AS sales_7d,
                g.sales_30d AS sales_30d,
                product_ref.release_date AS release_date,
                g.ask_snapshot_time AS ask_snapshot_time,
                g.risk_notes AS risk_notes,
                ROW_NUMBER() OVER (
                    PARTITION BY g.style_no, g.size
                    ORDER BY
                        CASE WHEN g.estimated_profit IS NULL THEN 1 ELSE 0 END ASC,
                        g.estimated_profit DESC,
                        g.buy_cost ASC,
                        g.pid ASC
                ) AS row_rank
            FROM goat_consignment_scores g
            LEFT JOIN product_ref ON product_ref.style_key = UPPER(REPLACE(g.style_no, ' ', '-'))
            {where_sql}
        )
        SELECT *
        FROM ranked
        WHERE row_rank = 1
        ORDER BY {sort_sql}
        LIMIT 1000
        """,
        tuple(params),
    )
    rows = [dict(row) for row in rows]
    if not rows:
        st.info("暂无结果。可以清空条件，或点击“补缺StockX并重评”。")
        return

    frame = pd.DataFrame(rows).rename(
        columns={
            "rating": "评级",
            "score": "分数",
            "pid": "PID",
            "style_no": "货号",
            "product_title": "商品名",
            "image_url": "图片",
            "size": "GOAT尺码",
            "matched_stockx_size": "StockX尺码",
            "total_profit": "预估利润",
            "estimated_profit": "单双利润",
            "estimated_days_to_sell": "售罄天数",
            "stockx_lowest_ask": "StockX最低Ask",
            "estimated_sell_price": "默认出售",
            "goat_price": "GOAT价格",
            "buy_cost": "采购成本",
            "profit_rate": "利润率%",
            "avg_7d": "7天均价",
            "avg_30d": "30天均价",
            "sales_7d": "7天销量",
            "sales_30d": "30天销量",
            "release_date": "发售日期",
            "ask_snapshot_time": "Ask快照",
            "risk_notes": "说明",
        }
    )
    display_cols = [
        "评级",
        "分数",
        "货号",
        "PID",
        "StockX尺码",
        "预估利润",
        "售罄天数",
        "StockX最低Ask",
        "默认出售",
        "GOAT价格",
        "采购成本",
        "利润率%",
        "7天均价",
        "30天均价",
        "7天销量",
        "30天销量",
        "GOAT尺码",
        "发售日期",
        "Ask快照",
        "说明",
    ]
    hover_cols = ["商品名", "图片"]
    frame = frame[[col for col in [*display_cols, *hover_cols] if col in frame.columns]]
    for col in ["预估利润", "单双利润", "StockX最低Ask", "默认出售", "GOAT价格", "采购成本", "7天均价", "30天均价"]:
        if col in frame.columns:
            frame[col] = frame[col].apply(money)
    for col in ["Ask快照"]:
        if col in frame.columns:
            frame[col] = frame[col].apply(_format_datetime_minute)
    if "发售日期" in frame.columns:
        frame["发售日期"] = frame["发售日期"].apply(_format_date_only)
    frame = _format_goat_sellout_columns(frame)
    export_cols = st.columns([1.2, 4])
    export_cols[0].download_button(
        "导出筛选结果 CSV",
        data=_csv_download_bytes(frame),
        file_name=_export_file_name("goat_consignment_filtered"),
        mime="text/csv",
        use_container_width=True,
    )
    export_cols[1].caption(f"导出范围：当前筛选结果 {len(frame)} 行。")
    table_frame = frame.copy()
    if "操作" not in table_frame.columns:
        insert_at = 3 if "PID" in table_frame.columns else 0
        table_frame.insert(insert_at, "操作", "隐藏货号")
    _render_goat_hover_table(table_frame, height=640)


def page_portfolio(conn) -> None:
    st.title("持仓管理")
    with st.form("trade_form", clear_on_submit=True):
        cols = st.columns([1.5, 1, 1, 1, 1, 2])
        style_no = cols[0].text_input("货号")
        size = cols[1].text_input("US 尺码")
        side_label = cols[2].selectbox("类型", ["买入", "卖出"])
        quantity = cols[3].number_input("数量", min_value=1, value=1, step=1)
        price = cols[4].number_input("价格（USD）", min_value=0.0, value=0.0, step=1.0)
        trade_time = cols[5].text_input("时间", value=datetime.utcnow().isoformat(timespec="seconds"))
        notes = st.text_input("备注", value="")
        submitted = st.form_submit_button("保存记录", use_container_width=True)
    if submitted:
        cleaned_style = normalize_style_no(style_no) or style_no.strip().upper()
        if not cleaned_style or not normalize_us_size(size):
            st.error("货号和尺码不能为空。")
        else:
            add_trade(
                conn,
                style_no=cleaned_style,
                size=normalize_us_size(size),
                side="buy" if side_label == "买入" else "sell",
                quantity=int(quantity),
                price=float(price),
                trade_time=trade_time,
                notes=notes or None,
            )
            bump_data_cache_version()
            st.success("已保存持仓记录。")
            st.rerun()

    signature = db_signature(get_settings().db_path)
    summary = load_portfolio_summary_cached(str(get_settings().db_path), signature)
    st.subheader("当前持仓")
    if summary:
        frame = pd.DataFrame(summary)
        frame["size"] = frame["size"].apply(us_size)
        for col in ["average_cost", "realized_profit", "current_lowest_ask"]:
            if col in frame.columns:
                frame[col] = frame[col].apply(money)
        frame = frame.rename(
            columns={
                "style_no": "货号",
                "size": "尺码",
                "current_position": "当前持仓",
                "average_cost": "平均成本",
                "sold_qty": "已卖数量",
                "remaining_qty": "剩余数量",
                "realized_profit": "已实现利润",
                "current_lowest_ask": "当前最低 Ask",
                "action": "建议",
            }
        )
        st.dataframe(frame, use_container_width=True, height=320)
    else:
        st.info("暂无持仓。")

    with st.expander("交易流水", expanded=False):
        trades = load_portfolio_trades_cached(str(get_settings().db_path), signature)
        if trades:
            st.dataframe(pd.DataFrame(trades), use_container_width=True, height=320)
        else:
            st.info("暂无交易流水。")


def page_logs(conn) -> None:
    st.title("接口日志 / 原始 JSON")
    tabs = st.tabs(["同步日志", "原始 JSON"])
    with tabs[0]:
        rows = query_rows(
            conn,
            """
            SELECT created_at, severity, event_type, endpoint, style_no, size, message, details_json
            FROM sync_logs
            ORDER BY created_at DESC
            LIMIT 300
            """,
        )
        if rows:
            st.dataframe(pd.DataFrame([dict(row) for row in rows]), use_container_width=True, height=520)
        else:
            st.info("暂无同步日志。")
    with tabs[1]:
        rows = query_rows(
            conn,
            """
            SELECT id, fetched_at, endpoint, params_json, status_code, error_message, response_json
            FROM raw_api_responses
            ORDER BY fetched_at DESC
            LIMIT 300
            """,
        )
        if not rows:
            st.info("暂无原始接口记录。")
            return
        labels = [f"#{row['id']} {row['fetched_at']} {row['endpoint']} {row['status_code'] or ''}" for row in rows]
        chosen = st.selectbox("选择记录", labels)
        row = dict(rows[labels.index(chosen)])
        st.write({k: v for k, v in row.items() if k != "response_json"})
        if row.get("error_message"):
            st.error(row["error_message"])
        st.json(json_loads(row.get("response_json"), {}))


REMEMBER_LOGIN_KEY = "stockx_goat_remember_token"
REMEMBER_LOGIN_DAYS = 30


def _remember_login_signature(payload: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()


def _make_remember_login_token(username: str, password_secret: str) -> str:
    payload_data = {
        "u": username,
        "exp": int(time_module.time()) + REMEMBER_LOGIN_DAYS * 24 * 60 * 60,
    }
    payload = base64.urlsafe_b64encode(json.dumps(payload_data, separators=(",", ":")).encode("utf-8")).decode("ascii")
    sig = _remember_login_signature(payload, password_secret)
    return f"{payload}.{sig}"


def _validate_remember_login_token(token: str, settings) -> bool:
    if not token or "." not in token or not settings.app_password:
        return False
    payload, sig = token.rsplit(".", 1)
    expected = _remember_login_signature(payload, settings.app_password)
    if not hmac.compare_digest(sig, expected):
        return False
    try:
        data = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")).decode("utf-8"))
    except Exception:
        return False
    if str(data.get("u") or "") != str(settings.app_username or ""):
        return False
    try:
        if int(data.get("exp") or 0) < int(time_module.time()):
            return False
    except Exception:
        return False
    return True


def _login_storage_bridge(*, token: str | None = None, clear: bool = False) -> None:
    token_json = json.dumps(token or "")
    clear_json = json.dumps(bool(clear))
    components.html(
        f"""
        <script>
        const KEY = {json.dumps(REMEMBER_LOGIN_KEY)};
        const TOKEN = {token_json};
        const CLEAR = {clear_json};
        try {{
          const target = window.parent || window;
          const url = new URL(target.location.href);
          url.search = "";
          if (CLEAR) {{
            localStorage.removeItem(KEY);
          }} else if (TOKEN) {{
            localStorage.setItem(KEY, TOKEN);
            url.searchParams.set("remember_token", TOKEN);
          }}
          target.location.href = url.toString();
        }} catch (err) {{}}
        </script>
        """,
        height=0,
    )


def _set_remember_login_query_token(token: str | None = None, *, clear: bool = False) -> None:
    try:
        if clear:
            st.query_params.clear()
        elif token:
            st.query_params["remember_token"] = token
    except Exception:
        pass


def _render_remember_login_probe() -> None:
    components.html(
        f"""
        <script>
        const KEY = {json.dumps(REMEMBER_LOGIN_KEY)};
        try {{
          const token = localStorage.getItem(KEY);
          const target = window.parent || window;
          const url = new URL(target.location.href);
          if (token && !url.searchParams.get("remember_token")) {{
            url.searchParams.set("remember_token", token);
            target.location.href = url.toString();
          }}
        }} catch (err) {{}}
        </script>
        """,
        height=0,
    )


def _query_param_first(name: str) -> str:
    value = st.query_params.get(name)
    if isinstance(value, list):
        return str(value[0]) if value else ""
    return str(value or "")


def _require_app_login(settings) -> bool:
    if not settings.app_login_enabled:
        return True
    if not settings.app_auth_ready:
        st.error("云端登录已开启，但 APP_PASSWORD 没有配置。请先在 .env / 服务器环境变量里设置 APP_USERNAME 和 APP_PASSWORD。")
        return False

    if _query_param_first("logout") == "1":
        st.session_state["app_authenticated"] = False
        _set_remember_login_query_token(clear=True)
        st.info("已退出登录。")
        return False

    remember_token = _query_param_first("remember_token")
    if remember_token:
        if _validate_remember_login_token(remember_token, settings):
            st.session_state["app_authenticated"] = True
            return True
        st.session_state["app_authenticated"] = False
        _set_remember_login_query_token(clear=True)
        st.error("免登录已过期，请重新登录。")
        return False

    if st.session_state.get("app_authenticated"):
        return True

    _render_remember_login_probe()

    st.markdown(
        """
        <style>
        div[data-testid="stAppViewContainer"] .main .block-container {
            padding-top: 6vh;
            max-width: 760px;
        }
        div[data-testid="stForm"] {
            padding: .85rem 1rem .75rem;
            border: 1px solid #d9dee7;
            border-radius: 12px;
            background: #ffffff;
            box-shadow: 0 12px 30px rgba(15, 23, 42, .08);
        }
        div[data-testid="stForm"] button {
            height: 2.6rem;
        }
        @media (max-width: 900px) {
            div[data-testid="column"]:empty {
                display: none;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    left, center, right = st.columns([1.15, 0.72, 1.15])
    with center:
        st.markdown("## 套利扫描器登录")
        st.caption("账号：admin；密码使用服务器环境变量 APP_PASSWORD。")
        with st.form("app_login_form"):
            username = st.text_input("账号")
            password = st.text_input("密码", type="password")
            remember_me = st.checkbox("记住登录，30 天内免输入", value=True)
            submitted = st.form_submit_button("登录", use_container_width=True)
    if submitted:
        username_ok = hmac.compare_digest(str(username or ""), str(settings.app_username or ""))
        password_ok = hmac.compare_digest(str(password or ""), str(settings.app_password or ""))
        if username_ok and password_ok:
            st.session_state["app_authenticated"] = True
            if remember_me:
                token = _make_remember_login_token(str(settings.app_username or ""), str(settings.app_password or ""))
                _set_remember_login_query_token(token)
                st.rerun()
            else:
                st.rerun()
        else:
            st.error("账号或密码不正确。")
    return False


def main() -> None:
    settings = get_settings()
    try:
        restore_sqlite_backup_if_needed(Path(settings.db_path))
        restore_core_tables_if_needed(Path(settings.db_path))
        restore_packaged_stockx_seed_if_empty(
            Path(settings.db_path),
            BASE_DIR / "sample_data" / "stockx_current_seed.json.gz",
        )
    except Exception:
        pass
    if not _require_app_login(settings):
        return
    conn = get_conn()
    try:
        restored_scores = restore_opportunity_scores_from_latest_history_if_empty(conn)
        if restored_scores:
            conn.commit()
            bump_data_cache_version()
            st.session_state["sync_notice"] = f"检测到机会评分为空，已从历史快照恢复 {restored_scores} 行。"
    except Exception as exc:
        try:
            log_sync(
                conn,
                f"历史评分恢复失败：{exc}",
                severity="error",
                event_type="opportunity_score_restore_error",
                details={"error": str(exc)},
            )
            conn.commit()
        except Exception:
            pass
    schedule_startup_core_backup_if_needed(settings)
    ensure_auto_hourly_full_sync_scheduler()
    _refresh_cache_after_sync_if_needed()
    st.sidebar.title("套利扫描器")
    if settings.app_login_enabled:
        st.sidebar.markdown('<a href="?logout=1" target="_self">退出登录并清除记住状态</a>', unsafe_allow_html=True)
    sync_state = _sync_state_snapshot()
    if sync_state.get("status") == "running":
        st.sidebar.info(f"后台运行中：{sync_state.get('completed', 0)}/{sync_state.get('total', 0)}")
    elif sync_state.get("status") == "done":
        st.sidebar.success(sync_state.get("message") or "同步完成")
    elif sync_state.get("status") == "error":
        st.sidebar.error(sync_state.get("message") or sync_state.get("error") or "同步失败")
    pages = ["今日机会", "goat寄存选品", "SKU 导入 / 同步", "持仓管理", "接口日志 / 原始 JSON", "设置"]
    page = st.sidebar.radio("页面", pages)
    st.sidebar.divider()
    st.sidebar.write(f"数据库：`{Path(settings.db_path).name}`")
    st.sidebar.write("凭证：" + ("已填写" if settings.credentials_ready else "未填写"))
    auto_marker = _read_auto_hourly_marker()
    auto_status = "开启" if settings.auto_full_sync_enabled else "关闭"
    auto_started_ts = _timestamp_from_marker(auto_marker.get("last_started_ts")) or _timestamp_from_marker(auto_marker.get("last_started_at"))
    auto_finished_ts = _timestamp_from_marker(auto_marker.get("last_finished_ts")) or _timestamp_from_marker(auto_marker.get("last_finished_at"))
    valid_auto_finished_at = auto_marker.get("last_finished_at") if auto_finished_ts and (not auto_started_ts or auto_finished_ts >= auto_started_ts) else None
    last_auto = _format_datetime_minute(valid_auto_finished_at)
    st.sidebar.caption(f"今日机会自动全量：{auto_status} / 完成后等 {settings.auto_full_sync_interval_minutes} 分钟 / 上次完成 {last_auto}")

    if page == "今日机会":
        page_opportunities(conn, settings)
    elif page == "goat寄存选品":
        page_goat_consignment_selection(conn, settings)
    elif page == "SKU 导入 / 同步":
        page_import_sync(conn, settings)
    elif page == "持仓管理":
        page_portfolio(conn)
    elif page == "接口日志 / 原始 JSON":
        page_logs(conn)
    else:
        page_settings(settings)


if __name__ == "__main__":
    main()

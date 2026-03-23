import asyncio
import html
import math
import re
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote_plus, urljoin, urlparse, urlunparse, unquote

import httpx
import markdown2
from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig
from crawl4ai.content_filter_strategy import PruningContentFilter
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from pypdf import PdfReader, PdfWriter
from weasyprint import HTML


# =========================================================
# Configurações
# =========================================================

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
OUTPUT_DIR = BASE_DIR / "output"
TEMP_DIR = BASE_DIR / "temp"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
TEMP_DIR.mkdir(parents=True, exist_ok=True)

MAX_TOTAL_MB = 100
SOFT_LIMIT_MB = 95
MAX_TOTAL_BYTES = MAX_TOTAL_MB * 1024 * 1024
SOFT_LIMIT_BYTES = SOFT_LIMIT_MB * 1024 * 1024

DEFAULT_MAX_RESULTS = 12

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
)

HTTP_TIMEOUT = httpx.Timeout(connect=15.0, read=90.0, write=30.0, pool=30.0)
SEARCH_HTTP_TIMEOUT = httpx.Timeout(connect=10.0, read=45.0, write=15.0, pool=20.0)

DEFAULT_SEARCH_PROVIDERS: list[dict[str, Any]] = [
    {
        "id": "google",
        "name": "Google",
        "homepage": "https://www.google.com",
        "search_url_template": "https://www.google.com/search?q={query}&hl=pt-BR",
        "enabled": True,
    },
    {
        "id": "bing",
        "name": "Bing",
        "homepage": "https://www.bing.com",
        "search_url_template": "https://www.bing.com/search?q={query}&setlang=pt-BR",
        "enabled": True,
    },
    {
        "id": "yahoo",
        "name": "Yahoo",
        "homepage": "https://search.yahoo.com",
        "search_url_template": "https://search.yahoo.com/search?p={query}",
        "enabled": True,
    },
    {
        "id": "duckduckgo",
        "name": "DuckDuckGo",
        "homepage": "https://duckduckgo.com",
        "search_url_template": "https://duckduckgo.com/html/?q={query}",
        "enabled": True,
    },
    {
        "id": "baidu",
        "name": "Baidu",
        "homepage": "https://www.baidu.com",
        "search_url_template": "https://www.baidu.com/s?wd={query}",
        "enabled": True,
    },
    {
        "id": "yandex",
        "name": "Yandex",
        "homepage": "https://yandex.com",
        "search_url_template": "https://yandex.com/search/?text={query}",
        "enabled": True,
    },
    {
        "id": "ecosia",
        "name": "Ecosia",
        "homepage": "https://www.ecosia.org",
        "search_url_template": "https://www.ecosia.org/search?q={query}",
        "enabled": True,
    },
    {
        "id": "startpage",
        "name": "Startpage",
        "homepage": "https://www.startpage.com",
        "search_url_template": "https://www.startpage.com/search?q={query}",
        "enabled": True,
    },
    {
        "id": "brave",
        "name": "Brave Search",
        "homepage": "https://search.brave.com",
        "search_url_template": "https://search.brave.com/search?q={query}",
        "enabled": True,
    },
    {
        "id": "aol",
        "name": "AOL Search",
        "homepage": "https://search.aol.com",
        "search_url_template": "https://search.aol.com/aol/search?q={query}",
        "enabled": True,
    },
]


# =========================================================
# FastAPI
# =========================================================

app = FastAPI(title="PDF Master Builder", version="3.0.0")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

jobs: dict[str, dict[str, Any]] = {}
job_locks: dict[str, asyncio.Lock] = {}
job_tasks: dict[str, asyncio.Task] = {}


# =========================================================
# Modelos
# =========================================================

class SearchProviderConfig(BaseModel):
    id: str = Field(..., min_length=2)
    name: str = Field(..., min_length=2)
    homepage: str = Field(..., min_length=8)
    search_url_template: str = Field(..., min_length=12)
    enabled: bool = True


class GenerateRequest(BaseModel):
    job_id: str = Field(..., min_length=3)
    prompt: str = Field(..., min_length=3)
    max_results: int = Field(default=DEFAULT_MAX_RESULTS, ge=1, le=50)
    search_providers: list[SearchProviderConfig] = Field(default_factory=list)


# =========================================================
# WebSocket manager
# =========================================================

class ConnectionManager:
    def __init__(self) -> None:
        self.active_connections: dict[str, set[WebSocket]] = {}

    async def connect(self, job_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections.setdefault(job_id, set()).add(websocket)

    def disconnect(self, job_id: str, websocket: WebSocket) -> None:
        if job_id in self.active_connections:
            self.active_connections[job_id].discard(websocket)
            if not self.active_connections[job_id]:
                self.active_connections.pop(job_id, None)

    async def send(self, job_id: str, payload: dict[str, Any]) -> None:
        connections = list(self.active_connections.get(job_id, set()))
        dead_connections: list[WebSocket] = []

        for ws in connections:
            try:
                await ws.send_json(payload)
            except Exception:
                dead_connections.append(ws)

        for ws in dead_connections:
            self.disconnect(job_id, ws)


manager = ConnectionManager()


# =========================================================
# Helpers
# =========================================================

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def sleep_small(seconds: float = 0.25) -> None:
    await asyncio.sleep(seconds)


def bytes_to_mb(num_bytes: int) -> float:
    return round(num_bytes / 1024 / 1024, 2)



def safe_slug(value: str, fallback: str = "arquivo") -> str:
    value = re.sub(r"[^a-zA-Z0-9_-]+", "_", value).strip("_")
    value = re.sub(r"_+", "_", value)
    return value[:80] if value else fallback



def extract_urls(text: str) -> list[str]:
    return re.findall(r"https?://[^\s]+", text)



def unique_keep_order(items: list[str]) -> list[str]:
    seen = set()
    output = []
    for item in items:
        key = item.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(key)
    return output



def remove_markdown_images(markdown_text: str) -> str:
    text = re.sub(r"!\[[^\]]*\]\([^)]+\)", "", markdown_text)
    text = re.sub(r"<img[^>]*>", "", text, flags=re.IGNORECASE)
    return text.strip()



def strip_img_tags(html_text: str) -> str:
    return re.sub(r"<img[^>]*>", "", html_text or "", flags=re.IGNORECASE)



def normalize_url(url: str) -> str:
    try:
        parsed = urlparse(url.strip())
        scheme = parsed.scheme.lower() or "https"
        netloc = parsed.netloc.lower()
        path = parsed.path.rstrip("/")
        clean = parsed._replace(
            scheme=scheme,
            netloc=netloc,
            path=path,
            fragment="",
        )
        return urlunparse(clean)
    except Exception:
        return url.strip()



def build_part_pdf_path(parts_dir: Path, source_id: str, url: str) -> Path:
    parsed = urlparse(url)
    stem = Path(parsed.path).stem or "source"
    base = safe_slug(f"{parsed.netloc}_{stem}", fallback=f"source_{source_id}")
    return parts_dir / f"{source_id}_{base}.pdf"



def build_pdf_html(title: str, source_url: str, body_html: str) -> str:
    safe_title = html.escape(title)
    safe_url = html.escape(source_url)

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <title>{safe_title}</title>
  <style>
    @page {{
      size: A4;
      margin: 16mm 14mm 18mm 14mm;
      @bottom-right {{
        content: "Página " counter(page);
        font-size: 10px;
        color: #666;
      }}
    }}
    body {{
      font-family: Arial, Helvetica, sans-serif;
      color: #111827;
      font-size: 12px;
      line-height: 1.55;
      word-wrap: break-word;
    }}
    h1, h2, h3, h4 {{
      color: #111827;
      margin-top: 18px;
      margin-bottom: 8px;
      page-break-after: avoid;
    }}
    h1 {{
      font-size: 22px;
      border-bottom: 1px solid #e5e7eb;
      padding-bottom: 8px;
      margin-bottom: 14px;
    }}
    h2 {{ font-size: 18px; }}
    h3 {{ font-size: 15px; }}
    p, li {{
      margin: 0 0 8px 0;
    }}
    ul, ol {{
      margin: 0 0 12px 22px;
    }}
    pre {{
      white-space: pre-wrap;
      word-break: break-word;
      background: #f3f4f6;
      border: 1px solid #e5e7eb;
      border-radius: 8px;
      padding: 12px;
      overflow-wrap: anywhere;
    }}
    code {{
      background: #f3f4f6;
      padding: 2px 4px;
      border-radius: 4px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin: 12px 0 16px 0;
      font-size: 11px;
    }}
    th, td {{
      border: 1px solid #d1d5db;
      padding: 6px 8px;
      text-align: left;
      vertical-align: top;
    }}
    th {{
      background: #f9fafb;
    }}
    blockquote {{
      border-left: 4px solid #d1d5db;
      padding-left: 10px;
      color: #374151;
      margin: 12px 0;
    }}
    a {{
      color: #2563eb;
      text-decoration: none;
      overflow-wrap: anywhere;
    }}
    .meta {{
      margin-bottom: 16px;
      font-size: 11px;
      color: #4b5563;
      padding: 10px 12px;
      background: #f9fafb;
      border: 1px solid #e5e7eb;
      border-radius: 8px;
    }}
    .content {{
      margin-top: 12px;
    }}
    img {{
      display: none;
    }}
  </style>
</head>
<body>
  <h1>{safe_title}</h1>
  <div class="meta">
    <strong>Fonte:</strong> <a href="{safe_url}">{safe_url}</a>
  </div>
  <div class="content">
    {body_html}
  </div>
</body>
</html>"""



def render_html_to_pdf(html_text: str, target_path: Path) -> None:
    HTML(string=html_text, base_url=str(BASE_DIR)).write_pdf(str(target_path))



def merge_pdf_files(pdf_paths: list[Path], target_path: Path) -> None:
    writer = PdfWriter()

    for pdf_path in pdf_paths:
        reader = PdfReader(str(pdf_path))
        if reader.is_encrypted:
            try:
                reader.decrypt("")
            except Exception as exc:
                raise ValueError(f"PDF criptografado não pôde ser aberto: {pdf_path.name}") from exc

        for page in reader.pages:
            writer.add_page(page)

    with open(target_path, "wb") as f:
        writer.write(f)



def extract_best_markdown(result: Any) -> str:
    markdown_obj = getattr(result, "markdown", None)

    if markdown_obj is None:
        return ""

    fit_markdown = getattr(markdown_obj, "fit_markdown", None)
    raw_markdown = getattr(markdown_obj, "raw_markdown", None)

    if isinstance(fit_markdown, str) and fit_markdown.strip():
        return fit_markdown.strip()

    if isinstance(raw_markdown, str) and raw_markdown.strip():
        return raw_markdown.strip()

    if isinstance(markdown_obj, str) and markdown_obj.strip():
        return markdown_obj.strip()

    return ""



def extract_best_html(result: Any) -> str:
    cleaned_html = getattr(result, "cleaned_html", None)
    if isinstance(cleaned_html, str) and cleaned_html.strip():
        return cleaned_html.strip()

    raw_html = getattr(result, "html", None)
    if isinstance(raw_html, str) and raw_html.strip():
        return raw_html.strip()

    return ""



def extract_best_title(result: Any, fallback_url: str) -> str:
    for attr in ("title", "page_title"):
        value = getattr(result, attr, None)
        if isinstance(value, str) and value.strip():
            return value.strip()

    parsed = urlparse(fallback_url)
    return parsed.netloc or fallback_url



def public_provider(provider: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": provider["id"],
        "name": provider["name"],
        "homepage": provider.get("homepage", ""),
        "search_url_template": provider["search_url_template"],
        "enabled": bool(provider.get("enabled", True)),
    }



def get_default_search_providers() -> list[dict[str, Any]]:
    return [public_provider(provider) for provider in DEFAULT_SEARCH_PROVIDERS]



def ensure_valid_provider(provider: dict[str, Any]) -> dict[str, Any]:
    search_url_template = (provider.get("search_url_template") or "").strip()
    homepage = (provider.get("homepage") or "").strip()

    if "{query}" not in search_url_template:
        raise ValueError(f"O buscador '{provider.get('name', provider.get('id', 'desconhecido'))}' precisa conter {{query}} na URL.")

    parsed_search = urlparse(search_url_template.replace("{query}", "teste"))
    parsed_homepage = urlparse(homepage or search_url_template.replace("{query}", ""))

    if parsed_search.scheme not in {"http", "https"} or not parsed_search.netloc:
        raise ValueError(f"URL de pesquisa inválida para '{provider.get('name', provider.get('id', 'desconhecido'))}'.")

    if parsed_homepage.scheme not in {"http", "https"} or not parsed_homepage.netloc:
        raise ValueError(f"Homepage inválida para '{provider.get('name', provider.get('id', 'desconhecido'))}'.")

    return {
        "id": str(provider.get("id") or uuid.uuid4().hex[:8]).strip(),
        "name": str(provider.get("name") or "Buscador").strip(),
        "homepage": homepage,
        "search_url_template": search_url_template,
        "enabled": bool(provider.get("enabled", True)),
    }



def normalize_provider_list(providers: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    raw = providers or get_default_search_providers()
    normalized = [ensure_valid_provider(item) for item in raw]
    enabled = [item for item in normalized if item["enabled"]]
    if not enabled:
        raise ValueError("Ative pelo menos um buscador antes de iniciar a pesquisa.")
    return normalized



def ensure_job(job_id: str) -> dict[str, Any]:
    job_dir = TEMP_DIR / job_id
    parts_dir = job_dir / "parts"
    output_path = OUTPUT_DIR / f"master_{job_id}.pdf"

    job_dir.mkdir(parents=True, exist_ok=True)
    parts_dir.mkdir(parents=True, exist_ok=True)

    if job_id not in jobs:
        jobs[job_id] = {
            "job_id": job_id,
            "status": "idle",
            "output_path": str(output_path),
            "current_size": 0,
            "sources": [],
            "searches": [],
            "last_prompt": None,
            "merged_files": 0,
            "active_search_id": None,
            "last_provider_snapshot": get_default_search_providers(),
        }

    if job_id not in job_locks:
        job_locks[job_id] = asyncio.Lock()

    return jobs[job_id]



def get_job_paths(job_id: str) -> dict[str, Path]:
    job_dir = TEMP_DIR / job_id
    return {
        "job_dir": job_dir,
        "parts_dir": job_dir / "parts",
        "temp_merged_path": job_dir / "master_tmp.pdf",
        "output_path": OUTPUT_DIR / f"master_{job_id}.pdf",
    }



def find_search(job: dict[str, Any], search_id: str) -> dict[str, Any] | None:
    return next((search for search in job["searches"] if search["id"] == search_id), None)



def source_to_public(source: dict[str, Any]) -> dict[str, Any]:
    pdf_path = Path(source["pdf_path"])
    return {
        "id": source["id"],
        "search_id": source.get("search_id"),
        "provider_id": source.get("provider_id"),
        "provider_name": source.get("provider_name"),
        "title": source["title"],
        "kind": source["kind"],
        "url": source["url"],
        "resolved_url": source["resolved_url"],
        "size_bytes": source["size_bytes"],
        "size_mb": bytes_to_mb(source["size_bytes"]),
        "file_name": pdf_path.name,
    }



def search_to_public(job: dict[str, Any], search: dict[str, Any]) -> dict[str, Any]:
    source_ids = set(search.get("source_ids", []))
    sources = [
        source_to_public(source)
        for source in job["sources"]
        if source["id"] in source_ids
    ]

    return {
        "id": search["id"],
        "prompt": search["prompt"],
        "status": search["status"],
        "current_message": search.get("current_message"),
        "created_at": search["created_at"],
        "updated_at": search.get("updated_at"),
        "progress_percent": search.get("progress_percent", 0),
        "total_links": search.get("total_links", 0),
        "processed_links": search.get("processed_links", 0),
        "added_count": search.get("added_count", 0),
        "skipped_count": search.get("skipped_count", 0),
        "failed_count": search.get("failed_count", 0),
        "providers": [public_provider(provider) for provider in search.get("providers", [])],
        "providers_summary": search.get("providers_summary", []),
        "source_count": len(source_ids),
        "sources": sources,
        "is_active": job.get("active_search_id") == search["id"],
        "pause_requested": bool(search.get("pause_requested", False)),
        "stop_requested": bool(search.get("stop_requested", False)),
    }



def workspace_state(job_id: str) -> dict[str, Any]:
    job = ensure_job(job_id)
    output_path = Path(job["output_path"])
    has_file = output_path.exists() and output_path.stat().st_size > 0

    searches_public = [search_to_public(job, search) for search in reversed(job["searches"])]
    active_search = None
    if job.get("active_search_id"):
        target = find_search(job, job["active_search_id"])
        if target:
            active_search = search_to_public(job, target)

    return {
        "job_id": job_id,
        "status": job["status"],
        "current_bytes": job["current_size"],
        "current_mb": bytes_to_mb(job["current_size"]),
        "max_bytes": MAX_TOTAL_BYTES,
        "max_mb": MAX_TOTAL_MB,
        "soft_limit_mb": SOFT_LIMIT_MB,
        "merged_files": len(job["sources"]),
        "download_url": f"/api/download/{job_id}" if has_file else None,
        "sources": [source_to_public(s) for s in job["sources"]],
        "searches": searches_public,
        "active_search": active_search,
        "default_search_providers": job.get("last_provider_snapshot") or get_default_search_providers(),
    }


async def ws_event(channel_id: str, event: str, **payload: Any) -> None:
    await manager.send(channel_id, {"event": event, **payload})


async def ws_log(job_id: str, message: str, level: str = "info", search_id: str | None = None, **extra: Any) -> None:
    await ws_event(job_id, "log", level=level, message=message, search_id=search_id, **extra)


async def emit_workspace_state(job_id: str) -> None:
    await ws_event(job_id, "workspace_state", **workspace_state(job_id))


async def emit_search_state(job_id: str, search_id: str) -> None:
    job = ensure_job(job_id)
    search = find_search(job, search_id)
    if not search:
        return
    await ws_event(job_id, "search_state", search=search_to_public(job, search))


async def update_search_state(
    job_id: str,
    search_id: str,
    *,
    status: str | None = None,
    current_message: str | None = None,
    processed_links: int | None = None,
    total_links: int | None = None,
    added_count: int | None = None,
    skipped_count: int | None = None,
    failed_count: int | None = None,
    providers_summary: list[dict[str, Any]] | None = None,
) -> None:
    job = ensure_job(job_id)
    search = find_search(job, search_id)
    if not search:
        return

    if status is not None:
        search["status"] = status
    if current_message is not None:
        search["current_message"] = current_message
    if processed_links is not None:
        search["processed_links"] = processed_links
    if total_links is not None:
        search["total_links"] = total_links
    if added_count is not None:
        search["added_count"] = added_count
    if skipped_count is not None:
        search["skipped_count"] = skipped_count
    if failed_count is not None:
        search["failed_count"] = failed_count
    if providers_summary is not None:
        search["providers_summary"] = providers_summary

    total = max(0, int(search.get("total_links", 0)))
    processed = max(0, int(search.get("processed_links", 0)))
    search["progress_percent"] = round((processed / total) * 100, 2) if total else 0
    search["updated_at"] = now_iso()

    await emit_search_state(job_id, search_id)
    await emit_workspace_state(job_id)



def build_search_url(template: str, query: str) -> str:
    return template.replace("{query}", quote_plus(query.strip()))



def should_skip_candidate(candidate_url: str, provider: dict[str, Any], search_url: str) -> bool:
    if not candidate_url:
        return True

    lowered = candidate_url.lower().strip()
    if lowered.startswith(("javascript:", "mailto:", "tel:", "data:")):
        return True

    parsed_candidate = urlparse(candidate_url)
    parsed_search = urlparse(search_url)
    parsed_homepage = urlparse(provider.get("homepage", ""))

    if parsed_candidate.scheme not in {"http", "https"} or not parsed_candidate.netloc:
        return True

    candidate_host = parsed_candidate.netloc.lower()
    search_host = parsed_search.netloc.lower()
    provider_host = parsed_homepage.netloc.lower()

    if candidate_host == search_host or candidate_host.endswith(f".{search_host}"):
        return True

    if provider_host and (candidate_host == provider_host or candidate_host.endswith(f".{provider_host}")):
        return True

    if "googleusercontent.com" in candidate_host:
        return True

    return False



def try_decode_redirect_url(href: str) -> str:
    href = html.unescape(href or "").strip()
    if not href:
        return ""

    parsed = urlparse(href)
    query = parse_qs(parsed.query)

    for key in ("q", "url", "uddg", "target", "u"):
        values = query.get(key)
        if values and values[0]:
            decoded = unquote(values[0])
            if decoded.startswith(("http://", "https://")):
                return decoded

    yahoo_match = re.search(r"/RU=(https?://.*?)/RK=", href)
    if yahoo_match:
        return unquote(yahoo_match.group(1))

    if href.startswith(("http://", "https://")):
        return href

    return href



def extract_result_links_from_html(html_text: str, provider: dict[str, Any], search_url: str) -> list[str]:
    if not html_text:
        return []

    hrefs = re.findall(r'href=["\']([^"\']+)["\']', html_text, flags=re.IGNORECASE)
    found: list[str] = []

    for href in hrefs:
        raw = try_decode_redirect_url(href)
        if raw.startswith("/"):
            raw = urljoin(search_url, raw)
        raw = raw.strip()
        if should_skip_candidate(raw, provider, search_url):
            continue
        found.append(normalize_url(raw))

    return unique_keep_order(found)



def extract_result_links_from_result(result: Any, provider: dict[str, Any], search_url: str) -> list[str]:
    collected: list[str] = []
    links = getattr(result, "links", None)

    if isinstance(links, dict):
        for bucket in ("external", "internal"):
            items = links.get(bucket, []) or []
            for item in items:
                href = ""
                if isinstance(item, str):
                    href = item
                elif isinstance(item, dict):
                    href = str(item.get("href") or item.get("url") or "").strip()
                if not href:
                    continue
                href = try_decode_redirect_url(href)
                if href.startswith("/"):
                    href = urljoin(search_url, href)
                if should_skip_candidate(href, provider, search_url):
                    continue
                collected.append(normalize_url(href))

    if collected:
        return unique_keep_order(collected)

    raw_html = extract_best_html(result)
    if raw_html:
        return extract_result_links_from_html(raw_html, provider, search_url)

    return []


async def crawl_search_provider(
    crawler: AsyncWebCrawler,
    provider: dict[str, Any],
    prompt: str,
    provider_budget: int,
) -> tuple[str, list[str]]:
    search_url = build_search_url(provider["search_url_template"], prompt)
    run_config = CrawlerRunConfig(cache_mode=CacheMode.BYPASS, word_count_threshold=1)
    result = await crawler.arun(url=search_url, config=run_config)
    links = extract_result_links_from_result(result, provider, search_url)
    return search_url, links[:provider_budget]


async def wait_if_paused_or_stopped(job_id: str, search_id: str) -> str | None:
    job = ensure_job(job_id)
    search = find_search(job, search_id)
    if not search:
        return "stopped"

    while search.get("pause_requested") and not search.get("stop_requested"):
        await update_search_state(
            job_id,
            search_id,
            status="paused",
            current_message="Pesquisa pausada pelo usuário.",
        )
        job["status"] = "paused"
        await ws_event(job_id, "status", value="paused")
        await sleep_small(0.35)
        search = find_search(job, search_id)
        if not search:
            return "stopped"

    if search.get("stop_requested"):
        await update_search_state(
            job_id,
            search_id,
            status="stopped",
            current_message="Pesquisa interrompida pelo usuário.",
        )
        job["status"] = "idle"
        job["active_search_id"] = None
        await ws_event(job_id, "status", value="idle")
        await emit_workspace_state(job_id)
        return "stopped"

    return None


async def search_links_with_providers(
    job_id: str,
    search_id: str,
    prompt: str,
    max_results: int,
    providers: list[dict[str, Any]],
    crawler: AsyncWebCrawler,
) -> tuple[list[str], list[dict[str, Any]]]:
    active_providers = [provider for provider in providers if provider.get("enabled")]
    provider_budget = max(3, min(10, math.ceil((max_results * 1.8) / max(1, len(active_providers)))))

    collected_links: list[str] = []
    provider_summary: list[dict[str, Any]] = []

    await update_search_state(
        job_id,
        search_id,
        status="discovering_links",
        current_message="Buscando links nos buscadores configurados...",
    )

    for index, provider in enumerate(active_providers, start=1):
        control = await wait_if_paused_or_stopped(job_id, search_id)
        if control == "stopped":
            return [], provider_summary

        await ws_log(
            job_id,
            f"[{index}/{len(active_providers)}] Consultando {provider['name']}...",
            search_id=search_id,
        )

        try:
            search_url, provider_links = await crawl_search_provider(
                crawler=crawler,
                provider=provider,
                prompt=prompt,
                provider_budget=provider_budget,
            )
        except Exception as exc:
            provider_summary.append(
                {
                    "provider_id": provider["id"],
                    "provider_name": provider["name"],
                    "search_url": build_search_url(provider["search_url_template"], prompt),
                    "links_found": 0,
                    "status": "error",
                    "message": str(exc),
                }
            )
            await ws_log(
                job_id,
                f"{provider['name']} falhou ao retornar resultados: {exc}",
                level="warning",
                search_id=search_id,
            )
            await update_search_state(job_id, search_id, providers_summary=provider_summary)
            continue

        provider_summary.append(
            {
                "provider_id": provider["id"],
                "provider_name": provider["name"],
                "search_url": search_url,
                "links_found": len(provider_links),
                "status": "ok",
                "message": f"{len(provider_links)} link(s) capturado(s)",
            }
        )
        collected_links.extend(provider_links)

        await ws_log(
            job_id,
            f"{provider['name']} retornou {len(provider_links)} link(s).",
            level="success" if provider_links else "warning",
            search_id=search_id,
        )
        await update_search_state(job_id, search_id, providers_summary=provider_summary)

    deduped = unique_keep_order([normalize_url(link) for link in collected_links])[:max_results]

    await update_search_state(
        job_id,
        search_id,
        total_links=len(deduped),
        current_message=f"{len(deduped)} link(s) único(s) encontrados para processar.",
        providers_summary=provider_summary,
    )

    return deduped, provider_summary


async def probe_url_type(client: httpx.AsyncClient, url: str) -> tuple[str, str]:
    lower_url = url.lower()
    if ".pdf" in lower_url:
        return "pdf", url

    try:
        response = await client.head(url, follow_redirects=True)
        content_type = response.headers.get("content-type", "").lower()
        resolved_url = str(response.url)

        if "application/pdf" in content_type or ".pdf" in resolved_url.lower():
            return "pdf", resolved_url

        return "web", resolved_url
    except Exception:
        return ("pdf", url) if ".pdf" in lower_url else ("web", url)


async def download_pdf(client: httpx.AsyncClient, url: str, target_path: Path) -> dict[str, Any]:
    async with client.stream("GET", url, follow_redirects=True) as response:
        response.raise_for_status()

        total = 0
        with open(target_path, "wb") as f:
            async for chunk in response.aiter_bytes():
                if not chunk:
                    continue
                total += len(chunk)
                if total > MAX_TOTAL_BYTES:
                    raise ValueError("PDF individual excede 100MB.")
                f.write(chunk)

        resolved_url = str(response.url)

    if not target_path.exists() or target_path.stat().st_size == 0:
        raise ValueError("Download do PDF resultou em arquivo vazio.")

    parsed = urlparse(resolved_url)
    title = Path(parsed.path).name or parsed.netloc or resolved_url

    return {
        "pdf_path": str(target_path),
        "resolved_url": resolved_url,
        "kind": "pdf",
        "title": title,
    }


async def crawl_page_to_pdf(
    crawler: AsyncWebCrawler,
    url: str,
    target_path: Path,
) -> dict[str, Any]:
    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        word_count_threshold=20,
        markdown_generator=DefaultMarkdownGenerator(
            content_filter=PruningContentFilter(
                threshold=0.48,
                threshold_type="fixed",
                min_word_threshold=20,
            )
        ),
    )

    result = await crawler.arun(url=url, config=run_config)

    markdown_text = remove_markdown_images(extract_best_markdown(result))
    cleaned_html = strip_img_tags(extract_best_html(result))
    title = extract_best_title(result, url)

    if markdown_text:
        body_html = markdown2.markdown(
            markdown_text,
            extras=[
                "fenced-code-blocks",
                "tables",
                "strike",
                "task_list",
                "cuddled-lists",
            ],
        )
    elif cleaned_html:
        body_html = cleaned_html
    else:
        raise ValueError("crawl4ai não retornou conteúdo útil para essa página.")

    html_doc = build_pdf_html(title=title, source_url=url, body_html=body_html)
    await asyncio.to_thread(render_html_to_pdf, html_doc, target_path)

    if not target_path.exists() or target_path.stat().st_size == 0:
        raise ValueError("Falha ao renderizar PDF da página.")

    return {
        "pdf_path": str(target_path),
        "resolved_url": url,
        "kind": "web",
        "title": title,
    }


async def materialize_source_as_pdf(
    client: httpx.AsyncClient,
    crawler: AsyncWebCrawler,
    url: str,
    target_path: Path,
    job_id: str,
    search_id: str,
) -> dict[str, Any]:
    source_type, resolved_url = await probe_url_type(client, url)

    if source_type == "pdf":
        await ws_log(job_id, f"Baixando PDF direto: {resolved_url}", search_id=search_id)
        return await download_pdf(client, resolved_url, target_path)

    await ws_log(job_id, f"Extraindo página com crawl4ai: {resolved_url}", search_id=search_id)
    return await crawl_page_to_pdf(crawler, resolved_url, target_path)


async def rebuild_master_pdf(job_id: str) -> None:
    job = ensure_job(job_id)
    paths = get_job_paths(job_id)

    output_path = paths["output_path"]
    temp_merged_path = paths["temp_merged_path"]

    sources = job["sources"]

    if not sources:
        output_path.unlink(missing_ok=True)
        temp_merged_path.unlink(missing_ok=True)
        job["current_size"] = 0
        job["merged_files"] = 0
        return

    pdf_paths = [Path(s["pdf_path"]) for s in sources]
    await asyncio.to_thread(merge_pdf_files, pdf_paths, temp_merged_path)

    merged_size = temp_merged_path.stat().st_size

    if merged_size > SOFT_LIMIT_BYTES:
        temp_merged_path.unlink(missing_ok=True)
        raise ValueError(
            f"O PDF mestre excederia o limite de segurança de {SOFT_LIMIT_MB}MB "
            f"({bytes_to_mb(merged_size)}MB)."
        )

    if merged_size > MAX_TOTAL_BYTES:
        temp_merged_path.unlink(missing_ok=True)
        raise ValueError(
            f"O PDF mestre excederia o limite rígido de {MAX_TOTAL_MB}MB "
            f"({bytes_to_mb(merged_size)}MB)."
        )

    shutil.move(str(temp_merged_path), str(output_path))
    job["current_size"] = output_path.stat().st_size
    job["merged_files"] = len(sources)



def source_exists(job: dict[str, Any], candidate_url: str) -> bool:
    normalized_candidate = normalize_url(candidate_url)
    for source in job["sources"]:
        if normalize_url(source["resolved_url"]) == normalized_candidate:
            return True
        if normalize_url(source["url"]) == normalized_candidate:
            return True
    return False



def create_search_group(job: dict[str, Any], prompt: str, providers: list[dict[str, Any]]) -> dict[str, Any]:
    search = {
        "id": uuid.uuid4().hex[:12],
        "prompt": prompt,
        "status": "queued",
        "current_message": "Aguardando início...",
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "progress_percent": 0,
        "total_links": 0,
        "processed_links": 0,
        "added_count": 0,
        "skipped_count": 0,
        "failed_count": 0,
        "providers": providers,
        "providers_summary": [],
        "source_ids": [],
        "pause_requested": False,
        "stop_requested": False,
    }
    job["searches"].append(search)
    job["active_search_id"] = search["id"]
    return search


# =========================================================
# Processo principal do batch
# =========================================================

async def process_job(job_id: str, search_id: str, prompt: str, max_results: int, providers: list[dict[str, Any]]) -> None:
    job = ensure_job(job_id)
    paths = get_job_paths(job_id)
    parts_dir = paths["parts_dir"]
    lock = job_locks[job_id]

    async with lock:
        job["status"] = "running"
        job["last_prompt"] = prompt
        job["last_provider_snapshot"] = [public_provider(provider) for provider in providers]

        await ws_event(job_id, "status", value="running")
        await update_search_state(
            job_id,
            search_id,
            status="running",
            current_message="Iniciando pesquisa...",
        )
        await ws_log(job_id, f"Iniciando pesquisa: {prompt}", search_id=search_id)
        await emit_workspace_state(job_id)

        try:
            direct_urls = extract_urls(prompt)

            browser_config = BrowserConfig(
                headless=True,
                verbose=False,
            )

            added_count = 0
            skipped_count = 0
            failed_count = 0
            processed_links = 0

            async with httpx.AsyncClient(
                headers={"User-Agent": USER_AGENT},
                timeout=HTTP_TIMEOUT,
                follow_redirects=True,
            ) as client:
                async with AsyncWebCrawler(config=browser_config) as crawler:
                    if direct_urls:
                        links = unique_keep_order(direct_urls)[:max_results]
                        provider_summary = [
                            {
                                "provider_id": "manual_urls",
                                "provider_name": "URLs informadas no prompt",
                                "search_url": "prompt",
                                "links_found": len(links),
                                "status": "ok",
                                "message": f"{len(links)} URL(s) direta(s)",
                            }
                        ]
                        await update_search_state(
                            job_id,
                            search_id,
                            status="running",
                            current_message=f"{len(links)} URL(s) informada(s) diretamente. Preparando processamento...",
                            total_links=len(links),
                            providers_summary=provider_summary,
                        )
                    else:
                        links, provider_summary = await search_links_with_providers(
                            job_id=job_id,
                            search_id=search_id,
                            prompt=prompt,
                            max_results=max_results,
                            providers=providers,
                            crawler=crawler,
                        )

                    if not links:
                        raise RuntimeError("Nenhum link relevante encontrado com os buscadores configurados.")

                    await ws_log(
                        job_id,
                        f"{len(links)} link(s) único(s) encontrado(s). Iniciando processamento...",
                        search_id=search_id,
                    )
                    await update_search_state(
                        job_id,
                        search_id,
                        status="running",
                        current_message=f"Processando {len(links)} link(s) encontrados.",
                        total_links=len(links),
                    )

                    active_provider_map = {
                        provider["id"]: provider["name"]
                        for provider in providers
                        if provider.get("enabled")
                    }

                    for index, url in enumerate(links, start=1):
                        control = await wait_if_paused_or_stopped(job_id, search_id)
                        if control == "stopped":
                            await ws_log(job_id, "Pesquisa interrompida antes do próximo link.", level="warning", search_id=search_id)
                            return

                        await ws_log(job_id, f"[{index}/{len(links)}] Preparando fonte: {url}", search_id=search_id)
                        await update_search_state(
                            job_id,
                            search_id,
                            status="running",
                            current_message=f"Processando link {index} de {len(links)}.",
                            processed_links=processed_links,
                            added_count=added_count,
                            skipped_count=skipped_count,
                            failed_count=failed_count,
                        )

                        if job["current_size"] >= SOFT_LIMIT_BYTES:
                            await ws_log(
                                job_id,
                                f"Limite de segurança já atingido ({bytes_to_mb(job['current_size'])}MB / {SOFT_LIMIT_MB}MB).",
                                level="warning",
                                search_id=search_id,
                            )
                            break

                        source_id = uuid.uuid4().hex[:12]
                        part_pdf_path = build_part_pdf_path(parts_dir, source_id, url)

                        try:
                            materialized = await materialize_source_as_pdf(
                                client=client,
                                crawler=crawler,
                                url=url,
                                target_path=part_pdf_path,
                                job_id=job_id,
                                search_id=search_id,
                            )
                        except Exception as exc:
                            processed_links += 1
                            failed_count += 1
                            await update_search_state(
                                job_id,
                                search_id,
                                processed_links=processed_links,
                                added_count=added_count,
                                skipped_count=skipped_count,
                                failed_count=failed_count,
                                current_message=f"Falha ao processar o link {index}.",
                            )
                            await ws_log(job_id, f"Falha ao processar {url}: {exc}", level="error", search_id=search_id)
                            continue

                        resolved_url = materialized["resolved_url"]
                        if source_exists(job, resolved_url):
                            Path(materialized["pdf_path"]).unlink(missing_ok=True)
                            processed_links += 1
                            skipped_count += 1
                            await update_search_state(
                                job_id,
                                search_id,
                                processed_links=processed_links,
                                added_count=added_count,
                                skipped_count=skipped_count,
                                failed_count=failed_count,
                                current_message=f"Fonte duplicada ignorada no link {index}.",
                            )
                            await ws_log(job_id, f"Fonte duplicada ignorada: {resolved_url}", level="warning", search_id=search_id)
                            continue

                        pdf_file = Path(materialized["pdf_path"])
                        provider_name = active_provider_map.get("manual_urls")
                        source_record = {
                            "id": source_id,
                            "search_id": search_id,
                            "provider_id": None,
                            "provider_name": provider_name,
                            "title": materialized["title"],
                            "kind": materialized["kind"],
                            "url": url,
                            "resolved_url": resolved_url,
                            "pdf_path": str(pdf_file),
                            "size_bytes": pdf_file.stat().st_size,
                        }

                        await ws_log(
                            job_id,
                            f"PDF temporário gerado: {pdf_file.name} ({bytes_to_mb(source_record['size_bytes'])}MB)",
                            search_id=search_id,
                        )

                        job["sources"].append(source_record)
                        search = find_search(job, search_id)
                        if search:
                            search["source_ids"].append(source_id)

                        try:
                            await rebuild_master_pdf(job_id)
                        except Exception as exc:
                            job["sources"] = [s for s in job["sources"] if s["id"] != source_id]
                            if search:
                                search["source_ids"] = [sid for sid in search["source_ids"] if sid != source_id]
                            pdf_file.unlink(missing_ok=True)
                            await ws_log(
                                job_id,
                                f"Fonte descartada por limite/tamanho: {source_record['title']} ({exc})",
                                level="warning",
                                search_id=search_id,
                            )
                            break

                        processed_links += 1
                        added_count += 1
                        await update_search_state(
                            job_id,
                            search_id,
                            processed_links=processed_links,
                            added_count=added_count,
                            skipped_count=skipped_count,
                            failed_count=failed_count,
                            current_message=f"Fonte {index} adicionada com sucesso.",
                        )
                        await ws_log(
                            job_id,
                            f"Fonte adicionada com sucesso. Tamanho atual: {bytes_to_mb(job['current_size'])}MB | Progresso: {round((processed_links / len(links)) * 100, 2)}%",
                            level="success",
                            search_id=search_id,
                        )
                        await emit_workspace_state(job_id)

            search = find_search(job, search_id)
            if search and search.get("stop_requested"):
                await update_search_state(
                    job_id,
                    search_id,
                    status="stopped",
                    current_message="Pesquisa interrompida pelo usuário.",
                    added_count=added_count,
                    skipped_count=skipped_count,
                    failed_count=failed_count,
                    processed_links=processed_links,
                )
                job["status"] = "idle"
                job["active_search_id"] = None
                await ws_event(job_id, "status", value="idle")
                await emit_workspace_state(job_id)
                return

            await update_search_state(
                job_id,
                search_id,
                status="completed",
                current_message="Pesquisa concluída.",
                added_count=added_count,
                skipped_count=skipped_count,
                failed_count=failed_count,
                processed_links=processed_links,
            )
            job["status"] = "idle"
            job["active_search_id"] = None
            await ws_event(job_id, "status", value="idle")
            await emit_workspace_state(job_id)
            await ws_event(
                job_id,
                "batch_done",
                search_id=search_id,
                added_count=added_count,
                skipped_count=skipped_count,
                failed_count=failed_count,
                current_mb=bytes_to_mb(job["current_size"]),
                merged_files=len(job["sources"]),
            )
            await ws_log(
                job_id,
                f"Pesquisa concluída. {added_count} nova(s) fonte(s) adicionada(s). PDF atual: {bytes_to_mb(job['current_size'])}MB",
                level="success",
                search_id=search_id,
            )

        except asyncio.CancelledError:
            await update_search_state(
                job_id,
                search_id,
                status="stopped",
                current_message="Pesquisa cancelada.",
            )
            job["status"] = "idle"
            job["active_search_id"] = None
            await ws_event(job_id, "status", value="idle")
            await emit_workspace_state(job_id)
            await ws_log(job_id, "Pesquisa cancelada manualmente.", level="warning", search_id=search_id)
            raise
        except Exception as exc:
            await update_search_state(
                job_id,
                search_id,
                status="error",
                current_message=str(exc),
            )
            job["status"] = "error"
            job["active_search_id"] = None
            await ws_event(job_id, "status", value="error")
            await ws_event(job_id, "error", message=str(exc), search_id=search_id)
            await ws_log(job_id, f"Erro fatal na pesquisa: {exc}", level="error", search_id=search_id)
            await emit_workspace_state(job_id)
        finally:
            task = job_tasks.get(job_id)
            if task and task.done():
                job_tasks.pop(job_id, None)


# =========================================================
# Rotas HTTP
# =========================================================

@app.get("/")
async def home() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/api/providers/defaults")
async def get_default_providers() -> JSONResponse:
    return JSONResponse({"providers": get_default_search_providers()})


@app.get("/api/workspace/{job_id}")
async def get_workspace(job_id: str) -> JSONResponse:
    ensure_job(job_id)
    return JSONResponse(workspace_state(job_id))


@app.post("/api/generate")
async def generate_pdf_master(payload: GenerateRequest) -> JSONResponse:
    prompt = payload.prompt.strip()
    job_id = payload.job_id.strip()

    if not prompt:
        raise HTTPException(status_code=400, detail="Prompt inválido.")

    job = ensure_job(job_id)

    if job["status"] in {"running", "paused"}:
        raise HTTPException(status_code=409, detail="Já existe um processamento em andamento para esse workspace.")

    try:
        providers = normalize_provider_list([provider.model_dump() for provider in payload.search_providers])
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    search = create_search_group(job, prompt=prompt, providers=providers)
    job["last_provider_snapshot"] = [public_provider(provider) for provider in providers]

    task = asyncio.create_task(
        process_job(
            job_id=job_id,
            search_id=search["id"],
            prompt=prompt,
            max_results=payload.max_results,
            providers=providers,
        )
    )
    job_tasks[job_id] = task
    return JSONResponse(
        {
            "ok": True,
            "job_id": job_id,
            "search_id": search["id"],
            "message": "Pesquisa iniciada com sucesso.",
        }
    )


@app.post("/api/search/{job_id}/{search_id}/pause")
async def pause_search(job_id: str, search_id: str) -> JSONResponse:
    job = ensure_job(job_id)
    search = find_search(job, search_id)
    if not search:
        raise HTTPException(status_code=404, detail="Pesquisa não encontrada.")
    if job.get("active_search_id") != search_id:
        raise HTTPException(status_code=409, detail="Somente a pesquisa ativa pode ser pausada.")

    search["pause_requested"] = True
    search["updated_at"] = now_iso()
    await update_search_state(job_id, search_id, status="paused", current_message="Pausa solicitada pelo usuário.")
    await ws_log(job_id, "Pausa solicitada. A execução será pausada no próximo checkpoint seguro.", level="warning", search_id=search_id)
    return JSONResponse({"ok": True})


@app.post("/api/search/{job_id}/{search_id}/resume")
async def resume_search(job_id: str, search_id: str) -> JSONResponse:
    job = ensure_job(job_id)
    search = find_search(job, search_id)
    if not search:
        raise HTTPException(status_code=404, detail="Pesquisa não encontrada.")
    if job.get("active_search_id") != search_id:
        raise HTTPException(status_code=409, detail="Somente a pesquisa ativa pode ser retomada.")

    search["pause_requested"] = False
    search["updated_at"] = now_iso()
    job["status"] = "running"
    await ws_event(job_id, "status", value="running")
    await update_search_state(job_id, search_id, status="running", current_message="Pesquisa retomada.")
    await ws_log(job_id, "Pesquisa retomada pelo usuário.", level="success", search_id=search_id)
    return JSONResponse({"ok": True})


@app.post("/api/search/{job_id}/{search_id}/stop")
async def stop_search(job_id: str, search_id: str) -> JSONResponse:
    job = ensure_job(job_id)
    search = find_search(job, search_id)
    if not search:
        raise HTTPException(status_code=404, detail="Pesquisa não encontrada.")
    if job.get("active_search_id") != search_id:
        raise HTTPException(status_code=409, detail="Somente a pesquisa ativa pode ser interrompida.")

    search["stop_requested"] = True
    search["pause_requested"] = False
    search["updated_at"] = now_iso()
    await update_search_state(job_id, search_id, status="stopping", current_message="Parada solicitada pelo usuário.")
    await ws_log(job_id, "Parada solicitada. A execução será encerrada no próximo checkpoint seguro.", level="warning", search_id=search_id)
    return JSONResponse({"ok": True})


@app.delete("/api/source/{job_id}/{source_id}")
async def delete_source(job_id: str, source_id: str) -> JSONResponse:
    job = ensure_job(job_id)
    lock = job_locks[job_id]

    if job["status"] in {"running", "paused"}:
        raise HTTPException(status_code=409, detail="Aguarde o processamento atual terminar para remover fontes.")

    async with lock:
        source = next((s for s in job["sources"] if s["id"] == source_id), None)
        if not source:
            raise HTTPException(status_code=404, detail="Fonte não encontrada.")

        job["sources"] = [s for s in job["sources"] if s["id"] != source_id]

        for search in job["searches"]:
            search["source_ids"] = [sid for sid in search.get("source_ids", []) if sid != source_id]

        pdf_path = Path(source["pdf_path"])
        pdf_path.unlink(missing_ok=True)

        try:
            await rebuild_master_pdf(job_id)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Erro ao reconstruir PDF: {exc}") from exc

        await ws_log(job_id, f"Fonte removida: {source['title']}", level="warning", search_id=source.get("search_id"))
        await ws_event(job_id, "source_removed", source_id=source_id, search_id=source.get("search_id"))
        await emit_workspace_state(job_id)

    return JSONResponse({"ok": True})


@app.get("/api/download/{job_id}")
async def download_result(job_id: str) -> FileResponse:
    job = ensure_job(job_id)
    output_path = Path(job["output_path"])

    if not output_path.exists():
        raise HTTPException(status_code=404, detail="Arquivo final não encontrado.")

    return FileResponse(
        path=output_path,
        media_type="application/pdf",
        filename=output_path.name,
    )


# =========================================================
# WebSocket
# =========================================================

@app.websocket("/ws/{job_id}")
async def websocket_endpoint(websocket: WebSocket, job_id: str) -> None:
    ensure_job(job_id)
    await manager.connect(job_id, websocket)
    await ws_log(job_id, f"WebSocket conectado ao workspace {job_id}")
    await ws_event(job_id, "status", value=jobs[job_id]["status"])
    await emit_workspace_state(job_id)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(job_id, websocket)
    except Exception:
        manager.disconnect(job_id, websocket)

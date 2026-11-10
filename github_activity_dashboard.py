#!/usr/bin/env python3
"""
GitHub Activity Dashboard Web Application
- Backend: Built-in Python HTTP Server with REST endpoints
- Frontend: Single-Page Application (HTML5, CSS3, Vanilla JS)
- Storage: dashboard_config.json for persisting users, repositories, and preferences
- Zero external dependencies required (No pip install needed)
"""

import os
import sys
import json
import csv
import time
import hashlib
import urllib.request
import urllib.parse
import urllib.error
import re
import calendar
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from datetime import date

CONFIG_FILE = "dashboard_config.json"
CACHE_FILE  = "query_cache_v3.json"  # v3: repo-set-aware entries (v1/v2 files are ignored)
PORT = 8080

# How long a cached result stays valid (seconds). Default: 6 hours.
CACHE_TTL = 6 * 60 * 60

# Completed (past) months never change meaningfully, so cache them much longer.
CACHE_TTL_FINAL = 30 * 24 * 60 * 60

# Max parallel GitHub requests. 5 is safe for both token and no-token paths.
MAX_WORKERS = 5

DEFAULT_CONFIG = {
    "users": [
        "giritrivedi",
        "octocat"
    ],
    "repositories": [
        "dotnet/runtime",
        "dotnet/roslyn",
        "dotnet/sdk",
        "dotnet/aspnetcore",
        "dotnet/efcore"
    ],
    "years": [
        2024,
        2025
    ]
}


def load_config():
    if not os.path.exists(CONFIG_FILE):
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return DEFAULT_CONFIG


def save_config(cfg):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)


# ---------------------------------------------------------------------------
# Query result cache  (query_cache_v3.json)
#
# Each entry stores: v = count, ts = fetched-at, ttl = lifetime,
#                    b = "<qualifier> <date range>", r = sorted repo list.
# Lookups are repo-set aware: a request for repos {A,B,C} is answered by
# summing disjoint cached entries that are subsets of {A,B,C}, and only the
# uncovered repos are fetched (as one combined query). A PR/issue lives in
# exactly one repo, so summing disjoint repo sets is exact.
# ---------------------------------------------------------------------------
_cache_lock = threading.Lock()
_cache = None   # in-memory copy, loaded lazily; guarded by _cache_lock

def _get_cache() -> dict:
    """Caller must hold _cache_lock."""
    global _cache
    if _cache is None:
        _cache = {}
        try:
            if os.path.exists(CACHE_FILE):
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    _cache = json.load(f)
        except Exception:
            _cache = {}
    return _cache

def _save_cache():
    """Caller must hold _cache_lock. Drops expired entries before writing."""
    cache = _get_cache()
    now = time.time()
    for k in [k for k, e in cache.items() if now - e["ts"] >= e.get("ttl", CACHE_TTL)]:
        del cache[k]
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f)
    except Exception:
        pass

def clear_cache():
    global _cache
    with _cache_lock:
        _cache = {}
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)

def _cache_key(query: str) -> str:
    return hashlib.md5(query.encode()).hexdigest()

def cached_fetch(qualifier: str, repos: list, date_query: str, token=None, ttl: int = CACHE_TTL) -> int:
    """Return the count for `qualifier` over `repos` in `date_query`, using cached
    per-repo-set results where possible. Raises FetchError on failure; failures
    are never cached."""
    want = {r.strip() for r in repos if r.strip()}
    base = f"{qualifier} {date_query}"
    now = time.time()

    covered, total = set(), 0
    with _cache_lock:
        cache = _get_cache()
        cands = [e for e in cache.values()
                 if e.get("b") == base and e.get("r")
                 and now - e["ts"] < e.get("ttl", CACHE_TTL)
                 and set(e["r"]) <= want]
        cands.sort(key=lambda e: -len(e["r"]))      # prefer the biggest cached groups
        for e in cands:
            rs = set(e["r"])
            if rs.isdisjoint(covered):
                covered |= rs
                total += e["v"]

    missing = sorted(want - covered)
    if missing:
        query = f"{qualifier} {' '.join('repo:' + r for r in missing)} {date_query}"
        count = fetch_query_count(query, token)
        with _cache_lock:
            _get_cache()[_cache_key(query)] = {
                "v": count, "ts": time.time(), "ttl": ttl, "b": base, "r": missing, "q": query[:160]
            }
            _save_cache()
        total += count
    return total


def get_months_for_years(years: list) -> list:
    """One entry per calendar month (up to today). Quarterly views are derived from these in the UI."""
    today = date.today()
    months = []
    for yr in sorted(years):
        for m in range(1, 13):
            start_dt = date(yr, m, 1)
            if start_dt > today:
                continue
            end_dt = date(yr, m, calendar.monthrange(yr, m)[1])
            in_progress = start_dt <= today <= end_dt
            eff_end = today if in_progress else end_dt
            name = calendar.month_abbr[m]
            months.append({
                "key": f"{yr}-M{m:02d}",
                "label": f"{name} {yr}" + (f" (Till Date: {today.isoformat()})" if in_progress else ""),
                "short_label": f"{name} {yr}" + ("*" if in_progress else ""),
                "year": yr,
                "month": m,
                "quarter": f"M{m:02d}",
                "final": not in_progress,
                "date_query": f"created:{start_dt.isoformat()}..{eff_end.isoformat()}"
            })
    return months


class FetchError(Exception):
    """A query could not be answered (as opposed to a genuine zero count)."""


# Global throttle so parallel workers stay within GitHub's search rate limits
# (API: 30 req/min authenticated; web search: much lower).
_throttle_lock = threading.Lock()
_next_slot = 0.0

def _throttle(interval: float):
    global _next_slot
    with _throttle_lock:
        now = time.time()
        start = max(now, _next_slot)
        _next_slot = start + interval
    wait = start - now
    if wait > 0:
        time.sleep(wait)


def parse_result_count_from_html(html: str):
    json_match = re.search(r'<script[^>]*data-target="react-app\.embeddedData"[^>]*>(.*?)</script>', html, re.DOTALL)
    if json_match:
        try:
            data = json.loads(json_match.group(1))
            count = data.get("payload", {}).get("result_count")
            if count is not None:
                return int(count)
        except Exception:
            pass

    match_key = re.search(r'"result_count":\s*([0-9]+)', html)
    if match_key:
        return int(match_key.group(1))

    match_text = re.search(r'([0-9,]+)\s+(?:issue\s+|pull\s+request\s+)?results?', html, re.IGNORECASE)
    if match_text:
        return int(match_text.group(1).replace(",", ""))

    if re.search(r'did not match any|No results', html, re.IGNORECASE):
        return 0

    return None


def validate_github_user(username: str, token=None) -> bool:
    """Check if a user exists on GitHub."""
    if not re.match(r"^[a-zA-Z0-9-]{1,39}$", username):
        return False
    url = f"https://api.github.com/users/{urllib.parse.quote(username)}"
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "GitHub-Dashboard-App"
    }
    auth_token = token or os.getenv("GITHUB_TOKEN")
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"
    
    req = urllib.request.Request(url, headers=headers)
    try:
        _throttle(1.0)
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise FetchError(f"HTTP {e.code}")
    except Exception as e:
        raise FetchError(str(e))


def validate_github_repo(repo: str, token=None) -> bool:
    """Check if a repository exists on GitHub in owner/repo format."""
    parts = [p.strip() for p in repo.split("/")]
    if len(parts) != 2 or not parts[0] or not parts[1]:
        return False
    if not re.match(r"^[a-zA-Z0-9-]{1,39}$", parts[0]) or not re.match(r"^[a-zA-Z0-9._-]{1,100}$", parts[1]):
        return False
    url = f"https://api.github.com/repos/{urllib.parse.quote(parts[0])}/{urllib.parse.quote(parts[1])}"
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "GitHub-Dashboard-App"
    }
    auth_token = token or os.getenv("GITHUB_TOKEN")
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"
    
    req = urllib.request.Request(url, headers=headers)
    try:
        _throttle(1.0)
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise FetchError(f"HTTP {e.code}")
    except Exception as e:
        raise FetchError(str(e))


def fetch_query_count(query: str, token=None) -> int:
    """Return the result count, or raise FetchError. Never returns a fake 0."""
    MAX_ATTEMPTS = 5
    last_err = "unknown error"

    # 1. API Path (if token present or in environment)
    auth_token = token or os.getenv("GITHUB_TOKEN")
    if auth_token:
        base_search = "https://api.github.com/search/issues"
        url = f"{base_search}?{urllib.parse.urlencode({'q': query})}"
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {auth_token}",
            "User-Agent": "GitHub-Dashboard-App"
        }
        req = urllib.request.Request(url, headers=headers)
        for attempt in range(MAX_ATTEMPTS):
            _throttle(2.2)   # ~27 req/min, under the 30/min search limit
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode('utf-8'))
                    return int(data.get("total_count", 0))
            except urllib.error.HTTPError as e:
                last_err = f"HTTP {e.code}"
                if e.code in (403, 429) or e.code >= 500:
                    wait = 5.0 * (attempt + 1)
                    retry_after = e.headers.get("Retry-After") if e.headers else None
                    reset = e.headers.get("X-RateLimit-Reset") if e.headers else None
                    if retry_after and retry_after.isdigit():
                        wait = int(retry_after) + 1
                    elif reset and reset.isdigit() and e.headers.get("X-RateLimit-Remaining") == "0":
                        wait = max(1, int(reset) - int(time.time()) + 1)
                    time.sleep(min(wait, 65))
                else:
                    # 401 bad token, 422 invalid query, etc. — retrying won't help
                    raise FetchError(f"{last_err} for query: {query[:100]}")
            except Exception as e:
                last_err = str(e)
                time.sleep(2.0 * (attempt + 1))
        raise FetchError(f"{last_err} (gave up) for query: {query[:100]}")

    # 2. Web search fallback (No token)
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    }
    url = f"https://github.com/search?{urllib.parse.urlencode({'q': query, 'type': 'issues'})}"
    req = urllib.request.Request(url, headers=headers)
    for attempt in range(MAX_ATTEMPTS):
        _throttle(3.0)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode('utf-8', errors='ignore')
                count = parse_result_count_from_html(html)
                if count is None:
                    raise FetchError("could not parse result count from GitHub page "
                                     "(login may be required — use a token) for query: " + query[:100])
                return count
        except FetchError:
            raise
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code}"
            if e.code in (429, 403) or e.code >= 500:
                time.sleep(min(10.0 * (attempt + 1), 60))
            else:
                raise FetchError(f"{last_err} for query: {query[:100]}")
        except Exception as e:
            last_err = str(e)
            time.sleep(2.0 * (attempt + 1))
    raise FetchError(f"{last_err} (gave up) for query: {query[:100]}")



def github_search_details(username: str, metric: str, repos: list, years: list,
                          token=None, period=None, limit=100) -> list:
    """Fetch clickable GitHub records for one contributor and metric."""
    if not re.match(r"^[A-Za-z0-9-]{1,39}$", username or ""):
        raise FetchError("Invalid GitHub username.")
    metric_queries = {
        "prs": f"type:pr author:{username}",
        "issues": f"type:issue author:{username}",
        # GitHub Search returns PRs the user reviewed, not individual review-event records.
        "reviews": f"type:pr reviewed-by:{username}",
    }
    if metric not in metric_queries:
        raise FetchError("Unsupported contribution metric.")
    qualifiers = [metric_queries[metric]]
    for repo in repos:
        if not re.match(r"^[A-Za-z0-9-]{1,39}/[A-Za-z0-9._-]{1,100}$", repo):
            continue
        qualifiers.append("repo:" + repo)
    if period:
        qualifiers.append("created:" + period)
    elif years:
        year_parts = []
        for year in years:
            try:
                y = int(year)
                if 2000 <= y <= date.today().year:
                    year_parts.append(f"created:{y}-01-01..{y}-12-31")
            except (TypeError, ValueError):
                pass
        if len(year_parts) == 1:
            qualifiers.append(year_parts[0])
        elif year_parts:
            # Multiple disjoint years are queried separately below.
            pass

    base = " ".join(qualifiers)
    year_ranges = []
    if not period and years:
        for year in sorted({int(y) for y in years if str(y).isdigit()}):
            if 2000 <= year <= date.today().year:
                year_ranges.append(f"{year}-01-01..{year}-12-31")
    else:
        year_ranges = [None]

    token = token or os.getenv("GITHUB_TOKEN")
    results, seen = [], set()
    for year_range in year_ranges or [None]:
        query = base
        if year_range:
            query += " created:" + year_range
        url = "https://api.github.com/search/issues?" + urllib.parse.urlencode(
            {"q": query, "sort": "created", "order": "desc", "per_page": 100, "page": 1}
        )
        page = 1
        while page <= 10 and len(results) < limit:
            page_url = url.rsplit("page=", 1)[0] + f"page={page}"
            headers = {"Accept": "application/vnd.github+json",
                       "User-Agent": "GitHub-Dashboard-App"}
            if token:
                headers["Authorization"] = f"Bearer {token}"
            req = urllib.request.Request(page_url, headers=headers)
            try:
                _throttle(2.2 if token else 3.0)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                raise FetchError(f"GitHub API HTTP {exc.code}: {exc.reason}")
            except Exception as exc:
                raise FetchError(str(exc))
            items = payload.get("items", [])
            if not items:
                break
            for item in items:
                repo_url = item.get("repository_url", "")
                repo_name = repo_url.split("/repos/")[-1] if "/repos/" in repo_url else ""
                if repos and repo_name not in repos:
                    continue
                html_url = item.get("html_url", "")
                if html_url in seen:
                    continue
                seen.add(html_url)
                results.append({
                    "title": item.get("title") or "(no title)",
                    "number": item.get("number"),
                    "html_url": html_url,
                    "repo": repo_name,
                    "state": item.get("state"),
                    "created_at": item.get("created_at"),
                    "closed_at": item.get("closed_at"),
                    "author": (item.get("user") or {}).get("login", "")
                })
                if len(results) >= limit:
                    break
            if len(items) < 100:
                break
            page += 1
    results.sort(key=lambda x: x.get("created_at") or "", reverse=True)
    return results[:limit]


INDEX_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>GitHub Contributions Matrix Dashboard</title>
<style>
  :root {
    --bg: #0f172a;
    --surface: #1e293b;
    --surface-card: #273549;
    --border: #334155;
    --text: #f8fafc;
    --text-muted: #94a3b8;
    --accent: #38bdf8;
    --accent-hover: #0284c7;
    --success: #34d399;
    --total-bg: #1e3a5f;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", sans-serif;
    background: var(--bg);
    color: var(--text);
    padding: 24px;
    min-height: 100vh;
  }
  .container { max-width: 1200px; margin: 0 auto; }
  header { margin-bottom: 24px; display: flex; justify-content: space-between; align-items: center; flex-wrap: gap; }
  h1 { font-size: 24px; font-weight: 700; color: #ffffff; display: flex; align-items: center; gap: 10px; }
  h1 .icon { color: var(--accent); }
  .subtitle { font-size: 13px; color: var(--text-muted); margin-top: 4px; }
  
  .panel {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 20px;
    margin-bottom: 24px;
    box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);
  }
  .sections-grid {
    display: flex;
    flex-direction: column;
    gap: 24px;
  }
  .section-block {
    background: var(--surface-card);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 16px;
  }
  .section-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 12px;
    padding-bottom: 8px;
    border-bottom: 1px solid var(--border);
  }
  .section-title {
    font-size: 14px;
    font-weight: 700;
    color: var(--accent);
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }
  .dual-column-layout {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
  }
  @media (max-width: 768px) {
    .dual-column-layout { grid-template-columns: 1fr; }
  }
  .col-box {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .col-label {
    font-size: 12px;
    font-weight: 600;
    color: var(--text-muted);
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
  select, input[type="text"], input[type="password"] {
    background: #182234;
    border: 1px solid var(--border);
    color: var(--text);
    padding: 8px 12px;
    border-radius: 8px;
    font-size: 13px;
    outline: none;
    transition: border-color 0.2s;
  }
  select:focus, input:focus { border-color: var(--accent); }
  select[multiple] { height: 110px; width: 100%; }
  
  .selected-container {
    background: #182234;
    border: 1px solid var(--border);
    border-radius: 8px;
    height: 110px;
    padding: 8px;
    overflow-y: auto;
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    align-content: flex-start;
  }
  .chip {
    background: #1e3a5f;
    color: #e0f2fe;
    border: 1px solid #0284c7;
    border-radius: 6px;
    padding: 4px 8px;
    font-size: 12px;
    display: inline-flex;
    align-items: center;
    gap: 6px;
  }
  .chip .remove-btn {
    cursor: pointer;
    font-weight: bold;
    color: #93c5fd;
  }
  .chip .remove-btn:hover { color: #fca5a5; }
  .empty-chip-msg {
    color: var(--text-muted);
    font-size: 12px;
    padding: 10px;
    font-style: italic;
  }
  
  .inline-add { display: flex; gap: 6px; margin-top: 4px; }
  .inline-add input { flex: 1; padding: 6px 10px; font-size: 12px; }
  
  .btn {
    background: var(--accent);
    color: #0f172a;
    font-weight: 600;
    padding: 8px 14px;
    border-radius: 8px;
    border: none;
    cursor: pointer;
    font-size: 13px;
    display: inline-flex;
    align-items: center;
    gap: 6px;
    transition: background 0.2s, transform 0.1s;
  }
  .btn:hover { background: var(--accent-hover); color: #fff; }
  .btn:active { transform: scale(0.98); }
  .btn-sm { padding: 6px 10px; font-size: 12px; }
  .btn-secondary { background: var(--surface-card); color: var(--text); border: 1px solid var(--border); }
  .btn-secondary:hover { background: var(--border); color: #fff; }
  .btn-danger { background: #7f1d1d; color: #fca5a5; border: 1px solid #991b1b; }
  .btn-danger:hover { background: #991b1b; color: #fff; }
  .btn-run { background: var(--success); color: #064e3b; font-size: 15px; padding: 12px 24px; font-weight: 700; width: 100%; justify-content: center; }
  .btn-run:hover { background: #10b981; color: #fff; }
  
  .action-bar { display: flex; justify-content: space-between; align-items: center; margin-top: 18px; padding-top: 18px; border-top: 1px solid var(--border); }
  
  /* Status / Loading */
  .status-bar {
    background: var(--surface-card);
    border-left: 4px solid var(--accent);
    padding: 12px 16px;
    border-radius: 6px;
    font-size: 14px;
    margin-bottom: 20px;
    display: none;
  }
  .status-bar.active { display: flex; flex-direction: column; align-items: stretch; gap: 8px; }
  .status-bar-row { display: flex; align-items: center; gap: 10px; }
  .progress-wrap {
    background: #0f172a;
    border-radius: 6px;
    height: 8px;
    overflow: hidden;
    width: 100%;
  }
  .progress-fill {
    height: 100%;
    background: var(--accent);
    border-radius: 6px;
    transition: width 0.25s ease;
    width: 0%;
  }
  .progress-label { font-size: 12px; color: var(--text-muted); text-align: right; }
  .spinner {
    width: 16px;
    height: 16px;
    border: 2px solid var(--text-muted);
    border-top-color: var(--accent);
    border-radius: 50%;
    animation: spin 0.8s linear infinite;
  }
  @keyframes spin { to { transform: rotate(360deg); } }

  /* Results Table */
  .results-wrapper { overflow-x: auto; margin-top: 16px; }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
    background: var(--surface);
    border-radius: 8px;
    overflow: hidden;
  }
  th {
    background: #0284c7;
    color: #ffffff;
    text-align: left;
    padding: 12px 14px;
    font-weight: 600;
    white-space: nowrap;
  }
  th.num, td.num { text-align: right; }
  td {
    padding: 10px 14px;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }
  tr:hover td { background: rgba(255, 255, 255, 0.02); }
  tr.user-header td { background: #1e293b; font-weight: 700; color: var(--accent); font-size: 14px; border-top: 2px solid var(--border); }
  tr.total-row td { background: var(--total-bg); font-weight: 700; color: #ffffff; }
  .badge { background: #0369a1; color: #e0f2fe; padding: 2px 6px; border-radius: 4px; font-size: 11px; font-weight: 600; }
  .empty-state { text-align: center; padding: 40px; color: var(--text-muted); font-size: 14px; }

  /* Dashboard View Styles */
  .view-tabs {
    display: flex;
    gap: 10px;
    margin-bottom: 20px;
    border-bottom: 1px solid var(--border);
    padding-bottom: 12px;
  }
  .tab-btn {
    background: transparent;
    color: var(--text-muted);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 8px 16px;
    cursor: pointer;
    font-size: 13px;
    font-weight: 600;
    display: inline-flex;
    align-items: center;
    gap: 8px;
    transition: all 0.2s;
  }
  .tab-btn:hover { background: var(--surface-card); color: var(--text); }
  .tab-btn.active {
    background: var(--accent);
    color: #0f172a;
    border-color: var(--accent);
  }

  /* KPI Summary Cards Grid */
  .kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 16px;
    margin-bottom: 24px;
  }
  .kpi-card {
    background: var(--surface-card);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 16px;
    display: flex;
    flex-direction: column;
    gap: 8px;
    position: relative;
    overflow: hidden;
  }
  .kpi-card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    width: 4px;
    height: 100%;
    background: var(--accent);
  }
  .kpi-card.prs::before { background: #38bdf8; }
  .kpi-card.issues::before { background: #f59e0b; }
  .kpi-card.reviews::before { background: #a855f7; }
  .kpi-card.total::before { background: #34d399; }
  .kpi-card.top::before { background: #ec4899; }

  .kpi-label { font-size: 11px; text-transform: uppercase; font-weight: 700; color: var(--text-muted); letter-spacing: 0.5px; }
  .kpi-val { font-size: 26px; font-weight: 800; color: #ffffff; }
  .kpi-sub { font-size: 12px; color: var(--text-muted); }

  /* Charts Grid */
  .charts-grid {
    display: grid;
    grid-template-columns: 2fr 1fr;
    gap: 20px;
    margin-bottom: 24px;
  }
  @media (max-width: 900px) {
    .charts-grid { grid-template-columns: 1fr; }
  }
  .chart-box {
    background: var(--surface-card);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 18px;
    display: flex;
    flex-direction: column;
    gap: 12px;
  }
  .chart-title {
    font-size: 14px;
    font-weight: 700;
    color: var(--text);
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
  .chart-canvas-wrap {
    position: relative;
    height: 260px;
    width: 100%;
  }
  .chart-canvas-wrap canvas {
    width: 100%;
    height: 100%;
    display: block;
  }

  /* User Breakdown Cards */
  .user-cards-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    gap: 16px;
    margin-top: 16px;
  }
  .user-stat-card {
    background: var(--surface-card);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 16px;
  }
  .user-stat-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 12px;
    padding-bottom: 8px;
    border-bottom: 1px solid var(--border);
  }
  .user-stat-row {
    display: flex;
    justify-content: space-between;
    padding: 6px 0;
    font-size: 13px;
    border-bottom: 1px solid rgba(255, 255, 255, 0.05);
  }

  .metric-link { background: none; border: 0; color: var(--accent); cursor: pointer; font: inherit; font-weight: 700; text-decoration: underline; text-underline-offset: 3px; }
  .metric-link:hover { color: #fff; }
  .details-backdrop { display:none; position:fixed; inset:0; z-index:1000; background:rgba(2,6,23,.82); padding:24px; overflow:auto; }
  .details-backdrop.open { display:flex; align-items:flex-start; justify-content:center; }
  .details-dialog { width:min(100%,1000px); margin:30px auto; background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:20px; box-shadow:0 20px 60px #0008; }
  .details-head { display:flex; justify-content:space-between; align-items:center; gap:12px; margin-bottom:16px; }
  .details-list { display:flex; flex-direction:column; gap:10px; max-height:65vh; overflow:auto; }
  .detail-item { border:1px solid var(--border); border-radius:8px; padding:12px; background:var(--surface-card); }
  .detail-item a { color:var(--accent); font-weight:700; text-decoration:none; }
  .detail-meta { color:var(--text-muted); font-size:12px; margin-top:6px; }
</style>
</head>
<body>
<div class="container">
  <header>
    <div>
      <h1><span class="icon">⚡</span> GitHub Contributions Matrix Dashboard</h1>
      <div class="subtitle">Quarterly analytics for Pull Requests, Issues, and Code Reviews</div>
    </div>
    <div style="display: flex; gap: 8px;">
      <button type="button" class="btn btn-secondary" onclick="exportData('csv')">📥 Export CSV</button>
      <button type="button" class="btn btn-secondary" onclick="window.print()">🖨️ Print / PDF</button>
      <button type="button" class="btn btn-secondary" onclick="clearCache()" title="Force re-fetch all data on next run">🗑️ Clear Cache</button>
    </div>
  </header>

  <div class="panel">
    <div class="sections-grid">
      <!-- Section 1: GitHub Users -->
      <div class="section-block">
        <div class="section-header">
          <span class="section-title">1. GitHub Users</span>
          <div style="display: flex; gap: 8px;">
            <button type="button" class="btn btn-sm btn-secondary" onclick="selectAllUsers()">Select All Available</button>
            <button type="button" class="btn btn-sm btn-secondary" onclick="clearSelectedUsers()">Clear Selected</button>
            <button type="button" class="btn btn-sm btn-danger" onclick="deleteSelectedUsers()" title="Permanently remove highlighted user from system">🗑️ Delete from Database</button>
          </div>
        </div>
        <div class="dual-column-layout">
          <!-- Left: Available Users Dropdown -->
          <div class="col-box">
            <div class="col-label">
              <span>Available Users (Left Dropdown)</span>
              <span style="font-size: 11px;">Select item & click Add 👉</span>
            </div>
            <select id="userSelect" multiple size="5" ondblclick="addHighlightedUsersToSelection()"></select>
            <div style="display: flex; gap: 6px; margin-top: 4px;">
              <button type="button" class="btn btn-sm" style="flex: 1;" onclick="addHighlightedUsersToSelection()">➡️ Add to Selected Box</button>
            </div>
            <div class="inline-add">
              <input type="text" id="newUserInput" placeholder="New user (e.g. torvalds)"
                     onkeydown="if(event.key==='Enter'){event.preventDefault();addUser();}">
              <button type="button" class="btn btn-sm btn-secondary" onclick="addUser()">+ Create & Save</button>
            </div>
          </div>
          <!-- Right: Selected Active Users -->
          <div class="col-box">
            <div class="col-label">
              <span>Selected Users for Query (Right Side)</span>
              <span id="userCountBadge" class="badge">0 selected</span>
            </div>
            <div id="selectedUsersContainer" class="selected-container"></div>
          </div>
        </div>
      </div>

      <!-- Section 2: GitHub Repositories -->
      <div class="section-block">
        <div class="section-header">
          <span class="section-title">2. GitHub Repositories</span>
          <div style="display: flex; gap: 8px;">
            <button type="button" class="btn btn-sm btn-secondary" onclick="selectAllRepos()">Select All Available</button>
            <button type="button" class="btn btn-sm btn-secondary" onclick="clearSelectedRepos()">Clear Selected</button>
            <button type="button" class="btn btn-sm btn-danger" onclick="deleteSelectedRepos()" title="Permanently remove highlighted repo from system">🗑️ Delete from Database</button>
          </div>
        </div>
        <div class="dual-column-layout">
          <!-- Left: Available Repos Dropdown -->
          <div class="col-box">
            <div class="col-label">
              <span>Available Repositories (Left Dropdown)</span>
              <span style="font-size: 11px;">Select item & click Add 👉</span>
            </div>
            <select id="repoSelect" multiple size="5" ondblclick="addHighlightedReposToSelection()"></select>
            <div style="display: flex; gap: 6px; margin-top: 4px;">
              <button type="button" class="btn btn-sm" style="flex: 1;" onclick="addHighlightedReposToSelection()">➡️ Add to Selected Box</button>
            </div>
            <div class="inline-add">
              <input type="text" id="newRepoInput" placeholder="New repo (e.g. dotnet/runtime)"
                     onkeydown="if(event.key==='Enter'){event.preventDefault();addRepo();}">
              <button type="button" class="btn btn-sm btn-secondary" onclick="addRepo()">+ Create & Save</button>
            </div>
          </div>
          <!-- Right: Selected Active Repositories -->
          <div class="col-box">
            <div class="col-label">
              <span>Selected Repositories for Query (Right Side)</span>
              <span id="repoCountBadge" class="badge">0 selected</span>
            </div>
            <div id="selectedReposContainer" class="selected-container"></div>
          </div>
        </div>
      </div>

      <!-- Section 3: Evaluation Years -->
      <div class="section-block">
        <div class="section-header">
          <span class="section-title">3. Evaluation Years (Quarterly Breakdown)</span>
          <div style="display: flex; gap: 8px;">
            <button type="button" class="btn btn-sm btn-secondary" onclick="selectAllYears()">Select All Available</button>
            <button type="button" class="btn btn-sm btn-secondary" onclick="clearSelectedYears()">Clear Selected</button>
            <button type="button" class="btn btn-sm btn-danger" onclick="deleteSelectedYears()" title="Permanently remove highlighted year">🗑️ Delete from Database</button>
          </div>
        </div>
        <div class="dual-column-layout">
          <!-- Left: Available Years Dropdown -->
          <div class="col-box">
            <div class="col-label">
              <span>Available Years (Left Dropdown)</span>
              <span style="font-size: 11px;">Select item & click Add 👉</span>
            </div>
            <select id="yearSelect" multiple size="4" ondblclick="addHighlightedYearsToSelection()"></select>
            <div style="display: flex; gap: 6px; margin-top: 4px;">
              <button type="button" class="btn btn-sm" style="flex: 1;" onclick="addHighlightedYearsToSelection()">➡️ Add to Selected Box</button>
            </div>
            <div class="inline-add">
              <input type="text" id="newYearInput" placeholder="New Year (e.g. 2023)"
                     onkeydown="if(event.key==='Enter'){event.preventDefault();addYear();}">
              <button type="button" class="btn btn-sm btn-secondary" onclick="addYear()">+ Create & Save</button>
            </div>
          </div>
          <!-- Right: Selected Active Years -->
          <div class="col-box">
            <div class="col-label">
              <span>Selected Years for Query (Right Side)</span>
              <span id="yearCountBadge" class="badge">0 selected</span>
            </div>
            <div id="selectedYearsContainer" class="selected-container"></div>
          </div>
        </div>
      </div>

      <!-- Section 4: Token Field & Action Button -->
      <div class="section-block" style="display: flex; flex-direction: column; gap: 14px;">
        <div class="dual-column-layout">
          <div>
            <label style="display: block; margin-bottom: 6px;">GitHub Token (Optional for higher limits)</label>
            <input type="password" id="tokenInput" placeholder="ghp_... (or leave blank for web scraping)" style="width: 100%;">
          </div>
          <div style="display: flex; align-items: flex-end;">
            <button type="button" class="btn btn-run" onclick="runAnalysis()">🚀 Generate Contributions Matrix</button>
          </div>
        </div>
      </div>
    </div>
  </div>

  <div id="statusBar" class="status-bar">
    <div class="status-bar-row">
      <div class="spinner"></div>
      <span id="statusText">Fetching data from GitHub...</span>
      <span id="progressLabel" class="progress-label" style="margin-left:auto;"></span>
    </div>
    <div class="progress-wrap"><div id="progressFill" class="progress-fill"></div></div>
  </div>

  <div id="detailsModal" class="details-backdrop" role="dialog" aria-modal="true" aria-labelledby="detailsTitle">
    <div class="details-dialog">
      <div class="details-head">
        <div><h2 id="detailsTitle">Contribution details</h2><div id="detailsSubtitle" class="subtitle"></div></div>
        <button type="button" class="btn btn-secondary" onclick="closeDetails()">✕ Close</button>
      </div>
      <div id="detailsContent" class="details-list"><div class="empty-state">Loading...</div></div>
    </div>
  </div>

  <div class="panel">
    <div class="view-tabs" id="viewTabs" style="display: none;">
      <button class="tab-btn active" id="tabDashboard" onclick="switchView('dashboard')">📊 Visual Dashboard</button>
      <button class="tab-btn" id="tabMatrix" onclick="switchView('matrix')">📋 Matrix Table</button>
      <button class="tab-btn" id="tabUsers" onclick="switchView('users')">👥 Contributor Cards</button>
      <span style="margin-left:auto; display:inline-flex; gap:6px; align-items:center;">
        <span style="font-size:12px; color:var(--text-muted);">Plot by:</span>
        <button class="tab-btn" id="granMonthly" onclick="setGranularity('monthly')">Monthly</button>
        <button class="tab-btn active" id="granQuarterly" onclick="setGranularity('quarterly')">Quarterly</button>
      </span>
    </div>

    <div id="resultsArea">
      <div class="empty-state">
        Select your users, repositories, and years above, then click <strong>"Generate Contributions Matrix"</strong> to view the dashboard.
      </div>
    </div>
  </div>
</div>

<script>
let currentData = null;
let currentQuarters = [];
let currentGranularity = 'quarterly';
let rawMonthlyData = null;   // as fetched: user -> metric -> "YYYY-Mxx" -> count
let rawMonths = [];
function isMonthly() { return currentGranularity === 'monthly'; }

let configCache = { users: [], repositories: [], years: [] };

// Persistent Active Selections (Sets)
let activeSelectedUsers = new Set();
let activeSelectedRepos = new Set();
let activeSelectedYears = new Set();
let isInitialLoad = true;

async function loadConfig() {
  const res = await fetch('/api/config');
  configCache = await res.json();

  // Populate Users dropdown (Left side)
  const uSel = document.getElementById('userSelect');
  uSel.innerHTML = '';
  configCache.users.forEach(u => {
    const opt = document.createElement('option');
    opt.value = u;
    opt.textContent = '@' + u;
    uSel.appendChild(opt);
  });

  // Populate Repos dropdown (Left side)
  const rSel = document.getElementById('repoSelect');
  rSel.innerHTML = '';
  configCache.repositories.forEach(r => {
    const opt = document.createElement('option');
    opt.value = r;
    opt.textContent = r;
    rSel.appendChild(opt);
  });

  // Populate Years dropdown (Left side)
  const ySel = document.getElementById('yearSelect');
  ySel.innerHTML = '';
  configCache.years.forEach(y => {
    const opt = document.createElement('option');
    opt.value = y;
    opt.textContent = y;
    ySel.appendChild(opt);
  });

  // On first load, select all defaults into the right box
  if (isInitialLoad) {
    configCache.users.forEach(u => activeSelectedUsers.add(u));
    configCache.repositories.forEach(r => activeSelectedRepos.add(r));
    configCache.years.forEach(y => activeSelectedYears.add(Number(y)));
    isInitialLoad = false;
  } else {
    // Keep active selection valid by removing any purged items
    activeSelectedUsers = new Set([...activeSelectedUsers].filter(u => configCache.users.includes(u)));
    activeSelectedRepos = new Set([...activeSelectedRepos].filter(r => configCache.repositories.includes(r)));
    activeSelectedYears = new Set([...activeSelectedYears].filter(y => configCache.years.includes(y)));
  }

  renderActiveUserChips();
  renderActiveRepoChips();
  renderActiveYearChips();
}

/* User Selection Management */
function addHighlightedUsersToSelection() {
  const highlighted = getSelectedValues(document.getElementById('userSelect'));
  if (highlighted.length === 0) return alert('Please highlight at least one user in the left dropdown to add.');
  highlighted.forEach(u => activeSelectedUsers.add(u));
  renderActiveUserChips();
}

function renderActiveUserChips() {
  const container = document.getElementById('selectedUsersContainer');
  const arr = Array.from(activeSelectedUsers);
  document.getElementById('userCountBadge').textContent = `${arr.length} selected`;
  if (arr.length === 0) {
    container.innerHTML = '<span class="empty-chip-msg">No users selected. Choose from the left list and click "➡️ Add to Selected Box".</span>';
    return;
  }
  container.innerHTML = arr.map(u =>
    `<span class="chip">@${u} <span class="remove-btn" title="Remove from selection" onclick="removeSelectedUser('${u}')">✕</span></span>`
  ).join('');
}

function removeSelectedUser(val) {
  activeSelectedUsers.delete(val);
  renderActiveUserChips();
}

function selectAllUsers() {
  configCache.users.forEach(u => activeSelectedUsers.add(u));
  renderActiveUserChips();
}

function clearSelectedUsers() {
  activeSelectedUsers.clear();
  renderActiveUserChips();
}

/* Repository Selection Management */
function addHighlightedReposToSelection() {
  const highlighted = getSelectedValues(document.getElementById('repoSelect'));
  if (highlighted.length === 0) return alert('Please highlight at least one repository in the left dropdown to add.');
  highlighted.forEach(r => activeSelectedRepos.add(r));
  renderActiveRepoChips();
}

function renderActiveRepoChips() {
  const container = document.getElementById('selectedReposContainer');
  const arr = Array.from(activeSelectedRepos);
  document.getElementById('repoCountBadge').textContent = `${arr.length} selected`;
  if (arr.length === 0) {
    container.innerHTML = '<span class="empty-chip-msg">No repositories selected. Choose from the left list and click "➡️ Add to Selected Box".</span>';
    return;
  }
  container.innerHTML = arr.map(r =>
    `<span class="chip">${r} <span class="remove-btn" title="Remove from selection" onclick="removeSelectedRepo('${r}')">✕</span></span>`
  ).join('');
}

function removeSelectedRepo(val) {
  activeSelectedRepos.delete(val);
  renderActiveRepoChips();
}

function selectAllRepos() {
  configCache.repositories.forEach(r => activeSelectedRepos.add(r));
  renderActiveRepoChips();
}

function clearSelectedRepos() {
  activeSelectedRepos.clear();
  renderActiveRepoChips();
}

/* Year Selection Management */
function addHighlightedYearsToSelection() {
  const highlighted = getSelectedValues(document.getElementById('yearSelect')).map(Number);
  if (highlighted.length === 0) return alert('Please highlight at least one year in the left dropdown to add.');
  highlighted.forEach(y => activeSelectedYears.add(y));
  renderActiveYearChips();
}

function renderActiveYearChips() {
  const container = document.getElementById('selectedYearsContainer');
  const arr = Array.from(activeSelectedYears).sort();
  document.getElementById('yearCountBadge').textContent = `${arr.length} selected`;
  if (arr.length === 0) {
    container.innerHTML = '<span class="empty-chip-msg">No years selected. Choose from the left list and click "➡️ Add to Selected Box".</span>';
    return;
  }
  container.innerHTML = arr.map(y =>
    `<span class="chip">${y} <span class="remove-btn" title="Remove from selection" onclick="removeSelectedYear(${y})">✕</span></span>`
  ).join('');
}

function removeSelectedYear(val) {
  activeSelectedYears.delete(Number(val));
  renderActiveYearChips();
}

function selectAllYears() {
  configCache.years.forEach(y => activeSelectedYears.add(Number(y)));
  renderActiveYearChips();
}

function clearSelectedYears() {
  activeSelectedYears.clear();
  renderActiveYearChips();
}

/* Add New Items */
async function addUser() {
  const input = document.getElementById('newUserInput');
  let val = input.value.trim().replace(/^@/, '');
  if (!val) return;
  const token = document.getElementById('tokenInput').value.trim();
  const res = await fetch('/api/config/user', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ user: val, token: token })
  });
  if (res.ok) {
    input.value = '';
    await loadConfig();          // refreshes configCache first
    activeSelectedUsers.add(val); // add after cache is current so filter keeps it
    renderActiveUserChips();
  } else {
    const errData = await res.json().catch(() => ({}));
    alert(errData.message || `Failed to add user '${val}'.`);
  }
}

async function deleteSelectedUsers() {
  const selected = getSelectedValues(document.getElementById('userSelect'));
  if (selected.length === 0) return alert('Please highlight the user(s) in the left list you wish to permanently delete from database.');
  if (!confirm(`Are you sure you want to permanently delete from database: ${selected.join(', ')}?`)) return;

  const res = await fetch('/api/config/user/delete', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ users: selected })
  });
  if (res.ok) {
    selected.forEach(u => activeSelectedUsers.delete(u));
    await loadConfig();
  }
}

async function addRepo() {
  const input = document.getElementById('newRepoInput');
  let val = input.value.trim();
  if (!val || !val.includes('/')) {
    alert('Please enter repository in owner/repo format (e.g. dotnet/runtime)');
    return;
  }
  const token = document.getElementById('tokenInput').value.trim();
  const res = await fetch('/api/config/repo', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ repo: val, token: token })
  });
  if (res.ok) {
    input.value = '';
    await loadConfig();           // refreshes configCache first
    activeSelectedRepos.add(val); // add after cache is current so filter keeps it
    renderActiveRepoChips();
  } else {
    const errData = await res.json().catch(() => ({}));
    alert(errData.message || `Failed to add repository '${val}'.`);
  }
}

async function deleteSelectedRepos() {
  const selected = getSelectedValues(document.getElementById('repoSelect'));
  if (selected.length === 0) return alert('Please highlight the repository/repositories in the left list you wish to permanently delete from database.');
  if (!confirm(`Are you sure you want to permanently delete from database: ${selected.join(', ')}?`)) return;

  const res = await fetch('/api/config/repo/delete', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ repos: selected })
  });
  if (res.ok) {
    selected.forEach(r => activeSelectedRepos.delete(r));
    await loadConfig();
  }
}

async function addYear() {
  const input = document.getElementById('newYearInput');
  let val = parseInt(input.value.trim(), 10);
  if (!val || isNaN(val)) return;
  const res = await fetch('/api/config/year', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ year: val })
  });
  if (res.ok) {
    input.value = '';
    await loadConfig();              // refreshes configCache first
    activeSelectedYears.add(val);    // add after cache is current so filter keeps it
    renderActiveYearChips();
  }
}

async function deleteSelectedYears() {
  const selected = getSelectedValues(document.getElementById('yearSelect')).map(Number);
  if (selected.length === 0) return alert('Please highlight the year(s) in the left list you wish to permanently delete from database.');
  if (!confirm(`Are you sure you want to delete year(s) from database: ${selected.join(', ')}?`)) return;

  const res = await fetch('/api/config/year/delete', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ years: selected })
  });
  if (res.ok) {
    selected.forEach(y => activeSelectedYears.delete(y));
    await loadConfig();
  }
}

function getSelectedValues(selectElem) {
  return Array.from(selectElem.selectedOptions).map(opt => opt.value);
}

async function runAnalysis() {
  const users = Array.from(activeSelectedUsers);
  const repos = Array.from(activeSelectedRepos);
  const years = Array.from(activeSelectedYears).sort();
  const token = document.getElementById('tokenInput').value.trim();

  if (users.length === 0) return alert('Please select at least one GitHub user.');
  if (repos.length === 0) return alert('Please select at least one repository.');
  if (years.length === 0) return alert('Please select at least one year.');

  const statusBar    = document.getElementById('statusBar');
  const statusText   = document.getElementById('statusText');
  const progressFill = document.getElementById('progressFill');
  const progressLabel = document.getElementById('progressLabel');

  statusBar.classList.add('active');
  progressFill.style.width = '0%';
  progressLabel.textContent = '';
  statusText.textContent = `Starting — building ${users.length} user(s) × ${repos.length} repo(s) query plan...`;

  try {
    const res = await fetch('/api/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ users, repos, years, token })
    });

    // The response is an SSE stream, not a single JSON blob.
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      // SSE frames are separated by double newlines
      const frames = buffer.split('\\n\\n');
      buffer = frames.pop(); // keep incomplete trailing frame

      for (const frame of frames) {
        const line = frame.trim();
        if (!line.startsWith('data:')) continue;
        let evt;
        try { evt = JSON.parse(line.slice(5).trim()); } catch { continue; }

        if (evt.type === 'progress') {
          const pct = Math.round((evt.done / evt.total) * 100);
          progressFill.style.width = pct + '%';
          progressLabel.textContent = `${evt.done} / ${evt.total}`;
          statusText.textContent = `Fetching @${evt.user} · ${evt.quarter} · ${evt.metric}`;
        } else if (evt.type === 'done') {
          rawMonthlyData = evt.matrix;
          rawMonths = evt.quarters;   // server always returns monthly periods
          applyGranularity();
          if (evt.errors && evt.errors.length) {
            setTimeout(() => alert(`${evt.errors.length} query(ies) failed and are shown as 0 (NOT real zeros). ` +
              `Likely GitHub rate limiting or a missing/invalid token. Wait a minute and re-run — successful results are cached.\\n\\n` +
              evt.errors.slice(0, 5).join('\\n')), 900);
          }
          progressFill.style.width = '100%';
          progressLabel.textContent = 'Complete ✓';
          statusText.textContent = 'Done!';
        }
      }
    }

    if (currentData) {
      document.getElementById('viewTabs').style.display = 'flex';
      switchView('dashboard');
    }
  } catch (err) {
    alert('Error fetching data: ' + err.message);
  } finally {
    setTimeout(() => {
      statusBar.classList.remove('active');
      progressFill.style.width = '0%';
    }, 800);
  }
}

async function clearCache() {
  const res = await fetch('/api/cache/clear');
  const data = await res.json();
  if (data.status === 'ok') {
    alert('Cache cleared. Next run will re-fetch all data from GitHub.');
  }
}

function aggregateToQuarters(matrix, months) {
  const quarters = [];
  const idx = {};
  months.forEach(m => {
    const qn = Math.ceil(m.month / 3);
    const key = `${m.year}-Q${qn}`;
    if (!(key in idx)) {
      idx[key] = quarters.length;
      quarters.push({ key, year: m.year, quarter: `Q${qn}`, months: [], partial: false });
    }
    const q = quarters[idx[key]];
    q.months.push(m.key);
    if (m.short_label.endsWith('*')) q.partial = true;
  });
  quarters.forEach(q => {
    q.short_label = q.key + (q.partial ? '*' : '');
    q.label = q.key + (q.partial ? ' (Till Date)' : '');
  });
  const out = {};
  for (const [user, u] of Object.entries(matrix)) {
    out[user] = { prs: {}, issues: {}, reviews: {}, totals: {} };
    quarters.forEach(q => {
      ['prs', 'issues', 'reviews', 'totals'].forEach(k => {
        out[user][k][q.key] = q.months.reduce((s, mk) => s + (u[k][mk] || 0), 0);
      });
    });
  }
  return { matrix: out, quarters };
}

// Derive currentData/currentQuarters from the monthly data — no extra GitHub queries.
function applyGranularity() {
  if (!rawMonthlyData) return;
  if (currentGranularity === 'monthly') {
    currentData = rawMonthlyData;
    currentQuarters = rawMonths;
  } else {
    const agg = aggregateToQuarters(rawMonthlyData, rawMonths);
    currentData = agg.matrix;
    currentQuarters = agg.quarters;
  }
  document.getElementById('granMonthly').classList.toggle('active', currentGranularity === 'monthly');
  document.getElementById('granQuarterly').classList.toggle('active', currentGranularity === 'quarterly');
}

function setGranularity(g) {
  currentGranularity = g;
  applyGranularity();
  switchView(activeView);
}

let activeView = 'dashboard';
let chartInstances = [];

function switchView(view) {
  activeView = view;
  document.getElementById('tabDashboard').classList.toggle('active', view === 'dashboard');
  document.getElementById('tabMatrix').classList.toggle('active', view === 'matrix');
  document.getElementById('tabUsers').classList.toggle('active', view === 'users');

  if (!currentData) return;

  if (view === 'dashboard') renderDashboardView();
  else if (view === 'matrix') renderMatrixTableView();
  else if (view === 'users') renderContributorCardsView();
}

function destroyCharts() {
  chartInstances.forEach(c => { try { c.destroy(); } catch(e){} });
  chartInstances = [];
}

function computeAggregates(data, quarters) {
  let totalPRs = 0;
  let totalIssues = 0;
  let totalReviews = 0;
  let userTotals = {};

  // Per-quarter timeline
  let timeline = quarters.map(q => ({
    key: q.key,
    label: q.short_label,
    prs: 0,
    issues: 0,
    reviews: 0,
    total: 0
  }));

  for (const [user, uData] of Object.entries(data)) {
    let uSum = 0;
    quarters.forEach((q, idx) => {
      const p = uData.prs[q.key] || 0;
      const i = uData.issues[q.key] || 0;
      const r = uData.reviews[q.key] || 0;

      totalPRs += p;
      totalIssues += i;
      totalReviews += r;
      uSum += (p + i + r);

      timeline[idx].prs += p;
      timeline[idx].issues += i;
      timeline[idx].reviews += r;
      timeline[idx].total += (p + i + r);
    });
    userTotals[user] = uSum;
  }

  const grandTotal = totalPRs + totalIssues + totalReviews;
  let topUser = "None";
  let maxScore = -1;
  for (const [u, score] of Object.entries(userTotals)) {
    if (score > maxScore) {
      maxScore = score;
      topUser = u;
    }
  }

  return { totalPRs, totalIssues, totalReviews, grandTotal, userTotals, topUser, maxScore, timeline };
}

function renderDashboardView() {
  destroyCharts();
  const container = document.getElementById('resultsArea');
  const agg = computeAggregates(currentData, currentQuarters);

  let html = `
    <!-- KPI Summary Row -->
    <div class="kpi-grid">
      <div class="kpi-card total">
        <span class="kpi-label">Grand Total Contributions</span>
        <span class="kpi-val">${agg.grandTotal}</span>
        <span class="kpi-sub">Across all selected users & quarters</span>
      </div>
      <div class="kpi-card prs">
        <span class="kpi-label">Pull Requests Created</span>
        <span class="kpi-val">${agg.totalPRs}</span>
        <span class="kpi-sub">${agg.grandTotal ? Math.round((agg.totalPRs/agg.grandTotal)*100) : 0}% of all activity</span>
      </div>
      <div class="kpi-card reviews">
        <span class="kpi-label">Code Reviews Done</span>
        <span class="kpi-val">${agg.totalReviews}</span>
        <span class="kpi-sub">${agg.grandTotal ? Math.round((agg.totalReviews/agg.grandTotal)*100) : 0}% of all activity</span>
      </div>
      <div class="kpi-card issues">
        <span class="kpi-label">Issues Opened</span>
        <span class="kpi-val">${agg.totalIssues}</span>
        <span class="kpi-sub">${agg.grandTotal ? Math.round((agg.totalIssues/agg.grandTotal)*100) : 0}% of all activity</span>
      </div>
      <div class="kpi-card top">
        <span class="kpi-label">Top Contributor</span>
        <span class="kpi-val" style="font-size: 20px;">@${agg.topUser}</span>
        <span class="kpi-sub">${agg.maxScore} contributions</span>
      </div>
    </div>

    <!-- Interactive Charts Grid -->
    <div class="charts-grid">
      <!-- Chart 1: Period Trend -->
      <div class="chart-box">
        <div class="chart-title">
          <span>📈 ${isMonthly() ? 'Monthly' : 'Quarterly'} Contribution Velocity</span>
          <span style="font-size: 11px; color: var(--text-muted);">PRs, Reviews, and Issues over time</span>
        </div>
        <div class="chart-canvas-wrap">
          <canvas id="quarterlyTrendChart"></canvas>
        </div>
      </div>

      <!-- Chart 2: Metric Share Breakdown -->
      <div class="chart-box">
        <div class="chart-title">
          <span>🍩 Contribution Distribution</span>
          <span style="font-size: 11px; color: var(--text-muted);">Activity Share</span>
        </div>
        <div class="chart-canvas-wrap">
          <canvas id="metricPieChart"></canvas>
        </div>
      </div>
    </div>

    <!-- Chart 3: Contributor Comparison -->
    <div class="chart-box">
      <div class="chart-title">
        <span>👥 Contributor Volume Comparison</span>
        <span style="font-size: 11px; color: var(--text-muted);">Total output per selected user</span>
      </div>
      <div class="chart-canvas-wrap" style="height: 220px;">
        <canvas id="contributorBarChart"></canvas>
      </div>
    </div>
  `;

  container.innerHTML = html;

  // Initialize Native Offline Canvas Charts
  setTimeout(() => {
    drawNativeCharts(agg);
  }, 50);
}

function setupCanvasDPI(canvas) {
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = (rect.width || 400) * dpr;
  canvas.height = (rect.height || 240) * dpr;
  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  return { ctx, width: rect.width || 400, height: rect.height || 240 };
}

function drawNativeCharts(agg) {
  // Chart 1: Quarterly Stacked Bar Chart
  const canvasTrend = document.getElementById('quarterlyTrendChart');
  if (canvasTrend) {
    const { ctx, width, height } = setupCanvasDPI(canvasTrend);
    const padding = { top: 35, right: 20, bottom: 58, left: 40 };
    const chartW = width - padding.left - padding.right;
    const chartH = height - padding.top - padding.bottom;

    // Draw Legend
    ctx.font = '11px sans-serif';
    ctx.fillStyle = '#94a3b8';
    
    // Legend items: PRs (#38bdf8), Reviews (#a855f7), Issues (#f59e0b)
    const legends = [
      { label: 'Pull Requests', color: '#38bdf8' },
      { label: 'Code Reviews', color: '#a855f7' },
      { label: 'Issues', color: '#f59e0b' }
    ];
    let legX = padding.left;
    legends.forEach(item => {
      ctx.fillStyle = item.color;
      ctx.fillRect(legX, 10, 10, 10);
      ctx.fillStyle = '#cbd5e1';
      ctx.fillText(item.label, legX + 15, 19);
      legX += ctx.measureText(item.label).width + 30;
    });

    const maxVal = Math.max(...agg.timeline.map(t => t.total), 5);
    const niceMax = Math.ceil(maxVal * 1.15);

    // Draw Grid Lines & Y-ticks
    ctx.strokeStyle = 'rgba(255,255,255,0.06)';
    ctx.fillStyle = '#64748b';
    ctx.textAlign = 'right';
    const gridSteps = 4;
    for (let s = 0; s <= gridSteps; s++) {
      const val = Math.round((niceMax / gridSteps) * s);
      const y = padding.top + chartH - (val / niceMax) * chartH;
      ctx.beginPath();
      ctx.moveTo(padding.left, y);
      ctx.lineTo(padding.left + chartW, y);
      ctx.stroke();
      ctx.fillText(val, padding.left - 8, y + 4);
    }

    // Draw Stacked Bars
    const numBars = agg.timeline.length;
    const barWidth = Math.min(chartW / (numBars || 1) * 0.55, 48);
    const gap = chartW / (numBars || 1);

    agg.timeline.forEach((t, i) => {
      const centerX = padding.left + (i + 0.5) * gap;
      const x = centerX - barWidth / 2;
      let currY = padding.top + chartH;

      // Draw Segments: PRs -> Reviews -> Issues
      const segments = [
        { count: t.prs, color: '#38bdf8' },
        { count: t.reviews, color: '#a855f7' },
        { count: t.issues, color: '#f59e0b' }
      ];

      segments.forEach(seg => {
        if (seg.count > 0) {
          const segH = (seg.count / niceMax) * chartH;
          ctx.fillStyle = seg.color;
          ctx.fillRect(x, currY - segH, barWidth, segH);
          currY -= segH;
        }
      });

      // Total label on top of bar
      if (t.total > 0) {
        ctx.fillStyle = '#ffffff';
        ctx.font = 'bold 11px sans-serif';
        ctx.textAlign = 'center';
        ctx.fillText(t.total, centerX, currY - 5);
      }

      // Period X label (rotated when crowded, e.g. monthly view)
      ctx.fillStyle = '#94a3b8';
      ctx.font = '11px sans-serif';
      const lblY = height - padding.bottom + 18;
      if (gap < 62) {
        ctx.save();
        ctx.translate(centerX, lblY - 4);
        ctx.rotate(-Math.PI / 4);
        ctx.textAlign = 'right';
        ctx.fillText(t.label, 0, 0);
        ctx.restore();
      } else {
        ctx.textAlign = 'center';
        ctx.fillText(t.label, centerX, lblY);
      }
    });
  }

  // Chart 2: Metric Distribution Doughnut Chart
  const canvasPie = document.getElementById('metricPieChart');
  if (canvasPie) {
    const { ctx, width, height } = setupCanvasDPI(canvasPie);
    const centerX = width / 2;
    const centerY = height / 2 - 18;
    const radius = Math.min(centerX, centerY) - 20;
    const innerRadius = radius * 0.62;

    const slices = [
      { label: 'PRs', count: agg.totalPRs, color: '#38bdf8' },
      { label: 'Reviews', count: agg.totalReviews, color: '#a855f7' },
      { label: 'Issues', count: agg.totalIssues, color: '#f59e0b' }
    ];

    const total = agg.grandTotal || 1;
    let startAngle = -Math.PI / 2;

    if (agg.grandTotal === 0) {
      ctx.beginPath();
      ctx.arc(centerX, centerY, radius, 0, Math.PI * 2);
      ctx.arc(centerX, centerY, innerRadius, Math.PI * 2, 0, true);
      ctx.fillStyle = '#334155';
      ctx.fill();
    } else {
      slices.forEach(slice => {
        const sliceAngle = (slice.count / total) * Math.PI * 2;
        if (sliceAngle > 0) {
          ctx.beginPath();
          ctx.arc(centerX, centerY, radius, startAngle, startAngle + sliceAngle);
          ctx.arc(centerX, centerY, innerRadius, startAngle + sliceAngle, startAngle, true);
          ctx.closePath();
          ctx.fillStyle = slice.color;
          ctx.fill();
          ctx.strokeStyle = '#1e293b';
          ctx.lineWidth = 2;
          ctx.stroke();
          startAngle += sliceAngle;
        }
      });
    }

    // Center Total Text
    ctx.fillStyle = '#ffffff';
    ctx.font = 'bold 20px sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText(agg.grandTotal, centerX, centerY + 2);
    ctx.fillStyle = '#94a3b8';
    ctx.font = '10px sans-serif';
    ctx.fillText('TOTAL', centerX, centerY + 18);

    // Bottom Legend with percentages
    let legY = height - 20;
    let lX = 20;
    ctx.font = '11px sans-serif';
    ctx.textAlign = 'left';
    slices.forEach(slice => {
      const pct = agg.grandTotal ? Math.round((slice.count / agg.grandTotal) * 100) : 0;
      ctx.fillStyle = slice.color;
      ctx.fillRect(lX, legY - 10, 10, 10);
      ctx.fillStyle = '#cbd5e1';
      ctx.fillText(`${slice.label}: ${slice.count} (${pct}%)`, lX + 14, legY - 1);
      lX += ctx.measureText(`${slice.label}: ${slice.count} (${pct}%)`).width + 22;
    });
  }

  // Chart 3: Contributor Volume Comparison Horizontal Bar Chart
  const canvasUser = document.getElementById('contributorBarChart');
  if (canvasUser) {
    const { ctx, width, height } = setupCanvasDPI(canvasUser);
    const padding = { top: 15, right: 60, bottom: 15, left: 120 };
    const chartW = width - padding.left - padding.right;
    const chartH = height - padding.top - padding.bottom;

    const users = Object.keys(agg.userTotals);
    const maxScore = Math.max(...Object.values(agg.userTotals), 1);
    const rowH = Math.min(chartH / (users.length || 1), 36);

    users.forEach((u, i) => {
      const val = agg.userTotals[u];
      const y = padding.top + i * rowH;
      const barH = Math.min(rowH * 0.65, 20);
      const barW = Math.max((val / maxScore) * chartW, 4);

      // User name on left
      ctx.fillStyle = '#cbd5e1';
      ctx.font = 'bold 12px sans-serif';
      ctx.textAlign = 'right';
      ctx.fillText('@' + u, padding.left - 12, y + barH / 2 + 4);

      // Background Track
      ctx.fillStyle = '#1e293b';
      ctx.fillRect(padding.left, y, chartW, barH);

      // Filled Bar
      ctx.fillStyle = '#34d399';
      ctx.fillRect(padding.left, y, barW, barH);

      // Value Label on right
      ctx.fillStyle = '#ffffff';
      ctx.font = 'bold 12px sans-serif';
      ctx.textAlign = 'left';
      ctx.fillText(`${val} contribs`, padding.left + barW + 8, y + barH / 2 + 4);
    });
  }
}

function renderMatrixTableView() {
  destroyCharts();
  const container = document.getElementById('resultsArea');
  const data = currentData;
  const quarters = currentQuarters;

  let html = `<div class="results-wrapper"><table><thead><tr><th>User / Metric</th>`;
  quarters.forEach(q => {
    html += `<th class="num">${q.short_label}</th>`;
  });
  html += `<th class="num">Grand Total</th></tr></thead><tbody>`;

  const metrics = [
    { label: "Pull Requests Opened", key: "prs", isTotal: false },
    { label: "Issues Opened", key: "issues", isTotal: false },
    { label: "Code Reviews Done", key: "reviews", isTotal: false },
    { label: "TOTAL CONTRIBUTIONS", key: "totals", isTotal: true }
  ];

  for (const [user, uData] of Object.entries(data)) {
    html += `<tr class="user-header"><td colspan="${quarters.length + 2}">👤 @${user} <span class="badge">Contributor</span></td></tr>`;
    
    metrics.forEach(m => {
      const rowClass = m.isTotal ? ' class="total-row"' : '';
      html += `<tr${rowClass}><td>${m.label}</td>`;
      let userMetricTotal = 0;
      quarters.forEach(q => {
        const val = uData[m.key][q.key] || 0;
        userMetricTotal += val;
        html += `<td class="num">${m.key === "totals" ? val : metricButton(user, m.key, val, q.key)}</td>`;
      });
      html += `<td class="num">${m.key === "totals" ? userMetricTotal : metricButton(user, m.key, userMetricTotal, "")}</td></tr>`;
    });
  }

  html += `</tbody></table></div>`;
  container.innerHTML = html;
}

function renderContributorCardsView() {
  destroyCharts();
  const container = document.getElementById('resultsArea');
  const data = currentData;
  const quarters = currentQuarters;

  let html = `<div class="user-cards-grid">`;

  for (const [user, uData] of Object.entries(data)) {
    let userPRs = 0;
    let userIssues = 0;
    let userReviews = 0;

    quarters.forEach(q => {
      userPRs += (uData.prs[q.key] || 0);
      userIssues += (uData.issues[q.key] || 0);
      userReviews += (uData.reviews[q.key] || 0);
    });
    const userTotal = userPRs + userIssues + userReviews;

    html += `
      <div class="user-stat-card">
        <div class="user-stat-header">
          <span style="font-weight: 700; color: var(--accent); font-size: 15px;">👤 @${user}</span>
          <span class="badge" style="background: var(--success); color: #064e3b; font-weight: 800;">${userTotal} total</span>
        </div>
        <div class="user-stat-row">
          <span style="color: var(--text-muted);">Pull Requests:</span>
          <span style="font-weight: 700; color: #38bdf8;">${metricButton(user, "prs", userPRs, "")}</span>
        </div>
        <div class="user-stat-row">
          <span style="color: var(--text-muted);">Code Reviews:</span>
          <span style="font-weight: 700; color: #a855f7;">${metricButton(user, "reviews", userReviews, "")}</span>
        </div>
        <div class="user-stat-row">
          <span style="color: var(--text-muted);">Issues Opened:</span>
          <span style="font-weight: 700; color: #f59e0b;">${metricButton(user, "issues", userIssues, "")}</span>
        </div>
        <div class="user-stat-row" style="margin-top: 8px; border-bottom: none; font-size: 11px; color: var(--text-muted);">
          <span>${isMonthly() ? 'Months' : 'Quarters'} active:</span>
          <span>${quarters.length}</span>
        </div>
      </div>
    `;
  }

  html += `</div>`;
  container.innerHTML = html;
}


function metricButton(user, metric, value, period) {
  const safeUser = String(user).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
  const safePeriod = String(period || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
  return `<button type="button" class="metric-link" data-user="${safeUser}" data-metric="${metric}" data-period="${safePeriod}">${value}</button>`;
}

async function showContributionDetails(user, metric, period = '') {
  const labels = { prs: 'Pull Requests opened', issues: 'Issues opened', reviews: 'Pull Requests reviewed' };
  const modal = document.getElementById('detailsModal');
  const content = document.getElementById('detailsContent');
  document.getElementById('detailsTitle').textContent = `${labels[metric] || metric} — @${user}`;
  document.getElementById('detailsSubtitle').textContent = period ? `Period: ${period}` : 'Selected repositories and years';
  modal.classList.add('open');
  content.innerHTML = '<div class="empty-state">Loading GitHub details…</div>';
  try {
    const res = await fetch('/api/details', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        user, metric, repos: Array.from(activeSelectedRepos),
        years: Array.from(activeSelectedYears),
        period, token: document.getElementById('tokenInput').value.trim()
      })
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.message || 'Unable to load details.');
    if (!data.items.length) {
      content.innerHTML = '<div class="empty-state">No matching GitHub records were found for this selection.</div>';
      return;
    }
    content.innerHTML = data.items.map(item => {
      const title = escapeHTML(item.title);
      const repo = escapeHTML(item.repo || '');
      const state = escapeHTML(item.state || '');
      const created = item.created_at ? new Date(item.created_at).toLocaleDateString() : '';
      return `<div class="detail-item">
        <a href="${item.html_url}" target="_blank" rel="noopener noreferrer">${title} (#${item.number}) ↗</a>
        <div class="detail-meta">${repo} · ${state} · Created ${created}</div>
      </div>`;
    }).join('');
  } catch (err) {
    content.innerHTML = `<div class="empty-state">${escapeHTML(err.message)}</div>`;
  }
}

function escapeHTML(value) {
  return String(value ?? '').replace(/[&<>"']/g, ch => ({
    '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'
  }[ch]));
}
function closeDetails() { document.getElementById('detailsModal').classList.remove('open'); }
document.addEventListener('click', event => {
  const button = event.target.closest('.metric-link');
  if (button) showContributionDetails(button.dataset.user, button.dataset.metric, button.dataset.period || '');
  if (event.target.id === 'detailsModal') closeDetails();
});

function exportData(format) {
  if (!currentData) return alert('Please generate data first before exporting.');
  let csvContent = "data:text/csv;charset=utf-8,";
  
  const headers = ["GitHub User", "Metric", ...currentQuarters.map(q => q.short_label), "Grand Total"];
  csvContent += headers.join(",") + "\\n";

  const metrics = [
    { label: "Pull Requests Opened", key: "prs" },
    { label: "Issues Opened", key: "issues" },
    { label: "Code Reviews Done", key: "reviews" },
    { label: "Total Contributions", key: "totals" }
  ];

  for (const [user, uData] of Object.entries(currentData)) {
    metrics.forEach(m => {
      let row = [user, `"${m.label}"`];
      let rowTotal = 0;
      currentQuarters.forEach(q => {
        const val = uData[m.key][q.key] || 0;
        rowTotal += val;
        row.push(val);
      });
      row.push(rowTotal);
      csvContent += row.join(",") + "\\n";
    });
  }

  const encodedUri = encodeURI(csvContent);
  const link = document.createElement("a");
  link.setAttribute("href", encodedUri);
  link.setAttribute("download", "github_contributions_matrix.csv");
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

window.onload = loadConfig;
</script>
</body>
</html>"""


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle each HTTP request in a separate thread so config saves,
    SSE streams, and other requests never block each other."""
    daemon_threads = True


class DashboardRequestHandler(BaseHTTPRequestHandler):
    def _send_json(self, data, status=200):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_sse_event(self, data: dict):
        """Write one Server-Sent Event frame (no buffering)."""
        msg = f"data: {json.dumps(data)}\n\n".encode("utf-8")
        self.wfile.write(msg)
        self.wfile.flush()

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            body = INDEX_HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/api/config":
            self._send_json(load_config())
        elif self.path == "/api/cache/clear":
            try:
                clear_cache()
                self._send_json({"status": "ok", "message": "Cache cleared"})
            except Exception as e:
                self._send_json({"status": "error", "message": str(e)})
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # suppress per-request console noise

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode("utf-8")
        try:
            payload = json.loads(body) if body else {}
        except json.JSONDecodeError:
            self._send_json({"status": "error", "message": "Invalid JSON"}, status=400)
            return

        cfg = load_config()

        if self.path == "/api/config/user":
            new_user = payload.get("user", "").strip().lstrip("@")
            token = payload.get("token") or None
            if not new_user:
                self._send_json({"status": "error", "message": "Username cannot be empty."}, status=400)
                return
            if new_user not in cfg["users"]:
                try:
                    if not validate_github_user(new_user, token):
                        self._send_json({"status": "error", "message": f"GitHub user '@{new_user}' does not exist."}, status=400)
                        return
                except Exception as e:
                    self._send_json({"status": "error", "message": f"Failed to validate user: {str(e)}"}, status=400)
                    return
                cfg["users"].append(new_user)
                save_config(cfg)
            self._send_json({"status": "ok", "users": cfg["users"]})

        elif self.path == "/api/config/user/delete":
            to_delete = payload.get("users", [])
            cfg["users"] = [u for u in cfg["users"] if u not in to_delete]
            save_config(cfg)
            self._send_json({"status": "ok", "users": cfg["users"]})

        elif self.path == "/api/config/repo":
            new_repo = payload.get("repo", "").strip()
            token = payload.get("token") or None
            if not new_repo or "/" not in new_repo:
                self._send_json({"status": "error", "message": "Repository must be in owner/repo format."}, status=400)
                return
            if new_repo not in cfg["repositories"]:
                try:
                    if not validate_github_repo(new_repo, token):
                        self._send_json({"status": "error", "message": f"GitHub repository '{new_repo}' does not exist or is private/inaccessible."}, status=400)
                        return
                except Exception as e:
                    self._send_json({"status": "error", "message": f"Failed to validate repository: {str(e)}"}, status=400)
                    return
                cfg["repositories"].append(new_repo)
                save_config(cfg)
            self._send_json({"status": "ok", "repositories": cfg["repositories"]})

        elif self.path == "/api/config/repo/delete":
            to_delete = payload.get("repos", [])
            cfg["repositories"] = [r for r in cfg["repositories"] if r not in to_delete]
            save_config(cfg)
            self._send_json({"status": "ok", "repositories": cfg["repositories"]})

        elif self.path == "/api/config/year":
            new_year = payload.get("year")
            if new_year and new_year not in cfg["years"]:
                cfg["years"].append(new_year)
                cfg["years"].sort()
                save_config(cfg)
            self._send_json({"status": "ok", "years": cfg["years"]})

        elif self.path == "/api/config/year/delete":
            to_delete = payload.get("years", [])
            cfg["years"] = [y for y in cfg["years"] if y not in to_delete]
            save_config(cfg)
            self._send_json({"status": "ok", "years": cfg["years"]})

        elif self.path == "/api/details":
            user = str(payload.get("user", "")).strip().lstrip("@")
            metric = str(payload.get("metric", "")).strip()
            repos = payload.get("repos") or []
            years = payload.get("years") or []
            token = payload.get("token") or None
            period_key = str(payload.get("period") or "")
            period = None
            if period_key:
                match = re.fullmatch(r"(\d{4})-M(\d{2})", period_key)
                qmatch = re.fullmatch(r"(\d{4})-Q([1-4])", period_key)
                if match:
                    y, m = int(match.group(1)), int(match.group(2))
                    if not 1 <= m <= 12:
                        self._send_json({"status": "error", "message": "Invalid period."}, 400)
                        return
                    start = date(y, m, 1)
                    end = date(y, m, calendar.monthrange(y, m)[1])
                    if end > date.today():
                        end = date.today()
                    period = f"{start.isoformat()}..{end.isoformat()}"
                elif qmatch:
                    y, q = int(qmatch.group(1)), int(qmatch.group(2))
                    sm = (q - 1) * 3 + 1
                    start = date(y, sm, 1)
                    em = sm + 2
                    end = date(y, em, calendar.monthrange(y, em)[1])
                    if end > date.today():
                        end = date.today()
                    period = f"{start.isoformat()}..{end.isoformat()}"
                else:
                    self._send_json({"status": "error", "message": "Invalid period key."}, 400)
                    return
            try:
                items = github_search_details(user, metric, repos, years, token, period)
                self._send_json({"status": "ok", "items": items})
            except Exception as exc:
                self._send_json({"status": "error", "message": str(exc)}, 400)

        elif self.path == "/api/query":
            users = payload.get("users", [])
            repos = payload.get("repos", [])
            years = payload.get("years", [])
            token = payload.get("token") or None

            # Always fetch monthly; the UI aggregates months into quarters.
            quarters = get_months_for_years(years)   # NB: "quarters" here means generic time periods

            # Build the full list of (user, quarter, metric) tasks upfront
            # so we can report accurate progress to the UI via SSE.
            tasks = []
            for u in users:
                u_clean = u.strip().lstrip("@")
                for q in quarters:
                    ttl = CACHE_TTL_FINAL if q["final"] else CACHE_TTL
                    for metric, qtype in [("prs",     f"type:pr author:{u_clean}"),
                                          ("issues",  f"type:issue author:{u_clean}"),
                                          ("reviews", f"type:pr reviewed-by:{u_clean}")]:
                        tasks.append((u_clean, q["key"], metric, qtype, q["date_query"], ttl))

            total = len(tasks)
            results = {}   # (user, q_key, metric) -> count

            # SSE stream: send headers then flush results as they arrive
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()

            completed = 0
            errors = []

            def run_task(args):
                u_clean, q_key, metric, qtype, date_query, ttl = args
                count = cached_fetch(qtype, repos, date_query, token, ttl)
                return (u_clean, q_key, metric, count)

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                futures = {pool.submit(run_task, t): t for t in tasks}
                for future in as_completed(futures):
                    try:
                        u_clean, q_key, metric, count = future.result()
                        results[(u_clean, q_key, metric)] = count
                        completed += 1
                        self._send_sse_event({
                            "type": "progress",
                            "done": completed,
                            "total": total,
                            "user": u_clean,
                            "quarter": q_key,
                            "metric": metric,
                            "count": count
                        })
                    except Exception as e:
                        completed += 1
                        t = futures[future]
                        errors.append(f"@{t[0]} {t[1]} {t[2]}: {e}")

            # Build the matrix from collected results
            matrix = {}
            for u in users:
                u_clean = u.strip().lstrip("@")
                matrix[u_clean] = {"prs": {}, "issues": {}, "reviews": {}, "totals": {}}
                for q in quarters:
                    prs     = results.get((u_clean, q["key"], "prs"),     0)
                    issues  = results.get((u_clean, q["key"], "issues"),  0)
                    reviews = results.get((u_clean, q["key"], "reviews"), 0)
                    matrix[u_clean]["prs"][q["key"]]     = prs
                    matrix[u_clean]["issues"][q["key"]]  = issues
                    matrix[u_clean]["reviews"][q["key"]] = reviews
                    matrix[u_clean]["totals"][q["key"]]  = prs + issues + reviews

            # Final SSE event carries the full result
            self._send_sse_event({
                "type": "done",
                "matrix": matrix,
                "quarters": quarters,
                "errors": errors
            })

        else:
            self.send_response(404)
            self.end_headers()


def run_server():
    server_address = ("", PORT)
    try:
        httpd = ThreadedHTTPServer(server_address, DashboardRequestHandler)
    except OSError as e:
        if e.errno == 48 or e.errno == 98:  # 48=macOS, 98=Linux "Address already in use"
            print("=" * 60)
            print(f"❌  Port {PORT} is already in use.")
            print(f"    Another instance is probably running.")
            print(f"    To find and stop it:")
            print(f"      lsof -i :{PORT}        # find the PID")
            print(f"      kill -9 <PID>          # stop it")
            print(f"    Or open http://localhost:{PORT} — it may already be serving.")
            print("=" * 60)
        else:
            raise
        return

    print("=" * 60)
    print(f"🚀 GitHub Dashboard Web Application Started!")
    print(f"👉 Open in your browser: http://localhost:{PORT}")
    print("=" * 60)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[+] Server stopped.")


if __name__ == "__main__":
    run_server()


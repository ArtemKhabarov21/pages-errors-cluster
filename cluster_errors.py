#!/usr/bin/env python3
"""Сбор и кластеризация ошибок Pages-схем.

Тянет все ошибки с эндпоинта менеджера, группирует похожие (одинаковый текст
с подставленными id, доменами, числами и т.п.) и пишет отчёты рядом со скриптом:
reports/latest.md, reports/latest.json и датированные копии.

Запуск:
  python3 cluster_errors.py              # цикл: раз в 30 минут
  python3 cluster_errors.py --once       # один проход (для cron)
  python3 cluster_errors.py --file x.json  # офлайн: взять ответ эндпоинта из файла

Настройки: файл .env рядом со скриптом (см. .env.example) или переменные окружения.
Переменные окружения важнее .env.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPORTS = HERE / "reports"
STATE = HERE / "state.json"
ENV_FILE = HERE / ".env"
URL_PLACEHOLDER = "https://YOUR-MANAGER-HOST/api/v1/pages-schemas/errors"
INTERVAL_SECONDS = 30 * 60
KEEP_REPORTS = 400  # датированных отчётов храним столько, остальные удаляем

# Порядок важен: сначала длинные шаблоны (URL, e-mail), потом короткие (числа).
NORMALIZERS = [
    (re.compile(r"https?://\S+"), "<url>"),
    (re.compile(r"\S+@\S+\.\w+"), "<email>"),
    (re.compile(r"\b[0-9a-f]{8,}\b", re.I), "<hex>"),
    (re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b"), "<ip>"),
    (re.compile(r"\b[a-z0-9-]+(?:\.[a-z0-9-]+)*\.[a-z]{2,}\b", re.I), "<domain>"),
    (re.compile(r"'[^']*'|\"[^\"]*\"|«[^»]*»"), "<str>"),
    (re.compile(r"\d+"), "<n>"),
]


def normalize(text: str, limit: int) -> str:
    """Шаблон сообщения: то, что меняется от случая к случаю, заменено."""
    text = (text or "").strip().splitlines()[0] if (text or "").strip() else ""
    text = text[:limit]
    for pattern, token in NORMALIZERS:
        text = pattern.sub(token, text)
    return re.sub(r"\s+", " ", text).strip().lower()


def cluster_key(error: dict) -> str:
    """Группа — по тексту сообщения. Детали у каждого домена свои, поэтому
    в ключ их не берём: они показываются внутри кластера вариантами."""
    raw = "|".join([
        str(error.get("source") or ""),
        str(error.get("level") or ""),
        normalize(error.get("message") or "", 300),
    ])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def parse_ts(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def build_clusters(errors: list[dict], now: dt.datetime) -> list[dict]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for error in errors:
        groups[cluster_key(error)].append(error)

    day_ago = now - dt.timedelta(hours=24)
    clusters = []
    for key, items in groups.items():
        first = min((parse_ts(e.get("first_seen_at")) for e in items if e.get("first_seen_at")), default=None)
        last = max((parse_ts(e.get("last_seen_at")) for e in items if e.get("last_seen_at")), default=None)
        # Варианты деталей: сколько разных текстов ошибки за этой группой и по одному примеру каждого.
        variants: dict[str, dict] = {}
        for e in sorted(items, key=lambda e: e.get("occurrences") or 0, reverse=True):
            variant_key = normalize(e.get("detail") or "", 200)
            if variant_key not in variants:
                variants[variant_key] = {
                    "message": e.get("message"),
                    "detail": (e.get("detail") or "")[:300],
                    "domain": e.get("domain"),
                    "schema": (e.get("schema") or {}).get("name"),
                    "count": 0,
                }
            variants[variant_key]["count"] += int(e.get("occurrences") or 0)
        samples = sorted(variants.values(), key=lambda v: v["count"], reverse=True)[:3]
        clusters.append({
            "key": key,
            "level": Counter(e.get("level") for e in items).most_common(1)[0][0],
            "source": items[0].get("source"),
            "source_label": items[0].get("source_label"),
            "template": normalize(items[0].get("message") or "", 300),
            "records": len(items),
            "occurrences": sum(int(e.get("occurrences") or 0) for e in items),
            "open": sum(1 for e in items if not e.get("resolved_at")),
            "resolved": sum(1 for e in items if e.get("resolved_at")),
            "last_24h": sum(1 for e in items if (parse_ts(e.get("last_seen_at")) or dt.datetime.min.replace(tzinfo=dt.timezone.utc)) >= day_ago),
            "domains": sorted({e.get("domain") for e in items if e.get("domain")})[:20],
            "domains_total": len({e.get("domain") for e in items if e.get("domain")}),
            "schemas": sorted({(e.get("schema") or {}).get("name") for e in items if e.get("schema")}),
            "first_seen": first.isoformat() if first else None,
            "last_seen": last.isoformat() if last else None,
            "detail_variants": len(variants),
            "samples": samples,
        })
    clusters.sort(key=lambda c: (c["open"], c["occurrences"]), reverse=True)
    return clusters


def load_previous() -> set[str]:
    if not STATE.exists():
        return set()
    try:
        return set(json.loads(STATE.read_text(encoding="utf-8")).get("keys", []))
    except (ValueError, OSError):
        return set()


def load_env_file(path: Path) -> None:
    """Читает простой KEY=VALUE из .env. Уже заданные переменные окружения не трогаем."""
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def fetch(url: str, token: str, timeout: int) -> dict:
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def render_markdown(stamp: str, errors: list[dict], clusters: list[dict], new_keys: set[str]) -> str:
    open_total = sum(1 for e in errors if not e.get("resolved_at"))
    level_counts = Counter(e.get("level") for e in errors if not e.get("resolved_at"))
    lines = [
        f"# Отчёт по ошибкам Pages-схем, {stamp}",
        "",
        f"- Записей всего: **{len(errors)}**, открытых: **{open_total}** "
        f"(ошибок {level_counts.get('error', 0)}, предупреждений {level_counts.get('warning', 0)})",
        f"- Кластеров: **{len(clusters)}**, новых с прошлого отчёта: **{len(new_keys)}**",
        "",
        "## Топ кластеров (по открытым, затем по числу повторов)",
        "",
        "| # | Уровень | Источник | Открытых | Повторов | За 24 ч | Доменов | Текст |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for i, c in enumerate(clusters[:25], 1):
        mark = " 🆕" if c["key"] in new_keys else ""
        text = c["template"].replace("|", "\\|")[:120]
        lines.append(f"| {i} | {c['level']} | {c['source_label'] or c['source']} | {c['open']} | {c['occurrences']} | {c['last_24h']} | {c['domains_total']} | {text}{mark} |")

    texts = {c["template"] for c in clusters}
    if "выкладка домена не удалась" in texts and "задача упала: deploypagesdomainjob" in texts:
        lines += ["", "> Одна неудачная выкладка даёт две записи: «выкладка домена не удалась» (предупреждение) "
                  "и «задача упала» (ошибка). Поэтому их счётчики совпадают, это одно и то же событие."]

    if new_keys:
        lines += ["", "## Новые кластеры с прошлого отчёта", ""]
        for c in clusters:
            if c["key"] in new_keys:
                lines.append(f"- **{c['source_label'] or c['source']}**, {c['level']}: {c['template'][:160]} (повторов {c['occurrences']}, доменов {c['domains_total']})")

    lines += ["", "## Подробно", ""]
    for c in clusters[:25]:
        lines += [
            f"### {c['source_label'] or c['source']}: {c['template'][:140]}",
            "",
            f"- Ключ: `{c['key']}`, уровень: {c['level']}, записей: {c['records']}, открытых: {c['open']}, решённых: {c['resolved']}",
            f"- Повторов всего: {c['occurrences']}, за последние 24 ч записей: {c['last_24h']}",
            f"- Схемы: {', '.join(c['schemas']) or '—'}",
            f"- Домены ({c['domains_total']}): {', '.join(c['domains']) or '—'}",
            f"- Первый раз: {c['first_seen'] or '—'}, последний: {c['last_seen'] or '—'}",
            f"- Вариантов деталей: {c['detail_variants']}",
        ]
        for s in c["samples"]:
            lines.append(f"  - {s['count']} раз: `{(s['message'] or '')[:200]}`" + (f", деталь: `{s['detail'][:200]}`" if s["detail"] else ""))
        lines.append("")
    return "\n".join(lines)


def prune_reports() -> None:
    files = sorted(REPORTS.glob("report-*.md"))
    for old in files[:-KEEP_REPORTS]:
        old.unlink(missing_ok=True)
        old.with_suffix(".json").unlink(missing_ok=True)


def run_once(args: argparse.Namespace) -> None:
    now = dt.datetime.now(dt.timezone.utc)
    if args.file:
        payload = json.loads(Path(args.file).read_text(encoding="utf-8"))
    else:
        token = os.environ.get("PAGES_ERRORS_TOKEN", "")
        url = os.environ.get("PAGES_ERRORS_URL", "")
        if not token:
            raise SystemExit("Нет токена: задайте PAGES_ERRORS_TOKEN в .env (см. .env.example)")
        if not url or url == URL_PLACEHOLDER:
            raise SystemExit("Нет адреса менеджера: задайте PAGES_ERRORS_URL в .env (см. .env.example)")
        payload = fetch(url, token, args.timeout)

    errors = payload.get("items", [])
    clusters = build_clusters(errors, now)
    previous = load_previous()
    new_keys = {c["key"] for c in clusters} - previous if previous else set()

    REPORTS.mkdir(exist_ok=True)
    stamp = now.astimezone().strftime("%Y-%m-%d %H:%M")
    slug = now.astimezone().strftime("%Y-%m-%d_%H-%M")
    markdown = render_markdown(stamp, errors, clusters, new_keys)
    report_json = {
        "generated_at": now.isoformat(),
        "source_generated_at": payload.get("generated_at"),
        "total_records": len(errors),
        "open_records": sum(1 for e in errors if not e.get("resolved_at")),
        "new_cluster_keys": sorted(new_keys),
        "clusters": clusters,
    }
    body = json.dumps(report_json, ensure_ascii=False, indent=2)

    (REPORTS / f"report-{slug}.md").write_text(markdown, encoding="utf-8")
    (REPORTS / f"report-{slug}.json").write_text(body, encoding="utf-8")
    (REPORTS / "latest.md").write_text(markdown, encoding="utf-8")
    (REPORTS / "latest.json").write_text(body, encoding="utf-8")
    # Накопительно: кластер, который пропал на цикл и вернулся, новым не считаем.
    seen = previous | {c["key"] for c in clusters}
    STATE.write_text(json.dumps({"keys": sorted(seen), "at": now.isoformat()}), encoding="utf-8")
    prune_reports()
    print(f"[{stamp}] записей {len(errors)}, кластеров {len(clusters)}, новых {len(new_keys)} -> reports/latest.md", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--once", action="store_true", help="один проход и выход")
    parser.add_argument("--file", help="офлайн: JSON-ответ эндпоинта из файла вместо запроса")
    parser.add_argument("--interval", type=int, default=INTERVAL_SECONDS, help="секунд между проходами (по умолчанию 1800)")
    parser.add_argument("--timeout", type=int, default=60, help="таймаут запроса, с")
    args = parser.parse_args()
    load_env_file(ENV_FILE)

    if args.once or args.file:
        run_once(args)
        return
    while True:
        try:
            run_once(args)
        except (urllib.error.URLError, urllib.error.HTTPError, ValueError, OSError) as exc:
            print(f"ошибка прохода: {exc}", file=sys.stderr, flush=True)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()

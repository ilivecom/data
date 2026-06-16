#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
宏观指标数据管道 — data.hiwd.com
通过 yfinance（Yahoo Finance）抓取 DXY/US10Y/VIX/NDX/BTC，
写出 public/macro-data.json 供前端直接 fetch。

运行：
  python scripts/macro_pipeline.py
  python scripts/macro_pipeline.py --json-out public/macro-data.json
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import sys
from pathlib import Path
from typing import Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("macro_pipeline")

SCRIPT_DIR  = Path(__file__).resolve().parent
DEFAULT_OUT = SCRIPT_DIR.parent / "public" / "macro-data.json"

# ── 要采集的指标（Yahoo Finance 代码）──────────────────────────────────
SYMBOLS = [
    {"ticker": "DX=F",    "key": "DXY",   "label": "美元指数",       "fmt": "price2"},
    {"ticker": "^TNX",    "key": "US10Y",  "label": "美债10Y收益率",  "fmt": "pct"},
    {"ticker": "^VIX",    "key": "VIX",    "label": "恐慌指数",       "fmt": "price2"},
    {"ticker": "^NDX",    "key": "NDX",    "label": "纳斯达克100",    "fmt": "int"},
    {"ticker": "BTC-USD", "key": "BTC",    "label": "比特币/USD",     "fmt": "btc"},
]


# ══════════════════════════════════════════════════════════════════════
#  单指标行情获取
# ══════════════════════════════════════════════════════════════════════

def get_quote(yf_ticker: str) -> Tuple[Optional[float], Optional[float]]:
    """
    返回 (最新价, 前一收盘价)。
    优先使用 fast_info（近实时），失败则回落到历史 K 线最近两根。
    """
    try:
        import yfinance as yf

        t = yf.Ticker(yf_ticker)

        # ── fast_info（近实时，延迟 ~15min）─────────────────────────
        try:
            fi = t.fast_info
            price = fi.last_price
            prev  = fi.previous_close
            if price is not None and prev is not None and price > 0:
                return float(price), float(prev)
        except Exception:
            pass

        # ── 兜底：历史 K 线最近两根收盘价 ──────────────────────────
        hist   = t.history(period="5d", interval="1d", auto_adjust=True)
        closes = hist["Close"].dropna()
        if len(closes) >= 2:
            return float(closes.iloc[-1]), float(closes.iloc[-2])
        if len(closes) == 1:
            p = float(closes.iloc[0])
            return p, p

    except Exception as exc:
        log.warning(f"  {yf_ticker} 获取失败: {exc}")

    return None, None


# ══════════════════════════════════════════════════════════════════════
#  格式化显示值
# ══════════════════════════════════════════════════════════════════════

def fmt_value(price: float, fmt: str) -> str:
    if fmt == "pct":
        return f"{price:.2f}%"        # 4.31%
    if fmt == "int":
        return f"{int(price):,}"      # 19,842
    if fmt == "btc":
        return f"${int(price):,}"     # $67,420
    return f"{price:.2f}"             # 104.23


# ══════════════════════════════════════════════════════════════════════
#  主拉取流程
# ══════════════════════════════════════════════════════════════════════

def fetch_macro() -> list:
    try:
        import yfinance  # noqa: F401
    except ImportError:
        log.error("yfinance 未安装，请执行: pip install yfinance>=0.2.40")
        sys.exit(1)

    results = []
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for s in SYMBOLS:
        price, prev = get_quote(s["ticker"])
        if price is None:
            log.warning(f"  ⚠ {s['key']} 无法获取数据，跳过")
            continue

        chg_pct = (price - prev) / prev * 100 if (prev and prev != 0) else 0.0
        direction = (
            "up"   if chg_pct >  0.001 else
            "dn"   if chg_pct < -0.001 else
            "flat"
        )
        pct_str = ("+" if chg_pct >= 0 else "") + f"{chg_pct:.2f}%"

        item = {
            "ticker":  s["key"],
            "label":   s["label"],
            "value":   fmt_value(price, s["fmt"]),
            "pct":     pct_str,
            "dir":     direction,
            "price":   round(float(price), 4),
            "chg_pct": round(float(chg_pct), 3),
            "updated": now_str,
        }
        results.append(item)
        log.info(f"  ✅  {s['key']:6s}  {item['value']:>13s}  {item['pct']}")

    return results


# ══════════════════════════════════════════════════════════════════════
#  入口
# ══════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(description="宏观指标数据管道 — data.hiwd.com")
    parser.add_argument(
        "--json-out",
        type=str,
        default=str(DEFAULT_OUT),
        help=f"JSON 输出路径（默认: {DEFAULT_OUT}）",
    )
    args = parser.parse_args()

    log.info("🌐 拉取全球宏观指标（yfinance / Yahoo Finance）…")
    data = fetch_macro()

    if not data:
        log.error("❌ 未获取到任何宏观数据，退出")
        sys.exit(1)

    payload = {
        "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "macro": data,
    }

    out = Path(args.json_out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log.info(f"✅  写入: {out}  ({len(data)} 条指标)")


if __name__ == "__main__":
    main()

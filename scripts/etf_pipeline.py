#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF 溢价率数据管道  —  data.hiwd.com  Module B
=====================================================
功能
  1. 通过 AKShare 拉取 A 股 ETF 实时行情（市价 + IOPV 估值）
  2. 计算折溢价率 = (市价 - IOPV) / IOPV × 100%
  3. 将结果写出为 JSON 文件（前端直接 fetch，零服务器依赖）
  4. 可选：同步写入 Google Sheets（需要服务账号凭证）

运行方式
  python etf_pipeline.py                            # 正常运行，写 JSON
  python etf_pipeline.py --dry-run                  # 只打印，不写文件
  python etf_pipeline.py --json-out /path/data.json # 指定输出路径
  python etf_pipeline.py --sheets                   # 同时写 Google Sheets

定时任务（工作日 A 股交易时段）
  30 9 * * 1-5         /path/venv/bin/python /path/etf_pipeline.py --json-out /var/www/etf-data.json
  0,30 10-11 * * 1-5   /path/venv/bin/python /path/etf_pipeline.py --json-out /var/www/etf-data.json
  0,30 13-14 * * 1-5   /path/venv/bin/python /path/etf_pipeline.py --json-out /var/www/etf-data.json
  0 15 * * 1-5         /path/venv/bin/python /path/etf_pipeline.py --json-out /var/www/etf-data.json

依赖安装
  pip install -r requirements.txt
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("etf_pipeline")

# ── 路径常量 ────────────────────────────────────────────────────────────────────
SCRIPT_DIR   = Path(__file__).resolve().parent
CONFIG_FILE  = SCRIPT_DIR / "config.json"
DEFAULT_OUT  = SCRIPT_DIR.parent / "public" / "etf-data.json"
TRADE_DATES_CACHE_FILE = SCRIPT_DIR / "cn-trade-dates.json"

# ── 内置默认配置（没有 config.json 时使用）─────────────────────────────────────
DEFAULT_CONFIG: Dict[str, Any] = {
    "etf_list": [
        {"code": "513100", "name": "纳指 ETF"},
        {"code": "513500", "name": "标普 ETF"},
        {"code": "513180", "name": "恒生科技 ETF"},
        {"code": "513880", "name": "日经225 ETF"},
        {"code": "518880", "name": "黄金 ETF"},
    ],
    "thresholds": {
        "danger":  10.0,   # 溢价率 > 10% → 🔴 警告
        "caution":  5.0,   # 溢价率 > 5%  → 🟡 关注
        "safe":     0.0,   # 溢价率 < 0%  → 🟢 套利
    },
    "google_sheets": {
        "spreadsheet_id":   "YOUR_SPREADSHEET_ID",
        "sheet_name":       "ETF溢价",
        "credentials_file": "credentials.json",  # 相对于 SCRIPT_DIR
    },
}

BJT = datetime.timezone(datetime.timedelta(hours=8))
_CN_TRADE_DATES_CACHE: Optional[Set[datetime.date]] = None


# ══════════════════════════════════════════════════════════════════════════════
#  配置加载
# ══════════════════════════════════════════════════════════════════════════════

def load_config() -> Dict[str, Any]:
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, encoding="utf-8") as f:
                cfg = json.load(f)
            log.info(f"已加载配置文件: {CONFIG_FILE}")
            return cfg
        except json.JSONDecodeError as e:
            log.error(f"config.json 解析失败: {e}，将使用内置默认配置")
    else:
        log.warning("config.json 不存在，使用内置默认配置（可复制 config.json.example 修改）")
    return DEFAULT_CONFIG


def _read_cn_trade_dates_cache() -> Optional[Set[datetime.date]]:
    if not TRADE_DATES_CACHE_FILE.exists():
        return None

    try:
        payload = json.loads(TRADE_DATES_CACHE_FILE.read_text(encoding="utf-8"))
        dates = {
            datetime.date.fromisoformat(str(raw)[:10])
            for raw in payload.get("trade_dates", [])
        }
        return dates or None
    except Exception as exc:
        log.warning(f"读取交易日历缓存失败，将尝试在线获取: {exc}")
        return None


def _fetch_and_cache_cn_trade_dates() -> Optional[Set[datetime.date]]:
    try:
        import akshare as ak

        df = ak.tool_trade_date_hist_sina()
        if "trade_date" not in df.columns:
            log.warning("交易日历缺少 trade_date 列，回退到工作日判断")
            return None

        dates: Set[datetime.date] = set()
        for raw in df["trade_date"].dropna().tolist():
            if isinstance(raw, datetime.datetime):
                dates.add(raw.date())
            elif isinstance(raw, datetime.date):
                dates.add(raw)
            else:
                dates.add(datetime.date.fromisoformat(str(raw)[:10]))

        global _CN_TRADE_DATES_CACHE
        _CN_TRADE_DATES_CACHE = dates
        try:
            TRADE_DATES_CACHE_FILE.write_text(
                json.dumps(
                    {
                        "generated_at": datetime.datetime.now(BJT).isoformat(timespec="seconds"),
                        "trade_dates": sorted(d.isoformat() for d in dates),
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
        except Exception as exc:
            log.warning(f"写入交易日历缓存失败: {exc}")
        return dates
    except Exception as exc:
        log.warning(f"交易日历获取失败，回退到工作日判断: {exc}")
        return None


def _load_cn_trade_dates(target: Optional[datetime.date] = None) -> Optional[Set[datetime.date]]:
    """加载 A 股交易日历；缓存不覆盖目标日期时会自动刷新。"""
    global _CN_TRADE_DATES_CACHE
    target_day = target or datetime.datetime.now(BJT).date()

    if _CN_TRADE_DATES_CACHE is not None and target_day <= max(_CN_TRADE_DATES_CACHE):
        return _CN_TRADE_DATES_CACHE

    cached_dates = _read_cn_trade_dates_cache()
    if cached_dates is not None:
        cached_max = max(cached_dates)
        if target_day <= cached_max:
            _CN_TRADE_DATES_CACHE = cached_dates
            return cached_dates
        log.info(f"交易日历缓存仅覆盖到 {cached_max.isoformat()}，尝试在线刷新")

    return _fetch_and_cache_cn_trade_dates()


def _is_cn_trade_day(day: Optional[datetime.date] = None) -> bool:
    """判断某天是否为 A 股交易日；优先使用交易日历，失败时回退到工作日。"""
    target = day or datetime.datetime.now(BJT).date()
    if target.weekday() >= 5:
        return False

    trade_dates = _load_cn_trade_dates(target)
    if trade_dates is None:
        return True
    return target in trade_dates


# ══════════════════════════════════════════════════════════════════════════════
#  数据获取 — AKShare
# ══════════════════════════════════════════════════════════════════════════════

def fetch_etf_data(etf_list: List[Dict], fallback: Optional[Dict[str, Dict]] = None) -> List[Dict]:
    """
    通过 AKShare fund_etf_spot_em() 拉取 ETF 实时行情。
    数据来源：东方财富（15 秒级别更新，交易时段可用）。

    返回字段：
        code        str   ETF 代码（6 位）
        name        str   ETF 简称
        price       float 最新市价（元）
        iopv        float IOPV 实时估值（元），盘中可用
        premium_pct float 折溢价率（%），正数=溢价，负数=折价
        updated     str   采集时间（北京时间）
    """
    try:
        import akshare as ak
    except ImportError:
        sys.exit("❌ 请先安装依赖：pip install akshare")

    log.info("正在从东方财富拉取 ETF 行情（AKShare）…")
    try:
        df = ak.fund_etf_spot_em()
    except Exception as exc:
        log.error(f"AKShare 请求失败: {exc}")
        if fallback:
            now_str = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
            log.warning("使用历史 ETF 数据兜底，并标记为 stale")
            return [
                _fallback_etf_item(etf_cfg, fallback[str(etf_cfg["code"]).zfill(6)], now_str)
                for etf_cfg in etf_list
                if str(etf_cfg["code"]).zfill(6) in fallback
            ]
        return []

    # 调试用：打印列名，便于排查 AKShare 版本差异
    log.debug(f"AKShare 返回列名: {list(df.columns)}")

    # 标准化代码列（保留前导零）
    code_col = _find_col(df.columns, ["代码", "code", "基金代码"])
    if code_col is None:
        log.error("无法识别代码列，请检查 AKShare 版本（列名变更）")
        return []
    df[code_col] = df[code_col].astype(str).str.zfill(6)

    now_str = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
    results = []

    for etf_cfg in etf_list:
        code = str(etf_cfg["code"]).zfill(6)
        row  = df[df[code_col] == code]
        if row.empty:
            if fallback and code in fallback:
                log.warning(f"  ⚠ 未找到 ETF {code}（{etf_cfg.get('name','')}），沿用历史数据")
                results.append(_fallback_etf_item(etf_cfg, fallback[code], now_str))
                continue
            log.warning(f"  ⚠ 未找到 ETF {code}（{etf_cfg.get('name','')}），跳过")
            continue
        r = row.iloc[0]

        # ── 市价 ─────────────────────────────────────────────────────
        price = _get_float(r, ["最新价", "当前价", "price", "收盘价"])

        # ── IOPV（盘中估算净值）────────────────────────────────────────
        iopv = _get_float(r, ["IOPV实时估值", "估算净值", "净值估算", "iopv"])

        # ── 折溢价率 ──────────────────────────────────────────────────
        # 优先用市价和 IOPV 自行计算，避免 AKShare 字段名/正负号语义变化。
        premium_pct = None
        if price is not None and iopv is not None and iopv != 0:
            premium_pct = (price - iopv) / iopv * 100.0
        else:
            premium_pct = _get_premium_col(r)
            if premium_pct is None and fallback and code in fallback:
                # 午休等非交易时段 IOPV=0，沿用上次已知溢价率（标注为估算）
                fb = fallback[code]
                log.info(f"  {code} IOPV 暂无，沿用上次溢价率 {fb['premium_pct']:+.2f}%（午休/非交易时段）")
                results.append({
                    "code":        code,
                    "name":        str(etf_cfg.get("name") or fb.get("name") or code),
                    "price":       round(float(price or fb.get("price", 0)), 4),
                    "iopv":        round(float(fb.get("iopv", 0)), 4),
                    "premium_pct": fb["premium_pct"],
                    "updated":     fb.get("updated", now_str),
                    "stale":       True,
                })
                continue
            if premium_pct is None:
                log.warning(f"  ⚠ ETF {code} 无法计算溢价率（缺少市价或 IOPV），跳过")
                continue

        # ── 名称（优先用 config，其次读 AKShare）─────────────────────
        name_col = _find_col(df.columns, ["简称", "基金简称", "name", "基金名称"])
        name = etf_cfg.get("name") or (r[name_col] if name_col else code)

        pct_rounded = round(float(premium_pct), 2)
        log.info(f"  {code}  {name:<12}  溢价率 {pct_rounded:+.2f}%")

        results.append({
            "code":        code,
            "name":        str(name),
            "price":       round(float(price or 0), 4),
            "iopv":        round(float(iopv or 0), 4),
            "premium_pct": pct_rounded,
            "updated":     now_str,
        })

    return results


def _find_col(columns, candidates: List[str]) -> Optional[str]:
    """在 columns 里找第一个匹配的候选列名"""
    for c in candidates:
        if c in columns:
            return c
    return None


def _get_float(row, candidates: List[str]) -> Optional[float]:
    """从 row 中按候选列名顺序读取浮点值"""
    for col in candidates:
        if col in row.index:
            try:
                return float(row[col])
            except (ValueError, TypeError):
                continue
    return None


def _get_premium_col(row) -> Optional[float]:
    """兼容多版本 AKShare 的折溢价率列名，并处理 '1.23%' 字符串格式"""
    candidates = ["折溢价率", "溢价率", "折溢价率(%)", "折价溢价率", "基金折价率", "premium_rate"]
    for col in candidates:
        if col in row.index:
            try:
                s = str(row[col]).replace("%", "").strip()
                return float(s)
            except (ValueError, TypeError):
                continue
    return None


def _fallback_etf_item(etf_cfg: Dict, fb: Dict, checked_at: str) -> Dict:
    """行情源失败时沿用上次数据，但保留原始行情时间，避免误报为实时数据。"""
    code = str(etf_cfg["code"]).zfill(6)
    return {
        "code":        code,
        "name":        str(etf_cfg.get("name") or fb.get("name") or code),
        "price":       round(float(fb.get("price", 0)), 4),
        "iopv":        round(float(fb.get("iopv", 0)), 4),
        "premium_pct": round(float(fb.get("premium_pct", 0)), 2),
        "updated":     fb.get("updated", checked_at),
        "checked_at":  checked_at,
        "stale":       True,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  信号判断
# ══════════════════════════════════════════════════════════════════════════════

def get_signal(premium_pct: float, thresholds: Dict) -> str:
    """
    根据溢价率返回信号标签：
      danger   > thresholds.danger  (default 10%)
      caution  > thresholds.caution (default 5%)
      safe     < thresholds.safe    (default 0%)
      neutral  其余
    """
    d = float(thresholds.get("danger",  10.0))
    c = float(thresholds.get("caution",  5.0))
    s = float(thresholds.get("safe",     0.0))

    if premium_pct > d:
        return "danger"
    if premium_pct > c:
        return "caution"
    if premium_pct < s:
        return "safe"
    return "neutral"


# ══════════════════════════════════════════════════════════════════════════════
#  南向资金（港股通）净流入
# ══════════════════════════════════════════════════════════════════════════════

def fetch_southbound_flow() -> Optional[Dict]:
    """获取今日南向资金（港股通沪+深）净买入额，单位亿元。"""
    try:
        import akshare as ak
    except ImportError:
        return None

    try:
        df = ak.stock_hsgt_fund_flow_summary_em()
    except Exception as exc:
        log.warning(f"南向资金数据获取失败: {exc}")
        return None

    south_rows = df[df["资金方向"] == "南向"]
    if south_rows.empty:
        log.warning("未找到南向资金数据")
        return None

    total_net = south_rows["成交净买额"].sum()
    now_str = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")

    return {
        "net_buy": round(float(total_net), 2),
        "updated": now_str,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  JSON 输出
# ══════════════════════════════════════════════════════════════════════════════

def build_payload(data: List[Dict], thresholds: Dict, southbound: Optional[Dict] = None) -> Dict:
    """构造前端所需的 JSON payload"""
    etfs = []
    for d in data:
        etfs.append({
            **d,
            "signal": get_signal(d["premium_pct"], thresholds),
        })
    now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).isoformat(timespec="seconds")
    updated_values = [str(d.get("updated", "")) for d in etfs if d.get("updated")]
    payload = {
        "generated_at": now,
        "checked_at": now,
        "data_updated_at": max(updated_values) if updated_values else now,
        "source_status": "stale_fallback" if any(d.get("stale") for d in etfs) else "live",
        "is_market_open": _is_cn_market_open(),
        "etfs": etfs,
    }
    if southbound:
        payload["southbound"] = southbound
    return payload


def write_json(payload: Dict, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log.info(f"✅ JSON 已写入: {out_path}")


def _is_cn_market_open() -> bool:
    """判断 A 股是否在交易时段（北京时间交易日 9:30–11:30、13:00–15:00）"""
    now = datetime.datetime.now(BJT)
    if not _is_cn_trade_day(now.date()):
        return False
    t = now.hour * 60 + now.minute
    return (570 <= t <= 690) or (780 <= t <= 900)


# ══════════════════════════════════════════════════════════════════════════════
#  Google Sheets 写入（可选）
# ══════════════════════════════════════════════════════════════════════════════

def write_to_sheets(payload: Dict, sheets_cfg: Dict) -> None:
    """
    将结果写入 Google Sheets（需要 gspread + google-auth）。

    前置准备：
      1. 在 Google Cloud Console 创建服务账号，下载 JSON 凭证
      2. 将凭证文件放在 scripts/credentials.json（已加入 .gitignore！）
      3. 把服务账号邮箱加为 Google Sheet 的编辑者
      4. 在 config.json 填写 spreadsheet_id 和 sheet_name
    """
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        log.error("gspread/google-auth 未安装：pip install gspread google-auth")
        return

    creds_path = SCRIPT_DIR / sheets_cfg.get("credentials_file", "credentials.json")
    if not creds_path.exists():
        log.error(f"凭证文件不存在: {creds_path}  →  跳过 Sheets 写入")
        return

    spreadsheet_id = sheets_cfg.get("spreadsheet_id", "")
    if not spreadsheet_id or spreadsheet_id == "YOUR_SPREADSHEET_ID":
        log.error("config.json 中 spreadsheet_id 未设置  →  跳过 Sheets 写入")
        return

    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds  = Credentials.from_service_account_file(str(creds_path), scopes=scopes)
        gc     = gspread.authorize(creds)
        sh     = gc.open_by_key(spreadsheet_id)
        ws     = sh.worksheet(sheets_cfg.get("sheet_name", "ETF溢价"))

        header = ["代码", "名称", "市价(元)", "IOPV(元)", "溢价率(%)", "信号", "更新时间"]
        rows   = [header]
        for etf in payload["etfs"]:
            rows.append([
                etf["code"],
                etf["name"],
                etf["price"],
                etf["iopv"],
                etf["premium_pct"],
                etf["signal"],
                etf["updated"],
            ])

        ws.update(range_name="A1", values=rows)
        log.info(f"✅ Google Sheets 已更新（{len(payload['etfs'])} 条）: {spreadsheet_id}")
    except Exception as exc:
        log.error(f"Sheets 写入失败: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
#  入口
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="ETF 溢价率数据管道 — data.hiwd.com Module B"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只拉数据打印，不写入任何文件",
    )
    parser.add_argument(
        "--json-out",
        type=str,
        default=str(DEFAULT_OUT),
        help=f"JSON 输出路径（默认: {DEFAULT_OUT}）",
    )
    parser.add_argument(
        "--sheets",
        action="store_true",
        help="同时写入 Google Sheets",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="输出调试信息（含 AKShare 列名）",
    )
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    cfg        = load_config()
    etf_list   = cfg.get("etf_list",       DEFAULT_CONFIG["etf_list"])
    thresholds = cfg.get("thresholds",     DEFAULT_CONFIG["thresholds"])
    sheets_cfg = cfg.get("google_sheets",  DEFAULT_CONFIG["google_sheets"])

    # 1. 加载上次已知数据（用于非交易时段 IOPV=0 时的兜底）
    out_path = Path(args.json_out)
    fallback: Dict[str, Dict] = {}
    if out_path.exists():
        try:
            prev = json.loads(out_path.read_text(encoding="utf-8"))
            for etf in prev.get("etfs", []):
                if etf.get("code"):
                    fallback[etf["code"]] = etf
            log.info(f"已加载历史数据作为兜底（{len(fallback)} 条）")
        except Exception as e:
            log.warning(f"读取历史 JSON 失败: {e}")

    # 2. 拉数据
    data = fetch_etf_data(etf_list, fallback=fallback)
    if not data:
        log.error("❌ 未获取到任何 ETF 数据，退出")
        sys.exit(1)

    # 2b. 拉南向资金
    southbound = fetch_southbound_flow()
    if southbound:
        log.info(f"  南向资金净买入: {southbound['net_buy']:+.2f} 亿")
    else:
        log.warning("  南向资金数据不可用，跳过")

    # 3. 构建 payload
    payload = build_payload(data, thresholds, southbound=southbound)

    if args.dry_run:
        log.info("[DRY RUN] 结果预览 ↓")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    # 4. 写 JSON
    write_json(payload, out_path)

    # 5. 可选：写 Google Sheets
    if args.sheets:
        write_to_sheets(payload, sheets_cfg)

    log.info("🏁 全部完成")


if __name__ == "__main__":
    main()

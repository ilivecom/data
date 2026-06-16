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

定时任务（工作日 9:31–15:59，每 15 分钟）
  15,30,45 9 * * 1-5   /path/venv/bin/python /path/etf_pipeline.py --json-out /var/www/etf-data.json
  */15 10-14 * * 1-5   /path/venv/bin/python /path/etf_pipeline.py --json-out /var/www/etf-data.json
  0,15,30 15 * * 1-5   /path/venv/bin/python /path/etf_pipeline.py --json-out /var/www/etf-data.json

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
from typing import Any, Dict, List, Optional

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

# ── 内置默认配置（没有 config.json 时使用）─────────────────────────────────────
DEFAULT_CONFIG: Dict[str, Any] = {
    "etf_list": [
        {"code": "513100", "name": "纳指 ETF"},
        {"code": "513500", "name": "标普 ETF"},
        {"code": "159509", "name": "纳指科技"},
        {"code": "159941", "name": "纳指 ETF（广发）"},
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


# ══════════════════════════════════════════════════════════════════════════════
#  数据获取 — AKShare
# ══════════════════════════════════════════════════════════════════════════════

def fetch_etf_data(etf_list: List[Dict]) -> List[Dict]:
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
        return []

    # 调试用：打印列名，便于排查 AKShare 版本差异
    log.debug(f"AKShare 返回列名: {list(df.columns)}")

    # 标准化代码列（保留前导零）
    code_col = _find_col(df.columns, ["代码", "code", "基金代码"])
    if code_col is None:
        log.error("无法识别代码列，请检查 AKShare 版本（列名变更）")
        return []
    df[code_col] = df[code_col].astype(str).str.zfill(6)

    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    results = []

    for etf_cfg in etf_list:
        code = str(etf_cfg["code"]).zfill(6)
        row  = df[df[code_col] == code]
        if row.empty:
            log.warning(f"  ⚠ 未找到 ETF {code}（{etf_cfg.get('name','')}），跳过")
            continue
        r = row.iloc[0]

        # ── 市价 ─────────────────────────────────────────────────────
        price = _get_float(r, ["最新价", "当前价", "price", "收盘价"])

        # ── IOPV（盘中估算净值）────────────────────────────────────────
        iopv = _get_float(r, ["IOPV实时估值", "估算净值", "净值估算", "iopv"])

        # ── 折溢价率 ──────────────────────────────────────────────────
        # 优先读 AKShare 直接提供的列（已帮我们算好）
        premium_pct = _get_premium_col(r)

        # 若无现成列，用 (price - iopv) / iopv × 100 自行计算
        if premium_pct is None:
            if price is not None and iopv is not None and iopv != 0:
                premium_pct = (price - iopv) / iopv * 100.0
            else:
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
    candidates = ["折溢价率", "溢价率", "折溢价率(%)", "折价溢价率", "premium_rate"]
    for col in candidates:
        if col in row.index:
            try:
                s = str(row[col]).replace("%", "").strip()
                return float(s)
            except (ValueError, TypeError):
                continue
    return None


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
#  JSON 输出
# ══════════════════════════════════════════════════════════════════════════════

def build_payload(data: List[Dict], thresholds: Dict) -> Dict:
    """构造前端所需的 JSON payload"""
    etfs = []
    for d in data:
        etfs.append({
            **d,
            "signal": get_signal(d["premium_pct"], thresholds),
        })
    return {
        "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "is_market_open": _is_cn_market_open(),
        "etfs": etfs,
    }


def write_json(payload: Dict, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log.info(f"✅ JSON 已写入: {out_path}")


def _is_cn_market_open() -> bool:
    """粗略判断 A 股是否在交易时段（9:30–11:30 / 13:00–15:00）"""
    now = datetime.datetime.now()
    if now.weekday() >= 5:  # 周六周日
        return False
    t = now.hour * 60 + now.minute
    return (570 <= t <= 690) or (780 <= t <= 900)  # 9:30–11:30 / 13:00–15:00


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

    # 1. 拉数据
    data = fetch_etf_data(etf_list)
    if not data:
        log.error("❌ 未获取到任何 ETF 数据，退出")
        sys.exit(1)

    # 2. 构建 payload
    payload = build_payload(data, thresholds)

    if args.dry_run:
        log.info("[DRY RUN] 结果预览 ↓")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    # 3. 写 JSON
    write_json(payload, Path(args.json_out))

    # 4. 可选：写 Google Sheets
    if args.sheets:
        write_to_sheets(payload, sheets_cfg)

    log.info("🏁 全部完成")


if __name__ == "__main__":
    main()

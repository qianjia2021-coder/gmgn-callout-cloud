#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GMGN 喊单(callout) 云端独立接管实例 —— 与本机 callout_push.py 同一业务口径。

独立边界（2026-11-16 用户要求：云端单独监测，不与其它任务冲突）：
  - 独立仓库、独立状态文件 callout_state.json、独立心跳 callout_heartbeat.json、
    独立 workflow 并发组；不读写 top1 / 热搜 / 精选 的任何状态。
  - 本机在线（心跳新鲜）时整轮跳过，不抢 GMGN 限流、不重复推送；
    本机心跳过期后云端才接管。
  - 每合约终身只推一次；本机与云端 pushed 集合双向并集合并。
  - 推送时机（2026-11-18 用户要求）：同一合约出现「第二条喊单记录」才推送，
    首条只入账；记录窗口 12 小时。记录表同样双向合并。
  - 不接 dbotx，只推送。

口径（与本机 callout_push.py 一致）：
  - 全部链；BSC 由本机 pause_bsc.flag 控制，云端用环境变量 PAUSE_CHAINS 对齐。
  - 消息渲染、链段映射、字段解析与本机保持一致。

Env:
  GITHUB_TOKEN        Actions 自动提供，读写仓库内状态/心跳
  REPO                仓库全名（默认 qianjia2021-coder/gmgn-callout-cloud）
  TG_API_ID / TG_API_HASH / TG_STRING_SESSION
  TG_CHAT_ID          目标群，默认 -5525201436（GMGN数据）
  HB_STALE            本机心跳过期阈值（秒），默认 120
  PAUSE_CHAINS       逗号分隔的暂停链，默认 bsc
  LIMIT               每页条数，默认 50
  DRY_RUN             只打印将推送的消息，不发送、不写状态
  SELF_TEST           只发一条自检消息，不取数
"""
import asyncio
import base64
import hashlib
import html
import json
import os
import subprocess
import sys
import time
import urllib.request

import requests
from telethon import TelegramClient
from telethon.sessions import StringSession

REPO = os.environ.get("REPO", "qianjia2021-coder/gmgn-callout-cloud")
STATE_PATH = "callout_state.json"
HB_PATH = "callout_heartbeat.json"
TG_CHAT = int(os.environ.get("TG_CHAT_ID", "-5525201436"))
HB_STALE = int(os.environ.get("HB_STALE", "120"))
HB_FUTURE_TOLERANCE = 300
LIMIT = int(os.environ.get("LIMIT", "50"))
PAUSED_CHAINS = {x.strip().lower() for x in os.environ.get("PAUSE_CHAINS", "bsc").split(",") if x.strip()}
DRY_RUN = os.environ.get("DRY_RUN", "") not in ("", "0", "false", "False")
SELF_TEST = os.environ.get("SELF_TEST", "") == "1"
GH_API = "https://api.github.com"

# 推送口径：同一合约必须有第二条喊单记录才推，窗口 12 小时（与本机 callout_push.py 对齐）
REC_KEEP_HOURS = 12
REC_MAX = 4000

CHAINS = [
    "sol", "bsc", "eth", "base", "megaeth", "hyperevm", "tron", "monad",
    "xlayer", "robinhood", "arbitrum", "arc", "stable", "avalanche", "elysium",
]

CHAIN_LABEL = {
    "sol": "SOL", "bsc": "BSC", "eth": "ETH", "base": "BASE",
    "megaeth": "MEGA", "hyperevm": "HYPER", "tron": "TRON", "monad": "MONAD",
    "xlayer": "X Layer", "robinhood": "RH", "rh": "RH", "arbitrum": "ARB",
    "arc": "ARC", "stable": "STABLE", "avalanche": "AVAX",
    "elysium": "ELYS", "elys": "ELYS",
}
CHAIN_SEG = {
    "sol": "sol", "bsc": "bsc", "eth": "eth", "base": "base",
    "megaeth": "megaeth", "hyperevm": "hyperevm", "tron": "tron",
    "monad": "monad", "xlayer": "xlayer", "robinhood": "robinhood",
    "rh": "robinhood", "arbitrum": "arbitrum", "arc": "arc",
    "stable": "stable", "avalanche": "avalanche", "elysium": "elysium",
    "elys": "elysium",
}


def log(msg):
    print("[{}] {}".format(time.strftime("%H:%M:%S"), msg), flush=True)


# ---------------------------------------------------------------- GitHub 状态
def gh_headers():
    return {
        "Authorization": "token " + os.environ["GITHUB_TOKEN"],
        "Accept": "application/vnd.github+json",
        "User-Agent": "gmgn-callout-cloud",
    }


def read_remote(path):
    r = requests.get("{}/repos/{}/contents/{}".format(GH_API, REPO, path),
                     headers=gh_headers(), timeout=20)
    if r.status_code == 404:
        return {}, None
    r.raise_for_status()
    j = r.json()
    return json.loads(base64.b64decode(j["content"]).decode("utf-8")), j["sha"]


# ---------------------------------------------------------------- 记录表工具
def key_base(key):
    """键统一为 "链:小写地址[:后缀]"；基键取前两段（链名与地址都不含冒号）。"""
    return ":".join(str(key).split(":")[:2]).lower()


def norm_records(obj):
    """{"chain:addr": {"s": [记录id...最多2个], "t": 最后喊单秒级时间}}"""
    out = {}
    if not isinstance(obj, dict):
        return out
    for k, v in obj.items():
        base = key_base(k)
        if not base or ":" not in base:
            continue
        ids, t = [], 0.0
        if isinstance(v, dict):
            raw_ids = v.get("s") or v.get("ids") or []
            try:
                t = float(v.get("t") or 0)
            except (TypeError, ValueError):
                t = 0.0
        elif isinstance(v, list):
            raw_ids = v
        else:
            continue
        for x in raw_ids:
            x = str(x).strip()
            if x and x not in ids:
                ids.append(x)
            if len(ids) >= 2:
                break
        if not ids:
            continue
        out[base] = {"s": ids[:2], "t": t}
    return out


def merge_records(a, b):
    out = norm_records(a)
    for base, ent in norm_records(b).items():
        cur = out.get(base)
        if cur is None:
            out[base] = {"s": list(ent["s"]), "t": ent["t"]}
            continue
        for x in ent["s"]:
            if x not in cur["s"] and len(cur["s"]) < 2:
                cur["s"].append(x)
        cur["t"] = max(cur["t"], ent["t"])
    return out


def prune_records(records, pushed_bases):
    now = time.time()
    keep = {}
    for base, ent in norm_records(records).items():
        if base in pushed_bases:
            continue
        if ent["t"] and now - ent["t"] > REC_KEEP_HOURS * 3600:
            continue
        keep[base] = ent
    if len(keep) > REC_MAX:
        keep = dict(sorted(keep.items(), key=lambda kv: kv[1]["t"])[-REC_MAX:])
    return keep


def record_id(it):
    rid = (it.get("ulid") or "").strip()
    if rid:
        return rid
    seed = "{}|{}|{}|{}".format(it.get("symbol") or "", it.get("create_time") or 0,
                                it.get("thesis_en") or "", it.get("thesis") or "")
    return "h" + hashlib.md5(seed.encode("utf-8")).hexdigest()[:16]


def pushed_base_set(raw):
    return {key_base(x) for x in (raw or []) if isinstance(x, str) and x.strip()}


def merged_state(remote, pushed_extra=(), records_extra=None):
    """远端最新状态 + 本次新增 pushed + 本次记录表，三合一后写回。"""
    rp = pushed_base_set((remote or {}).get("pushed"))
    rp |= {key_base(x) for x in pushed_extra if str(x).strip()}

    recs = norm_records((remote or {}).get("records"))
    if records_extra:
        recs = merge_records(recs, records_extra)
    recs = prune_records(recs, rp)

    try:
        old_updated = int((remote or {}).get("updated") or 0)
    except (TypeError, ValueError):
        old_updated = 0
    return {"pushed": sorted(rp), "records": recs,
            "updated": max(old_updated, int(time.time()))}


def write_remote_merged(path, merge, msg, max_attempts=3):
    for attempt in range(1, max_attempts + 1):
        try:
            remote, sha = read_remote(path)
        except Exception as e:
            log("状态读取失败({}/{})：{}；本轮放弃写入".format(
                attempt, max_attempts, type(e).__name__))
            return None
        obj = merge(remote if isinstance(remote, dict) else {})
        payload = json.dumps(obj, ensure_ascii=False, indent=1).encode("utf-8")
        body = {"message": msg, "content": base64.b64encode(payload).decode("utf-8")}
        if sha:
            body["sha"] = sha
        try:
            r = requests.put("{}/repos/{}/contents/{}".format(GH_API, REPO, path),
                             headers=gh_headers(), json=body, timeout=25)
        except Exception as e:
            log("状态写入网络失败({}/{})：{}".format(attempt, max_attempts, type(e).__name__))
            time.sleep(3)
            continue
        if r.status_code in (200, 201):
            return r.json()["content"]["sha"]
        if r.status_code in (409, 422):
            log("状态写入冲突({}/{}) HTTP {}；重取并集后重试".format(
                attempt, max_attempts, r.status_code))
            time.sleep(1)
            continue
        log("状态写入失败 HTTP {} {}".format(r.status_code, r.text[:200]))
        return None
    log("状态写入连续 {} 次未成功".format(max_attempts))
    return None


# ---------------------------------------------------------------- 心跳判活
def local_online():
    """True 本机在线 / False 可接管 / None 心跳不可信，本轮不接管。"""
    try:
        hb, _ = read_remote(HB_PATH)
    except Exception as e:
        log("心跳读取失败，本轮不接管：{}".format(type(e).__name__))
        return None
    if not isinstance(hb, dict):
        return None
    try:
        ts = float(hb.get("ts") or 0)
    except (TypeError, ValueError):
        return None
    if ts <= 0:
        return None
    age = time.time() - ts
    if age < -HB_FUTURE_TOLERANCE:
        log("心跳时间戳来自未来（偏差 {:.0f} 秒），时钟不同步，本轮不接管".format(-age))
        return None
    log("本机心跳龄 {:.0f} 秒（阈值 {} 秒）".format(age, HB_STALE))
    return age < HB_STALE


# ---------------------------------------------------------------- 取数
def fetch_callouts():
    """与本机 callout_push.py 同口径：一次请求跨链混排，解析后跳过暂停链。"""
    args = ["gmgn-cli", "callout", "global", "--limit", str(LIMIT), "--raw"]
    for ch in CHAINS:
        args += ["--chain", ch]
    out = None
    for attempt in (1, 2):
        try:
            p = subprocess.run(args, capture_output=True, text=True, timeout=180)
        except Exception as e:
            raise RuntimeError("gmgn-cli 调用异常：{} {}".format(type(e).__name__, str(e)[:120]))
        if p.returncode == 0:
            out = p.stdout
            break
        err = (p.stderr or p.stdout or "")[:300]
        if ("429" in err or "RATE_LIMIT" in err) and attempt == 1:
            log("命中限流，等待 35 秒重试")
            time.sleep(35)
            continue
        raise RuntimeError("gmgn-cli 失败 rc={} {}".format(p.returncode, err))
    if out is None:
        raise RuntimeError("gmgn-cli 两次调用均失败")
    data = json.loads(out)
    raw_list = data.get("list") or []
    items = []
    for t in raw_list:
        if not isinstance(t, dict):
            continue
        chain = (t.get("chain") or "").lower()
        addr = (t.get("call_token") or "").strip()
        if not chain or len(addr) < 10:
            continue
        if chain in PAUSED_CHAINS:
            continue
        thesis = t.get("call_thesis") or {}
        trans = thesis.get("translation_content") or {}
        items.append({
            "chain": chain,
            "address": addr,
            "ulid": t.get("ulid") or "",
            "create_time": t.get("create_time") or 0,
            "symbol": t.get("token_symbol") or "",
            "thesis": (trans.get("zh_hans") or thesis.get("source_content") or "").strip(),
            "thesis_en": (trans.get("en") or "").strip(),
            "caller": (t.get("display_name") or t.get("username")
                       or t.get("twitter_name") or t.get("twitter_username") or "").strip(),
            "twitter": (t.get("twitter_username") or "").strip(),
            "is_kol": bool(t.get("is_kol")),
            "is_safe": t.get("is_safe") or "",
            "token_market_cap": t.get("token_market_cap") or "",
            "call_mc": t.get("call_mc") or "",
            "price_change": t.get("price_change") or "",
            "holding_usd": t.get("current_holding_usd") or "",
            "pnl_usd": t.get("current_pnl_usd") or "",
            "launchpad": t.get("launchpad_platform") or t.get("launchpad") or "",
        })
    return items


# ---------------------------------------------------------------- 消息
def _money(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return ""
    if x >= 1_000_000:
        return "${:.2f}M".format(x / 1_000_000)
    if x >= 1_000:
        return "${:.1f}K".format(x / 1_000)
    return "${:.0f}".format(x)


def _pct(v):
    try:
        x = float(v) * 100
    except (TypeError, ValueError):
        return ""
    sign = "+" if x >= 0 else ""
    return "{}{:.1f}%".format(sign, x)


def token_url(chain, address):
    seg = CHAIN_SEG.get(chain, chain)
    return "https://gmgn.ai/{}/token/{}".format(seg, address)


def build_message(it):
    chain = it["chain"]
    label = CHAIN_LABEL.get(chain, chain.upper())
    symbol = html.escape(it["symbol"] or "未知代币")
    addr = html.escape(it["address"])
    url = token_url(chain, it["address"])
    lines = []
    badge = " 🅺" if it["is_kol"] else ""
    lines.append("📣 <b>GMGN 喊单{}</b>".format(badge))
    lines.append("")
    lines.append("<b>{}</b>".format(symbol))
    lines.append("链：{}".format(label))
    lines.append("合约：<code>{}</code>".format(addr))
    thesis = it["thesis"] or it["thesis_en"]
    if thesis:
        lines.append("喊单：{}".format(html.escape(thesis[:300])))
    who = it["caller"] or it["twitter"]
    if who:
        who_txt = html.escape(who)
        if it["twitter"]:
            who_txt += " (@{})".format(html.escape(it["twitter"]))
        lines.append("喊单人：{}".format(who_txt))
    detail = []
    mc = _money(it["token_market_cap"] or it["call_mc"])
    if mc:
        detail.append("市值 {}".format(mc))
    chg = _pct(it["price_change"])
    if chg:
        detail.append("涨跌 {}".format(chg))
    hold = _money(it["holding_usd"])
    if hold:
        detail.append("持仓 {}".format(hold))
    pnl = _money(it["pnl_usd"])
    if pnl:
        detail.append("浮盈 {}".format(pnl))
    if it["launchpad"]:
        detail.append("平台 {}".format(html.escape(str(it["launchpad"]))))
    if detail:
        lines.append(" | ".join(detail))
    lines.append("")
    lines.append("打开：{}".format(url))
    return "\n".join(lines)


# ---------------------------------------------------------------- 发送
async def send_message(msg):
    client = TelegramClient(StringSession(os.environ["TG_STRING_SESSION"]),
                            int(os.environ["TG_API_ID"]), os.environ["TG_API_HASH"])
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise RuntimeError("TG 会话未授权（StringSession 无效或已吊销）")
        entity = await client.get_entity(TG_CHAT)
        sent = await client.send_message(entity, msg, parse_mode="html", link_preview=False)
        return sent.id
    finally:
        await client.disconnect()


# ---------------------------------------------------------------- 主流程
def load_state():
    """返回 {"pushed": set(基键), "records": dict}；读不了返回 None。"""
    try:
        data, _ = read_remote(STATE_PATH)
    except Exception as e:
        log("状态读取失败，按空状态处理：{}".format(type(e).__name__))
        return None
    if not isinstance(data, dict):
        return {"pushed": set(), "records": {}}
    return {"pushed": pushed_base_set(data.get("pushed")),
            "records": norm_records(data.get("records"))}


def remote_pushed_set():
    try:
        data, _ = read_remote(STATE_PATH)
    except Exception as e:
        log("推送前状态复核失败：{}".format(type(e).__name__))
        return None
    if not isinstance(data, dict):
        return set()
    return pushed_base_set(data.get("pushed"))


def main():
    if SELF_TEST:
        try:
            msg_id = asyncio.run(send_message(
                "云端喊单自检：Telegram 会话可用 {}".format(time.strftime("%Y-%m-%d %H:%M:%S"))))
            log("自检发送成功 msg_id={}".format(msg_id))
            return 0
        except Exception as e:
            log("自检发送失败：{} {}".format(type(e).__name__, str(e)[:200]))
            return 1

    online = local_online()
    if online is None:
        log("心跳状态不可信，本轮退出且绝不推送")
        return 1
    if online:
        log("本机在线，云端本轮不接管")
        return 0

    try:
        items = fetch_callouts()
    except Exception as e:
        log("取数失败：{} {}".format(type(e).__name__, str(e)[:200]))
        return 1

    state = load_state()
    if state is None:
        log("状态不可读，本轮只记录不推送")
        return 1

    pushed = state["pushed"]
    recs = state["records"]
    now = time.time()

    # 1) 本轮所有喊单入账（已推过的合约不再记录），同合约最多留 2 条记录 id
    changed = False
    for it in items:
        base = "{}:{}".format(it["chain"], it["address"].lower())
        if base in pushed:
            continue
        rid = record_id(it)
        ent = recs.get(base) or {"s": [], "t": 0.0}
        if rid not in ent["s"] and len(ent["s"]) < 2:
            ent["s"].append(rid)
            changed = True
        ent["t"] = now
        recs[base] = ent

    # 2) 已有第二条记录、且从未推过 -> 候选；同合约本轮只取一条
    cands = []
    for it in items:
        base = "{}:{}".format(it["chain"], it["address"].lower())
        if base in pushed:
            continue
        if len((recs.get(base) or {}).get("s") or []) >= 2:
            if all(c[0] != base for c in cands):
                cands.append((base, it))

    log("本轮 {} 条 | 已推 {} 个 | 待观测 {} 个 | 到第二条 {} 个".format(
        len(items), len(pushed), len(recs), len(cands)))

    # 3) 记录表先落盘：即使本轮没有候选，下一轮本机掉线换手时也不会丢计数
    if changed and not DRY_RUN:
        new_sha = write_remote_merged(
            STATE_PATH,
            lambda remote: merged_state(remote, records_extra=recs),
            "record callout seen [skip ci]")
        if new_sha is None:
            log("警告：记录表写入失败，本轮计数可能丢；下一轮会自动补")
        else:
            log("记录表已写入（{} 个合约待观测）".format(len(recs)))

    rc = 0
    for k, it in cands:
        rp = remote_pushed_set()
        if rp is None:
            log("推送前状态复核不可读，本轮停止推送（宁可漏一轮，不可重复推）")
            return 1
        if k in rp or k in pushed:
            pushed.add(k)
            continue

        msg = build_message(it)
        if DRY_RUN:
            log("[DRY] 不发送、不写 pushed；将推送「第二次喊单」：")
            print(msg, flush=True)
            continue
        try:
            msg_id = asyncio.run(send_message(msg))
        except Exception as e:
            log("TG 推送失败：{} {}".format(type(e).__name__, str(e)[:200]))
            rc = 1
            continue

        new_sha = write_remote_merged(
            STATE_PATH,
            lambda remote, a="{}:{}".format(k, int(time.time())): merged_state(remote, pushed_extra=(a,)),
            "push callout record [skip ci]")
        if new_sha is None:
            log("严重：消息已发出 msg_id={} 但去重状态未落盘；下一轮可能重复推送，"
                "请把 {} 人工补进 {} 的 pushed".format(msg_id, k, STATE_PATH))
            return 1
        pushed.add(k)
        recs.pop(k, None)
        log("TG 推送成功 msg_id={} | {} {} {}（第二次喊单）".format(
            msg_id, it["chain"], it["address"], it["symbol"]))

    if DRY_RUN:
        log("DRY 轮结束，未写 pushed、未发消息")
    return rc


if __name__ == "__main__":
    sys.exit(main())

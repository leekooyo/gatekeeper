# -*- coding: utf-8 -*-
"""gatekeeper —— 按群总控 bot 的发言与插件使用权限（AstrBot 自建插件 · v2 纯运行时）。

设计
----
本插件**不写入任何持久配置**（不碰 disabled_plugins / session_enabled / llm_enabled），
只在每条消息被处理时于运行时决定：
  1. 哪些插件的 handler 允许执行 —— 原地修剪 event.activated_handlers；
  2. 哪些插件的钩子 / LLM 工具可见 —— 修剪 event.plugins_name；
  3. 是否放行 LLM —— 默认 @bot 用 should_call_llm()，一切 LLM 请求（含空 @bot、
     handler 主动 request_llm）由 on_llm_request 钩子兜断。

四种模式（每群一个）：
  mute   全禁：stop_event()（掐掉本事件后续 handler / LLM / 回复）
  llm    只开 LLM：摘除全部非 reserved 插件 handler，@bot LLM 走概率闸门
  plugin 只开插件：关默认 LLM + 钩子兜断一切 LLM 请求（默认态）
  both   全开：仅套 @bot 概率闸门

唯一持久文件：plugin_data/gatekeeper/modes.json
  {"version": 3,
   "groups": {"<gid>": {"mode": "...", "manual_off": ["插件名", ...]}},
   "meta": {"on": true, "updated_at": "...",
            "bans": {"<gid>": {"until": 0, "duration": 0, "restore": "plugin",
                               "by": "", "whole": false}}}}
  meta.on = 总闸（gate on / gate off）。off 期间插件完全 no-op：
  不拦截、不对账、不落盘。物理级停用请直接在面板停用本插件。
  meta.bans = 「被禁言自动 mute」的记账：until 为截止 epoch 秒（已含 1 分钟宽限），
  0 表示无限期；生效期间该群一律按 mute 处理，到期 / 手动解除后恢复原模式。

被禁言自动 mute（真实状态一律拉取确认，事件只作加速器）
  ⚠️ 本机 platform_settings.ignore_bot_self_message=True，而 OneBot notice 把 user_id
  设为 sender —— bot 自己被禁言时该事件在核心 waking_check 阶段即被 stop_event()
  掐掉，到不了插件。因此个人禁言**只能靠轮询发现**，事件不是可靠触发器。
  - 主机制：周期性 sweep（默认 60s，可配 ban_sweep_interval，0=关）逐群拉取
    get_group_info().group_all_shut 与 get_group_member_info().shut_up_timestamp
    判定真实状态（NapCat 对全体禁言的 sub_type 反推不可靠，故绝不依赖事件里的
    sub_type / duration）。
  - 生效 → 该群按 mute 处理；到期恢复 = 读时判定 + 到点定时器（精确）。
  - 自然到期 OneBot 不发 lift_ban，故到期恢复只能靠本地计时。
  - 启动 / gate on 时立即做一次全量对账；off 期间 sweep 与定时器全部停止。

不在策略里的群按默认态 plugin 处理。私聊一律放行，并作为主人管理入口。

机制要点（全部官方机制，零核心修改）
----------------------------------
  - gate handler priority=99998（低于 wakepro 99999，高于其他一切插件 handler）。
  - 运行时摘 handler：star_request 先取 activated_handlers 引用再顺序迭代；
    本插件在自身位置之后做切片赋值（L[i+1:] = [...]），迭代器下一个索引正好是 i+1，
    故能安全地把指定插件 handler 从后续执行中剔除。
  - LLM 中断：@filter.on_llm_request 回调里 event.stop_event() → call_event_hook 返回
    True → internal.py 直接 return，覆盖所有 LLM 旁路。
  - 事件同步：on_astrbot_loaded 启动对账 + gate 内读 raw_message 处理群增减 notice。
"""

from __future__ import annotations

import asyncio
import json
import random
import time
from datetime import datetime
from pathlib import Path

from quart import jsonify, request

from astrbot.api import logger
from astrbot.api.event import MessageChain, filter
from astrbot.api.star import Context, Star, StarTools
from astrbot.core.config.astrbot_config import AstrBotConfig
from astrbot.core.message.components import At, Plain, Reply
from astrbot.core.platform import AstrMessageEvent
from astrbot.core.star.filter.event_message_type import EventMessageType
from astrbot.core.star.star_handler import star_map

PLUGIN_NAME = "gatekeeper"
MODES = ("mute", "llm", "plugin", "both")
DEFAULT_MODE = "plugin"
POLICY_VERSION = 3
POLICY_MAX_BYTES = 1 << 20  # 1 MB，有界读
DEFAULT_PROBABILITY = 30.0
DEFAULT_WAKE_PREFIXES = ("/",)
BLOCK_LOG_MAX = 200
GATE_PRIORITY = 999999

# 被禁言自动 mute
BAN_GRACE = 60                  # 禁言到期后的宽限秒数（到期 + 1min 才恢复）
BAN_RECONCILE_CONCURRENCY = 8   # 全量对账的并发上限
BAN_SWEEP_INTERVAL = 60.0       # 周期性兜底对账间隔（秒）；0 = 关闭
SELF_ID_FILE = "self_id.json"   # 记住 bot 自身 QQ，供重启后拉取对账使用

# 群体别名：群号 / 插件名位写这些即表示「全部」
ALL_TOKENS = frozenset(
    {"all", "*", "full", "全部", "所有", "全部群", "所有群", "全部插件", "-a", "--all"}
)


def _is_all_token(token) -> bool:
    return isinstance(token, str) and token.strip().lower() in ALL_TOKENS


# --------------------------------------------------------------------------- #
# 小工具
# --------------------------------------------------------------------------- #
def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _bounded_read(path: Path, limit: int) -> str | None:
    """有界读取。不存在 → None；超过 limit → ValueError；解码失败 → ValueError。"""
    if not path.exists():
        return None
    with path.open("rb") as fh:
        data = fh.read(limit + 1)
    if len(data) > limit:
        raise ValueError(f"文件超过大小上限 {limit} 字节")
    return data.decode("utf-8", errors="strict")


def _is_str_list(value) -> bool:
    return isinstance(value, list) and all(isinstance(x, str) for x in value)


def _to_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _sanitize_bans(raw) -> dict[str, dict]:
    """规范化 meta.bans → {gid: {until, duration, restore, by, whole}}。

    until = 禁言截止 epoch 秒（已含 BAN_GRACE）；0 表示无限期。
    """
    bans: dict[str, dict] = {}
    if not isinstance(raw, dict):
        return bans
    for gid, entry in raw.items():
        if not isinstance(entry, dict):
            continue
        restore = entry.get("restore")
        bans[str(gid)] = {
            "until": _to_int(entry.get("until")),
            "duration": _to_int(entry.get("duration")),
            "restore": restore if restore in MODES else None,
            "by": str(entry.get("by") or ""),
            "whole": bool(entry.get("whole")),
        }
    return bans


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


# --------------------------------------------------------------------------- #
# 插件
# --------------------------------------------------------------------------- #
class GatekeeperPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self._lock = asyncio.Lock()
        self._blocks: list[dict] = []
        self._refresh_pending = False   # on_boot 对账因平台未连接而失败 → 收到消息后自动补跑
        self._refreshing = False        # 防止同一时刻堆积多个补跑任务
        self._ban_reconcile_pending = False  # 禁言全量对账尚未跑过
        self._ban_reconciling = False        # 防止全量对账重叠
        self._ban_cleaning: set[str] = set() # 正在清理到期禁言的 gid
        self._ban_timers: dict[str, asyncio.TimerHandle] = {}  # 到点自动解除
        self._ban_sweep: asyncio.TimerHandle | None = None    # 周期性兜底对账
        self._ban_sweep_runs = 0                              # 已跑轮数（仅用于首轮日志）
        self._group_names: dict[str, str] = {}  # gid → 群名（通知文案用）
        self._self_id = ""

        self._register_web_apis()
        if self._on():
            # 热重载不会触发 on_astrbot_loaded，故这里也启动一次（幂等）
            self._reschedule_bans()
            self._start_ban_sweep()
        logger.info(
            "[gatekeeper] v3 已加载。策略文件：%s（on=%s, probability=%s, 禁言轮询=%ss）",
            self._policy_path(),
            self._on(),
            self._probability(),
            self._ban_sweep_interval(),
        )

    # ===================== 配置 =====================
    def _probability(self) -> float:
        raw = self.config.get("probability", DEFAULT_PROBABILITY)
        try:
            prob = float(raw)
        except (TypeError, ValueError):
            logger.warning("[gatekeeper] probability 非法(%r)，回退 %s", raw, DEFAULT_PROBABILITY)
            return DEFAULT_PROBABILITY
        if prob != prob:  # NaN
            return DEFAULT_PROBABILITY
        return min(100.0, max(0.0, prob))

    def _allow_upstream_wake(self) -> bool:
        return bool(self.config.get("allow_upstream_wake", False))

    def _wake_prefixes(self) -> tuple[str, ...]:
        try:
            prefixes = self.context.get_config().get("wake_prefix") or []
        except Exception:
            return DEFAULT_WAKE_PREFIXES
        cleaned = tuple(p for p in prefixes if isinstance(p, str) and p)
        return cleaned or DEFAULT_WAKE_PREFIXES

    # ===================== 路径 =====================
    def _data_dir(self) -> Path:
        return Path(StarTools.get_data_dir(PLUGIN_NAME))

    def _policy_path(self) -> Path:
        return self._data_dir() / "modes.json"

    # ===================== 策略读写 =====================
    def _read_policy(self) -> dict:
        """读 + 最小规范化。任何异常/坏结构 → 返回空策略（等价全部默认 plugin）。

        v1（旧版）兼容读取：meta.released=True 视作 off。
        """
        empty = {"version": POLICY_VERSION, "groups": {}, "meta": {"on": True, "bans": {}}}
        try:
            raw = _bounded_read(self._policy_path(), POLICY_MAX_BYTES)
        except Exception as exc:
            logger.error("[gatekeeper] 读策略失败：%s（按默认 plugin 处理）", exc)
            return empty
        if raw is None:
            return empty
        try:
            data = json.loads(raw)
        except Exception as exc:
            logger.error("[gatekeeper] 策略不是合法 JSON：%s（按默认 plugin 处理）", exc)
            return empty
        if not isinstance(data, dict) or not isinstance(data.get("groups"), dict):
            logger.error("[gatekeeper] 策略根结构非法（按默认 plugin 处理）")
            return empty
        meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
        if data.get("version") == POLICY_VERSION:
            on = bool(meta.get("on", True))
        else:  # v1 兼容：released=True ⇒ off
            on = not bool(meta.get("released", False))
        return {
            "version": POLICY_VERSION,
            "groups": {str(k): v for k, v in data["groups"].items()},
            "meta": {
                "on": on,
                "updated_at": meta.get("updated_at"),
                "bans": _sanitize_bans(meta.get("bans")),
            },
        }

    def _write_policy(self, policy: dict) -> None:
        """规范化落盘（永远写 v2）。"""
        groups: dict[str, dict] = {}
        for gid, entry in (policy.get("groups") or {}).items():
            entry = entry if isinstance(entry, dict) else {}
            mode = entry.get("mode")
            off = entry.get("manual_off")
            off = [n for n in off if isinstance(n, str) and n != PLUGIN_NAME] if isinstance(off, list) else []
            groups[str(gid)] = {
                "mode": mode if mode in MODES else DEFAULT_MODE,
                "manual_off": off,
            }
        out = {
            "version": POLICY_VERSION,
            "groups": groups,
            "meta": {
                "on": bool((policy.get("meta") or {}).get("on", True)),
                "updated_at": _now_iso(),
                "bans": _sanitize_bans((policy.get("meta") or {}).get("bans")),
            },
        }
        _atomic_write(self._policy_path(), json.dumps(out, ensure_ascii=False, indent=2))

    def _on(self) -> bool:
        return bool(self._read_policy()["meta"].get("on", True))

    def _set_on(self, value: bool) -> None:
        policy = self._read_policy()
        policy["meta"]["on"] = bool(value)
        self._write_policy(policy)

    def _mode_and_off(self, policy: dict, gid: str) -> tuple[str, list[str]]:
        """返回 (mode, manual_off)。未登记 / 值级坏 → 默认态 plugin。"""
        entry = policy["groups"].get(gid)
        if not isinstance(entry, dict):
            return DEFAULT_MODE, []
        mode = entry.get("mode")
        if mode not in MODES:
            logger.warning("[gatekeeper] 群 %s mode 非法(%r)，按默认 %s", gid, mode, DEFAULT_MODE)
            return DEFAULT_MODE, []
        manual_off = entry.get("manual_off")
        if manual_off is None:
            return mode, []
        if not _is_str_list(manual_off):
            logger.warning("[gatekeeper] 群 %s manual_off 非法，按空处理", gid)
            return mode, []
        cleaned = [n for n in manual_off if n != PLUGIN_NAME]
        if len(cleaned) != len(manual_off):
            logger.warning("[gatekeeper] 群 %s manual_off 含 gatekeeper，已剔除（禁止自禁）", gid)
        return mode, cleaned

    def _active_ban(self, policy: dict, gid: str) -> dict | None:
        """生效中的临时禁言记录；已到期的返回 None 并惰性调度清理。"""
        ban = (policy["meta"].get("bans") or {}).get(gid)
        if not isinstance(ban, dict):
            return None
        until = _to_int(ban.get("until"))
        if until and time.time() >= until:
            self._schedule_ban_cleanup(gid)
            return None
        return ban

    def _resolve(self, policy: dict, gid: str) -> tuple[str, list[str], dict | None]:
        """(mode, manual_off, ban)。临时禁言优先于一切显式模式。"""
        mode, manual_off = self._mode_and_off(policy, gid)
        ban = self._active_ban(policy, gid)
        if ban is not None:
            return "mute", manual_off, ban
        return mode, manual_off, None

    def _all_gids(self) -> list[str]:
        """全部已登记群（策略 keys；排除 all/* 之类别名本身）。"""
        policy = self._read_policy()
        return [
            str(g)
            for g in sorted(policy["groups"].keys())
            if not _is_all_token(g) and str(g).strip()
        ]

    def _set_modes(self, gids: list[str], mode: str) -> int:
        """批量设置模式（一次读、一次写）。保留各自已有 manual_off。

        人为优先：显式设模式会一并清掉该群的临时禁言标记（否则事件丢了会卡在 mute）。
        """
        policy = self._read_policy()
        bans = policy["meta"].get("bans") or {}
        cleared = [g for g in gids if g in bans]
        n = 0
        for gid in gids:
            entry = policy["groups"].get(gid)
            prev = entry.get("manual_off") if isinstance(entry, dict) else None
            prev = [x for x in prev if isinstance(x, str) and x != PLUGIN_NAME] if isinstance(prev, list) else []
            policy["groups"][gid] = {"mode": mode, "manual_off": prev}
            n += 1
        if n:
            for g in cleared:
                bans.pop(g, None)
            self._write_policy(policy)
            for g in cleared:
                self._cancel_ban_timer(g)
        if cleared:
            self._fire_manual_lift(cleared)
        return n

    def _set_mode(self, gid: str, mode: str) -> int:
        return self._set_modes([gid], mode)

    def _set_plugins(self, gids: list[str], names: list[str], enable: bool) -> int:
        """批量启用/禁用若干插件（一次读、一次写）。未登记群隐式以默认态建条目。"""
        names = [x for x in names if x and x != PLUGIN_NAME]
        if not names:
            return 0
        policy = self._read_policy()
        n = 0
        for gid in gids:
            entry = policy["groups"].get(gid)
            mode = entry.get("mode") if isinstance(entry, dict) and entry.get("mode") in MODES else DEFAULT_MODE
            off = entry.get("manual_off") if isinstance(entry, dict) else None
            off = [x for x in off if isinstance(x, str) and x != PLUGIN_NAME] if isinstance(off, list) else []
            if enable:
                off = [x for x in off if x not in names]
            else:
                for nm in names:
                    if nm not in off:
                        off.append(nm)
            policy["groups"][gid] = {"mode": mode, "manual_off": off}
            n += 1
        if n:
            self._write_policy(policy)
        return n

    def _remove_group(self, gid: str) -> bool:
        policy = self._read_policy()
        if gid not in policy["groups"]:
            return False
        policy["groups"].pop(gid, None)
        bans = policy["meta"].get("bans") or {}
        had_ban = gid in bans
        if had_ban:
            bans.pop(gid, None)
        self._write_policy(policy)
        self._cancel_ban_timer(gid)
        return True

    # ===================== 插件清单 / 群清单 =====================
    def _all_plugin_names(self) -> list[str]:
        names: list[str] = []
        for md in list(star_map.values()):
            try:
                if getattr(md, "reserved", False):
                    continue
                name = getattr(md, "name", None)
                if name and name != PLUGIN_NAME and name not in names:
                    names.append(name)
            except Exception:
                continue
        return names

    def _aiocqhttp_platforms(self) -> list:
        mgr = getattr(self.context, "platform_manager", None)
        out = []
        for p in getattr(mgr, "platform_insts", []) or []:
            try:
                if p.meta().name == "aiocqhttp":
                    out.append(p)
            except Exception:
                continue
        return out

    async def _group_inventory(self) -> dict:
        """只读 QQ 实际群列表；查询失败不能冒充 0 群。"""
        platforms = self._aiocqhttp_platforms()

        async def query(platform):
            try:
                rows = await asyncio.wait_for(platform.get_client().get_group_list(), timeout=5)
                if not isinstance(rows, list) or any(
                    not isinstance(row, dict) or not str(row.get("group_id") or "").isdigit()
                    for row in rows
                ):
                    raise ValueError("群列表格式非法")
                names = {str(row["group_id"]): str(row.get("group_name") or "") for row in rows}
                self._group_names.update(names)
                return names
            except Exception:
                logger.warning("[gatekeeper] QQ 群列表查询失败(platform=%s)", platform.meta().id)
                return None

        results = await asyncio.gather(*(query(p) for p in platforms))
        complete = bool(platforms) and all(result is not None for result in results)
        groups = {}
        for result in results:
            if result is not None:
                groups.update(result)
        return {"groups": groups, "complete": complete,
                "error": None if complete else "QQ 群列表读取失败或平台未连接；实测群数暂不可确认。"}

    async def _refresh(self) -> dict:
        """refresh 对账：拉 QQ 群列表，删掉「已退群」的策略条目（幂等）。

        返回 {complete, error, removed, joined}。只删条目，绝不新增 —— 新群按默认态生效。
        """
        async with self._lock:
            inv = await self._group_inventory()
            if not inv["complete"]:
                return {"complete": False, "error": inv["error"], "removed": [], "joined": None}
            joined = set(inv["groups"])
            policy = self._read_policy()
            removed = sorted(g for g in list(policy["groups"]) if g not in joined)
            if removed:
                for g in removed:
                    policy["groups"].pop(g, None)
                self._write_policy(policy)
            self._refresh_pending = False
            return {"complete": True, "error": None, "removed": removed, "joined": inv["groups"]}

    # ===================== 被禁言自动 mute =====================
    def _notify_ban(self) -> bool:
        return bool(self.config.get("notify_ban", True))

    def _admin_ids(self) -> list[str]:
        """全局管理员 QQ（优先全局配置，回退插件自身配置）。"""
        sources = []
        try:
            sources.append(self.context.get_config().get("admins_id") or [])
        except Exception:
            pass
        try:
            sources.append(self.context.astrbot_config_mgr.get_conf(None).get("admins_id") or [])
        except Exception:
            pass
        for src in sources:
            ids = [str(x).strip() for x in src if str(x).strip()]
            if ids:
                return ids
        return []

    def _platform_id_of(self) -> str:
        for plat in self._aiocqhttp_platforms():
            try:
                pid = plat.meta().id
                if pid:
                    return str(pid)
            except Exception:
                continue
        return ""

    def _group_name(self, gid: str) -> str:
        return self._group_names.get(gid) or ""

    # ---- bot 自身 QQ（重启后拉取对账用）----
    def _self_id_path(self) -> Path:
        return self._data_dir() / SELF_ID_FILE

    def _remember_self_id(self, value) -> None:
        sid = str(value or "").strip()
        if not sid.isdigit():
            return
        if self._self_id == sid:
            return
        self._self_id = sid
        try:
            _atomic_write(self._self_id_path(),
                          json.dumps({"self_id": sid}, ensure_ascii=False, indent=2))
        except Exception:
            pass

    def _load_self_id(self) -> str:
        if self._self_id:
            return self._self_id
        try:
            raw = _bounded_read(self._self_id_path(), 4096)
            if raw:
                sid = str(json.loads(raw).get("self_id") or "")
                if sid.isdigit():
                    self._self_id = sid
                    return sid
        except Exception:
            pass
        return ""

    # ---- 拉取真实禁言状态（不信任事件语义）----
    async def _pull_ban_state(self, gid: str) -> dict | None:
        """拉取 bot 在该群的真实禁言状态；查询失败返回 None（绝不据此改状态）。

        依据（NapCat 已实现）：
          get_group_info().group_all_shut == -1        → 全体禁言（可永久）
          get_group_member_info().shut_up_timestamp > now → 个人禁言截止时间戳
        """
        self_id = self._load_self_id()
        if not self_id:
            logger.warning("[gatekeeper] 尚未记录到 bot 自身 QQ，跳过群 %s 的禁言状态拉取", gid)
            return None
        for plat in self._aiocqhttp_platforms():
            pid = str(plat.meta().id)
            try:
                client = plat.get_client()
                info = await asyncio.wait_for(
                    client.get_group_info(group_id=int(gid), no_cache=True), timeout=5)
                if not isinstance(info, dict):
                    continue
                if _to_int(info.get("group_all_shut")) == -1:
                    return {"banned": True, "whole": True, "until": 0, "duration": -1}
                member = await asyncio.wait_for(
                    client.get_group_member_info(group_id=int(gid), user_id=int(self_id),
                                                 no_cache=True),
                    timeout=5)
                ts = _to_int((member or {}).get("shut_up_timestamp"))
                now = time.time()
                if ts > now:
                    return {"banned": True, "whole": False, "until": int(ts),
                            "duration": max(0, int(ts) - int(now))}
                return {"banned": False, "whole": False, "until": 0, "duration": 0}
            except Exception as exc:
                logger.warning("[gatekeeper] 拉取群 %s 禁言状态失败(platform=%s)：%s", gid, pid, exc)
        return None

    # ---- 登记 / 清除 ----
    async def _register_ban(self, gid: str, state: dict, operator: str = "") -> None:
        until = _to_int(state.get("until"))
        if until:
            until += BAN_GRACE  # 到期后再多待 1 分钟，避免 QQ 端与本地时钟偏差
        whole = bool(state.get("whole"))
        async with self._lock:
            policy = self._read_policy()
            bans = policy["meta"].setdefault("bans", {})
            prev = bans.get(gid)
            changed = not (isinstance(prev, dict)
                           and _to_int(prev.get("until")) == until
                           and bool(prev.get("whole")) == whole)
            entry = policy["groups"].get(gid)
            restore = entry.get("mode") if isinstance(entry, dict) and entry.get("mode") in MODES else None
            if changed:
                bans[gid] = {
                    "until": until,
                    "duration": _to_int(state.get("duration")),
                    "restore": restore,
                    "by": str(operator or ""),
                    "whole": whole,
                }
                self._write_policy(policy)
            self._schedule_ban_timer(gid, until)
            mode_now = self._mode_and_off(policy, gid)[0]
        if changed:
            self._audit(gid, "mute", "ban")
            logger.info("[gatekeeper] 群 %s 被禁言 → 自动 mute（%s，until=%s，操作者=%s）",
                        gid, "全体禁言" if whole else "个人禁言",
                        until or "无限期", operator or "未知")
            await self._notify_ban_event(gid, "ban", state=state, operator=operator, restore=mode_now)

    async def _clear_ban(self, gid: str, reason: str = "lift") -> None:
        async with self._lock:
            policy = self._read_policy()
            bans = policy["meta"].get("bans") or {}
            ban = bans.pop(gid, None)
            if ban is None:
                self._cancel_ban_timer(gid)
                return
            self._write_policy(policy)
            self._cancel_ban_timer(gid)
            restore = self._mode_and_off(policy, gid)[0]
        self._audit(gid, restore, f"ban-clear-{reason}")
        logger.info("[gatekeeper] 群 %s 解除禁言 → 恢复模式 %s（%s）", gid, restore, reason)
        await self._notify_ban_event(gid, "lift", reason=reason, restore=restore)

    def _schedule_ban_cleanup(self, gid: str) -> None:
        """读时发现已到期 → 后台清一次（幂等，防堆积）。"""
        if gid in self._ban_cleaning:
            return
        self._ban_cleaning.add(gid)

        async def run():
            try:
                await self._clear_ban(gid, reason="expire")
            finally:
                self._ban_cleaning.discard(gid)

        try:
            asyncio.get_running_loop().create_task(run())
        except RuntimeError:
            self._ban_cleaning.discard(gid)

    # ---- 定时器 / 看门狗 ----
    def _cancel_ban_timer(self, gid: str) -> None:
        handle = self._ban_timers.pop(gid, None)
        if handle is not None:
            try:
                handle.cancel()
            except Exception:
                pass

    def _schedule_ban_timer(self, gid: str, until: int) -> None:
        self._cancel_ban_timer(gid)
        if not until:
            return  # 无限期，交给看门狗
        delay = max(1.0, until - time.time())
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._ban_timers[gid] = loop.call_later(delay, self._on_ban_expired, gid)

    def _on_ban_expired(self, gid: str) -> None:
        async def run():
            await self._clear_ban(gid, reason="expire")

        try:
            asyncio.get_running_loop().create_task(run())
        except RuntimeError:
            pass

    def _ban_sweep_interval(self) -> float:
        raw = self.config.get("ban_sweep_interval", BAN_SWEEP_INTERVAL)
        try:
            val = float(raw)
        except (TypeError, ValueError):
            return BAN_SWEEP_INTERVAL
        if val != val or val < 0:  # NaN / 负值
            return BAN_SWEEP_INTERVAL
        return min(3600.0, val)

    def _start_ban_sweep(self) -> None:
        """周期性兜底对账。

        ⚠️ 这是**主检测机制**而非备胎：本机 platform_settings.ignore_bot_self_message=True，
        而 OneBot notice 把 user_id 设为 sender —— bot 自己被禁言时该事件在核心
        waking_check 阶段就被 stop_event() 掐掉，根本到不了插件。故个人禁言只能靠轮询发现。
        """
        if self._ban_sweep is not None:
            return
        interval = self._ban_sweep_interval()
        if interval <= 0:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._ban_sweep = loop.call_later(interval, self._on_ban_sweep)

    def _stop_ban_sweep(self) -> None:
        if self._ban_sweep is not None:
            try:
                self._ban_sweep.cancel()
            except Exception:
                pass
            self._ban_sweep = None

    def _on_ban_sweep(self) -> None:
        self._ban_sweep = None

        async def run():
            if not self._on():
                return
            self._ban_sweep_runs += 1
            r = await self._reconcile_bans()
            if self._ban_sweep_runs == 1:
                if r.get("skipped"):
                    logger.info("[gatekeeper] 禁言轮询已启动（每 %ss），本轮跳过：%s",
                                self._ban_sweep_interval(), r.get("error"))
                else:
                    logger.info("[gatekeeper] 禁言轮询已启动（每 %ss），本轮核查 %s 个群",
                                self._ban_sweep_interval(), r.get("checked"))
            self._start_ban_sweep()

        try:
            asyncio.get_running_loop().create_task(run())
        except RuntimeError:
            pass

    def _reschedule_bans(self) -> None:
        """重启后按 meta.bans 重建定时器与看门狗。"""
        bans = self._read_policy()["meta"].get("bans") or {}
        for gid, ban in bans.items():
            self._schedule_ban_timer(gid, _to_int(ban.get("until")))

    # ---- 全量对账（启动 / gate on）----
    async def _reconcile_bans(self) -> dict:
        """逐群拉取禁言状态，修正漏报与漏解除。仅在总控 on 时调用。"""
        if self._ban_reconciling:
            return {"skipped": True}
        self._ban_reconciling = True
        try:
            if not self._load_self_id():
                logger.warning("[gatekeeper] 尚未记录到 bot 自身 QQ，跳过本轮禁言对账"
                               "（收到任意群消息后自动记录）")
                return {"skipped": True, "error": "self_id 未知"}
            inv = await self._group_inventory()
            if not inv["complete"]:
                return {"skipped": True, "error": inv["error"]}
            gids = sorted(inv["groups"])
            sem = asyncio.Semaphore(BAN_RECONCILE_CONCURRENCY)

            async def one(gid: str):
                async with sem:
                    state = await self._pull_ban_state(gid)
                    if state is None:
                        return
                    if state["banned"]:
                        await self._register_ban(gid, state)
                    else:
                        await self._clear_ban(gid, reason="lift")

            await asyncio.gather(*(one(g) for g in gids), return_exceptions=True)
            return {"checked": len(gids)}
        except Exception as exc:
            logger.warning("[gatekeeper] 禁言全量对账异常：%s", exc)
            return {"error": str(exc)}
        finally:
            self._ban_reconciling = False

    # ---- 通知 ----
    @staticmethod
    def _fmt_duration(seconds) -> str:
        sec = _to_int(seconds)
        if sec <= 0:
            return "无限期"
        days, rem = divmod(sec, 86400)
        hours, rem = divmod(rem, 3600)
        mins, _ = divmod(rem, 60)
        parts = []
        if days:
            parts.append(f"{days} 天")
        if hours:
            parts.append(f"{hours} 小时")
        if mins or not parts:
            parts.append(f"{mins} 分钟")
        return " ".join(parts)

    def _ban_text(self, gid: str, state: dict, operator: str, restore: str) -> str:
        whole = bool(state.get("whole"))
        until = _to_int(state.get("until"))
        who = str(operator or "").strip() or "某位管理员"
        lines = [
            "🐧【南珊的小本本 · 被关小黑屋了】",
            f"呜哇——南珊在「{self._group_name(gid) or gid}」被 {who} 关进小黑屋啦 (｡•́︿•̀｡)",
            f"├ 群号：{gid}",
            f"├ 罪名：{'全体禁言（群里谁都别想说话）' if whole else '个人禁言'}",
            f"├ 刑期：{self._fmt_duration(state.get('duration'))}",
            "└ 南珊先乖乖待机，不吱声也不闹 qwq",
        ]
        if until:
            eta = datetime.fromtimestamp(until).astimezone().strftime("%m-%d %H:%M")
            lines.append(f"⏰ 预计 {eta} 自动出狱（已含 1 分钟保险期）")
        else:
            lines.append("🔒 这次是无限期套餐，只能等谁来手动开门")
        lines.append(f"📌 出狱后恢复模式：{restore}")
        return "\n".join(lines)

    def _lift_text(self, gid: str, reason: str, restore: str) -> str:
        how = {
            "lift": "管理员手动开门",
            "expire": "到点自动刑满释放",
            "manual": "主人手动解除",
        }.get(reason, reason)
        return "\n".join([
            "🐧【南珊的小本本 · 出狱啦】",
            f"嘿嘿——「{self._group_name(gid) or gid}」的小黑屋开门咯，南珊又回来啦 (๑•̀ㅂ•́)و✧",
            f"├ 群号：{gid}",
            f"├ 解除方式：{how}",
            f"└ 已恢复模式：{restore}",
        ])

    async def _notify_ban_event(self, gid: str, kind: str, *, state=None, operator: str = "",
                                reason: str = "", restore: str = "") -> None:
        if not self._notify_ban():
            return
        pid = self._platform_id_of()
        admins = self._admin_ids()
        if not pid or not admins:
            logger.warning("[gatekeeper] 无法通知禁言事件：platform_id=%r admins=%d", pid, len(admins))
            return
        if kind == "ban":
            text = self._ban_text(gid, state or {}, operator, restore)
        else:
            text = self._lift_text(gid, reason, restore)
        for aid in admins:
            try:
                await self.context.send_message(
                    f"{pid}:FriendMessage:{aid}", MessageChain([Plain(text)]))
            except Exception as exc:
                logger.warning("[gatekeeper] 通知管理员 %s 失败：%s", aid, exc)

    def _fire_manual_lift(self, gids: list[str]) -> None:
        """人为显式设模式 → 清掉临时禁言并补一条出狱通知。"""
        async def run():
            for gid in gids:
                await self._clear_ban(gid, reason="manual")

        try:
            asyncio.get_running_loop().create_task(run())
        except RuntimeError:
            pass

    async def _handle_group_ban(self, raw: dict) -> None:
        """group_ban notice：只当触发器，真实状态以拉取为准。

        NapCat 的 parseGroupBanEvent 对「全体禁言」会用原始 duration 反推 sub_type，
        而全体禁言在 QQ 侧通常没有 duration → 可能误报成 lift_ban。故绝不依赖 sub_type。
        """
        self_id = str(raw.get("self_id") or "")
        uid = str(raw.get("user_id") or "")
        gid = str(raw.get("group_id") or "")
        if not gid:
            return
        if uid and uid != self_id and uid != "0":
            return  # 别人被禁言，与本插件无关
        state = await self._pull_ban_state(gid)
        if state is None:
            return
        if state["banned"]:
            await self._register_ban(gid, state, operator=str(raw.get("operator_id") or ""))
        else:
            await self._clear_ban(gid, reason="lift")

    # ===================== 运行时闸门（核心） =====================
    @staticmethod
    def _handler_plugin_name(handler) -> str | None:
        md = star_map.get(getattr(handler, "handler_module_path", ""))
        if md is None:
            return None
        return getattr(md, "name", None)

    def _find_self_index(self, handlers: list) -> int | None:
        for i, h in enumerate(handlers):
            if (
                getattr(h, "handler_name", "") == "gate"
                and self._handler_plugin_name(h) == PLUGIN_NAME
            ):
                return i
        return None

    def _prune_handlers(self, event: AstrMessageEvent, mode: str, manual_off: list[str]) -> None:
        """原地剔除「本 handler 之后」的受限插件 handler。

        star_request 已持有本 list 的引用并按索引迭代；本插件位于其中某个索引 i，
        改写 L[i+1:] 后迭代器下一个索引正好是 i+1，故安全。
        """
        try:
            handlers = event.get_extra("activated_handlers")
        except Exception:
            return
        if not isinstance(handlers, list) or not handlers:
            return
        idx = self._find_self_index(handlers)
        if idx is None:
            return
        blocked = {n for n in manual_off if n != PLUGIN_NAME}

        def keep(h) -> bool:
            md = star_map.get(getattr(h, "handler_module_path", ""))
            if md is None:
                return True
            if getattr(md, "reserved", False):
                return True
            name = getattr(md, "name", None)
            if name in (None, PLUGIN_NAME):
                return True
            if mode == "llm":
                return False
            return name not in blocked

        handlers[idx + 1:] = [h for h in handlers[idx + 1:] if keep(h)]

    def _apply_plugins_name(self, event: AstrMessageEvent, mode: str, manual_off: list[str]) -> None:
        """按模式修剪 event.plugins_name（钩子 / LLM 工具按此字段过滤）。只减不增。

        ⚠️ 必须始终保留 gatekeeper 自身，否则本插件的 on_llm_request 钩子会被过滤掉。
        """
        try:
            if mode == "llm":
                event.plugins_name = [PLUGIN_NAME]
                return
            blocked = {n for n in manual_off if n != PLUGIN_NAME}
            if not blocked:
                return
            orig = getattr(event, "plugins_name", None)
            base = self._all_plugin_names() if orig is None else [n for n in orig if n != "*"]
            allowed = [n for n in base if n not in blocked]
            if PLUGIN_NAME not in allowed:
                allowed.append(PLUGIN_NAME)
            event.plugins_name = allowed
        except Exception as exc:
            logger.warning("[gatekeeper] 修剪 plugins_name 失败：%s", exc)

    def _llm_allowed(self, event: AstrMessageEvent) -> bool:
        """@bot 概率闸门（原 llm_gate 逻辑）。"""
        try:
            chain = event.get_messages()
        except Exception:
            return True
        self_id = str(event.get_self_id())
        at_bot = any(isinstance(seg, At) and str(seg.qq) == self_id for seg in chain)
        reply_bot = any(isinstance(seg, Reply) and str(seg.sender_id) == self_id for seg in chain)
        if at_bot:
            return random.random() * 100.0 < self._probability()
        if reply_bot:
            return False
        if self._has_cmd_prefix(chain):
            return False
        if self._allow_upstream_wake():
            return bool(getattr(event, "is_at_or_wake_command", False))
        return False

    def _has_cmd_prefix(self, chain) -> bool:
        raw = "".join(seg.text for seg in chain if isinstance(seg, Plain) and seg.text).strip()
        if not raw:
            return False
        return any(raw.startswith(p) for p in self._wake_prefixes())

    # ===================== 事件（群增减 notice） =====================
    @staticmethod
    def _is_notice(event: AstrMessageEvent) -> bool:
        raw = getattr(getattr(event, "message_obj", None), "raw_message", None)
        return isinstance(raw, dict) and raw.get("post_type") == "notice"

    async def _handle_notice(self, event: AstrMessageEvent) -> None:
        """群增减自动同步。仅在总控 on 时执行（off 期间当死人）。"""
        raw = getattr(getattr(event, "message_obj", None), "raw_message", None)
        if not isinstance(raw, dict):
            return
        try:
            notice = raw.get("notice_type")
            self_id = str(raw.get("self_id") or "")
            uid = str(raw.get("user_id") or "")
            gid = str(raw.get("group_id") or "")
            if not gid:
                return
            if notice == "group_ban":
                # 只当触发器；真实状态（含全体禁言 / 手动解除）以拉取为准
                self._schedule_ban_notice(raw)
            elif notice == "group_decrease" and uid and uid == self_id:
                # 被踢 / 主动退群：删掉该群策略条目
                if self._remove_group(gid):
                    self._audit(gid, "-", "notice-leave")
            elif notice == "group_increase" and uid and uid == self_id:
                # 被拉进新群：按默认态 plugin 生效，无需写条目
                self._audit(gid, "-", "notice-join")
        except Exception as exc:
            logger.error("[gatekeeper] 处理群增减 notice 失败：%s", exc)

    def _schedule_ban_notice(self, raw: dict) -> None:
        """禁言 notice 后台处理：拉取禁言状态要 1~2 次 API 调用，不能堵住 gate 管线。"""
        async def run():
            await self._handle_group_ban(raw)

        try:
            asyncio.get_running_loop().create_task(run())
        except RuntimeError:
            pass

    # ===================== 审计（内存环形，绝不阻断） =====================
    def _audit(self, gid: str, mode: str, action: str) -> None:
        try:
            self._blocks.append(
                {"t": _now_iso(), "gid": gid, "mode": mode, "action": action}
            )
            if len(self._blocks) > BLOCK_LOG_MAX:
                del self._blocks[: len(self._blocks) - BLOCK_LOG_MAX]
        except Exception:
            pass

    # ===================== 主闸门 =====================
    @filter.event_message_type(EventMessageType.GROUP_MESSAGE, priority=GATE_PRIORITY)
    async def gate(self, event: AstrMessageEvent):
        policy = self._read_policy()
        if not policy["meta"].get("on", True):
            return  # 总控 off：当死人
        if self._refresh_pending:
            self._schedule_pending_refresh()
        self._remember_self_id(event.get_self_id())
        if self._is_notice(event):
            await self._handle_notice(event)
            return
        gid = event.get_group_id()
        if not gid:
            return
        mode, manual_off, ban = self._resolve(policy, gid)

        if mode == "mute":
            event.stop_event()  # 掐掉本事件全部后续 handler / 默认 LLM / 回复
            self._audit(gid, mode, "ban-mute" if ban is not None else "block")
            return

        # 允许分支：运行时修剪 handler 与钩子可见性
        self._prune_handlers(event, mode, manual_off)
        self._apply_plugins_name(event, mode, manual_off)

        if mode == "plugin":
            event.should_call_llm(True)  # 关默认 @bot LLM（handler 主动 request_llm 由钩子兜断）
        elif mode in ("llm", "both"):
            if not self._llm_allowed(event):
                event.should_call_llm(True)
        self._audit(gid, mode, "pass")

    # ===================== LLM 钩子（兜断一切 LLM 请求） =====================
    @filter.on_llm_request()
    async def llm_gate(self, event: AstrMessageEvent, req) -> None:
        """一切 LLM 请求的最终闸门。

        覆盖默认 @bot 之外的旁路：空 @bot（handle_empty_mention）、handler 主动
        yield request_llm。回调内 stop_event() 会让 call_event_hook 返回 True，
        从而让 internal.py 直接 return 中断该次 LLM。
        """
        policy = self._read_policy()
        if not policy["meta"].get("on", True):
            return
        gid = event.get_group_id()
        if not gid:
            return  # 私聊放行
        mode, _off, _ban = self._resolve(policy, gid)
        if mode in ("mute", "plugin"):
            event.stop_event()
            self._audit(gid, mode, "llm-block")
            return
        if mode in ("llm", "both") and not self._llm_allowed(event):
            event.stop_event()
            self._audit(gid, mode, "llm-block")

    # ===================== 启动对账 =====================
    @filter.on_astrbot_loaded()
    async def on_boot(self):
        try:
            if not self._on():
                self._stop_ban_sweep()
                logger.info("[gatekeeper] 总控处于 off，启动对账跳过。")
                return
            self._reschedule_bans()
            self._start_ban_sweep()
            r = await self._refresh()
            if r.get("complete"):
                logger.info(
                    "[gatekeeper] 启动对账完成，剔除退群条目 %d 个：%s",
                    len(r.get("removed") or []), r.get("removed") or [],
                )
                rr = await self._reconcile_bans()
                if rr.get("skipped"):
                    self._ban_reconcile_pending = True
                else:
                    logger.info("[gatekeeper] 启动禁言对账完成，核查 %s 个群", rr.get("checked"))
            else:
                self._refresh_pending = True
                self._ban_reconcile_pending = True
                logger.warning(
                    "[gatekeeper] 启动对账未完成：%s（将在收到消息时自动补跑）",
                    r.get("error"),
                )
        except Exception as exc:
            logger.error("[gatekeeper] 启动对账异常：%s", exc)

    def _schedule_pending_refresh(self) -> None:
        """启动对账曾失败（平台尚未连接）时，收到消息后在后台补跑一次对账。"""
        if not self._refresh_pending or self._refreshing:
            return
        self._refreshing = True
        try:
            asyncio.get_running_loop().create_task(self._lazy_refresh())
        except RuntimeError:
            self._refreshing = False

    async def _lazy_refresh(self) -> None:
        try:
            if not self._refresh_pending:
                return
            r = await self._refresh()
            if r.get("complete"):
                logger.info(
                    "[gatekeeper] 启动对账已自动补跑完成，剔除退群条目 %d 个：%s",
                    len(r.get("removed") or []), r.get("removed") or [],
                )
                if self._ban_reconcile_pending:
                    self._ban_reconcile_pending = False
                    rr = await self._reconcile_bans()
                    if not rr.get("skipped"):
                        logger.info("[gatekeeper] 启动禁言对账已补跑完成，核查 %s 个群",
                                    rr.get("checked"))
        except Exception as exc:
            logger.warning("[gatekeeper] 自动补跑对账异常：%s", exc)
        finally:
            self._refreshing = False

    # ===================== 鉴权 =====================
    @staticmethod
    def _is_master(event: AstrMessageEvent) -> bool:
        return bool(event.is_admin())

    @staticmethod
    def _is_group_admin_here(event: AstrMessageEvent) -> bool:
        gid = event.get_group_id()
        if not gid:
            return False
        group = getattr(event.message_obj, "group", None)
        if group is None:
            return False
        uid = str(event.get_sender_id())
        owner = getattr(group, "group_owner", None)
        admins = getattr(group, "group_admins", None) or []
        return uid == str(owner) or uid in {str(x) for x in admins}

    # ===================== 管理指令：gate <...> =====================
    @filter.command("gate")
    async def gate_cmd(self, event: AstrMessageEvent):
        args = event.message_str.split()[1:]
        sub = (args[0].lower() if args else "help")
        master = self._is_master(event)
        gid_here = event.get_group_id()

        if sub in ("help", "?"):
            yield event.plain_result(self._help_text())
            return

        if sub == "list":
            r = await self._refresh()
            policy = self._read_policy()
            if r["complete"]:
                joined = r["joined"]
                configured = set(policy["groups"])
                head = (
                    f"总控{'已启用' if policy['meta'].get('on', True) else '已停用'}"
                    f"｜实际加入 {len(joined)} 群｜已登记 {len(configured)} 群\n"
                    + (f"本次对账剔除退群条目 {len(r['removed'])} 个：{'、'.join(r['removed'])}\n"
                       if r["removed"] else "")
                )
                body = self._list_text(joined=joined)
            else:
                head = r["error"] + "\n"
                body = self._list_text()
            yield event.plain_result(head + body).use_t2i(True)
            return

        if sub == "on":
            if not master:
                yield event.plain_result("仅机器人管理员可操作总控开关。")
                return
            if self._on():
                yield event.plain_result("总控已处于启用状态。")
                return
            self._set_on(True)
            self._reschedule_bans()
            self._start_ban_sweep()
            r = await self._refresh()
            n = len(r.get("removed") or [])
            rr = await self._reconcile_bans()
            tail = f"启动对账剔除退群条目 {n} 个。" if n else ""
            if rr.get("skipped"):
                self._ban_reconcile_pending = True
                tail += "禁言对账已排入后台（待平台就绪）。"
            else:
                tail += f"禁言对账核查 {rr.get('checked')} 个群。"
            yield event.plain_result("总控已启用，开始按策略接管。" + tail)
            return

        if sub == "off":
            if not master:
                yield event.plain_result("仅机器人管理员可操作总控开关。")
                return
            if not self._on():
                yield event.plain_result("总控已处于停用状态。")
                return
            self._set_on(False)
            self._stop_ban_sweep()
            for gid in list(self._ban_timers):
                self._cancel_ban_timer(gid)
            yield event.plain_result("总控已停用（当死人：不拦截、不对账、不落盘）。")
            return

        if sub == "mode":
            if not master:
                yield event.plain_result("仅机器人管理员可修改群模式。")
                return
            if len(args) < 3 or args[2] not in MODES:
                yield event.plain_result("用法：/gate mode <群号|all|*> <mute|llm|plugin|both>")
                return
            target, mode = args[1], args[2]
            if _is_all_token(target):
                inv = await self._group_inventory()
                if not inv["complete"]:
                    yield event.plain_result(inv["error"] + "未执行批量修改。")
                    return
                gids = sorted(inv["groups"])
                if not gids:
                    yield event.plain_result("QQ 当前没有已加入群，未执行批量修改。")
                    return
                self._set_modes(gids, mode)
                yield event.plain_result(f"已对当前加入的 {len(gids)} 个群写入 {mode} 策略（无状态，立即生效）。")
                return
            if not target.isdigit():
                yield event.plain_result("群号须为数字，或使用 all / * 表示全部群。")
                return
            self._set_mode(target, mode)
            yield event.plain_result(f"已设置 {target} -> {mode}（无状态，立即生效）。")
            return

        if sub == "plugin":
            if len(args) < 4 or args[3].lower() not in ("on", "off"):
                yield event.plain_result("用法：/gate plugin <群号|all|*> <插件名|all|*> <on|off>")
                return
            gid, name, enable = args[1], args[2], args[3].lower() == "on"
            if _is_all_token(name):
                names = self._all_plugin_names()
                label = f"全部 {len(names)} 个插件"
            else:
                if name == PLUGIN_NAME:
                    yield event.plain_result("不能禁用 gatekeeper 自身。")
                    return
                names = [name]
                label = name
            if not names:
                yield event.plain_result("没有可操作的插件。")
                return
            if _is_all_token(gid):
                if not master:
                    yield event.plain_result("权限不足：批量操作仅机器人管理员。")
                    return
                inv = await self._group_inventory()
                if not inv["complete"]:
                    yield event.plain_result(inv["error"] + "未执行批量修改。")
                    return
                gids = sorted(inv["groups"])
                if not gids:
                    yield event.plain_result("QQ 当前没有已加入群，未执行批量修改。")
                    return
                n = self._set_plugins(gids, names, enable)
                yield event.plain_result(
                    f"已在 {n} 个群{'启用' if enable else '禁用'} {label}（无状态，立即生效）。"
                )
                return
            if not gid.isdigit():
                yield event.plain_result("群号须为数字，或使用 all / * 表示全部群。")
                return
            if not master:
                if not (gid_here and gid == gid_here and self._is_group_admin_here(event)):
                    yield event.plain_result("权限不足：仅机器人管理员，或本群管理员管理本群。")
                    return
                policy = self._read_policy()
                mode_here, _ = self._mode_and_off(policy, gid)
                if mode_here not in ("plugin", "both"):
                    yield event.plain_result("本群未开放插件管理（需 plugin / both 模式）。")
                    return
            n = self._set_plugins([gid], names, enable)
            if n:
                yield event.plain_result(f"已为群 {gid} {'启用' if enable else '禁用'} {label}（无状态，立即生效）。")
            else:
                yield event.plain_result("未执行（插件名为空或包含 gatekeeper）。")
            return

        yield event.plain_result(self._help_text())

    def _help_text(self) -> str:
        return (
            "总控闸门帮助\n"
            "/gate list — 刷新对账并显示当前加入的群与模式\n"
            "以下仅机器人管理员可用：\n"
            "/gate on — 启用总控（开始按策略接管）\n"
            "/gate off — 停用总控（当死人：不拦截、不对账、不落盘）\n"
            "/gate mode <群号|all|*> <mute|llm|plugin|both> — 设置群模式\n"
            "  模式：mute 全禁 / llm 只聊天 / plugin 只插件 / both 全开\n"
            "/gate plugin <群号|all|*> <插件名|all|*> <on|off> — 设置插件开关\n"
            "  例：/gate plugin 123456 all off（该群一键全禁插件）\n"
            "批量目标可用 all、*、全部；插件名位写 all/* 表示全部插件。\n"
            "未设置策略的群按默认 plugin（LLM 关、插件开）。\n"
            "群内插件开关仅本群管理员或机器人管理员可操作。\n"
            "bot 被禁言时自动把该群切到 mute，到期后 1 分钟或手动解除时恢复并私聊通知管理员。"
        )

    def _list_text(self, joined: dict | None = None) -> str:
        policy = self._read_policy()
        groups = policy["groups"]
        bans = policy["meta"].get("bans") or {}
        if joined is not None:
            entries = [(gid, groups.get(gid, {})) for gid in sorted(joined)]
            if not entries:
                return "QQ 当前没有已加入群。"
        else:
            entries = sorted(groups.items())
            if not entries:
                return "当前没有群策略。未设置策略的群按默认 plugin（LLM 关、插件开）。"
        lines = [f"当前群列表 · 共 {len(entries)} 个"]
        labels = {"mute": "全禁", "llm": "只聊天", "plugin": "只插件", "both": "全开"}
        for gid, entry in entries:
            entry = entry if isinstance(entry, dict) else {}
            mode = entry.get("mode") if gid in groups else None
            label = labels.get(mode, "默认 plugin") if mode else "默认 plugin"
            off = entry.get("manual_off")
            off = ", ".join(n for n in off if n != PLUGIN_NAME) if _is_str_list(off) else ""
            lines.append(f"群 {gid}｜模式：{label}" + (f"｜禁用插件：{off}" if off else ""))
        if bans:
            lines.append("")
            lines.append("临时禁言中：")
            for gid, ban in bans.items():
                until = _to_int(ban.get("until"))
                if until:
                    eta = datetime.fromtimestamp(until).astimezone().strftime("%m-%d %H:%M")
                    tail = f"{eta} 自动解除（已含 1 分钟保险期）"
                else:
                    tail = "无限期，等手动解除"
                kind = "全体禁言" if ban.get("whole") else "个人禁言"
                lines.append(f"群 {gid}｜{kind}｜{tail}")
        return "\n".join(lines)

    # ===================== Web API =====================
    def _register_web_apis(self) -> None:
        routes = [
            ("state", self.api_state, ["GET"], "读取策略与插件清单"),
            ("mode", self.api_mode, ["POST"], "设置群模式"),
            ("plugin", self.api_plugin, ["POST"], "启用/禁用插件"),
            ("on", self.api_on, ["POST"], "启用总控"),
            ("off", self.api_off, ["POST"], "停用总控"),
            ("logs", self.api_logs, ["GET"], "最近拦截日志"),
        ]
        for route, handler, methods, desc in routes:
            self.context.register_web_api(
                f"/{PLUGIN_NAME}/{route}",
                handler,
                methods,
                desc,
            )

    async def api_state(self):
        policy = self._read_policy()
        inventory = await self._group_inventory()
        joined = inventory["groups"]
        configured = policy["groups"]
        groups = []
        for gid in sorted(set(configured) | set(joined)):
            entry = configured.get(gid)
            entry = entry if isinstance(entry, dict) else {}
            mode = entry.get("mode")
            off = entry.get("manual_off")
            groups.append(
                {
                    "gid": gid,
                    "name": joined.get(gid, ""),
                    "registered": gid in configured,
                    "joined": gid in joined if inventory["complete"] else None,
                    "mode": (mode if mode in MODES else DEFAULT_MODE) if gid in configured else DEFAULT_MODE,
                    "manual_off": [n for n in off if n != PLUGIN_NAME] if _is_str_list(off) else [],
                }
            )
        bans_out = [
            {
                "gid": gid,
                "until": _to_int(b.get("until")),
                "duration": _to_int(b.get("duration")),
                "restore": b.get("restore"),
                "by": b.get("by", ""),
                "whole": bool(b.get("whole")),
            }
            for gid, b in (policy["meta"].get("bans") or {}).items()
        ]
        return {
            "on": policy["meta"].get("on", True),
            "probability": self._probability(),
            "allow_upstream_wake": self._allow_upstream_wake(),
            "notify_ban": self._notify_ban(),
            "default_mode": DEFAULT_MODE,
            "modes": list(MODES),
            "plugins": self._all_plugin_names(),
            "groups": groups,
            "bans": bans_out,
            "configured_count": len(configured),
            "joined_count": len(joined) if inventory["complete"] else None,
            "stale_count": len(set(configured) - set(joined)) if inventory["complete"] else None,
            "inventory_error": inventory["error"],
        }

    async def api_mode(self):
        payload = await request.get_json(silent=True) or {}
        gid = str(payload.get("gid", "")).strip()
        mode = payload.get("mode")
        if not gid or mode not in MODES:
            return jsonify({"status": "error", "message": "参数非法"}), 400
        if _is_all_token(gid):
            inv = await self._group_inventory()
            if not inv["complete"]:
                return jsonify({"status": "error", "message": inv["error"] + "未执行批量修改。"}), 503
            gids = sorted(inv["groups"])
            if not gids:
                return jsonify({"status": "error", "message": "QQ 当前没有已加入群"}), 400
            n = self._set_modes(gids, mode)
            return {"status": "ok", "gid": "all", "mode": mode, "groups": n}
        if not gid.isdigit():
            return jsonify({"status": "error", "message": "群号须为数字"}), 400
        self._set_mode(gid, mode)
        return {"status": "ok", "gid": gid, "mode": mode}

    async def api_plugin(self):
        payload = await request.get_json(silent=True) or {}
        gid = str(payload.get("gid", "")).strip()
        name = str(payload.get("name", "")).strip()
        enable = bool(payload.get("enable", True))
        if not gid or not name:
            return jsonify({"status": "error", "message": "参数非法"}), 400
        if _is_all_token(name):
            names = self._all_plugin_names()
        else:
            if name == PLUGIN_NAME:
                return jsonify({"status": "error", "message": "不能禁用 gatekeeper 自身"}), 400
            names = [name]
        if not names:
            return jsonify({"status": "error", "message": "没有可操作的插件"}), 400
        if _is_all_token(gid):
            inv = await self._group_inventory()
            if not inv["complete"]:
                return jsonify({"status": "error", "message": inv["error"] + "未执行批量修改。"}), 503
            gids = sorted(inv["groups"])
            if not gids:
                return jsonify({"status": "error", "message": "QQ 当前没有已加入群"}), 400
            n = self._set_plugins(gids, names, enable)
            return {"status": "ok", "gid": "all", "name": name, "enable": enable, "groups": n}
        if not gid.isdigit():
            return jsonify({"status": "error", "message": "群号须为数字"}), 400
        n = self._set_plugins([gid], names, enable)
        return {"status": "ok", "gid": gid, "name": name, "enable": enable, "groups": n}

    async def api_on(self):
        self._set_on(True)
        self._reschedule_bans()
        self._start_ban_sweep()
        r = await self._refresh()
        rr = await self._reconcile_bans()
        if rr.get("skipped"):
            self._ban_reconcile_pending = True
        return {"status": "ok", "on": True, "removed": r.get("removed") or [],
                "ban_reconcile": rr}

    async def api_off(self):
        self._set_on(False)
        self._stop_ban_sweep()
        for gid in list(self._ban_timers):
            self._cancel_ban_timer(gid)
        return {"status": "ok", "on": False}

    async def api_logs(self):
        return {"status": "ok", "logs": list(reversed(self._blocks))}

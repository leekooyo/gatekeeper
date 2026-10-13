"""gatekeeper 的 pokepro 侧查权守卫（可选补丁，独立于 gatekeeper 主体）。

作用：让 pokepro 的定时戳 / 主动戳一戳在发送前查一次 gatekeeper 的权威策略，
      避免向 mute/llm 群或已在 manual_off 中禁用的群发送。

部署（两处插入点，均在既有授权范围内；不改动其他发送方）：
  1) 把本文件复制到 `astrbot_plugin_pokepro/core/gate_guard.py`
  2) `core/scheduler.py::_on_trigger`，在循环内 `await self.sender.client_send(...)` 之前：
         from .gate_guard import gate_allows_target
         if not gate_allows_target(gid):
             continue
  3) `core/send_poke.py::poke_func`，在 group_poke 之前：
         from .gate_guard import gate_allows_target
         if normalized_group_id is not None and not gate_allows_target(normalized_group_id):
             return

语义：
  - 私聊（gid 为空）由调用方保证放行。
  - gatekeeper 未部署 → 放行（无管辖，保证卸载后 pokepro 自愈）。
  - 任何读取 / 解析 / 结构异常 → 停发（fail-closed）。
  - 直读唯一权威源 `plugin_data/gatekeeper/modes.json`，无派生缓存、无陈旧许可。
"""

from __future__ import annotations

import json
from pathlib import Path

GATEKEEPER_PLUGIN = "gatekeeper"
POKE_PLUGIN_NAME = "astrbot_plugin_pokepro"
POLICY_MAX_BYTES = 1 << 20


def _policy_path() -> Path:
    try:
        from astrbot.api.star import StarTools

        return Path(StarTools.get_data_dir(GATEKEEPER_PLUGIN)) / "modes.json"
    except Exception:
        from astrbot.core.utils.astrbot_path import get_astrbot_data_path

        return Path(get_astrbot_data_path()) / "plugin_data" / GATEKEEPER_PLUGIN / "modes.json"


def gate_allows_target(gid) -> bool:
    """True = 允许向该群发送戳一戳。fail-closed；未部署则放行。"""
    if not gid:
        return True
    path = _policy_path()
    try:
        if not path.exists():
            return True  # gatekeeper 未部署 → 无管辖
        with path.open("rb") as fh:
            data = fh.read(POLICY_MAX_BYTES + 1)
        if len(data) > POLICY_MAX_BYTES:
            return False
        parsed = json.loads(data.decode("utf-8"))
    except Exception:
        return False  # 任何异常 → 停发

    try:
        if not isinstance(parsed, dict) or parsed.get("version") != 1:
            return False
        meta = parsed.get("meta") if isinstance(parsed.get("meta"), dict) else {}
        if meta.get("released") is True:
            return True  # 已交管
        if meta.get("hold") is True:
            return False
        groups = parsed.get("groups")
        if not isinstance(groups, dict):
            return False
        entry = groups.get(str(gid))
        if not isinstance(entry, dict):
            return False  # 未准入 → 默认拒绝
        if entry.get("mode") not in ("plugin", "both"):
            return False  # mute / llm → 停发
        manual_off = entry.get("manual_off")
        if manual_off is None:
            return True
        if not isinstance(manual_off, list) or any(not isinstance(x, str) for x in manual_off):
            return False  # 坏 manual_off → 停发
        return POKE_PLUGIN_NAME not in manual_off
    except Exception:
        return False

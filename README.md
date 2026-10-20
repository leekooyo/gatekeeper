# 总控闸门（gatekeeper）

## 安装与兼容

在 AstrBot 中选择「通过仓库地址安装」，填写 `https://github.com/leekooyo/gatekeeper`。**当前版本：0.4.0。** `metadata.yaml` 将 AstrBot 版本限定为 **4.28.2**；其他框架版本需要先修改并验证这一约束。插件主要处理 QQ 群消息，管理员控制与禁言状态检测依赖已配置的 aiocqhttp 平台。当前环境允许机器人自身的群 notice 派发；若其他环境过滤此类事件，轮询仍可检测禁言，但进退群通知可能受限。

仓库只保存代码和默认配置结构。群策略保存在本机插件数据目录的 `gatekeeper/modes.json`，通知目标在 AstrBot 插件配置中；群号和实际配置不要提交到 GitHub。插件还提供 WebUI 管理页，可查看和调整各群模式。

按群保存 `mute`（全静默）、`llm`（仅聊天）、`plugin`（仅插件）、`both`（全开）四种模式。未单独设置的群默认为 `plugin`。群策略及临时禁言状态保存在插件数据中，不修改 AstrBot 的全局会话开关。

## 被禁言与管理员通知

机器人在某群被个人禁言或全体禁言时，该群临时按 `mute` 处理；手动解除或禁言到期并经过约一分钟宽限后，恢复此前设置的模式。状态以 QQ 接口轮询结果为准，群 notice 只用于加快发现。`ban_sweep_interval` 默认 60 秒；设为 0 后只在启动及 `/gate on` 时对账，之后可能漏掉变化。

`probability` 默认 `30%`，只影响 `llm` / `both` 模式中 `@bot` 的放行概率；`allow_upstream_wake` 默认关闭，保持严格的 `@bot` 唤醒边界。需要沿用 wakepro 等上游唤醒结果时再开启。

`notify_ban` 控制禁言和解禁通知。`notify_target=admin_private`（默认）逐人私聊 AstrBot 配置中的机器人管理员；`notify_target=review_group` 则发送到 `review_group_id`。审核群号无效时回退管理员私聊。各接收目标的发送异常会分别记录。机器人实际进退群、被踢和普通成员变动的通知由 relationship 插件负责，避免重复。

## 指令

`/gate help` 查看完整指令。`/gate list` 查询并对账已加入群；`/gate on` 与 `/gate off` 控制总闸；`/gate mode <群号|all> <mute|llm|plugin|both>` 设置模式；`/gate plugin <群号|all> <插件名|all> <on|off>` 设置插件开关。总闸关闭时停止自动拦截和对账，手动管理指令仍可用。

离群 notice 到达时会移除该群的模式和临时禁言状态；启动、`/gate list` 与重新启用总闸时也会依据 QQ 群列表对账。若平台离线、群列表读取失败，对账不会把未知群误判为已退群。机器人自身群 notice 能否到达插件还取决于 AstrBot 核心的自发消息过滤规则；本机已保留普通自发消息过滤，同时允许自身 notice 派发。

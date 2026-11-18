# GMGN 喊单（callout）云端独立接管

与本机 `callout_push.py` 同一业务口径，**独立仓库 / 独立状态 / 独立心跳 / 独立并发组**，
不与 top1、热搜、精选等任何其它云端任务混用状态或限流。

## 双端衔接（无缝切换）

```
本机 callout_push.py ──(写 callout_seen.json / callout_hb.json)──┐
                                                                 ├── cloud_callout_sync.py（本机独立协同进程）
云端 callout_cloud 仓库 ◄── 心跳 callout_heartbeat.json ◄───────┘
                          └── 去重 callout_state.json ◄── 双向并集合并
```

- **本机在线**：协同进程每分钟写心跳到 `callout_heartbeat.json`；云端 Actions 每 5 分钟先读心跳，
  新鲜（<120 秒）则整轮跳过，不抢 GMGN 限流、不重复推送。
- **本机离线**：心跳过期后云端接管，拉 `callout global` 推送到 TG 群；推送结果并集写回 `callout_state.json`。
- **去重**：每合约终身只推一次；本机与云端 pushed 集合经乐观锁（GET 最新 → 并集 → 带 sha PUT）双向合并。

## 文件

| 文件 | 职责 |
|---|---|
| `cloud_callout.py` | 云端接管脚本：心跳判活 → 取喊单流 → 过滤暂停链 → 去重 → 推送 → 回写状态 |
| `.github/workflows/callout-cloud.yml` | `*/5` 调度 + 手动 dispatch；独立并发组 `callout-cloud` |
| `callout_state.json` | 云端去重表（pushed 并集） |
| `callout_heartbeat.json` | 本机存活心跳占位/实时信号 |

## Secrets（仓库 Actions → Secrets）

`GMGN_API_KEY`、`TG_API_ID`、`TG_API_HASH`、`TG_STRING_SESSION`。
`GITHUB_TOKEN` 由 Actions 自动提供。

## 口径

- 全部链；BSC 由环境变量 `PAUSE_CHAINS`（默认 `bsc`）暂停，与本机 `pause_bsc.flag` 对齐。
- **推送时机**：同一合约出现「第二条喊单记录」才推送，首条只入账不推；
  记录窗口 12 小时，窗口内没等到第二条就作废。整生命周期仍只推一次。
- 不接 dbotx，只推送。
- 消息渲染、链段映射与本机 `callout_push.py` 一致。

## 状态文件结构

```json
{
  "pushed":  ["链:小写地址", ...],              // 已推过，终身不再推
  "records": {"链:小写地址": {"s": ["记录id", ...最多2个], "t": 最后喊单时间戳}},
  "updated": 时间戳
}
```

`records` 只保留未推送的合约；两端（本机 / 云端）双向并集合并，
同一合约两端各看到一条不同记录时合并后即为「第二次」，可正常触发推送。

## 手动调试（Actions → Run workflow）

| 输入 | 作用 |
|---|---|
| `dry_run=1` | 只打印将推送的消息，不发送、不写状态 |
| `self_test=1` | 只发一条 TG 自检消息，不取数 |
| `force_takeover=1` | 忽略心跳、强制走接管路径 |

## 本机侧对应文件（上级目录）

- `callout_push.py` —— 本机推送器
- `cloud_callout_sync.py` —— 本机云端协同（心跳 + 状态合并）
| `callout_seen.json` / `callout_hb.json` —— 本机去重、记录表与存活信号
- `pause_bsc.flag` —— 本机 BSC 暂停开关
- `pause_callout_cloud.flag` —— 喊单云端协同的独立暂停开关（存在=暂停协同，默认不存在=常开）

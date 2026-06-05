# Web 管理页面 CRUD 升级 — 设计文档

> 状态：草案 v1  
> 日期：2026-06-05  
> 关联提交：`44f1cca`（Web 管理界面初始版，仅 Reload+Restart）

## 一、问题陈述

当前 `gb28181_pusher.py` 的内置 Web 管理界面（`WebAdminServer` / `_WebHandler`）仅提供：

| API | 能力 |
|---|---|
| `GET /api/status` | 读全部实例状态 |
| `POST /api/reload` | 重新扫描 `config/*.json` 并 diff 重启 |
| `POST /api/restart/<cid>` | 重启单实例 |

**没有**添加 / 修改 / 删除 channel 的能力，运维仍需 SSH 登录手工编辑 JSON 文件 + 点 Reload。
不符合"管理页面"的预期。

## 二、目标

将 Web 升级为真正的 **Channel CRUD 管理平台**：
- 浏览器内完成通道的增 / 改 / 删 / 查
- 每个 channel 对应一个独立 JSON 配置文件（多 config 文件模式）
- 字段明文显示、RTSP 密码明文（不做额外加密）
- 硬删（删除 JSON 文件 + 停止运行实例）
- 0.0.0.0 暴露时不做鉴权（依赖 127.0.0.1 默认绑定 + 部署侧防火墙）

## 三、API 设计

### 3.1 新增端点

| Method | Path | 行为 | 返回 |
|---|---|---|---|
| `GET` | `/api/channels` | 列出所有 channel（详尽） | `[{channel_id, instance_name, source, server_ip, server_id, agent_id, registered, thread_alive, config_file, raw_config}, …]` |
| `GET` | `/api/channels/<cid>` | 取单条 channel 完整配置 | `{"ok": true, "channel_id": "...", "config_file": "camera1.json", "config": {…}}` |
| `PUT` | `/api/channels/<cid>` | **整条覆盖**更新现有 channel（写回 JSON + reload 单实例） | `{"ok": true, "updated": true, "restarted": true}` |
| `POST` | `/api/channels` | 创建新 channel（form 字段 + 文件名，详见 §四） | `{"ok": true, "channel_id": "...", "config_file": "new.json", "started": true}` |
| `DELETE` | `/api/channels/<cid>` | **硬删**：停实例 + 删 JSON 文件 | `{"ok": true, "deleted": true, "config_file": "..."}` |

### 3.2 文件名规则

| 操作 | 文件名来源 |
|---|---|
| 创建 | 前端 form 提供 `config_file`（必填，规则：`^[a-zA-Z0-9_\-]+\.json$`），不能与现有 `config/*.json` 冲突 |
| 更新 | 固定写到原文件（路径由后端按 `channel_id` 在 `self._index` 找到原 `instance_name` 推断） |
| 删除 | 按 `channel_id` 在 `self._index` 找到原实例名 → 删 `config/<instance_name>.json` |

### 3.3 字段校验

`server_ip` / `server_id` / `domain` / `agent_id` / `agent_password` / `channel_id` / `source` 全部必填且非空字符串。
`udp` 接受 `true|false` 字符串，转换为 bool。
`rtsp_precheck` 接受 `true|false` 字符串，默认 `true`。
`rtsp_precheck_timeout` 接受整数 1-30，默认 5。
其他字段（`reconnect_interval` / `max_reconnect_attempts` / `connection_timeout` / `verbose`）使用 `load_config()` 里的 defaults，如用户提供则覆盖。

## 四、POST 请求体格式

`POST /api/channels` 使用 `application/x-www-form-urlencoded`（前端简单 form 提交，无需 multipart）：

```
config_file=camera_lobby.json
server_ip=192.168.1.100
server_port=5060
server_id=11009000000000000000
domain=1100900000
agent_id=300000000010000000001
agent_password=000000
channel_id=340000000000000000001
source=rtsp://admin:admin@192.168.1.122/h264/ch1/main/av_stream
udp=false
rtsp_precheck=true
rtsp_precheck_timeout=5
manufacturer=StrawberryInno
devicename=Lobby
```

`PUT /api/channels/<cid>` 使用相同字段集，但 `config_file` 字段被忽略（不能改文件名）。

## 五、并发与原子性

1. **写文件前**做 `.tmp` 临时文件 + `os.replace()` 原子替换，避免半截写入
2. **写文件后**调用 `manager.reload()` 让 manager 自动 diff 出"新增/修改"并启动新实例
3. **删除前**先 `manager._stop_one()` 停实例，再删文件
4. 所有写操作期间 manager 持 `self._lock` 短暂锁（仅覆盖"找到实例 / 写文件 / 触发重启"这一段，不持锁跑 gst）

## 六、前端 UI 改造

保留现有暗色主题，加：

- 卡片右下角新增 **Edit**、**Delete** 按钮（仅这两个 CRUD 操作）
- 顶部新增 **+ Add Channel** 按钮
- **Add/Edit** 弹一个模态框（form），字段同 §四
- 模态框 Edit 模式预填现有值
- Add/Edit/Delete 失败 toast 错误信息；成功 toast "Channel xxx created/updated/deleted"
- **RTSP 密码 / agent_password 用 `type="password"` 展示**（虽然后端不加密，但前端不直接明文铺给背后偷窥的人；用户主动点 "show" 切到明文）
- 删除有 `confirm()` 二次确认

## 七、错误码

| HTTP | 触发 |
|---|---|
| 200 | 成功 |
| 400 | 字段缺失 / `config_file` 不合法 / JSON 字段类型错 |
| 404 | `channel_id` 不存在（GET/PUT/DELETE） |
| 409 | 创建时 `config_file` 冲突（已存在） |
| 500 | 写文件失败 / 启动实例失败 |

## 八、安全边界

- 默认监听 `127.0.0.1`，已通过 `--web-host 0.0.0.0` 显式开放才对外
- **不做** web 鉴权（避免引入 token / session 复杂度；用户确认接受）
- 写文件用 `os.path.join(self._config_dir, config_file)` 后用 `os.path.realpath()` 校验仍在 `self._config_dir` 之内，**防 `../` 越权**
- 所有用户输入经 `esc()` 转义后渲染

## 九、测试

新增 8 个单元测试（mock manager，零网络）：

1. `GET /api/channels` 返回所有实例
2. `GET /api/channels/<cid>` 命中返回 200 + config 详情
3. `GET /api/channels/<cid>` 不存在返回 404
4. `PUT /api/channels/<cid>` 写文件 + reload + 实例重启（mock reload）
5. `POST /api/channels` 创建新文件 + 新实例
6. `POST /api/channels` 重名返回 409
7. `POST /api/channels` `config_file="../etc/passwd"` 返回 400
8. `DELETE /api/channels/<cid>` 停实例 + 删文件

## 十、不在本次范围

- 字段级 PATCH（只改某项）—— 你说"整条覆盖"
- RTSP 密码/agent_password 加密 —— 你说"明文"
- Web 登录鉴权 —— 依赖 127.0.0.1 默认
- 软删/回收站 —— 你说"硬删"
- 多文件合一个 JSON（多 config 文件）—— 你说"加新文件"
- 把 9266219 的预检 bug 一起改掉 —— 留作单独 commit

## 十一、实现计划（1 个 commit，约 350-400 行）

```
M  gb28181_pusher.py
   + _WebHandler 新增 5 个路由 (do_PUT/do_DELETE/do_POST /api/channels)
   + do_GET 扩展为支持 /api/channels 和 /api/channels/<cid>
   + _WEB_INDEX_HTML 升级为支持 CRUD 的 SPA
   + _validate_config_fields() 工具函数
   + _safe_config_path() 防越权
   + WebAdminServer 不变（注入 manager 的方式已够）

+ tests/test_web_crud.py  (8 个测试)
+ docs/WEB_CRUD_DESIGN.md  (本文档)
```

---

**待你确认后我开始动手**。如果你希望改任何字段、UI、行为，现在告诉我；否则按此实施。

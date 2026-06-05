# skygb28181 Pusher

[English](Readme_EN.md)

> **基于 Python + GStreamer 的轻量级 GB28181 设备示例** —— 自处理 SIP REGISTER、响应 INVITE,并使用 GStreamer 推送符合国标的 **PS / H.264** 视频流。

本仓库提供一个简洁的 **GB28181** 参考实现,可将任意视频源模拟为摄像头侧设备并推流到 GB28181 媒体服务器。信令交互由 Python 脚本负责,媒体发送则依赖自制 `gb28181sink` GStreamer 插件。

特别适合以下场景:
- **国标协议学习 / 教学** —— 单文件,标准库 + GStreamer,无外部 Python 框架依赖
- **多路视频接入** —— 支持 N 个通道同时注册到同一国标平台,每个通道一个 JSON 配置文件
- **边缘设备部署** —— 内置 Web 管理界面,无 SSH 即可增/改/删通道、监控状态

---

## 🎯 主要特性

| 功能                       | 说明                                                            |
| ------------------------ | ------------------------------------------------------------- |
| **TCP / UDP 信令**         | 默认使用 **TCP**,可通过 `--udp` 切换为 UDP                          |
| **TCP / UDP 媒体**         | 支持 UDP 或 *被动* TCP 模式的 PS / RTP 视频流传输                       |
| **标准 PS 封装**            | 基于 `gb28181sink` 插件推送符合国标的 PS 流(96/PS 或按 SDP 协商)         |
| **命令行参数化**              | 平台 IP/端口、平台 ID、密码等均可在启动时指定                                 |
| **自动 REGISTER**         | 内置 Digest-401 质询处理,支持多次重试                                |
| **SDP / INVITE 解析**     | 遵循 GB28181 规范,优先选择 96/PS、98/H264 负载类型                       |
| **动态 GStreamer pipeline** | 根据 SDP 动态拼接 `gst-launch-1.0` 管道,支持 PS / H.264 / TCP / UDP  |
| **心跳与查询响应**            | 周期 *Keepalive*,响应 *Catalog* / *DeviceInfo* 查询                 |
| **自动重连机制**              | 连接断开时自动重连,可配置重连间隔和最大重试次数                                 |
| **RTSP 预检**              | 注册到平台前先探活 RTSP 源,避免无效注册                                   |
| **多路推流**                | 一个进程同时管理 N 个通道,每个通道一个 `config/<name>.json`                    |
| **配置热重载**               | 修改 JSON 或新增文件后点 *重载* 按钮即可生效(SIGHUP 或 HTTP 触发)            |
| **Web 管理界面**            | 内置 `http.server` SPA,**图形化增/改/删通道**(中文 UI,127.0.0.1 默认绑定)  |
| **结构化日志**              | `--verbose` 输出完整 SIP 报文,便于抓包与调试                            |

---

## 📦 依赖环境

* **Python ≥ 3.9**
* **GStreamer ≥ 1.18**
* 自编译的 `gb28181sink` 插件(本仓库 `gst-gb28181sink/` 目录下提供)

### 一键安装 + 编译(推荐)

仓库自带脚本 [`Tools/install_and_build.sh`](Tools/install_and_build.sh),5 步走完:

```bash
./Tools/install_and_build.sh            # 装 apt 依赖 + 编译 + 用户级安装
sudo ./Tools/install_and_build.sh --system   # 装到系统插件目录
./Tools/install_and_build.sh --no-install --skip-apt   # CI / 已有环境:只编译
./Tools/install_and_build.sh --help    # 全部参数
```

脚本会自动:
1. 检测 OS / 架构 / Python / GStreamer 环境
2. `apt install` 编译 + 运行时依赖(gstreamer-dev / meson / ninja / x264 / libav 等)
3. 检查 Python 标准库依赖(本项目无 pip 依赖)
4. `meson setup` + `ninja` 编译 `gb28181sink`
5. 安装到 `~/.local/lib/gstreamer-1.0`(默认)或 `/usr/local/lib/<arch>/gstreamer-1.0`(`--system`)
6. 跑 `gst-inspect-1.0 gb28181sink` 验证

> 💡 想手动编译,见 `gst-gb28181/README.md` 里的 `meson setup build && meson compile -C build`。

### 本地环回测试

```bash
# 1) 终端 A: 监听 TCP 9527 端口
nc -l 9527 | hexdump -C | less

# 2) 终端 B: 用 gb28181sink 推测试视频到 9527
gst-launch-1.0 videotestsrc is-live=true ! video/x-raw,width=640,height=480,framerate=25/1 ! \
  x264enc key-int-max=50 tune=zerolatency bitrate=800 ! \
  h264parse ! mpegpsmux ! \
  gb28181sink protocol=tcp host=127.0.0.1 port=9527 pt=96 ssrc=0x01020304
```

---

## 🚀 快速开始

### 1. 单通道推流(命令行模式)

```bash
python3 gb28181_pusher.py \
  --server-ip 101.226.23.126 --server-port 8116 \
  --server-id 41010500002000000001 --domain 4101050000 \
  --agent-id 300000000010000000001 --agent-password 12345678 \
  --channel-id 340000000000000000001 \
  --source rtsp://192.168.1.122/stream_0 \
  --verbose
```

> 若平台使用 UDP 信令,在最后加上 `--udp`。

### 2. 多通道推流(推荐生产用法)

`config/` 目录下每放一个 JSON 就是一个通道,**启动时自动加载所有 JSON**,并以通道为单位注册到国标平台。

```bash
# 1) 准备 config/camera1.json
cat > config/camera1.json <<'JSON'
{
  "server_ip": "101.226.23.126",
  "server_port": 8116,
  "server_id": "41010500002000000001",
  "domain": "4101050000",
  "agent_id": "300000000010000000001",
  "agent_password": "12345678",
  "channel_id": "340000000000000000001",
  "source": "rtsp://192.168.1.122/stream_0",
  "udp": false,
  "manufacturer": "gzhaibaogd",
  "devicename": "LobbyCam"
}
JSON

# 2) 复制并修改,得到 camera2.json, camera3.json ...

# 3) 启动(无参数 = 默认读 ./config/*.json)
python3 gb28181_pusher.py
```

启动后脚本将:
1. 扫描 `config/*.json`,为每个文件启动一个 pusher 线程
2. 各自 **REGISTER** 到平台(通道间隔 0.5s 错开,避免同时打平台)
3. 各自 60s 发送一次 *Keepalive*
4. 各自等待平台 **INVITE**,收到后自动 100 Trying → 200 OK 并解 SDP
5. 根据 SDP 拼 GStreamer 推 PS / H.264 到平台指定 IP/端口
6. 处理 **BYE**、**MESSAGE**、**SUBSCRIBE** 并返 200 OK
7. 各自 **自动重连**,互不影响

### 3. Web 管理界面

启动时默认启用 Web,绑定 `127.0.0.1:8080`:

```bash
python3 gb28181_pusher.py
# 日志里会显示: Web admin UI available at http://127.0.0.1:8080/
```

打开浏览器访问 `http://<服务器IP>:8080/`,可以:

- **+ 新增通道** —— 弹窗填写,自动写 JSON + 启动 pusher
- **✎ 编辑** —— 弹窗回填现有值,保存后整条覆盖 + 重启实例
- **✕ 删除** —— 二次确认,硬删 JSON 文件 + 停止 pusher
- **↻ 重启** —— 临时重启单实例(不修改配置)
- **↻ 重载配置** —— 全量重扫 `config/`,diff 出新增/更新/删除
- **每 5 秒自动刷新** 通道状态(已注册 / 未注册 / 已停止)

如需远程访问(⚠️ 默认 127.0.0.1 仅本机可访问):

```bash
python3 gb28181_pusher.py --web-host 0.0.0.0 --web-port 8080
```

> ⚠️ 0.0.0.0 绑定意味着局域网任意人都能改你的通道配置。**请配合网络层防火墙** 限制来源 IP。

禁用 Web(纯命令行 / 无头场景):

```bash
python3 gb28181_pusher.py --disable-web
```

### 4. 配置热重载

JSON 改完后,不需要重启进程。两种触发方式:

- **HTTP**:`POST /api/reload` 或在 Web 界面点 *↻ 重载配置*
- **信号**:`kill -HUP <pid>`(POSIX 系统,Windows 不支持)

热重载会:
- `+` 新增 `config/` 下新出现的 JSON
- `~` 更新已存在但内容变了的 JSON(停旧实例 → 起新实例)
- `-` 删掉 JSON 已不存在的实例
- 错误信息(字段缺失、JSON 解析失败等)会汇总返回,**不会中断其他通道**

---

## 🛠️ 配置参数

`load_config()` 会在 `config/<name>.json` 缺失字段时自动填默认值。下表是所有支持字段:

| 字段                          | 必填 | 默认值                    | 说明                                |
| --------------------------- | -- | ---------------------- | --------------------------------- |
| `server_ip`                 | ✅  | —                      | 平台 SIP 地址                         |
| `server_port`               |    | `8116`                 | 平台 SIP 端口                         |
| `server_id`                 | ✅  | —                      | 平台国标编号                            |
| `domain`                    |    | `4101050000`           | SIP 域                              |
| `agent_id`                  | ✅  | —                      | 本设备国标编号                           |
| `agent_password`            | ✅  | —                      | REGISTER Digest 密码                |
| `channel_id`                | ✅  | —                      | 上报给平台的通道编号                        |
| `source`                    |    | `"test"`               | 视频源                               |
| `udp`                       |    | `false`                | 使用 **UDP** 而非默认 **TCP** 进行 SIP 交互 |
| `local_ip`                  |    | `null`(自动探测)           | 绑定本地网卡                            |
| `verbose`                   |    | `false`                | 输出调试日志及完整 SIP 报文                  |
| `reconnect_interval`        |    | `5`                    | 重连间隔时间(秒)                         |
| `max_reconnect_attempts`    |    | `0`                    | 最大重连次数(0 = 无限重连)                  |
| `connection_timeout`        |    | `10`                   | 连接超时时间(秒)                         |
| `manufacturer`              |    | `gzhaibaogd`           | 厂商标识                              |
| `devicename`                |    | `Superdock`            | 设备名称                              |
| `rtsp_precheck`             |    | `true`                 | 注册前是否先 RTSP 探活                    |
| `rtsp_precheck_timeout`     |    | `5`                    | RTSP 探活超时(秒)                      |

> 📌 Web 新增/编辑弹窗的默认值已与上表同步。Web 增删改时,文件以 `config_file` 字段命名(必须匹配 `[A-Za-z0-9_-]+\.json`)。

### `source` 视频源格式

| 示例                                                                   | 效果                |
| -------------------------------------------------------------------- | ----------------- |
| `test`                                                               | 内置 `videotestsrc` |
| `rtsp://admin:admin@192.168.1.122/h264/ch1/main/av_stream`           | 拉 RTSP 摄像机流       |
| `udp://:5000`                                                        | 监听组播 UDP 码流       |
| `file://sample.mp4`                                                  | 播放本地文件并推流         |
| `v4l2:///dev/video0`                                                 | 直接采集本地摄像头         |

---

## 📡 HTTP API(Web 管理界面)

所有路由均接受 `application/x-www-form-urlencoded` 请求体。

| Method | Path                       | 行为                                       |
| ------ | -------------------------- | ---------------------------------------- |
| GET    | `/`                        | 渲染 Web SPA(中文 UI)                       |
| GET    | `/api/channels`            | 列出所有通道(含 `config_file`、运行状态)            |
| GET    | `/api/channels/<cid>`      | 单条通道完整配置(`200` 或 `404`)                 |
| POST   | `/api/channels`            | 新建通道(form 字段 + `config_file`),返回 `409` 重名 |
| PUT    | `/api/channels/<cid>`      | 整条覆盖,重启实例(无 diff/PATCH)                 |
| DELETE | `/api/channels/<cid>`      | **硬删**:停实例 + 删 JSON 文件                 |
| POST   | `/api/reload`              | 全量重扫 `config/` 并 diff                  |
| POST   | `/api/restart/<cid>`       | 重启单实例(不改配置)                            |

错误码:`400` 字段缺失/越权文件名,`404` 通道不存在,`409` 文件名冲突,`500` 内部错误。

文件名校验:必须匹配 `^[A-Za-z0-9_-]+\.json$`,**禁止** `..` / 绝对路径 / 包含 `/`。

---

## 🧪 测试

```bash
# 单元测试(8 个,覆盖所有 CRUD 路由)
python3 -m unittest tests.test_web_crud -v

# 端到端烟测(8 步,真起 HTTP server)
python3 tests/test_web_crud_e2e.py
```

两个测试都 mock 了 `MultiPusherManager._start_one`,不会真发 SIP/启动 gst 管线,可在 CI 跑。

---

## 📚 附加工具

- [`Tools/BuildGB28181Server.md`](Tools/BuildGB28181Server.md):手把手搭建 GB28181 媒体服务器(**ZLMediaKit + wvp-GB28181-pro** 组合)。
- [`Tools/gb28181_proxy.py`](Tools/gb28181_proxy.py):GB28181 信令转发代理,记录信令交互和原始媒体包,主要给 Wireshark 抓包分析用。

```bash
# 让海康或其他支持 GB28181 的设备向本机 5060 端口注册,
# 即可转发到 xxx.xxx.xxx.xxx:5060,顺便看完整交互过程
python3 Tools/gb28181_proxy.py \
  --listen-host 0.0.0.0 --listen-port 5060 \
  --server-host xxx.xxx.xxx.xxx --server-port 5060
```

---

## 🧑‍💻 辅助编程

OpenAI o3 和 Gemini 2.5 Pro,以及 Anthropic MiniMax。

---

## 📜 许可

本仓库采用 **MIT** 协议开源,详见 `LICENSE`(如未提供请补充)。

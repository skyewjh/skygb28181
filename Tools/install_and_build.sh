#!/usr/bin/env bash
# ============================================================================
#  install_and_build.sh — 一键安装编译环境并编译 gb28181sink 插件
#  sbgb28181 项目的官方引导脚本
#
#  用法:
#    ./Tools/install_and_build.sh                # 推荐默认 (用户级安装)
#    ./Tools/install_and_build.sh --system       # 装到 /usr/local (需 sudo)
#    ./Tools/install_and_build.sh --no-install   # 只编译, 不安装
#    ./Tools/install_and_build.sh --skip-apt     # 跳过 apt 步骤 (CI/已装好)
#    ./Tools/install_and_build.sh --clean        # 清理 build/ 目录后重新编译
#    ./Tools/install_and_build.sh --help
#
#  设计目标:
#    - 在 Ubuntu 22.04+ / Debian 12+ / Armbian / 树莓派 OS 上"开箱即用"
#    - 不假设用户有 sudo (默认用户级安装到 ./build)
#    - 每个步骤都做"已经满足条件就跳过"的判断, 反复跑也安全
#    - 出错时给出具体可执行的修复提示
# ============================================================================
set -euo pipefail

# ----------------------------------------------------------------------------
# 路径与配置
# ----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SINK_DIR="$REPO_ROOT/gst-gb28181sink"
BUILD_DIR="$SINK_DIR/build"

# 颜色 (只在 tty 时启用)
if [[ -t 1 ]]; then
    RED='\033[0;31m'; GRN='\033[0;32m'; YLW='\033[0;33m'; BLU='\033[0;34m'; RST='\033[0m'
else
    RED=''; GRN=''; YLW=''; BLU=''; RST=''
fi

log()   { echo -e "${BLU}[$(date +%H:%M:%S)]${RST} $*"; }
ok()    { echo -e "${GRN}[  OK  ]${RST} $*"; }
warn()  { echo -e "${YLW}[ WARN ]${RST} $*"; }
err()   { echo -e "${RED}[ FAIL ]${RST} $*" >&2; }

# ----------------------------------------------------------------------------
# 参数解析
# ----------------------------------------------------------------------------
INSTALL_MODE="user"   # user | system | none
SKIP_APT=0
CLEAN=0
for arg in "$@"; do
    case "$arg" in
        --system)      INSTALL_MODE="system" ;;
        --no-install)   INSTALL_MODE="none" ;;
        --skip-apt)     SKIP_APT=1 ;;
        --clean)        CLEAN=1 ;;
        -h|--help)
            sed -n '2,18p' "$0"
            exit 0
            ;;
        *)
            err "未知参数: $arg (用 --help 查看用法)"
            exit 2
            ;;
    esac
done

# ----------------------------------------------------------------------------
# 工具函数
# ----------------------------------------------------------------------------
have() { command -v "$1" >/dev/null 2>&1; }

# 检测 apt 系发行版 (Debian / Ubuntu / Armbian / Raspbian / Mint)
is_apt() { have apt-get && have dpkg; }

detect_arch() {
    local m
    m="$(uname -m)"
    case "$m" in
        x86_64|amd64)  echo "amd64" ;;
        aarch64|arm64) echo "arm64" ;;
        armv7l|armhf)  echo "armhf" ;;
        i686|i386)     echo "i386"  ;;
        *)             echo "$m"   ;;
    esac
}

# ----------------------------------------------------------------------------
# Step 0: 环境自检
# ----------------------------------------------------------------------------
log "Step 0/5  环境自检"
ARCH="$(detect_arch)"
log "  架构        : $ARCH"
log "  OS          : $(grep -E '^PRETTY_NAME=' /etc/os-release 2>/dev/null | cut -d= -f2 | tr -d '\"' || echo 'unknown')"
log "  工作目录    : $REPO_ROOT"
log "  安装模式    : $INSTALL_MODE"

if ! is_apt; then
    warn "当前系统不是 apt 系 (Debian/Ubuntu/Armbian 等)"
    warn "脚本会尝试继续, 但 apt 步骤会被跳过"
    warn "请自行安装: gstreamer1.0-tools, gstreamer1.0-plugins-{base,good,bad},"
    warn "             libgstreamer1.0-dev, libgstreamer-plugins-base1.0-dev, libglib2.0-dev,"
    warn "             meson, ninja-build"
    SKIP_APT=1
fi

if ! have python3; then
    err "找不到 python3 (≥ 3.9 必需)"
    err "Ubuntu:  sudo apt install python3"
    exit 1
fi
PY_VERSION="$(python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
log "  Python      : $(python3 --version) (要求 ≥ 3.9)"
if ! python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3,9) else 1)'; then
    err "Python 版本低于 3.9 ($PY_VERSION)"
    exit 1
fi

if [[ ! -d "$SINK_DIR" ]]; then
    err "找不到 gb28181sink 源码目录: $SINK_DIR"
    err "请在 sbgb28181 仓库根目录下运行此脚本"
    exit 1
fi
if [[ ! -f "$SINK_DIR/meson.build" ]]; then
    err "$SINK_DIR/meson.build 不存在, 源码不完整"
    exit 1
fi

# ----------------------------------------------------------------------------
# Step 1: 安装系统依赖 (apt)
# ----------------------------------------------------------------------------
if [[ $SKIP_APT -eq 0 ]]; then
    log "Step 1/5  安装系统依赖 (apt)"
    if ! have sudo && [[ $EUID -ne 0 ]]; then
        err "需要 sudo 来安装系统包, 但找不到 sudo 命令, 当前用户也不是 root"
        err "请以 root 身份运行, 或安装 sudo, 或传 --skip-apt 跳过"
        exit 1
    fi
    SUDO=""
    if [[ $EUID -ne 0 ]]; then SUDO="sudo"; fi

    # 一次性更新一次 index, 但只在缓存过期时
    if [[ ! -d /var/lib/apt/lists ]] || \
       [[ -z "$(ls -A /var/lib/apt/lists 2>/dev/null)" ]] || \
       [[ $(find /var/lib/apt/lists -name '*_Packages' -mmin -60 2>/dev/null | wc -l) -eq 0 ]]; then
        log "  更新 apt 缓存 (可能需要 30s)..."
        $SUDO apt-get update -qq
    else
        log "  apt 缓存新鲜 (1h 内), 跳过 update"
    fi

    PACKAGES=(
        # 编译工具链
        build-essential
        meson
        ninja-build
        pkg-config
        # GStreamer 核心 + 头文件
        libgstreamer1.0-dev
        libgstreamer-plugins-base1.0-dev
        libglib2.0-dev
        # 运行时 (gb28181sink 是 GStreamer 插件, 运行也需要这些)
        gstreamer1.0-tools
        gstreamer1.0-plugins-base
        gstreamer1.0-plugins-good
        gstreamer1.0-plugins-bad
        gstreamer1.0-plugins-ugly
        # 编解码 (H.264 编码器, Readme 里的环回测试用到)
        gstreamer1.0-libav
        # 可选: x264 (部分 Ubuntu 发行版有独立包, 装了 x264enc 一定能用)
        x264
    )

    # 哪些包已经装了
    TO_INSTALL=()
    for pkg in "${PACKAGES[@]}"; do
        if dpkg -s "$pkg" >/dev/null 2>&1; then
            log "  已装: $pkg"
        else
            TO_INSTALL+=("$pkg")
        fi
    done

    if [[ ${#TO_INSTALL[@]} -gt 0 ]]; then
        log "  待装: ${TO_INSTALL[*]}"
        $SUDO apt-get install -y --no-install-recommends "${TO_INSTALL[@]}"
        ok "系统依赖安装完成"
    else
        ok "所有系统依赖已就绪"
    fi
else
    log "Step 1/5  跳过 apt (--skip-apt)"
    for cmd in meson ninja pkg-config; do
        if ! have "$cmd"; then
            err "缺少 $cmd, 但 --skip-apt 已指定"
            err "请手动安装: $cmd"
            exit 1
        fi
    done
    ok "meson / ninja / pkg-config 都在"
fi

# ----------------------------------------------------------------------------
# Step 2: Python 依赖检查
# ----------------------------------------------------------------------------
log "Step 2/5  Python 依赖检查"
log "  sbgb28181 是纯标准库实现, 不需要 pip install"
# 但顺便测一下我们用的库都在
python3 -c 'import http.server, urllib.parse, hashlib, socket, threading, json, re, subprocess' \
    && ok "标准库导入正常 (http.server, urllib.parse, hashlib, socket, ...)" \
    || { err "标准库导入失败 (Python 安装可能不完整)"; exit 1; }

# ----------------------------------------------------------------------------
# Step 3: 编译 gb28181sink
# ----------------------------------------------------------------------------
log "Step 3/5  编译 gb28181sink"
cd "$SINK_DIR"

if [[ $CLEAN -eq 1 ]] && [[ -d "$BUILD_DIR" ]]; then
    log "  --clean: 清理 $BUILD_DIR"
    rm -rf "$BUILD_DIR"
fi

# meson setup 幂等: 已存在 build/ 时它会报错, 我们捕获并重用
if [[ ! -f "$BUILD_DIR/build.ninja" ]]; then
    log "  meson setup build/"
    meson setup "$BUILD_DIR" --buildtype=release
else
    log "  复用现有 build/ 目录 (传 --clean 强制重新)"
fi

log "  meson compile"
meson compile -C "$BUILD_DIR"

# 编译产物位置
SO_FILE="$BUILD_DIR/libgstgb28181sink.so"
if [[ ! -f "$SO_FILE" ]]; then
    err "编译产物未找到: $SO_FILE"
    err "请检查上面的编译错误"
    exit 1
fi
ok "编译成功: $SO_FILE ($(du -h "$SO_FILE" | cut -f1))"

# ----------------------------------------------------------------------------
# Step 4: 安装 (可选)
# ----------------------------------------------------------------------------
log "Step 4/5  安装到 GStreamer 插件路径"

case "$INSTALL_MODE" in
    user)
        # 用户级: 保持 build/ 目录, 通过 GST_PLUGIN_PATH 加载
        log "  用户级安装: 不写系统目录, 通过环境变量加载"
        log "  把下面这行加到 ~/.bashrc (或临时 export):"
        log ""
        log "      export GST_PLUGIN_PATH=\"$BUILD_DIR:\${GST_PLUGIN_PATH:-}\""
        log ""
        # 立即给当前 shell 也 export 一下
        export GST_PLUGIN_PATH="$BUILD_DIR:${GST_PLUGIN_PATH:-}"
        ok "已为本进程设置 GST_PLUGIN_PATH=$BUILD_DIR"
        ;;
    system)
        SUDO=""
        if [[ $EUID -ne 0 ]]; then SUDO="sudo"; fi
        log "  系统级安装: meson install (写到 $(meson intro -C "$BUILD_DIR" 2>/dev/null | grep -i libdir | head -1 || echo '/usr/local/lib/<arch>/gstreamer-1.0'))"
        $SUDO meson install -C "$BUILD_DIR"
        ok "已安装到系统 GStreamer 插件目录"
        # 触发缓存重建, gst-inspect 才能立刻看到
        if have gst-inspect-1.0; then
            log "  重建 GStreamer 插件缓存..."
            $SUDO /usr/lib/x86_64-linux-gnu/gstreamer-1.0/gst-plugin-scanner \
                /usr/lib/x86_64-linux-gnu/gstreamer-1.0/gst-plugins-1.0.so \
                >/dev/null 2>&1 || true
        fi
        ;;
    none)
        log "  --no-install: 跳过安装, 插件留在 $BUILD_DIR/"
        export GST_PLUGIN_PATH="$BUILD_DIR:${GST_PLUGIN_PATH:-}"
        ok "已为本进程设置 GST_PLUGIN_PATH=$BUILD_DIR"
        ;;
esac

# ----------------------------------------------------------------------------
# Step 5: 验证
# ----------------------------------------------------------------------------
log "Step 5/5  验证安装"
if ! have gst-inspect-1.0; then
    warn "找不到 gst-inspect-1.0, 跳过验证"
    warn "手动验证:  export GST_PLUGIN_PATH=$BUILD_DIR && gst-inspect-1.0 gb28181sink"
else
    log "  跑 gst-inspect-1.0 gb28181sink ..."
    if GST_PLUGIN_PATH="$BUILD_DIR:${GST_PLUGIN_PATH:-}" \
       gst-inspect-1.0 gb28181sink >/dev/null 2>&1; then
        ok "✓ gst-inspect-1.0 gb28181sink 成功 (插件可被 GStreamer 加载)"
        # 显示插件的基本信息
        GST_PLUGIN_PATH="$BUILD_DIR:${GST_PLUGIN_PATH:-}" \
            gst-inspect-1.0 gb28181sink 2>&1 | \
            grep -E "Plugin Details|filename|version|Source pad|Sink pad" | head -8
    else
        err "插件虽然编译成功, 但 GStreamer 加载失败"
        err "调试: GST_PLUGIN_PATH=$BUILD_DIR gst-inspect-1.0 --gst-debug=3 gb28181sink 2>&1 | tail -30"
        exit 1
    fi
fi

# ----------------------------------------------------------------------------
# 收尾
# ----------------------------------------------------------------------------
echo
ok "============================================"
ok " ✓ sbgb28181 编译环境就绪, gb28181sink 已可用"
ok "============================================"
echo
log "下一步:"
log "  1) cd $REPO_ROOT"
log "  2) python3 gb28181_pusher.py \\"
log "       --server-ip <IP> --server-port <PORT> --server-id <ID> \\"
log "       --agent-id <ID> --agent-password <PWD> --channel-id <CID> \\"
log "       --source <URL> --verbose"
log ""
log "或启动 web 管理界面 (默认 127.0.0.1:8080):"
log "  python3 gb28181_pusher.py --web-host 0.0.0.0 --web-port 8080"
echo

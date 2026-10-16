"""tunneld_offload.py — 把 tunneld 裡「會卡住整個事件迴圈」的 Windows 網卡呼叫移到執行緒去做。

為什麼(2026-09-28,1.2.1 量測的實機證據,logs/tunneld.log):
  B 手機走完路 → tunneld 拆 B 的閒置隧道 → wintun 關閉網卡是同步呼叫,一次 10~18 秒;
  建立網卡 6~10 秒、查詢網卡清單(get_adapters)也會卡。這段期間 tunneld 唯一的事件迴圈完全不動,
  /hello 不通,正在走路的 A 手機的隧道資料也跟著停 → A 斷線(16:33:15),自癒後又斷,被快速斷線閘門停下。
  → 一支手機的網卡操作不可以拖住另一支手機的隧道。

做法(★不改 pymobiledevice3 的任何檔案★,都是啟動時掛上去的包裝;任何一步失敗就不掛、照原本的樣子跑):
  ① wintun 的 up() / close():改成回傳 awaitable,真正的呼叫丟到「網卡專用的單一執行緒」做。
     tunnel_service.py 呼叫這兩個時本來就會「是 awaitable 就 await」(為 userspace 網卡寫的),所以呼叫端一行不用改。
  ② 建立網卡(create_tun_device → TunTapDevice.__init__)呼叫端不會 await → 包住 RemotePairingTunnel.start_tunnel:
     先在專用執行緒建好網卡,再用 ContextVar 交給原本的 create_tun_device(同一個 task 才拿得到,兩支手機同時建不會拿錯)。
  ③ 查詢網卡清單:tunneld 每秒 1 次的 USB 偵測(get_ipv6_ips)、Wi-Fi 探索(bonjour 的 ifaddr.get_adapters)
     改成回上一次的結果、同時在背景執行緒更新(最多晚一輪)。

★網卡專用的單一執行緒★(1.2.2 debug):建立/啟用/關閉一次只做一個(順序同原本單一迴圈),而且不佔用 asyncio 共用的
  執行緒池 —— 那個池也是 Windows 隧道讀封包(async_read)用的;網卡呼叫萬一卡死,不會把正在走路那支的讀封包一起拖垮。
★取消時不漏關網卡★(1.2.2 debug,審查實測重現):
  · close 一旦送出就一定做完(shield;被取消的只是「等它」,不是它)。
  · start_tunnel 在建立/啟用中途被取消或失敗 → 把這次建好的網卡排進同一條執行緒關掉(一定排在建立/啟用之後)。

關掉:在 _tunneld.bat 加一行 set "TUNNELD_OFFLOAD=0"。
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import logging
import threading

log = logging.getLogger("tunneld_offload")

_EXEC = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="wintun")   # 網卡操作一次一個
_PRE_TUN: "contextvars.ContextVar" = contextvars.ContextVar("flowerbot_pre_tun", default=None)


def _in_exec(fn, *a, **k) -> "asyncio.Future":
    """丟到網卡專用執行緒,回 asyncio future(可 await)。"""
    return asyncio.wrap_future(_EXEC.submit(fn, *a, **k))


def _raw(fn):
    """拿掉本模組的包裝(不拿掉量測的計時包裝 —— 計時要量的就是真的呼叫)。"""
    return getattr(fn, "__wrapped__", fn) if getattr(fn, "__offloaded__", False) else fn


def _close_later(dev) -> None:
    """把網卡的關閉排進專用執行緒(不等它;排在任何已送出的建立/啟用之後)。"""
    close = _raw(getattr(type(dev), "close", None))
    if close is None:
        return

    def run():
        try:
            close(dev)
        except Exception as e:                       # noqa: BLE001 — 清理失敗只記錄
            log.warning(f"[offload] 清理沒用到的網卡失敗:{e}")
    _EXEC.submit(run)


def offload_methods(cls, names=("up", "close")) -> list:
    """cls 的這些方法改成回傳 awaitable(在網卡專用執行緒跑原本的函式);已掛過就不重掛。回掛上的名字。"""
    done = []
    for name in names:
        orig = getattr(cls, name)
        if getattr(orig, "__offloaded__", False):
            continue

        def make(orig=orig, name=name):
            def wrapper(self, *a, **k):
                fut = _in_exec(orig, self, *a, **k)
                # close:一旦送出就一定做完 —— 取消的只是「等它」,網卡照樣會被關(不漏關)
                return asyncio.shield(fut) if name == "close" else fut
            wrapper.__offloaded__ = True
            wrapper.__wrapped__ = orig
            return wrapper
        setattr(cls, name, make())
        done.append(name)
    return done


def offload_create(ts) -> bool:
    """ts = pymobiledevice3.remote.tunnel_service 模組。包住 RemotePairingTunnel.start_tunnel 與 create_tun_device。"""
    base = ts.RemotePairingTunnel
    if getattr(base.start_tunnel, "__offloaded__", False):
        return False
    orig_start = base.start_tunnel
    orig_create = ts.create_tun_device
    default_name = ts.DEFAULT_INTERFACE_NAME

    def create_tun_device(interface_name=default_name):
        pre = _PRE_TUN.get()
        if pre is not None:
            _PRE_TUN.set(None)                       # 用過就清掉,同一個 task 下次要重新建
            return pre
        return orig_create(interface_name)
    create_tun_device.__wrapped__ = orig_create

    async def start_tunnel(self, address, mtu, interface_name=default_name):
        fut = _in_exec(orig_create, interface_name)
        try:
            dev = await asyncio.shield(fut)
        except BaseException:
            # 等建立的時候被取消/出錯:執行緒還是會把網卡建完 → 建完就關掉,不留沒人管的網卡
            def cleanup(f):
                if not f.cancelled() and f.exception() is None:
                    _close_later(f.result())
            fut.add_done_callback(cleanup)
            raise
        token = _PRE_TUN.set(dev)
        try:
            return await orig_start(self, address, mtu, interface_name=interface_name)
        except BaseException:
            # 啟用中途被取消,或原本的 start_tunnel 出錯:上層(start_tcp_tunnel 只接 Exception)不一定會 stop_tunnel
            # → 這次建的網卡在這裡關掉(排在啟用之後),self.tun 清掉
            if getattr(self, "tun", None) is dev:
                self.tun = None
            _close_later(dev)
            raise
        finally:
            _PRE_TUN.reset(token)
    start_tunnel.__offloaded__ = True
    start_tunnel.__wrapped__ = orig_start
    ts.create_tun_device = create_tun_device
    base.start_tunnel = start_tunnel
    return True


def cache_in_background(obj, name: str) -> bool:
    """obj.name() 改成:第一次照常(同步)取,之後回上一次的結果、同時在背景執行緒更新。更新失敗保留舊值。
    obj 可以是物件(取到綁定方法)或模組(取到函式)。★有帶參數的呼叫照原本的方式跑★(快取只管無參數那種)。"""
    orig = getattr(obj, name)
    if getattr(orig, "__offloaded__", False):
        return False
    state = {"val": None, "have": False, "busy": False}
    lock = threading.Lock()

    def refresh():
        try:
            v = orig()
            with lock:
                state["val"], state["have"] = v, True
        except Exception as e:                       # noqa: BLE001 — 背景更新失敗:留舊值,下次再試
            log.info(f"[offload] 背景更新 {name} 失敗(沿用上一次):{e}")
        finally:
            with lock:
                state["busy"] = False

    def cached(*a, **k):
        if a or k:
            return orig(*a, **k)
        with lock:
            have, busy = state["have"], state["busy"]
            if have and not busy:
                state["busy"] = True
        if not have:
            refresh()                                 # 第一次:同步取(啟動時)
            with lock:
                return list(state["val"] or [])
        if not busy:
            threading.Thread(target=refresh, name=f"offload-{name}", daemon=True).start()
        with lock:
            return list(state["val"] or [])
    cached.__offloaded__ = True
    cached.__wrapped__ = orig
    setattr(obj, name, cached)
    return True


def install_all() -> list:
    """實際掛到 tunneld 上(Windows)。每一項各自 try:掛不上就記一行、那一項照原本的樣子。回掛上的項目名稱。"""
    done = []
    try:
        from pytun_pmd3 import wintun as W
        done += [f"wintun.{n}" for n in offload_methods(W.TunTapDevice)]
    except Exception as e:                           # noqa: BLE001
        log.warning(f"[offload] wintun 啟用/關閉 沒掛上({type(e).__name__}: {e}),照原本的方式")
    try:
        from pymobiledevice3.remote import tunnel_service as TS
        if offload_create(TS):
            done.append("建立網卡")
    except Exception as e:                           # noqa: BLE001
        log.warning(f"[offload] 建立網卡 沒掛上({type(e).__name__}: {e}),照原本的方式")
    try:
        # OsUtils.create() 是全程共用的單例(tunneld/server.py 的 OSUTILS 就是它)→ 掛在這個物件上(綁定方法)
        from pymobiledevice3.osu.os_utils import get_os_utils
        if cache_in_background(get_os_utils(), "get_ipv6_ips"):
            done.append("網卡清單(USB 偵測)")
    except Exception as e:                           # noqa: BLE001
        log.warning(f"[offload] 網卡清單(USB 偵測)沒掛上({type(e).__name__}: {e}),照原本的方式")
    try:
        # Wi-Fi 探索(bonjour,每 ~9 秒)也會同步查網卡清單;它是 `import ifaddr` 後呼叫 ifaddr.get_adapters()
        import ifaddr
        if cache_in_background(ifaddr, "get_adapters"):
            done.append("網卡清單(Wi-Fi 探索)")
    except Exception as e:                           # noqa: BLE001
        log.warning(f"[offload] 網卡清單(Wi-Fi 探索)沒掛上({type(e).__name__}: {e}),照原本的方式")
    return done

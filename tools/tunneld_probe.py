"""tunneld_probe.py — 用「量測模式」啟動 pymobiledevice3 tunneld。★量測本身只記錄、不改任何行為★。

(1.2.2 起這支也負責掛上 tunneld_offload.py 的連線修補 —— 那一項會改行為:網卡操作移到執行緒,不卡事件迴圈。
 兩者分開開關:量測 = TUNNELD_PROBE,修補 = TUNNELD_OFFLOAD(在 _tunneld.bat 加 set "TUNNELD_OFFLOAD=0" 就關掉)。)

為什麼(2026-09-28 連線穩定研究,第 3 項第 1 步):日誌顯示兩支手機會互相拖累 —— 一支的 Wi-Fi 隧道
拆掉或重建時,tunneld 整個沒回應 14~165 秒,另一支正在走路的手機就斷了。讀 9.36.0 原始碼的推論是:
建/拆 Wi-Fi 虛擬網卡(wintun 的 WintunCreateAdapter / up / WintunCloseAdapter)是同步呼叫,
直接卡在 tunneld 唯一的事件迴圈上。這支程式在不改行為的前提下量兩件事,寫進 logs/tunneld.log:
  ① 每次建立 / 啟用 / 關閉虛擬網卡花了幾秒        → 行首 [probe] wintun
  ② 事件迴圈被卡住超過 STALL_S 秒時,卡在哪一行     → 行首 [probe] 事件迴圈卡住

★為什麼不用 PYTHONASYNCIODEBUG★:pymobiledevice3 的 __main__ 把 "asyncio" 這個 logger 設成
CRITICAL+1(__main__.py:69),慢回呼警告會被整個吞掉 —— 量了等於沒量,空白的日誌還會被誤讀成「沒卡住」。

★鐵則:量測壞掉也絕不可以讓 tunneld 起不來★(起不來 = 兩支手機都不能走路):
掛量測的每一步都包在 try 裡,失敗就記一行、照常啟動原本的 tunneld。
拿掉量測:_tunneld.bat 裡 set "TUNNELD_PROBE=0"(或刪掉這支檔),回到原本的啟動方式。

用法(_tunneld.bat 會這樣叫;參數原封不動交給 pymobiledevice3):
    python tools\\tunneld_probe.py remote tunneld [其他參數]
"""
from __future__ import annotations

import logging
import os
import sys
import threading
import time

STALL_S = 1.0             # 事件迴圈連續忙超過幾秒才記(正常的單次回呼是毫秒級)
SAMPLE_S = 0.2            # 每隔幾秒看一次主執行緒在做什麼
STACK_DEPTH = 6           # 記錄卡住位置時列出最內層幾層

log = logging.getLogger("tunneld_probe")

# 事件迴圈「閒著等事件」時,主執行緒最內層會停在這幾個地方(selector 或 proactor 兩種迴圈都涵蓋)
_IDLE = {("selectors.py", "select"), ("selectors.py", "_select"), ("windows_events.py", "_poll"),
         ("windows_events.py", "select")}


def frame_key(filename: str, func: str) -> "tuple[str, str]":
    return os.path.basename(str(filename)), str(func)


def is_idle(stack: "list[tuple[str, int, str]]") -> bool:
    """stack = 由內到外的 [(檔名, 行號, 函式)]。最內兩層有任何一層是「等事件」就算閒著。純函式。"""
    return any(frame_key(f, fn) in _IDLE for f, _, fn in stack[:2])


def describe(stack: "list[tuple[str, int, str]]", depth: int = STACK_DEPTH) -> str:
    """最內 depth 層 → 「wintun.py:279 __init__ ← tunnel_service.py:170 create_tun_device ← …」。純函式。"""
    return " ← ".join(f"{os.path.basename(f)}:{ln} {fn}" for f, ln, fn in stack[:depth])


class StallMeter:
    """把一連串「(時間, 閒著嗎, 堆疊, 迴圈輪數)」取樣變成「卡住 N 秒、卡在哪、是哪一種」的事件。純邏輯(selftest 直測)。

    卡住的位置取「忙到剛好超過一半門檻時」那次取樣 —— 太早可能只是正常回呼的尾巴,
    太晚(結束前最後一次)可能已經走到下一件事。

    ★分辨兩種「忙」★(1.2 審查:只看取樣分不出來):
      · 迴圈這段期間幾乎沒轉(≤ 2 輪)= 單一個同步呼叫卡住整個迴圈 —— 這次要驗證的就是這種(wintun)。
      · 迴圈一直在轉(很多輪)= 滿載:一堆短工作接連跑,不是某一個呼叫卡住。
    ★1.2.2 修正(實機資料抓到):只看「整段總輪數」會誤判★ —— 卡住 10 秒的前後常有一段短工作,
    總輪數被灌到幾百,就被說成「滿載」,但每次取樣其實都停在 wintun close。所以另外記
    「最長一段迴圈完全沒轉」的秒數與位置;這段夠長(≥ 門檻)就判成單一個同步呼叫。
    回 (秒數, 位置, 輪數或 None, 最長沒轉秒數, 那段的位置);輪數 None = 沒有提供輪數(分不出來)。"""

    def __init__(self, stall_s: float = STALL_S):
        self.stall_s = stall_s
        self.busy_since: "float | None" = None
        self.where = ""
        self.ticks0: "int | None" = None
        self._reset_frozen()

    def _reset_frozen(self):
        self.prev_t: "float | None" = None
        self.prev_ticks: "int | None" = None
        self.frozen_since: "float | None" = None
        self.frozen_where = ""
        self.max_frozen, self.max_frozen_where = 0.0, ""

    def feed(self, now: float, idle: bool, stack, ticks: "int | None" = None):
        if idle:
            if self.busy_since is None:
                return None
            dur, where = now - self.busy_since, self.where
            turns = (ticks - self.ticks0) if (ticks is not None and self.ticks0 is not None) else None
            frozen, fwhere = self.max_frozen, self.max_frozen_where
            self.busy_since, self.where, self.ticks0 = None, "", None
            self._reset_frozen()
            return (dur, where, turns, frozen, fwhere) if dur >= self.stall_s else None
        if self.busy_since is None:
            self.busy_since, self.where, self.ticks0 = now, describe(stack), ticks
        elif not self.where or now - self.busy_since <= self.stall_s / 2:
            self.where = describe(stack)
        if ticks is not None and self.prev_ticks is not None:
            if ticks == self.prev_ticks:                       # 從上一次取樣到現在,迴圈一輪都沒轉
                if self.frozen_since is None:
                    self.frozen_since, self.frozen_where = self.prev_t, describe(stack)
                if now - self.frozen_since > self.max_frozen:
                    self.max_frozen, self.max_frozen_where = now - self.frozen_since, self.frozen_where
            else:
                self.frozen_since = None
        self.prev_t, self.prev_ticks = now, ticks
        return None


def stall_line(ev) -> str:
    """StallMeter 的事件 → 寫進日誌的一句話。純函式。"""
    dur, where, turns = ev[:3]
    frozen, fwhere = (ev[3], ev[4]) if len(ev) >= 5 else (0.0, "")
    if frozen >= STALL_S:
        return (f"[probe] 事件迴圈卡住 {frozen:.1f} 秒(迴圈完全沒轉:單一個同步呼叫),卡在:{fwhere}"
                f"(整段忙碌 {dur:.1f} 秒)")
    if turns is None:
        kind = "事件迴圈忙碌"
    elif turns <= 2:
        kind = "事件迴圈卡住(迴圈沒有轉:單一個同步呼叫)"
    else:
        kind = f"事件迴圈滿載(期間轉了 {turns} 輪:很多短工作接連跑,不是單一呼叫卡住)"
    return f"[probe] {kind} {dur:.1f} 秒,卡在:{where}"


_TURNS = [0]                  # 事件迴圈轉了幾輪(install_loop_counter 掛上後才會動)


def install_loop_counter() -> bool:
    """在 asyncio 的 BaseEventLoop._run_once 前面加一個計數(每輪 +1,不改任何行為)。
    selector / proactor 兩種迴圈都繼承這個方法。要在迴圈開始跑之前掛。"""
    import asyncio.base_events as BE
    orig = BE.BaseEventLoop._run_once
    if getattr(orig, "__wrapped__", None):
        return True

    def counted(self):
        _TURNS[0] += 1
        return orig(self)
    counted.__wrapped__ = orig
    BE.BaseEventLoop._run_once = counted
    return True


def _stack_of(thread_id: int) -> "list[tuple[str, int, str]]":
    f = sys._current_frames().get(thread_id)
    out = []
    while f is not None and len(out) < 40:
        out.append((f.f_code.co_filename, f.f_lineno, f.f_code.co_name))
        f = f.f_back
    return out


def start_sampler(main_id: int, meter: "StallMeter | None" = None, sample_s: float = SAMPLE_S) -> threading.Thread:
    """背景執行緒:定時看主執行緒的堆疊,事件迴圈卡住超過門檻就記一行。daemon,tunneld 結束就跟著結束。"""
    meter = meter or StallMeter()

    def run():
        while True:
            try:
                stack = _stack_of(main_id)
                if stack:
                    ev = meter.feed(time.monotonic(), is_idle(stack), stack, _TURNS[0] or None)
                    if ev:
                        log.warning(stall_line(ev))
            except Exception as e:                     # noqa: BLE001 — 量測本身不能出事
                log.info(f"[probe] 取樣出錯(略過):{e}")
            time.sleep(sample_s)
    t = threading.Thread(target=run, name="tunneld-probe", daemon=True)
    t.start()
    return t


def _timed(fn, label: str):
    def wrapper(*a, **k):
        t0 = time.perf_counter()
        try:
            return fn(*a, **k)
        finally:
            dt = time.perf_counter() - t0
            (log.warning if dt >= STALL_S else log.info)(f"[probe] wintun {label} 花了 {dt:.2f} 秒")
    wrapper.__wrapped__ = fn
    return wrapper


def install_wintun_timers() -> bool:
    """在 Windows 的 wintun 網卡類別上掛計時(建立 / 啟用 / 關閉)。不改參數、不改回傳、例外照丟。"""
    from pytun_pmd3 import wintun as W
    cls = W.TunTapDevice
    for name, label in (("__init__", "建立網卡"), ("up", "啟用網卡"), ("close", "關閉網卡")):
        fn = getattr(cls, name)
        if not getattr(fn, "__wrapped__", None):
            setattr(cls, name, _timed(fn, label))
    return True


class AccessNoise(logging.Filter):
    """1.2.72T9(使用者 10-15:要能傳記錄回來判斷問題):tunneld.log 有 99% 是每秒一次的例行查詢 ——
    「GET /」(隧道清單:網頁後端 / 控制台每秒問一次)與「GET /hello」(健康檢查)。成功的那些不寫;
    /start-tunnel、/cancel、失敗的查詢照寫。★只決定寫不寫,不改任何行為★。純邏輯(selftest 直測)。"""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            a = record.args
            # uvicorn 的存取紀錄:(client_addr, method, full_path, http_version, status_code)
            if isinstance(a, tuple) and len(a) >= 5 and a[1] == "GET" and a[2] in ("/", "/hello") \
                    and int(a[4]) == 200:
                return False
        except Exception:  # noqa: BLE001 — 看不懂的照寫
            pass
        return True


def stamp_formats(config: dict) -> bool:
    """uvicorn 的日誌格式前面加上時間(以前「INFO: … /start-tunnel …」那幾行沒有時間,對不上是幾點發生的)。
    改的是傳進來的那份設定(uvicorn.config.LOGGING_CONFIG;uvicorn.run 沒給 log_config 時就用它)。純函式。"""
    done = False
    for key in ("default", "access"):
        f = (config.get("formatters") or {}).get(key)
        if isinstance(f, dict) and isinstance(f.get("fmt"), str) and "%(asctime)s" not in f["fmt"]:
            f["fmt"] = "%(asctime)s " + f["fmt"]
            f["datefmt"] = "%Y-%m-%d %H:%M:%S"
            done = True
    return done


def quiet_access_log() -> str:
    """掛上 AccessNoise + 時間格式。★失敗就照原樣跑★(回說明字串給日誌)。"""
    import uvicorn.config as UC
    stamped = stamp_formats(UC.LOGGING_CONFIG)
    logging.getLogger("uvicorn.access").addFilter(AccessNoise())
    return "時間格式" + ("已加" if stamped else "本來就有") + "、每秒的例行查詢(GET / 與 /hello 成功)不寫"


def main() -> None:
    # 參數原封不動交給 pymobiledevice3(argv[0] 換成它的名字,說明文字才正確)
    sys.argv = ["pymobiledevice3"] + sys.argv[1:]
    import pymobiledevice3.__main__ as M               # 這一行會設定好 logging(coloredlogs,INFO)
    log.setLevel(logging.INFO)
    # ⓪ 1.2.72T9:記錄瘦身(只改「寫不寫、格式」,不改 tunneld 的任何行為;失敗照原樣跑)
    try:
        log.info(f"[log] {quiet_access_log()}")
    except Exception as e:                             # noqa: BLE001
        log.warning(f"[log] 記錄瘦身沒掛上({type(e).__name__}: {e}),照原樣記")
    # ① 量測(只記錄)
    try:
        if sys.platform == "win32":
            install_wintun_timers()
        install_loop_counter()
        start_sampler(threading.get_ident())
        log.info(f"[probe] 量測已掛上:記錄 wintun 建立/啟用/關閉的秒數,與事件迴圈卡住超過 {STALL_S:.0f} 秒的位置"
                 "(量測只記錄,不改行為)")
    except Exception as e:                             # noqa: BLE001 — ★量測失敗也要照常啟動 tunneld★
        log.warning(f"[probe] 量測沒掛上({type(e).__name__}: {e}),照常啟動 tunneld")
    # ② 連線修補(會改行為:網卡操作移到執行緒)—— 與量測分開 try,一個失敗不拖另一個;要在計時之後掛,
    #   計時量的才是執行緒裡真的花的時間
    # ★1.2.4 預設關閉★:09-30 實機(1.2.3 那 6 分鐘)這個修補沒有解決中斷,還多了一個新卡點 —— 迴圈凍在
    #   cache_in_background 開新執行緒那一行(4~17 秒)。原因未查明(推測 Windows 建執行緒時被網卡驅動作業卡住),
    #   在本機做過實驗證明有效之前不預設掛上;要試:_tunneld.bat 加一行 set "TUNNELD_OFFLOAD=1"。
    if sys.platform == "win32" and os.environ.get("TUNNELD_OFFLOAD", "0") == "1":
        try:
            # ★1.2.2 debug(擋發版):可攜版/安裝版的 python 是隔離模式(._pth),不會把腳本所在資料夾加進 sys.path
            #   → 不補這行,朋友版 import 會失敗、修補安靜地沒掛上(control_panel / setup_gui / launcher 都有同樣這行)
            here = os.path.dirname(os.path.abspath(__file__))
            if here not in sys.path:
                sys.path.insert(0, here)
            import tunneld_offload
            done = tunneld_offload.install_all()
            log.info(f"[offload] 網卡操作改在執行緒做(不卡事件迴圈):{'、'.join(done) or '沒有項目掛上'}"
                     "(關掉:在 _tunneld.bat 加一行 set \"TUNNELD_OFFLOAD=0\")")
        except Exception as e:                         # noqa: BLE001 — 掛不上就照原本的方式跑
            log.warning(f"[offload] 沒掛上({type(e).__name__}: {e}),照原本的方式")
    elif sys.platform == "win32":
        log.info("[offload] 連線修補預設關閉(1.2.4;要試:在 _tunneld.bat 加一行 set \"TUNNELD_OFFLOAD=1\")")
    # ③ 按需模式的守門(1.2.5):同一支手機只建一條隧道 —— 1.2.4 第一次上線就踩到 /start-tunnel 對重複的探索結果
    #   開兩個任務、第二個把第一條踢出清單。預設開;關掉:set "TUNNELD_ONDEMAND_GUARD=0"。
    if os.environ.get("TUNNELD_ONDEMAND_GUARD", "1") != "0":
        try:
            here = os.path.dirname(os.path.abspath(__file__))
            if here not in sys.path:
                sys.path.insert(0, here)
            import tunneld_ondemand
            done = tunneld_ondemand.install_all()
            log.info(f"[ondemand] 同一支手機只建一條隧道:{'、'.join(done) or '沒有項目掛上'}"
                     "(關掉:在 _tunneld.bat 加一行 set \"TUNNELD_ONDEMAND_GUARD=0\")")
        except Exception as e:                         # noqa: BLE001 — 掛不上就照原本的方式跑
            log.warning(f"[ondemand] 沒掛上({type(e).__name__}: {e}),照原本的方式")
    M.main()


if __name__ == "__main__":
    main()

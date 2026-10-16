"""GPL 那一側的測試(會在同一個行程裡載入 pymobiledevice3)—— selftest 另開一個行程跑這支。

Copyright (C) 2026 種花小幫手作者
SPDX-License-Identifier: GPL-3.0-or-later(授權全文見 tools/pmd_bridge.py 開頭與 LICENSE)

★Route B(使用者 10-16 裁決)★:主程式和 selftest 行程裡都不可以載入 pymobiledevice3(selftest 最後有一格檢查)。
需要真的 pymobiledevice3 的測試都放這裡:這支只 import 標準庫、pymobiledevice3 和 GPL 那一側的工具
(tools/tunneld_offload.py、tunneld_ondemand.py、tunneld_probe.py、pmd_bridge.py),★絕不 import src/ 或 tests.selftest★。

    python -I tests/gpl_side.py        → 每格印一行 [OK ] / [FAIL];全過回 0
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import sys
import traceback
from pathlib import Path

_FAILS: "list[str]" = []


def check(name: str, cond: bool, extra: str = "") -> None:
    print(("[OK ] " if cond else "[FAIL] ") + name + (f"  {extra}" if extra else ""), flush=True)
    if not cond:
        _FAILS.append(name)


def test_bridge_contract():
    """橋接程式(tools/pmd_bridge.py)依賴的 pymobiledevice3 介面還在、而且長得跟它以為的一樣。

    升級 pymobiledevice3 時這格先紅,就不會等到真機才發現「名字改了、結果碼對不上」:
      ・開發者模式沒開的例外 → 結果碼 devmode_off(網頁靠它講白話);逾時 → timeout;其他 → error
      ・它用到的類別 / 函式都在,參數順序對(RSD(address, name=…)、LocationSimulation.set(lat, lon))"""
    import inspect
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root / "tools"))
    import pmd_bridge as B
    from pymobiledevice3.exceptions import DeveloperModeIsNotEnabledError
    from pymobiledevice3.remote.remote_service_discovery import RemoteServiceDiscoveryService
    from pymobiledevice3.services.dvt.instruments.dvt_provider import DvtProvider
    from pymobiledevice3.services.dvt.instruments.location_simulation import LocationSimulation
    from pymobiledevice3.services.mobile_image_mounter import AlreadyMountedError, auto_mount
    res = {}
    res["開發者模式沒開 → devmode_off"] = B._code_of(DeveloperModeIsNotEnabledError()) == "devmode_off"
    res["逾時 → timeout"] = B._code_of(asyncio.TimeoutError()) == "timeout"
    res["其他 → error"] = B._code_of(RuntimeError("x")) == "error" and B._code_of(AlreadyMountedError()) == "error"
    rsd_params = list(inspect.signature(RemoteServiceDiscoveryService.__init__).parameters)
    res["RSD(address, name=…)"] = rsd_params[:3] == ["self", "address", "name"]
    res["LocationSimulation.set(lat, lon) / clear()"] = (
        list(inspect.signature(LocationSimulation.set).parameters)[1:3] == ["latitude", "longitude"]
        and inspect.iscoroutinefunction(LocationSimulation.set) and inspect.iscoroutinefunction(LocationSimulation.clear))
    res["DvtProvider / LocationSimulation 是 async with 用的"] = all(
        hasattr(c, "__aenter__") and hasattr(c, "__aexit__") for c in (DvtProvider, LocationSimulation))
    res["auto_mount 是 async"] = inspect.iscoroutinefunction(auto_mount)
    bad = [k for k, v in res.items() if not v]
    check("Route B 橋接程式依賴的 pymobiledevice3 介面還在(結果碼對得上、參數順序對)", not bad, f"沒過={bad}")


async def test_tunneld_offload():
    """★1.2.2 連線修補(tools/tunneld_offload.py)★:網卡的建立/啟用/關閉、網卡清單,不可以卡住 tunneld 的事件迴圈。

    實機證據(09-28 16:33):B 手機的網卡關閉一次 10~18 秒卡住整個迴圈 → 正在走的 A 跟著斷。
    這格用★真的★ pymobiledevice3 9.36 的 RemotePairingTunnel.start_tunnel / stop_tunnel(呼叫方式原封不動),
    配一張「建立/啟用/關閉 各卡 0.5 秒」的假網卡(絕不碰真的 wintun),量同一迴圈上心跳的最長間隔:
      掛修補前 ≥ 0.45 秒(= 真的卡住,證明測試抓得到)、掛修補後 < 0.2 秒。
    也釘住:兩支手機同時開隧道各拿到自己的網卡、網卡操作一次只做一個、建好卻沒被用到的網卡會被關掉、
    網卡清單第二次起立刻回上一次的結果並在背景更新、更新失敗保留舊值。測完把所有掛上的東西還原。
    """
    import sys as _s
    import threading as _th
    import time as _t
    from pathlib import Path
    _s.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
    import tunneld_offload as O
    from pymobiledevice3.remote import tunnel_service as TS

    state = {"now": 0, "max": 0, "made": [], "closed": 0}
    lock = _th.Lock()

    def busy(sec):
        with lock:
            state["now"] += 1
            state["max"] = max(state["max"], state["now"])
        _t.sleep(sec)
        with lock:
            state["now"] -= 1

    class FakeTun:
        def __init__(self, name="x"):
            busy(0.5); state["made"].append(self); self.addr = None; self.mtu = 1280
        def up(self):
            busy(0.5)
        def close(self):
            busy(0.5); state["closed"] += 1
        async def async_read(self):
            await asyncio.sleep(3600)

    class Tunnel(TS.RemotePairingTunnel):
        async def send_packet_to_device(self, packet): pass
        async def request_tunnel_establish(self): return {}
        async def wait_closed(self): pass

    async def max_gap(coro):
        gaps, last, done = [0.0], [_t.perf_counter()], [False]
        async def beat():
            while not done[0]:
                await asyncio.sleep(0.02)
                now = _t.perf_counter(); gaps.append(now - last[0]); last[0] = now
        hb = asyncio.create_task(beat())
        await asyncio.sleep(0.05)
        try:
            await coro
        finally:
            done[0] = True
            await hb
        return max(gaps)

    async def open_close(tn):
        await tn.start_tunnel("fd00::1", 1280)
        await tn.stop_tunnel()

    orig_start, orig_create = TS.RemotePairingTunnel.start_tunnel, TS.create_tun_device
    orig_up, orig_close = FakeTun.up, FakeTun.close
    TS.create_tun_device = lambda interface_name=TS.DEFAULT_INTERFACE_NAME: FakeTun(interface_name)
    try:
        before = await max_gap(open_close(Tunnel()))                 # 沒掛修補:真的會卡
        O.offload_methods(FakeTun); O.offload_create(TS)
        after = await max_gap(open_close(Tunnel()))                  # 掛上:迴圈照轉
        closed_ok = state["closed"] == 2 and all(t is not None for t in state["made"])
        # 兩支同時開:各拿各的網卡、網卡操作一次只做一個
        state["max"] = 0
        a, b = Tunnel(), Tunnel()
        await asyncio.gather(a.start_tunnel("fd00::a", 1280), b.start_tunnel("fd00::b", 1280))
        own_ok = a.tun is not None and b.tun is not None and a.tun is not b.tun
        serial_ok = state["max"] == 1
        await asyncio.gather(a.stop_tunnel(), b.stop_tunnel())
        # ★1.2.2 debug:取消落在「建立網卡」或「啟用網卡」的中途,網卡不可以漏關★
        #   (原本全同步、中間不會被取消插隊;移到執行緒後中間有 await —— USB 拔線、/clear_tunnels、關 tunneld 都會取消)
        cancel_rows = []

        async def drained():
            """等網卡專用執行緒把排隊的工作做完(它是單一執行緒、照順序做 → 送一個空工作,它跑到就代表前面都做完了)。
            送兩次:第一個空工作跑完時,asyncio 那邊可能才剛把「清理」排進去。"""
            for _ in range(2):
                await asyncio.wrap_future(O._EXEC.submit(lambda: None))
                await asyncio.sleep(0.05)

        async def until(cond, limit=6.0):
            """★1.2.3 打包關卡抓到的隨機失敗★:以前固定 sleep 1.6/0.8 秒,出貨版的 Python(剛啟動、還在編譯)偶爾比較慢
            → 關閉還沒做完就數 → 紅(開發環境 5 次、出貨版 5 次裡 1 次)。改成「等到做到為止,最多 limit 秒」再排空佇列 ——
            真的漏關的話照樣等不到、照樣紅,只是不再受機器快慢影響。"""
            t0 = _t.monotonic()
            while not cond() and _t.monotonic() - t0 < limit:
                await asyncio.sleep(0.05)
            await drained()
        for when in (0.2, 0.75):                          # 0.2 秒=建立中;0.75 秒=建立完、啟用中
            await drained()                               # 上一段的關閉(排在執行緒裡)先做完,才開始數
            made0, closed0 = len(state["made"]), state["closed"]
            tn = Tunnel()
            task = asyncio.create_task(tn.start_tunnel("fd00::d", 1280))
            await asyncio.sleep(when)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await until(lambda: state["closed"] - closed0 >= 1)   # 讓執行緒裡的建立/啟用/清理跑完
            cancel_rows.append((len(state["made"]) - made0, state["closed"] - closed0))
        cancel_ok = all(m == 1 and c == 1 for m, c in cancel_rows)
        # 原本的 start_tunnel 在用到網卡之前就失敗 → 預先建好的網卡要被關掉
        class Boom(Tunnel):
            pass
        await drained()                                   # 取消那兩段的關閉都做完了,才開始數
        n_closed = state["closed"]
        async def fail_first(self, address, mtu, interface_name=TS.DEFAULT_INTERFACE_NAME):
            raise RuntimeError("在建網卡之前就失敗")
        # 換成「一開始就失敗」的 start_tunnel 再掛一次修補
        TS.create_tun_device = lambda interface_name=TS.DEFAULT_INTERFACE_NAME: FakeTun(interface_name)
        TS.RemotePairingTunnel.start_tunnel = fail_first
        O.offload_create(TS)
        try:
            await Boom().start_tunnel("fd00::c", 1280)
            leak_ok = False
        except RuntimeError:
            await until(lambda: state["closed"] >= n_closed + 1)   # 關閉是排進網卡執行緒、不等它 → 等它做完再數
            leak_ok = state["closed"] == n_closed + 1              # 剛好一次(不可以漏,也不可以重複關)
    finally:
        TS.RemotePairingTunnel.start_tunnel, TS.create_tun_device = orig_start, orig_create
        FakeTun.up, FakeTun.close = orig_up, orig_close

    # 網卡清單:第一次同步取;之後立刻回舊值、背景更新;更新失敗保留舊值
    class OsU:
        def __init__(self): self.n = 0; self.fail = False
        def get_ipv6_ips(self):
            _t.sleep(0.3); self.n += 1
            if self.fail:
                raise OSError("adapters busy")
            return [f"fe80::{self.n}"]
    u = OsU()
    O.cache_in_background(u, "get_ipv6_ips")
    first = u.get_ipv6_ips()
    t0 = _t.perf_counter(); second = u.get_ipv6_ips(); quick = _t.perf_counter() - t0
    await asyncio.sleep(0.5)
    third = u.get_ipv6_ips()
    u.fail = True
    await asyncio.sleep(0.5)
    fourth = u.get_ipv6_ips()
    cache_ok = (first == ["fe80::1"] and second == ["fe80::1"] and quick < 0.15
                and third == ["fe80::2"] and fourth in (["fe80::2"], ["fe80::3"]))

    # ★1.2.2 debug(擋發版):可攜版/安裝版的 python 是隔離模式(._pth),不會把腳本所在資料夾加進 sys.path ——
    #   舊版 tunneld_probe 直接 import tunneld_offload → 朋友版安靜地沒掛上。這格用 -I(隔離)真的跑包裝程式,
    #   不需管理員:tunneld 會停在權限檢查,但修補要先掛上。(上面為了直測先 insert 了 tools,不能拿來證明這件事。)
    import locale as _lc
    import subprocess as _sp
    root = Path(__file__).resolve().parents[1]

    def child_text(cp) -> str:
        # ★照子程式實際用的編碼解,不假設 UTF-8★(1.2.72T6 打包關卡擋下):子程式是隔離模式(-I,跟安裝版一樣),
        #   PYTHONUTF8 / PYTHONIOENCODING 都不看,輸出導向時用系統的 ANSI 字碼頁 —— 開「UTF-8 Beta」是 65001,
        #   一般台灣電腦是 950。這台 10-08 重開機後變回 950,照舊用 UTF-8 解,中文的掛上訊息全對不到、整格紅。
        raw = (cp.stdout or b"") + (cp.stderr or b"")
        for enc in (_lc.getencoding(), "utf-8"):
            try:
                return raw.decode(enc)
            except (UnicodeDecodeError, LookupError):
                pass
        return raw.decode("utf-8", errors="replace")

    if _s.platform == "win32":
        base_env = {k: v for k, v in os.environ.items() if k != "TUNNELD_OFFLOAD"}
        pr = _sp.run([_s.executable, "-I", str(root / "tools" / "tunneld_probe.py"), "remote", "tunneld"],
                     capture_output=True, timeout=120, cwd=str(root), env={**base_env, "TUNNELD_OFFLOAD": "1"})
        outp = child_text(pr)
        isolated_ok = "網卡操作改在執行緒做" in outp and "沒掛上" not in outp and "建立網卡" in outp
        # ★1.2.4:預設不掛★(實機證明沒效還多一個卡點)—— 不帶環境變數就要看到「預設關閉」,而且真的沒掛上
        pr2 = _sp.run([_s.executable, "-I", str(root / "tools" / "tunneld_probe.py"), "remote", "tunneld"],
                      capture_output=True, timeout=120, cwd=str(root), env=base_env)
        outp2 = child_text(pr2)
        isolated_ok = isolated_ok and "預設關閉" in outp2 and "改在執行緒做" not in outp2 and "量測已掛上" in outp2
        # 1.2.5:按需守門要在隔離模式下真的掛上(探索去重 + start-tunnel 排隊)
        isolated_ok = isolated_ok and "[ondemand] 同一支手機只建一條隧道:探索去重、start-tunnel 排隊" in outp2 and "[ondemand] 沒掛上" not in outp2
    else:
        isolated_ok = True
    # 門檻用「相對」:電腦很忙(例如同時在跑別的測試)時心跳本身會變慢,絕對 0.2 秒會誤判(審查實際遇到)
    ok = (before >= 0.45 and after < min(0.3, before / 3) and closed_ok and own_ok and serial_ok and leak_ok and cache_ok
          and isolated_ok and cancel_ok)
    check("tunneld 網卡操作移到執行緒:不卡迴圈(真 start/stop_tunnel)・各拿各的網卡・一次一個・沒用到的網卡會關・網卡清單快取",
          ok, f"心跳最長間隔 修補前={before:.2f}s 修補後={after:.2f}s 關閉={closed_ok} 各自網卡={own_ok} "
              f"一次一個={serial_ok}(最多同時 {state['max']}) 沒用到會關={leak_ok} 清單快取={cache_ok}(第二次 {quick*1000:.0f} ms) "
              f"隔離模式真的掛上={isolated_ok} 取消不漏關={cancel_ok}(建立中/啟用中 建立,關閉={cancel_rows})")


async def test_tunneld_ondemand_guard():
    """★1.2.5 按需守門(tools/tunneld_ondemand.py)★:同一支手機永遠只建一條隧道。

    1.2.4 實機第一次上線就踩到(09-30 17:00):多網卡電腦上 bonjour 把同一支手機的回應收到兩份 → tunneld 的 /start-tunnel
    對每一份都開任務、用手機 IP 當 key → 第二個蓋掉第一個的登記、退出時 pop → 第一條隧道活著卻從清單消失 → 程式一直重建、
    網卡越積越多(建網卡 7 秒變 50 秒)。這格用★真的★ pymobiledevice3 9.36 TunneldRunner(自動探索全關、不綁埠、不碰網卡):
      ① 探索去重:同一支的重複服務只留第一個,其餘 close()。
      ② 兩個同時的 /start-tunnel(同一支):只開一個建立任務、兩邊都拿到同一個隧道、清單裡剛好一條。
      ③ 隧道已在:再叫 /start-tunnel 直接回傳,不再建。
      ④ 另一支手機不受影響(不同鎖)。"""
    import json as _j
    import sys as _s
    from pathlib import Path
    from types import SimpleNamespace as NS
    root = Path(__file__).resolve().parents[1]
    _s.path.insert(0, str(root / "tools"))
    import tunneld_ondemand as O
    from pymobiledevice3.remote.common import TunnelProtocol
    from pymobiledevice3.tunneld import server as TS

    closed = []

    def svc(udid, host):
        s = NS(remote_identifier=udid, hostname=host)

        async def close():
            closed.append((udid, host))
        s.close = close
        return s
    kept = await O.dedupe_services([svc("U1", "h1"), svc("U1", "h1"), svc("U1", "h2"), svc("U2", "h3")])
    dedupe_ok = [(s.remote_identifier, s.hostname) for s in kept] == [("U1", "h1"), ("U2", "h3")] and closed == [("U1", "h1"), ("U1", "h2")]

    saved_get, saved_init = TS.get_remote_pairing_tunnel_services, TS.TunneldRunner.__init__
    starts, closed[:] = [], []

    async def fake_get(bonjour_timeout=0.0, udid=None):
        n = "1" if udid == "U1" else "2"                                                  # 兩支手機各自的 IP(實機就是不同 IP)
        return [svc(udid, f"10.0.0.{n}"), svc(udid, f"10.0.0.{n}"), svc(udid, f"fe80::{n}")]   # 實機:同一支回兩份 + 兩個位址
    TS.get_remote_pairing_tunnel_services = fake_get       # ★先換成假的探索、再掛守門★:守門會包住這個假的,走的是真實安裝路徑
    installed = O.install_all()
    runner = TS.TunneldRunner("127.0.0.1", 0, protocol=TunnelProtocol.TCP, usb_monitor=False, wifi_monitor=False,
                              usbmux_monitor=False, mobdev2_monitor=False)
    core = runner._tunneld_core
    app = runner._app

    async def fake_start(task_identifier, protocol_handler, queue=None, protocol=None):
        starts.append(task_identifier)
        await asyncio.sleep(0.4)                                   # 建網卡
        tt = core.tunnel_tasks[task_identifier]
        tt.tunnel = NS(address="fd00::1", port=60000 + len(starts), interface=task_identifier)
        tt.udid = protocol_handler.remote_identifier
        if queue is not None:
            queue.put_nowait(tt.tunnel)
        try:
            await asyncio.sleep(3600)
        finally:
            core.tunnel_tasks.pop(task_identifier, None)
    core.start_tunnel_task = fake_start

    async def call(path):
        got = {"status": None, "body": b""}
        p, _, q = path.partition("?")
        scope = {"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "method": "GET", "scheme": "http",
                 "path": p, "raw_path": p.encode(), "root_path": "", "query_string": q.encode(), "headers": [],
                 "client": ("127.0.0.1", 1), "server": ("127.0.0.1", 49151)}

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(m):
            if m["type"] == "http.response.start":
                got["status"] = m["status"]
            elif m["type"] == "http.response.body":
                got["body"] += m.get("body", b"")
        await app(scope, receive, send)
        return got["status"], (_j.loads(got["body"]) if got["body"] else None)
    try:
        async with app.router.lifespan_context(app):
            r1, r2 = await asyncio.gather(call("/start-tunnel?udid=U1&connection_type=wifi"),
                                          call("/start-tunnel?udid=U1&connection_type=wifi"))
            one_ok = (r1[0] == 200 and r2[0] == 200 and r1[1]["port"] == r2[1]["port"] == 60001
                      and starts == ["10.0.0.1"] and len(core.tunnel_tasks) == 1 and closed == [("U1", "10.0.0.1"), ("U1", "fe80::1")])
            r3 = await call("/start-tunnel?udid=U1&connection_type=wifi")
            lst = await call("/")
            again_ok = r3[0] == 200 and r3[1]["port"] == 60001 and starts == ["10.0.0.1"] and list(lst[1].keys()) == ["U1"] and len(lst[1]["U1"]) == 1
            closed.clear()
            r4 = await call("/start-tunnel?udid=U2&connection_type=wifi")
            other_ok = r4[0] == 200 and r4[1]["port"] == 60002 and len(starts) == 2 and len(core.tunnel_tasks) == 2
            for tt in list(core.tunnel_tasks.values()):
                tt.task.cancel()
            await asyncio.sleep(0.05)
    finally:
        TS.get_remote_pairing_tunnel_services, TS.TunneldRunner.__init__ = saved_get, saved_init
        O._LOCKS.clear()
    check("tunneld 按需守門:探索去重、同一支同時兩個 start-tunnel 只建一條、已有就回傳、別支不受影響",
          dedupe_ok and one_ok and again_ok and other_ok and "探索去重" in installed and "start-tunnel 排隊" in installed,
          f"去重={dedupe_ok} 只建一條={one_ok}(starts={starts} tasks={len(core.tunnel_tasks)} closed={closed}) 已有={again_ok} 別支={other_ok} 掛上={installed}")


def main() -> int:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except Exception:  # noqa: BLE001
            pass
    for t in (test_bridge_contract,):
        try:
            t()
        except Exception:  # noqa: BLE001
            _FAILS.append(t.__name__)
            print(f"[FAIL] {t.__name__}", flush=True)
            traceback.print_exc()
    for t in (test_tunneld_offload, test_tunneld_ondemand_guard):
        try:
            asyncio.run(t())
        except Exception:  # noqa: BLE001
            _FAILS.append(t.__name__)
            print(f"[FAIL] {t.__name__}", flush=True)
            traceback.print_exc()
    print("GPL_SIDE_LOADED_PMD3=" + str("pymobiledevice3" in sys.modules), flush=True)
    return 0 if not _FAILS else 1


if __name__ == "__main__":
    sys.exit(main())

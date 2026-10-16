"""devmode_wifi.py — 不插線、經 Wi-Fi 隧道處理「開發者模式」:查狀態 / 讓選項出現 / 連著追蹤(1.2.72T8)。

用 tunneld 那個 Python 跑(要 pymobiledevice3;開發環境是 .venv313,安裝版是內嵌的那一個):
    python devmode_wifi.py status <UDID>
    python devmode_wifi.py reveal <UDID>
    python devmode_wifi.py watch  <UDID> --since off|on|gone|none --max-s 40
最後一行印一個 JSON(launcher 讀它);結束代碼一律 0,錯誤寫在 JSON 的 why 裡:
    {"ok": true, "devmode": false}                         status
    {"ok": true, "devmode": false, "revealed": true}       reveal
    {"ok": true, "state": "off" | "on" | "gone"}           watch
    {"ok": false, "why": "tunneld" | "not_found" | "connect" | "service" | "error", "detail": "..."}

★2026-10-13 真機實測(使用者的 iPhone tst,iOS 26,先關掉開發者模式)—— 這支的每個做法都是那天量出來的★
  · 開發者模式關著,Wi-Fi 隧道照樣建得起來、讀得到開關狀態(2 秒)。
  · 讓選項出現(reveal)不用插線:RSD 的服務清單裡就有 com.apple.amfi.lockdown.shim.remote。
    pymobiledevice3 9.36.0 的 AmfiService 寫死插線用的 com.apple.amfi.lockdown(原文讀過)→ CLI
    `amfi reveal-developer-mode --tunnel` 丟 InvalidServiceError,又被 CLI 吞成「Device is not connected」(誤導)。
    這裡用 RSD 的名稱送一模一樣的 {"action": 0} → {'success': True};使用者把「設定」App 關掉重開,
    選項就出現在「隱私權與安全性」下面「安全性」那一區的第一個。
  · ★連線一關,隧道 1 秒內就死★:同一條連線開著、每 3 秒讀一次,60 秒 20 次全部成功;關掉之後 1 秒、5 秒、
    9 秒重連全部逾時。tunneld 不知道它死了、還留在清單上 → 下一個用的人(網頁後端第一次移動)要連兩次失敗才重建。
    所以:① 進來先試連,連不上就 /cancel + /start-tunnel 重建;② 用完等 1 秒再試連,連不上就 /cancel
    (清單才是真的);連得上 = 還有別人在用這條(例如走路中的網頁後端)→ 不動它。
  · 手機重開機的那一分鐘:/start-tunnel 回 404 / 501(Wi-Fi 上找不到這支)→ watch 回 "gone"。
★只讀 + 讓選項出現★:不打開開發者模式(手機有密碼時 Apple 也不准)、不配對、不動定位。
★呼叫的人要保證沒有別支手機在走路★:重建隧道會拆/建虛擬網卡,在這台電腦上曾讓連線服務停擺 6~20 秒、
  把正在走的那支弄斷(1.2.4 的教訓)。設定視窗只在沒有手機執行中時才開得起來;控制台在別支走路時不叫它。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

TUNNELD_ADDR = ("127.0.0.1", 49151)
AMFI_RSD_SERVICE = "com.apple.amfi.lockdown.shim.remote"   # 插線版是 com.apple.amfi.lockdown(pymobiledevice3 用的那個)
REVEAL_REQUEST = {"action": 0}                               # AmfiService.DEVELOPER_MODE_REVEAL
CONNECT_S = 8.0          # 剛建好的隧道連一次 RSD 的上限
FIRST_CONNECT_S = 4.0    # 清單上本來就有的那條先試連的上限:活的 1 秒內就連上,死的會一直等到逾時(實測)
PROBE_S = 2.5            # 用完之後「這條還活著嗎」那一下
PROBE_WAIT_S = 1.0       # 關掉之後等多久再試(實測 1 秒後就連不上了)
START_S = 25.0           # /start-tunnel 的上限(平常 3~9 秒;找不到手機 2~11 秒就回 404/501)
READ_S = 6.0             # 讀一次開關狀態的上限
HOLD_EVERY_S = 2.0       # watch:連著的時候多久讀一次
GONE_RETRY_S = 3.0       # watch:手機不見時多久再找一次
STATES = ("off", "on", "gone")


def norm(u: str) -> str:
    """比對用的 UDID:只留 0-9a-f(大小寫、'-' 都不管)。"""
    return re.sub(r"[^0-9a-f]", "", str(u or "").lower())


class Fail(Exception):
    def __init__(self, why: str, detail: str = ""):
        super().__init__(why)
        self.why, self.detail = why, detail


class HttpTunneld:
    """本機 tunneld 的三個端點。全部帶逾時;連不上一律當「查不到」,絕不丟出去。"""

    def __init__(self, addr=TUNNELD_ADDR):
        self.base = f"http://{addr[0]}:{addr[1]}"

    def _get(self, path: str, params: "dict | None", timeout: float) -> "tuple[int, bytes]":
        url = self.base + path + ("?" + urllib.parse.urlencode(params) if params else "")
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, b""
        except Exception:                           # noqa: BLE001 — 連不上/逾時
            return 0, b""

    def tunnels(self) -> "dict | None":
        code, body = self._get("/", None, 5.0)
        if code != 200:
            return None
        try:
            d = json.loads(body.decode("utf-8", "replace"))
        except ValueError:
            return None
        return d if isinstance(d, dict) else None

    def cancel(self, udid: str) -> None:
        self._get("/cancel", {"udid": udid}, 15.0)

    def start(self, udid: str) -> int:
        """回 HTTP 狀態碼(200 = 建好;404/501 = Wi-Fi 上找不到這支;0 = 連線服務沒回應)。"""
        return self._get("/start-tunnel", {"udid": udid, "connection_type": "wifi"}, START_S)[0]


async def rsd_connect(det: dict, timeout: float):
    """照 tunneld 清單裡的一筆連 RSD(跟 src/device/session.get_device 同一個做法)。"""
    from pymobiledevice3.remote.remote_service_discovery import RemoteServiceDiscoveryService
    rsd = RemoteServiceDiscoveryService((det["tunnel-address"], det["tunnel-port"]))
    try:
        await asyncio.wait_for(rsd.connect(), timeout)
    except BaseException:
        await close_quietly(rsd)
        raise
    return rsd


async def close_quietly(rsd) -> None:
    try:
        await asyncio.wait_for(rsd.close(), 3.0)
    except BaseException:                           # noqa: BLE001 — 已經斷掉的連線關不掉是正常的
        pass


def entry_for(tmap: "dict | None", udid: str) -> "tuple[str, dict] | None":
    """清單裡這支的 (原樣 key, 第一筆明細);沒有就 None。純函式。"""
    for k, v in (tmap or {}).items():
        if norm(k) == norm(udid) and isinstance(v, list) and v and isinstance(v[0], dict):
            return k, v[0]
    return None


class DevmodeWifi:
    """一次只處理一支手機。tunneld / connect / sleep 都可以換成假的(selftest 離線測完整流程)。"""

    def __init__(self, udid: str, tunneld=None, connect=rsd_connect, sleep=asyncio.sleep, clock=time.monotonic):
        self.udid = udid
        self.td = tunneld or HttpTunneld()
        self.connect = connect
        self.sleep = sleep
        self.clock = clock
        self.rebuilt = 0                 # 這一趟重建了幾次隧道(記錄用)

    async def acquire(self):
        """拿到一條活的 RSD 連線。清單裡那條連不上(半死)→ /cancel + 用 Wi-Fi 重建一次。"""
        tmap = self.td.tunnels()
        if tmap is None:
            raise Fail("tunneld", "連線服務沒有回應")
        hit = entry_for(tmap, self.udid)
        if hit is not None:
            try:
                return await self.connect(hit[1], FIRST_CONNECT_S)
            except Exception:                       # noqa: BLE001 — 半死:拆掉重建
                self.td.cancel(hit[0])
        code = self.td.start(self.udid)
        self.rebuilt += 1
        if code == 0:
            raise Fail("tunneld", "連線服務沒有回應")
        if code != 200:
            raise Fail("not_found", f"HTTP {code}")
        hit = entry_for(self.td.tunnels(), self.udid)
        if hit is None:
            raise Fail("not_found", "建好了卻不在清單上")
        try:
            return await self.connect(hit[1], CONNECT_S)
        except Exception as e:                      # noqa: BLE001
            raise Fail("connect", type(e).__name__) from e

    async def release(self, rsd) -> bool:
        """關掉自己的連線;那條隧道因此死了就從清單拿掉(回 True)。還有別人在用就不動(回 False)。"""
        if rsd is not None:
            await close_quietly(rsd)
        for _ in range(2):
            await self.sleep(PROBE_WAIT_S)
            hit = entry_for(self.td.tunnels(), self.udid)
            if hit is None:
                return False                        # 已經不在清單上了
            try:
                probe = await self.connect(hit[1], PROBE_S)
            except Exception:                       # noqa: BLE001 — 死了:拿掉,下一個用的人才會重建
                self.td.cancel(hit[0])
                return True
            await close_quietly(probe)              # 活著:可能是別人在用,也可能還沒死透 → 再看一次
        return False

    async def read(self, rsd) -> bool:
        return bool(await asyncio.wait_for(rsd.get_developer_mode_status(), READ_S))

    async def status(self) -> dict:
        rsd = await self.acquire()
        try:
            dm = await self.read(rsd)
        finally:
            await self.release(rsd)
        return {"ok": True, "devmode": dm}

    async def reveal(self) -> dict:
        rsd = await self.acquire()
        try:
            dm = await self.read(rsd)
            try:
                svc = await asyncio.wait_for(rsd.start_lockdown_service(AMFI_RSD_SERVICE), CONNECT_S)
            except Exception as e:                  # noqa: BLE001 — 這支 iOS 沒有這個服務(很舊或很新)
                return {"ok": False, "why": "service", "detail": type(e).__name__, "devmode": dm}
            try:
                resp = await asyncio.wait_for(svc.send_recv_plist(REVEAL_REQUEST), READ_S)
            finally:
                try:
                    await asyncio.wait_for(svc.close(), 3.0)
                except BaseException:               # noqa: BLE001
                    pass
            ok = isinstance(resp, dict) and resp.get("success") is True
            return {"ok": ok, "devmode": dm, "revealed": ok,
                    **({} if ok else {"why": "error", "detail": str(resp)[:120]})}
        finally:
            await self.release(rsd)

    async def watch(self, since: str, max_s: float) -> dict:
        """一直看到狀態跟 since 不一樣(或時間到)才回:"off" 開關沒開 / "on" 開了 / "gone" Wi-Fi 上找不到這支
        (多半是在重開機)。連著的時候每 2 秒讀一次(連線開著隧道才不會死);找不到時每 3 秒再找一次。"""
        end = self.clock() + max_s
        while True:
            try:
                rsd = await self.acquire()
            except Fail as f:
                if f.why == "tunneld":
                    return {"ok": False, "why": "tunneld", "detail": f.detail}
                if since != "gone" or self.clock() >= end:
                    return {"ok": True, "state": "gone"}
                await self.sleep(GONE_RETRY_S)
                continue
            state = await self._hold(rsd, since, end)
            if state != "gone":
                await self.release(rsd)
                return {"ok": True, "state": state}
            # 讀到一半斷了(手機多半開始重開機):那條一定死了 → 關掉、直接從清單拿掉,重開機後才建得起新的
            await close_quietly(rsd)
            hit = entry_for(self.td.tunnels(), self.udid)
            if hit is not None:
                self.td.cancel(hit[0])
            if since != "gone" or self.clock() >= end:
                return {"ok": True, "state": "gone"}

    async def _hold(self, rsd, since: str, end: float) -> str:
        """連著讀,直到跟 since 不一樣或時間到;讀到一半斷掉回 "gone"。"""
        while True:
            try:
                state = "on" if await self.read(rsd) else "off"
            except Exception:                       # noqa: BLE001 — 斷線/逾時
                return "gone"
            if state != since or self.clock() >= end:
                return state
            await self.sleep(HOLD_EVERY_S)


def run(action: str, udid: str, since: str = "none", max_s: float = 40.0, tool: "DevmodeWifi | None" = None) -> dict:
    """命令列的本體(selftest 直接呼叫,給假的 tool)。任何意外都變成 {"ok": false, "why": "error"},不丟出去。"""
    tool = tool or DevmodeWifi(udid)

    async def go():
        if action == "status":
            return await tool.status()
        if action == "reveal":
            return await tool.reveal()
        if action == "watch":
            return await tool.watch(since if since in STATES else "none", max_s)
        return {"ok": False, "why": "error", "detail": f"不認得的動作 {action}"}
    try:
        out = asyncio.run(go())
    except Fail as f:
        out = {"ok": False, "why": f.why, "detail": f.detail}
    except Exception as e:                          # noqa: BLE001
        out = {"ok": False, "why": "error", "detail": type(e).__name__}
    out["rebuilt"] = tool.rebuilt
    return out


def main(argv: "list[str] | None" = None) -> int:
    ap = argparse.ArgumentParser(description="不插線處理開發者模式(只讀 + 讓選項出現)")
    ap.add_argument("action", choices=("status", "reveal", "watch"))
    ap.add_argument("udid")
    ap.add_argument("--since", default="none")
    ap.add_argument("--max-s", type=float, default=40.0)
    a = ap.parse_args(argv)
    # ★ensure_ascii=True(B-1.0 審查 T7)★:launcher 用 UTF-8 讀這一行,但我們的 stdout 接到管線時用的是系統編碼
    #   (一般台灣電腦 cp950)—— detail 裡有中文就會變亂碼,Big5 第二個位元組是反斜線的字還會讓整行 JSON 解不開
    #   (launcher 當成「查不到」)。全部轉成 \uXXXX 就跟編碼無關。
    print(json.dumps(run(a.action, a.udid, a.since, a.max_s), ensure_ascii=True), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

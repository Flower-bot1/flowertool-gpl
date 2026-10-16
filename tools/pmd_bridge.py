"""pmd_bridge.py — 定位模擬橋接程式:主程式透過標準輸入/輸出,請它用 pymobiledevice3 跟 iPhone 說話。

Copyright (C) 2026 種花小幫手作者
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify it under the terms of the
GNU General Public License as published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version. This program is distributed in the hope that it
will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with this program.
If not, see <https://www.gnu.org/licenses/>.

★為什麼是一支獨立的程式(Route B,使用者 10-16 裁決)★:pymobiledevice3 是 GPL-3.0。
主程式只透過這支的標準輸入/輸出交換「一行一則的 JSON」,內容只有簡單的值(位址、埠、緯度、經度、
結果碼、訊息文字),不交換任何內部資料結構 —— 依 GNU GPL FAQ(MereAggregation / GPLPlugins)
這是兩支獨立程式之間的溝通。所以:★這支程式只准 import 標準庫和 pymobiledevice3,絕不 import 主程式★。

協定(每一行一則,ASCII JSON):
    要求:{"id": 1, "op": "<動作>", ...參數}
    回覆:{"id": 1, "ok": true, ...結果}  或  {"id": 1, "ok": false, "code": "<結果碼>", "type": "<例外名>", "msg": "<說明>"}
    動作:hello                                   → {"proto", "pmd3"}
          connect  address port [interface] [timeout] → {"udid", "product_version"}(一支橋接程式只連一支手機)
          mount                                   → {"mounted": "now" | "already"}(掛載開發者映像)
          loc_open / loc_close                    → 開 / 關定位模擬服務
          loc_set  lat lon                        → 設定位置(不是有效經緯度的一律不送)
          loc_clear                               → 還原真實定位
          close                                   → 還原、斷線、結束程式
    結果碼:devmode_off(手機開發者模式沒開)、not_connected、not_open、already_connected、
            bad_request、bad_coord、timeout、error(其他)。
標準輸入關閉(主程式結束或被關掉)= 自己收尾結束;收尾最多等幾秒,超過就直接結束。
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import os
import sys
import threading

PROTO = 1
SHUTDOWN_STEP_S = 3.0      # 收尾時每一步最多等幾秒(連線半死時 clear / 斷線都可能等不到回覆)
HARD_EXIT_S = 12.0         # 標準輸入關了以後,不管收尾到哪裡,最晚這麼久一定結束

log = logging.getLogger("pmd_bridge")


class Fail(Exception):
    """回給主程式的失敗(結果碼 + 說明)。"""

    def __init__(self, code: str, msg: str):
        super().__init__(msg)
        self.code = code


def _num(v, name: str) -> float:
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
        raise Fail("bad_coord", f"{name} 不是有效的數字")
    return float(v)


class Bridge:
    def __init__(self, out):
        self.out = out
        self.wlock = threading.Lock()
        self.rsd = None
        self.dvt_cm = None
        self.loc_cm = None
        self.loc = None
        self.tasks: "dict[asyncio.Task, str]" = {}

    # ── 輸出 ───────────────────────────────────────────────
    def send(self, obj: dict) -> None:
        data = (json.dumps(obj, ensure_ascii=True) + "\n").encode("ascii")
        with self.wlock:
            try:
                self.out.write(data)
                self.out.flush()
            except (OSError, ValueError):      # 主程式已經不在了:收尾由標準輸入關閉那條路處理
                pass

    # ── 主迴圈 ─────────────────────────────────────────────
    async def run(self, stdin) -> None:
        loop = asyncio.get_running_loop()
        q: "asyncio.Queue[bytes | None]" = asyncio.Queue()

        def reader() -> None:
            try:
                for raw in stdin:
                    loop.call_soon_threadsafe(q.put_nowait, raw)
            except (OSError, ValueError):
                pass
            loop.call_soon_threadsafe(q.put_nowait, None)
            _arm_hard_exit()

        threading.Thread(target=reader, name="stdin", daemon=True).start()
        while True:
            raw = await q.get()
            if raw is None:
                break
            try:
                req = json.loads(raw)
                if not isinstance(req, dict):
                    raise ValueError
            except ValueError:
                self.send({"id": None, "ok": False, "code": "bad_request", "type": "", "msg": "不是 JSON 物件"})
                continue
            if req.get("op") == "close":
                await self.handle(req)
                break
            t = asyncio.create_task(self.handle(req))
            self.tasks[t] = str(req.get("op"))
            t.add_done_callback(lambda tt: self.tasks.pop(tt, None))
        await self.shutdown()

    async def handle(self, req: dict) -> None:
        rid, op = req.get("id"), req.get("op")
        fn = getattr(self, "op_" + op, None) if isinstance(op, str) and op.isidentifier() else None
        try:
            if fn is None:
                raise Fail("bad_request", f"沒有這個動作:{op!r}")
            res = await fn(req)
            self.send({"id": rid, "ok": True, **(res or {})})
        except Fail as e:
            self.send({"id": rid, "ok": False, "code": e.code, "type": "", "msg": str(e)})
        except asyncio.CancelledError:
            self.send({"id": rid, "ok": False, "code": "cancelled", "type": "CancelledError", "msg": "被取消"})
        except Exception as e:  # noqa: BLE001 — 任何例外都回成結果碼,不讓橋接程式掛掉
            self.send({"id": rid, "ok": False, "code": _code_of(e), "type": type(e).__name__,
                       "msg": str(e)[:500]})

    def _cancel_loc_ops(self) -> None:
        """關 / 重開定位服務時,把還卡著的 loc_set / loc_clear 取消(等同以前主程式逾時就取消那一步)。"""
        me = asyncio.current_task()
        for t, op in list(self.tasks.items()):
            if t is not me and op in ("loc_set", "loc_clear"):
                t.cancel()

    # ── 動作 ───────────────────────────────────────────────
    async def op_hello(self, req: dict) -> dict:
        import importlib.metadata as md
        try:
            ver = md.version("pymobiledevice3")
        except Exception:  # noqa: BLE001
            ver = "?"
        return {"proto": PROTO, "pmd3": ver}

    async def op_connect(self, req: dict) -> dict:
        from pymobiledevice3.remote.remote_service_discovery import RemoteServiceDiscoveryService
        if self.rsd is not None:
            raise Fail("already_connected", "這支橋接程式已經連著一支手機")
        addr, port = req.get("address"), req.get("port")
        if not isinstance(addr, str) or not addr or isinstance(port, bool) or not isinstance(port, int):
            raise Fail("bad_request", "缺少位址或埠")
        iface = req.get("interface")
        timeout = req.get("timeout") or 10.0
        cand = RemoteServiceDiscoveryService((addr, port), name=iface if isinstance(iface, str) else None)
        try:
            await asyncio.wait_for(cand.connect(), timeout=float(timeout))
        except BaseException:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(cand.close(), timeout=SHUTDOWN_STEP_S)
            raise
        self.rsd = cand
        return {"udid": str(getattr(cand, "udid", "") or ""),
                "product_version": str(getattr(cand, "product_version", "?") or "?")}

    async def op_mount(self, req: dict) -> dict:
        from pymobiledevice3.services.mobile_image_mounter import AlreadyMountedError, auto_mount
        self._need_rsd()
        try:
            await auto_mount(self.rsd)
        except AlreadyMountedError:
            return {"mounted": "already"}
        return {"mounted": "now"}

    async def op_loc_open(self, req: dict) -> dict:
        from pymobiledevice3.services.dvt.instruments.dvt_provider import DvtProvider
        from pymobiledevice3.services.dvt.instruments.location_simulation import LocationSimulation
        self._need_rsd()
        self._cancel_loc_ops()
        await self._loc_teardown()                 # 上一次沒關乾淨的先收掉(重開時通道可能已經死了)
        dvt_cm = DvtProvider(self.rsd)
        dvt = await dvt_cm.__aenter__()
        loc_cm = LocationSimulation(dvt)
        try:
            loc = await loc_cm.__aenter__()
        except BaseException:                      # 第二層進入失敗 → 先收尾第一層,避免連線洩漏
            with contextlib.suppress(Exception):
                await dvt_cm.__aexit__(None, None, None)
            raise
        self.dvt_cm, self.loc_cm, self.loc = dvt_cm, loc_cm, loc
        return {}

    async def op_loc_set(self, req: dict) -> dict:
        lat, lon = _num(req.get("lat"), "緯度"), _num(req.get("lon"), "經度")
        if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
            raise Fail("bad_coord", "座標超出經緯度範圍,不送給手機")
        await self._need_loc().set(lat, lon)
        return {}

    async def op_loc_clear(self, req: dict) -> dict:
        await self._need_loc().clear()
        return {}

    async def op_loc_close(self, req: dict) -> dict:
        self._cancel_loc_ops()
        await self._loc_teardown(strict=True)
        return {}

    async def op_close(self, req: dict) -> dict:
        await self.shutdown()
        return {}

    # ── 收尾 ───────────────────────────────────────────────
    def _need_rsd(self):
        if self.rsd is None:
            raise Fail("not_connected", "還沒連上手機")
        return self.rsd

    def _need_loc(self):
        if self.loc is None:
            raise Fail("not_open", "定位模擬服務沒開")
        return self.loc

    async def _loc_teardown(self, strict: bool = False) -> None:
        """依序關掉定位服務、DVT(後一層一定要關,前一層失敗也一樣)。strict=第一個錯誤要丟回去。"""
        loc_cm, dvt_cm = self.loc_cm, self.dvt_cm
        self.loc = self.loc_cm = self.dvt_cm = None
        err = None
        for cm in (loc_cm, dvt_cm):
            if cm is None:
                continue
            try:
                await asyncio.wait_for(cm.__aexit__(None, None, None), timeout=SHUTDOWN_STEP_S)
            except Exception as e:  # noqa: BLE001
                err = err or e
        if strict and err is not None:
            raise err

    async def shutdown(self) -> None:
        for t in list(self.tasks):
            if t is not asyncio.current_task():
                t.cancel()
        if self.loc is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.loc.clear(), timeout=SHUTDOWN_STEP_S)   # 還原真實定位
        with contextlib.suppress(Exception):
            await self._loc_teardown()
        rsd, self.rsd = self.rsd, None
        if rsd is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(rsd.close(), timeout=SHUTDOWN_STEP_S)


def _code_of(e: BaseException) -> str:
    """例外 → 結果碼。只看類別名稱(不必為了分類而 import 整個例外模組)。"""
    names = {c.__name__ for c in type(e).__mro__}
    if "DeveloperModeIsNotEnabledError" in names:
        return "devmode_off"
    if isinstance(e, (asyncio.TimeoutError, TimeoutError)):
        return "timeout"
    return "error"


def _arm_hard_exit() -> None:
    t = threading.Timer(HARD_EXIT_S, lambda: os._exit(0))
    t.daemon = True
    t.start()


def _preload() -> None:
    """一開始就把要用的模組載好(約 1~2 秒):hello 回來 = 準備好了,之後 connect 的逾時不會被載入時間吃掉。"""
    import pymobiledevice3.exceptions  # noqa: F401
    import pymobiledevice3.remote.remote_service_discovery  # noqa: F401
    import pymobiledevice3.services.dvt.instruments.dvt_provider  # noqa: F401
    import pymobiledevice3.services.dvt.instruments.location_simulation  # noqa: F401
    import pymobiledevice3.services.mobile_image_mounter  # noqa: F401


def main() -> int:
    out = sys.stdout.buffer
    with contextlib.suppress(Exception):   # 隔離模式下標準錯誤預設是系統字碼頁(950 / 65001 都有);固定 UTF-8,主程式照 UTF-8 解
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    sys.stdout = sys.stderr            # ★協定通道只給回覆用★:任何 print / 函式庫輸出都改到標準錯誤
    logging.basicConfig(level=logging.WARNING, stream=sys.stderr,
                        format="%(levelname)s %(name)s: %(message)s")
    _preload()
    asyncio.run(Bridge(out).run(sys.stdin.buffer))
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""tunneld_ondemand.py — 讓 pymobiledevice3 9.36 tunneld 的 /start-tunnel 在「按需模式」下安全:同一支手機永遠只建一條隧道。

為什麼(2026-09-30 17:00 實機,1.2.4 第一次上線就踩到):
  多網卡的電腦上 bonjour 會把同一支手機的回應收到好幾份(實測 answers=2、內容一模一樣),
  get_remote_pairing_tunnel_services(udid=…) 就回好幾個「同一支手機」的服務。/start-tunnel 端點對每一個都開一個
  start_tunnel_task,而且用「手機 IP」當 tunnel_tasks 的 key —— 第二個把第一個的登記蓋掉;第二個發現已有隧道就退出,
  退出時 pop 掉那個 key,結果**第一條隧道還活著、網卡還在,卻從清單消失**。
  程式看清單以為沒連上 → 再建 → 再消失 → 網卡越積越多(建網卡從 7 秒變到 50 秒),手機一直「連不上」。
  自動探索模式(monitor_wifi_task)沒這個問題:它先查「這個 IP / 這支手機已經有隧道」才開任務。

做法(★不改 pymobiledevice3 的任何檔案★,啟動時掛上;任何一步失敗就不掛、照原本的樣子跑):
  ① 探索結果去重:同一支手機(remote_identifier)只留第一個服務,其餘的關掉。
  ② /start-tunnel 每支手機一把鎖:同一支的請求排隊做(第二個進來時第一個已建好,端點開頭那段「已有隧道就回傳」會接住),
     不會同時開兩個任務去蓋同一個 key。
  ③ 已經有人在替這支建(任務在、隧道還沒好)→ 等它,不再開一個。

關掉:在 _tunneld.bat 加一行 set "TUNNELD_ONDEMAND_GUARD=0"。
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional

log = logging.getLogger("tunneld_ondemand")

WAIT_INPROGRESS_S = 90.0      # 別人正在替這支建隧道時最多等多久(建網卡在慢的電腦上 10~50 秒)
_LOCKS: "dict[str, asyncio.Lock]" = {}


def _lock_for(udid: str) -> asyncio.Lock:
    lock = _LOCKS.get(udid)
    if lock is None:
        lock = _LOCKS[udid] = asyncio.Lock()
    return lock


async def dedupe_services(services: list) -> list:
    """同一支手機只留第一個服務,其餘的關掉(關不掉也不影響)。純邏輯,selftest 直測。"""
    keep, seen = [], set()
    for s in services:
        ident = getattr(s, "remote_identifier", None)
        if ident in seen:
            try:
                await s.close()
            except Exception:  # noqa: BLE001 — 多出來的那份關不掉就算了
                pass
            continue
        seen.add(ident)
        keep.append(s)
    if len(keep) != len(services):
        log.info(f"[ondemand] 探索回來 {len(services)} 個服務,同一支手機重複的去掉 {len(services) - len(keep)} 個")
    return keep


def install_dedupe() -> bool:
    """把 tunneld.server 模組裡的 get_remote_pairing_tunnel_services 換成「呼叫原本的 → 去重」。
    ★要換 tunneld.server 那個名字★:它是 from … import 進來的,換 tunnel_service 模組的那個沒有用。"""
    from pymobiledevice3.tunneld import server as S
    orig = S.get_remote_pairing_tunnel_services
    if getattr(orig, "__ondemand__", False):
        return False

    async def deduped(*a, **k):
        return await dedupe_services(await orig(*a, **k))
    deduped.__ondemand__ = True
    deduped.__wrapped__ = orig
    S.get_remote_pairing_tunnel_services = deduped
    return True


def _inprogress(core, udid: str):
    """tunnel_tasks 裡「這支手機、任務還在跑、隧道還沒好」的那個 TunnelTask(沒有就 None)。"""
    for tt in list(core.tunnel_tasks.values()):
        if getattr(tt, "udid", None) == udid and getattr(tt, "tunnel", None) is None:
            task = getattr(tt, "task", None)
            if task is not None and not task.done():
                return tt
    return None


def _existing(core, udid: str):
    for tt in list(core.tunnel_tasks.values()):
        if getattr(tt, "udid", None) == udid and getattr(tt, "tunnel", None) is not None:
            return tt.tunnel
    return None


def install_serialized_start(app, core) -> bool:
    """把 FastAPI 的 /start-tunnel 路由換成「每支手機一把鎖 + 等進行中的」的包裝;裡面照樣呼叫原本的端點函式。"""
    import fastapi
    route = next((r for r in app.router.routes if getattr(r, "path", None) == "/start-tunnel"), None)
    if route is None or getattr(route.endpoint, "__ondemand__", False):
        return False
    orig = route.endpoint
    app.router.routes.remove(route)

    async def start_tunnel(udid: str, ip: Optional[str] = None, connection_type: Optional[str] = None):
        async with _lock_for(udid):
            inprog = _inprogress(core, udid)
            if inprog is not None:
                log.info(f"[ondemand] {udid[:12]}… 已經有人在建隧道,等它(最多 {WAIT_INPROGRESS_S:.0f} 秒)")
                t0 = asyncio.get_running_loop().time()
                while asyncio.get_running_loop().time() - t0 < WAIT_INPROGRESS_S:
                    tun = _existing(core, udid)
                    if tun is not None:
                        data = {"interface": tun.interface, "port": tun.port, "address": tun.address}
                        return fastapi.Response(status_code=200, media_type="application/json", content=json.dumps(data))
                    if inprog.task.done():
                        break
                    await asyncio.sleep(0.5)
            return await orig(udid=udid, ip=ip, connection_type=connection_type)
    start_tunnel.__ondemand__ = True
    start_tunnel.__wrapped__ = orig
    app.add_api_route("/start-tunnel", start_tunnel, methods=["GET"])
    return True


def install_all() -> list:
    """實際掛到 tunneld 上。每一項各自 try:掛不上就記一行、那一項照原本的樣子。回掛上的項目名稱。"""
    done = []
    try:
        if install_dedupe():
            done.append("探索去重")
    except Exception as e:  # noqa: BLE001
        log.warning(f"[ondemand] 探索去重沒掛上({type(e).__name__}: {e}),照原本的方式")
    try:
        from pymobiledevice3.tunneld import server as S
        cls = S.TunneldRunner
        if not getattr(cls.__init__, "__ondemand__", False):
            orig_init = cls.__init__

            def __init__(self, *a, **k):
                orig_init(self, *a, **k)
                try:
                    if install_serialized_start(self._app, self._tunneld_core):
                        log.info("[ondemand] /start-tunnel 已改成每支手機一把鎖(同一支不會同時建兩條)")
                except Exception as e:  # noqa: BLE001
                    log.warning(f"[ondemand] /start-tunnel 包裝沒掛上({type(e).__name__}: {e}),照原本的方式")
            __init__.__ondemand__ = True
            __init__.__wrapped__ = orig_init
            cls.__init__ = __init__
            done.append("start-tunnel 排隊")
    except Exception as e:  # noqa: BLE001
        log.warning(f"[ondemand] start-tunnel 排隊沒掛上({type(e).__name__}: {e}),照原本的方式")
    return done

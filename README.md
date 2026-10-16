# 種花小幫手 — GPL components

These are the components of 種花小幫手 (FlowerTool) that talk to the iPhone through
[pymobiledevice3](https://github.com/doronz88/pymobiledevice3). They are licensed under the
GNU General Public License, version 3 or (at your option) any later version — see `LICENSE`.

The rest of 種花小幫手 (the web console, control panel, setup window and launcher) is a separate,
proprietary program. It never imports these components or pymobiledevice3: it starts them as separate
processes and exchanges only simple values (addresses, ports, latitude/longitude, result codes) over
standard input/output or the command line.

| File | What it does |
|---|---|
| `tools/pmd_bridge.py` | Location-simulation bridge. One JSON object per line on stdin/stdout (protocol in the file header). |
| `tools/devmode_wifi.py` | Checks / reveals Developer Mode over a Wi-Fi tunnel. Prints a JSON result on its last line. |
| `tools/tunneld_probe.py` | Starts `pymobiledevice3 remote tunneld` with the two runtime patches below and stall logging. |
| `tools/tunneld_offload.py` | Runtime patch: TUN adapter operations run on a worker thread. |
| `tools/tunneld_ondemand.py` | Runtime patch: one tunnel per device for on-demand `/start-tunnel`. |
| `tools/patches/pymobiledevice3-9.36.0/bonjour.py` | Modified `pymobiledevice3/bonjour.py` (multi-adapter mDNS; changes are marked). |
| `tests/gpl_side.py` | Tests for the above (`python -I tests/gpl_side.py`). |

Requirements: Python 3.12+ and `pymobiledevice3==9.36.0` (`pip install -r requirements.txt`).

這些是種花小幫手裡「跟手機說話」的元件,以 GNU GPL 第 3 版(或任何更新的版本)授權。
種花小幫手的主程式是另外一支程式,不在這裡、也不以 GPL 授權。

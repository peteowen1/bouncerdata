"""Kill all running cricinfo scraper processes and their Chrome instances.

Usage: python scripts/kill_scrapers.py
"""
import subprocess
import sys
import os
from pathlib import Path


def main():
    # 1. Find and kill scraper Python processes (by PID files)
    cricinfo_dir = Path(__file__).resolve().parent.parent / "cricinfo"
    pidfiles = list(cricinfo_dir.glob(".cricinfo_scraper_*.pid"))

    killed_pids = set()
    for pf in pidfiles:
        try:
            content = pf.read_text()
            for line in content.strip().split("\n"):
                if "=" not in line:
                    continue
                key, val = line.split("=", 1)
                if val != "unknown":
                    pid = int(val)
                    killed_pids.add(pid)
                    if sys.platform == "win32":
                        subprocess.run(
                            ["taskkill", "/F", "/T", "/PID", str(pid)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                        )
                    else:
                        try:
                            os.kill(pid, 9)
                        except ProcessLookupError:
                            pass
        except Exception as e:
            print(f"  Warning: {pf.name}: {e}")
        finally:
            # Always clean up the PID file, even if parsing failed
            pf.unlink(missing_ok=True)

    if killed_pids:
        print(f"Killed {len(killed_pids)} processes from PID files: {killed_pids}")
    else:
        print("No PID files found.")

    # 2. Fallback: find Python processes running cricinfo_scraper
    if sys.platform == "win32":
        try:
            result = subprocess.run(
                ["powershell", "-Command",
                 "Get-CimInstance Win32_Process -Filter \"Name = 'python.exe'\" "
                 "| Where-Object { $_.CommandLine -match 'cricinfo_scraper' } "
                 "| Select-Object -ExpandProperty ProcessId"],
                capture_output=True, text=True, timeout=10,
            )
            for line in result.stdout.strip().split("\n"):
                line = line.strip()
                if line.isdigit():
                    pid = int(line)
                    if pid not in killed_pids and pid != os.getpid():
                        subprocess.run(
                            ["taskkill", "/F", "/T", "/PID", str(pid)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                        )
                        print(f"Killed scraper Python process {pid}")
        except Exception as e:
            print(f"  Warning scanning processes: {e}")

    # 3. Kill orphaned Playwright Chrome (identified by --disable-blink-features)
    if sys.platform == "win32":
        try:
            result = subprocess.run(
                ["powershell", "-Command",
                 "Get-CimInstance Win32_Process -Filter \"Name = 'chrome.exe'\" "
                 "| Where-Object { $_.CommandLine -match 'disable-blink-features' } "
                 "| Select-Object -ExpandProperty ProcessId"],
                capture_output=True, text=True, timeout=15,
            )
            chrome_pids = [int(l.strip()) for l in result.stdout.strip().split("\n") if l.strip().isdigit()]
            for pid in chrome_pids:
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(pid)],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                )
            if chrome_pids:
                print(f"Killed {len(chrome_pids)} Playwright Chrome processes")
            else:
                print("No orphaned Playwright Chrome processes found")
        except Exception as e:
            print(f"  Warning scanning Chrome: {e}")

    print("Done.")


if __name__ == "__main__":
    main()

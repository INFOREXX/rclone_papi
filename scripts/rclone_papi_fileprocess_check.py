
import psutil
"""
Rclone Process Monitor 
This provides information which files are currenntly handled by rclone.
"""

import re

def show_running_rclone_jobs():
    print("Running rclone jobs (with source, target, and current file if available):")
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['name'] and 'rclone' in proc.info['name'].lower():
                cmdline = proc.info['cmdline']
                source = target = "N/A"
                if len(cmdline) >= 4 and cmdline[1] == "sync":
                    source = cmdline[2]
                    target = cmdline[3]
                print("--------------------------------------------------------------------------------------------------------------------------")
                print("--------------------------------------------------------------------------------------------------------------------------")
                print(f"PID: {proc.info['pid']}, Source: {source}, Target: {target}, CMD: {' '.join(cmdline)}")
                # Try to show currently copied file (if any)
                try:
                    p = psutil.Process(proc.info['pid'])
                    open_files = p.open_files()
                    # Filter out files from c:\windows (case-insensitive)
                    user_files = [f.path for f in open_files if not f.path.lower().startswith(r'c:\windows')]
                    user_files.sort()  # Sort file paths alphabetically
                    if user_files:
                        print("-------------------------------------------------------------")
                        print("Open files (may include currently copied file):")
                        for path in user_files:
                            print(f"    {path}")
                    else:
                        for line in p.cmdline():
                            match = re.search(r'--progress', line)
                            if match:
                                print("  Progress flag detected, but cannot read live progress from subprocess.")
                except Exception as e:
                    print(f"  Could not determine current file: {e}")
        except (psutil.NoSuchProcess, psutil.AccessDenied, IndexError):
            continue


def kill_process(pid):
    try:
        p = psutil.Process(pid)
        p.terminate()  # or p.kill() for force kill
        p.wait(timeout=3)
        print(f"Process {pid} terminated.")
    except Exception as e:
        print(f"Failed to terminate process {pid}: {e}")


if __name__ == "__main__":
    show_running_rclone_jobs()

    # Example usage:
    # kill_process(12676)
@echo off
REM ===============================================================
REM Batch wrapper for rclone with logging enabled by default
REM Usage: rclonesuffix [rclone-subcommand-and-args]
REM Example:
REM   rclonesuffix bisync "c:\TTT_rclone1" OneDrive_T1_crypt:TTT_rclone1 --dry-run
REM ===============================================================

REM === Configurable variables ===

set "LOG_FILE=.OneDrive_T1_crypt.log"
set "LOG_LEVEL=INFO"


REM rclone bisync "c:\TTT_rclone1" "OneDrive_T1_crypt:TTT_rclone1" --log-file="%LOG_FILE%" --log-level %LOG_LEVEL% --dry-run
rclone bisync "c:\TTT_rclone1" "OneDrive_T1_crypt:TTT_rclone1" --log-file="%LOG_FILE%" --log-level %LOG_LEVEL% 


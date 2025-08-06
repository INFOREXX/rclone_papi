"""
Rclone Python API Wrapper for Large Folder Backup
This module provides a comprehensive solution for backuping large folder structures
using the rclone command-line tool through its Python API wrapper. It supports
folder structure analysis, difference computation, and efficient backup operations.
Key Features:
    - Automatic rclone configuration detection from Windows APPDATA
    - Folder structure comparison and backuping
    - Batch processing from CSV file input
    - Comprehensive logging with timestamps
    - Dry-run mode for safe testing
    - Error handling and retry mechanisms
    - Support for large file transfers with progress tracking
Main Functions:
    mkdir(): Creates directories using rclone mkdir command
    complete_list_check(): Compares source and destination folder structures
    large_folder_backup_with_analysis(): Performs file-level backup with change analysis
Configuration:
    - Uses TOML configuration files for customizable parameters
    - CSV input format: source_path, destination_path
    - Automatic log file generation with timestamps
    - Configurable transfer parameters (threads, retries, timeouts)
Dependencies:
    - rclone_api: Python wrapper for rclone
    - pathlib: Path manipulation
    - logging: Comprehensive logging support
    - csv: CSV file processing
    - toml: Configuration file parsing
    - argparse: Command-line argument parsing
Usage:
    python rclone_papi_4v0.py --config config.toml
   
version: 4v1
author: Inforexx

"""
from rclone_api import Rclone, DiffItem, DiffType, DiffOption, ListingOption, DirListing
from pathlib import Path
import logging
import os
from datetime import datetime
from collections import deque
import csv
import toml
import argparse
import sys

csv_file = "rclone_papi_folder_list.csv"
log_folder = "log"

# -----------------------------------------------------------------------

start_datetime = datetime.now().strftime('%Y-%m-%d-%H%M%S')
filemode = 'w'  #w - appendwrite always new, a -append. When w, it's useful for debugging

# Ensure the log folder exists
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

# Add this before logging.basicConfig to clear any existing handlers and ensure logs go only to the file
logging.getLogger().handlers = []  # Clear all existing handlers to prevent console output

# Set up logging with detailed format including timestamps
# Adjusted format to handle multi-line messages better by including newline handling
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=f'{log_folder}/{start_datetime}_rclone_papi.log.txt',  # Redirect logs to this file
    filemode=filemode   # 'w' to overwrite each run; change to 'a' to append    
)

# Custom logging handler to capture warnings and multi-line errors
import warnings
class RcloneWarningFilter(logging.Filter):
    def filter(self, record):
        if "UserWarning" in record.msg or "NOTICE" in record.msg:
            return True
        return super().filter(record)


def mkdir(rclone: Rclone, path: str):
    """Helper function to create a directory using rclone."""
    try:
        cmd = ['mkdir', path]
        rclone.impl._run(cmd, check=True, capture=False)
        logging.info(f"Successfully created directory: {path}")
    except Exception as e:
        logging.error(f"Failed to create directory {path}: {str(e)}")
        raise

def complete_list_check(root_src: str, root_dst: str, dry_run: bool = True):
    # Initialize Rclone (reusing your script's config detection logic)
    appdata_path = os.environ.get('APPDATA')
    if not appdata_path:
        raise ValueError("APPDATA environment variable not found. Set it or hardcode the config path.")
    config_path = Path(appdata_path) / 'rclone' / 'rclone.conf'
    if not config_path.exists():
        raise FileNotFoundError(f"rclone.conf not found at {config_path}. Create it via 'rclone config'.")
    rclone = Rclone(config_path)

    logging.info(f"Starting folder structure check for source: {root_src} -> destination: {root_dst} (dry_run={dry_run})")

    # Helper to collect folder structure using lsjson (recursive, filter for dirs)
    def collect_structure(path: str, is_source: bool) -> set:
        try:
            # Use lsjson for recursive listing; adjust args for dirs_only if supported
            listing = rclone.lsjson(
                path=path,
                recursive=True,
                other_args=["--fast-list", "--dirs-only"]  # --dirs-only to get only folders
            )
            # Assuming listing is a list of dicts or DirListing objects; adjust based on actual return type
            structure = set(item['Path'] for item in listing if item['IsDir'])
            logging.info(f"Collected {len(structure)} folders from {'source' if is_source else 'target'}.")
            return structure
        except AttributeError:
            # Fallback if lsjson not directly available: raw command
            cmd = ['lsjson', path, '--recursive', '--fast-list', '--dirs-only']
            result = rclone.impl._run(cmd, check=True, capture=True)
            import json
            listing = json.loads(result.stdout)
            structure = set(item['Path'] for item in listing if item['IsDir'])
            logging.info(f"Collected {len(structure)} folders from {'source' if is_source else 'target'} (via fallback).")
            return structure
        except Exception as e:
            logging.error(f"Failed to collect {'source' if is_source else 'target'} structure: {str(e)}")
            raise

    # Step 1: Collect source folder structure
    logging.info("Collecting source folder structure...")
    source_list = collect_structure(root_src, is_source=True)

    # Step 2: Collect target folder structure
    logging.info("Collecting target folder structure...")
    target_structure = collect_structure(root_dst, is_source=False)

    # Step 3: Save both structures to folder_list.txt
    log_file_path = f'{log_folder}/{start_datetime}_folder_list.txt'
    try:
        with open(log_file_path, filemode, encoding='utf-8') as f:  # Use filemode for write/append
            f.write(f"ST,DRIVE,PATH\n")
            for path in sorted(source_list):
                f.write(f"SOURCE,{root_src},{path}\n")
            for path in sorted(target_structure):
                f.write(f"TARGET,{root_dst},{path}\n")
        logging.info(f"Saved folder structures to {log_file_path}")
    except Exception as e:
        logging.error(f"Failed to save folder lists: {str(e)}")

    # Step 4: Compare and identify differences
    to_create = sorted(source_list - target_structure)  # Folders missing on target
    to_delete = sorted(target_structure - source_list)  # Extra folders on target

    if dry_run:
        logging.info("Dry run mode: No folder creations or deletions performed.")
        return

    # # Step 5: Perform creations (mkdir for each missing folder)
    # currently off - its'not needed, as rclone copy will create folders automatically  
    # logging.info("Starting folder creations...")
    # created_count = 0
    # for rel_path in to_create:
    #     full_dst_path = f"{root_dst}/{rel_path}".rstrip('/')  # Avoid trailing slash issues
    #     try:
    #         mkdir(rclone, full_dst_path)  # Reuse your mkdir helper
    #         created_count += 1
    #     except Exception as e:
    #         logging.error(f"Failed to create {full_dst_path}: {str(e)}")
    # logging.info(f"Created {created_count} folders successfully.")

    # Step 6: Perform deletions (deepest first to avoid parent-child conflicts)
    logging.info(f"Starting folder deletions purging")
    # Sort deepest first (by number of path components)
    sorted_to_delete = sorted(to_delete, key=lambda p: len(p.split('/')), reverse=True)
    deleted_count = 0
    for rel_path in sorted_to_delete:
        full_dst_path = f"{root_dst}/{rel_path}".rstrip('/')
        try:
            result = rclone.impl._run(['purge', full_dst_path], check=True, capture=True)
            logging.info(f"Purged folder and contents: {full_dst_path}")            
            deleted_count += 1
        except Exception as e:
            error_msg = str(e).lower()
            # Try to capture stderr if available
            try:
                result = e.__context__  # Access underlying result if possible
                if hasattr(result, 'stderr'):
                    error_msg += f" (stderr: {result.stderr})"
            except:
                pass
            if "directory not empty" in error_msg:
                logging.warning(f"Skipped non-empty folder {full_dst_path}: {error_msg}")
            elif "not found" in error_msg or "exit status 3" in error_msg:
                logging.warning(f"Folder already deleted or not found {full_dst_path}: {error_msg}")
            else:
                logging.error(f"Failed to delete {full_dst_path}: {error_msg}")
    logging.info(f"Purged {deleted_count} folders successfully.")

    logging.info("Folder structure sync completed.")



def large_folder_backup_with_analysis(root_src: str, root_dst: str, dry_run: bool = True):
    # Automatically detect rclone.conf on Windows (adjust for other OS if needed)
    appdata_path = os.environ.get('APPDATA')
    if not appdata_path:
        raise ValueError("APPDATA environment variable not found. Set it or hardcode the config path.")
    
    config_path = Path(appdata_path) / 'rclone' / 'rclone.conf'
    if not config_path.exists():
        raise FileNotFoundError(f"rclone.conf not found at {config_path}. Create it via 'rclone config'.")
    
    rclone = Rclone(config_path)
    
    # Step 1: Change analysis phase - compute all differences upfront and create diffs variable
    logging.info("Starting change analysis phase: Computing all differences...")
    try:
        diffs = list(rclone.diff(
            src=root_src,
            dst=root_dst,
            diff_option=DiffOption.COMBINED,
            fast_list=True,
            size_only=True,
            checkers=64
        ))
    except Exception as e:
        logging.error(f"Error during diff computation: {str(e)}")
        raise

    # logging.info(f"diffs: {diffs}")    
    
    # Log planned actions from diffs
    logging.info("-" * 100)
    logging.info("Planned actions based on differences:")
    if diffs:
        logging.info("")
        for item in sorted(diffs, key=lambda x: (str(x.type).lower(), x.path.lower())):
            match item.type:
                case DiffType.MISSING_ON_DST:
                    logging.info(f"NEWFILE {item.path}")
                case DiffType.DIFFERENT:
                    logging.info(f"CHANGED {item.path}")
                case DiffType.MISSING_ON_SRC:
                    logging.info(f"DELETED {item.path}")
                case DiffType.EQUAL:
                    pass
                case _:
                    logging.info(f"UNKNOWN {item.path}")
    logging.info("-" * 100)

    logging.info("Differentiation analysis complete.")

    if dry_run:
        logging.info("Dry run mode: No actual operations performed (analysis only).")
        return
    
    # Step 2: Traverse folders using the diffs variable and perform copy/deletion on folder level
    def perform_sync():
        logging.info("Starting sync operations using diffs only")

        # Collect files to copy/update and delete from diffs
        to_copy = []
        to_delete = []
        for item in diffs:
            if item.type == DiffType.MISSING_ON_DST or item.type == DiffType.DIFFERENT:
                to_copy.append(item.path)
            elif item.type == DiffType.MISSING_ON_SRC:
                to_delete.append(f"{root_dst}/{item.path}")

        # Log planned actions
        logging.info(f"Planned copies/updates: {len(to_copy)} files")
        logging.info(f"Planned deletions: {len(to_delete)} files")

        # Collect and create necessary parent directories for copies (derived from to_copy paths)
        from pathlib import Path
        needed_dirs = set()
        for path in to_copy:
            p = Path(path)
            for parent in p.parents:
                if str(parent) != '.':
                    needed_dirs.add(f"{root_dst}/{parent}")


        # Perform copies if any
        if to_copy:
            logging.info(f"Copying {len(to_copy)} files...")
            try:
                copy_results = rclone.copy_files(
                    src=root_src,
                    dst=root_dst,
                    files=to_copy,
                    check=user_check,
                    transfers=user_transfers,
                    checkers=user_checkers,
                    multi_thread_streams=user_multi_thread_streams,
                    low_level_retries=user_low_level_retries,
                    retries=user_retries,
                    retries_sleep=user_retries_sleep,
                    timeout=user_timeout,
                    max_backlog=user_max_backlog,
                    other_args=["--progress", "--stats=1m"]
                )
                failed = [res for res in copy_results if res.returncode != 0]
                if failed:
                    logging.error(f"{len(failed)} files failed to copy. Details: {failed}")
                else:
                    logging.info("Copy completed successfully.")
            except Exception as e:
                logging.error(f"Copy operation failed: {str(e)}")

        # Perform deletions if any
        if to_delete:
            logging.info(f"Deleting {len(to_delete)} files...")
            try:
                delete_result = rclone.delete_files(
                    files=to_delete,
                    rmdirs=True,  # Remove empty directories after deletions
                    verbose=True,
                    other_args=["--progress"]
                )
                if delete_result.returncode != 0:
                    logging.error(f"Deletions failed: {delete_result.stderr}")
                else:
                    logging.info("Deletions completed successfully.")
            except Exception as e:
                logging.error(f"Deletion operation failed: {str(e)}")


    # Perform phase (if not overall dry-run)
    if not dry_run:
        logging.info("Starting sync operations using diffs...")
        perform_sync()
        logging.info("Sync completed.")
    else:
        logging.info("Dry run mode: No actual operations performed (analysis only).")        

# ----------------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Rclone sync script with configurable TOML file')
    parser.add_argument('--config', '-c', default='rclone_papi_config.toml', help='Path to TOML configuration file (default: rclone_papi_config.toml)')
    args = parser.parse_args()
    config_file = args.config
    
    # load configuration with the specified file
    try:
        with open(config_file, 'r') as f:
            config = toml.load(f)
        
        # Extract user input
        csv_file = config['csv_file']
        log_folder = config['log_folder']

        # Extract tweaking parameters
        user_check = config['user_check']
        user_transfers = config['user_transfers']
        user_checkers = config['user_checkers']
        user_multi_thread_streams = config['user_multi_thread_streams']
        user_low_level_retries = config['user_low_level_retries']
        user_retries = config['user_retries']
        user_retries_sleep = config['user_retries_sleep']
        user_timeout = config['user_timeout']
        user_max_backlog = config['user_max_backlog']

    except FileNotFoundError:
        print(f"Config file {config_file} not found.")
        logging.warning(f"Config file {config_file} not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error loading config file {config_file}: {e}")
        logging.error(f"Error loading config file {config_file}: {e}")
        sys.exit(1)


    logging.info(f"-" * 200)
    logging.info(f"START SYNCING FOLDERS FROM CSV FILE: {csv_file}")
    logging.info(f"Using config file: {config_file}")
    logging.info(f"-" * 200)
    # Read source-target pairs from CSV file
    folders_to_sync = []
    
    try:
        with open(csv_file, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)  # Skip header row (source, target)
            for row in reader:
                if len(row) < 2 or not row[0].strip() or not row[1].strip():
                    continue  # Skip empty/invalid rows
                src = row[0].strip().replace('\\', '/')  # Normalize to forward slashes
                dst = row[1].strip().strip('"')  # Strip quotes and whitespace
                folders_to_sync.append((src, dst))

       
        if not folders_to_sync:
            logging.warning("No valid source-target pairs found in the CSV file.")
        else:
            logging.info(f"Folders to process:")
            for src, dst in folders_to_sync:
                logging.info(f"Source: {src}")
                logging.info(f"Destination: {dst}")
                logging.info("")

            for src, dst in folders_to_sync:
                logging.info(f"-" * 200)
                logging.info(f"Processing source: {src} -> destination: {dst}")
                complete_list_check(src, dst, dry_run=False)  # Set dry_run=False to perform actions
                large_folder_backup_with_analysis(src, dst, dry_run=False)  # Set dry_run=False to perform actions
    except Exception as e:
        logging.error(f"Error reading or processing CSV file: {str(e)}")
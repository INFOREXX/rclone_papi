"""
Simple Check of the File Structure using RClone Python API

Purpose:
This script compares file structures between source and destination locations
using the RClone Python API. It performs detailed analysis of differences including:
- Files missing in target
- Files missing in source
- Files with different sizes or modification times

The script reads source-destination pairs from a CSV file and generates detailed logs
and reports of all differences found.

Features:
- Automatic rclone.conf detection from APPDATA
- Detailed logging with timestamps
- CSV output of differences
- Support for multiple source-destination pairs
- File attribute comparison (size, modification time)

Requirements:
- rclone_api Python package
- Valid rclone.conf configuration
- CSV file with source,destination pairs

"""
from rclone_api import Rclone
from pathlib import Path
import logging
import os
from datetime import datetime
import csv

# USER INPUT
csv_file = 'rclone_papi_folder_list.csv'  # Path to your CSV file
log_folder = 'log'  # You can change this to a full path if needed, e.g., '/path/to/log'
# -----------------------------------------------------------------------

start_datetime = datetime.now().strftime('%Y-%m-%d-%H%M%S')

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
    filename=f'{log_folder}/{start_datetime}_st_check.log.txt',  # Redirect logs to this file
    filemode='w'  # 'w' to overwrite each run; change to 'a' to append    
)

# Custom logging handler to capture warnings and multi-line errors
import warnings
class RcloneWarningFilter(logging.Filter):
    def filter(self, record):
        if "UserWarning" in record.msg or "NOTICE" in record.msg:
            return True
        return super().filter(record)

def complete_file_list_check(root_src: str, root_dst: str, dry_run: bool = True):
    """
    Function to collect and compare full folder and file structures of source and target,
    log differences, and perform necessary folder creation or deletion on the target.
    
    Args:
        root_src (str): Source path for comparison.
        root_dst (str): Destination path for comparison and operations.
        dry_run (bool): If True, only analyze and log without performing operations.
    """
    # Automatically detect rclone.conf
    appdata_path = os.environ.get('APPDATA')
    if not appdata_path:
        raise ValueError("APPDATA environment variable not found. Set it or hardcode the config path.")
    
    config_path = Path(appdata_path) / 'rclone' / 'rclone.conf'
    if not config_path.exists():
        raise FileNotFoundError(f"rclone.conf not found at {config_path}. Create it via 'rclone config'.")
    
    rclone = Rclone(config_path)
    
    # Helper function to perform lsjson via _run (since lsjson method is not directly available)
    def rclone_lsjson(path: str, recursive: bool = True, files_only: bool = True, hash: bool = True):
        cmd = ['lsjson', path]
        if recursive:
            cmd.append('--recursive')
        if files_only:
            cmd.append('--files-only')
        # Note: To include both files and dirs, omit --files-only and --dirs-only (default behavior)
        if hash:
            cmd.append('--hash')
        try:
            result = rclone.impl._run(cmd, check=True, capture=True)
            # Assuming the output is JSON, parse it
            import json
            return json.loads(result.stdout)
        except Exception as e:
            logging.error(f"Failed to run lsjson on {path}: {str(e)}")
            raise
    
    # Step 1: Collect full structure of source and target with attributes
    logging.info("Collecting full structure of source and target...")
    
    try:
        # Get source structure (include both files and dirs)
        source_list = rclone_lsjson(root_src, recursive=True, files_only=True, hash=True)
        source_structure = []
        for item in source_list:
            path = item.get('Path', '')
            mod_time = item.get('ModTime', '')
            size = item.get('Size', -1)
            crc = item.get('Hashes', {}).get('CRC-32', '') if 'Hashes' in item else ''
            source_structure.append({
                'path': path,
                'mod_time': mod_time,
                'size': size,
                'crc': crc,
                'is_dir': item.get('IsDir', False)
            })
        
        # Get target structure (include both files and dirs)
        target_list = rclone_lsjson(root_dst, recursive=True, files_only=True, hash=True)
        target_structure = []
        for item in target_list:
            path = item.get('Path', '')
            mod_time = item.get('ModTime', '')
            size = item.get('Size', -1)
            crc = item.get('Hashes', {}).get('CRC-32', '') if 'Hashes' in item else ''
            target_structure.append({
                'path': path,
                'mod_time': mod_time,
                'size': size,
                'crc': crc,
                'is_dir': item.get('IsDir', False)
            })
       
        logging.info(f"Collected {len(source_structure)} items from source.")
        logging.info(f"Collected {len(target_structure)} items from target.")
        
        # Step 2: Save structures to file
        with open(f'{log_folder}/{start_datetime}_st_check_filelist.txt', 'w', encoding='utf-8') as f:
            f.write("DRIVE,PATH,MODTIME,SIZE,CRC,ISDIR\n")
            for item in sorted(source_structure, key=lambda x: x['path'].lower()):
                f.write(f"SOURCE,{item['path']},{item['mod_time']},{item['size']},{item['crc']},{item['is_dir']}\n")
            for item in sorted(target_structure, key=lambda x: x['path'].lower()):
                f.write(f"TARGET,{item['path']},{item['mod_time']},{item['size']},{item['crc']},{item['is_dir']}\n")
        logging.info(f"Saved source and target structures to {log_folder}/{start_datetime}_st_check_filelist.txt")
        
        
        # Step 3: Compare structures to identify differences
        logging.info("Comparing source and target structures...")
        
        # Create dictionaries for faster lookup
        source_dict = {item['path']: item for item in source_structure}
        target_dict = {item['path']: item for item in target_structure}
        
        # Get all unique paths
        all_paths = set(source_dict.keys()) | set(target_dict.keys())

        differences = []
        
        for path in sorted(all_paths):
            source_item = source_dict.get(path)
            target_item = target_dict.get(path)
            
            if source_item and not target_item:
                # File/folder exists in source but not in target
                differences.append({
                    'type': 'MISSING_IN_TARGET',
                    'path': path,
                    'source_size': source_item['size'],
                    'source_modtime': source_item['mod_time'],
                    'target_size': None,
                    'target_modtime': None
                })
            elif target_item and not source_item:
                # File/folder exists in target but not in source
                differences.append({
                    'type': 'MISSING_IN_SOURCE',
                    'path': path,
                    'source_size': None,
                    'source_modtime': None,
                    'target_size': target_item['size'],
                    'target_modtime': target_item['mod_time']
                })
            elif source_item and target_item:
                # File/folder exists in both, check for differences
                size_different = source_item['size'] != target_item['size']
                # Parse mod_time strings and compare only up to seconds precision
                try:
                    source_dt = datetime.fromisoformat(source_item['mod_time'].replace('Z', '+00:00'))
                    target_dt = datetime.fromisoformat(target_item['mod_time'].replace('Z', '+00:00'))
                    # Compare only year, month, day, hour, minute, second (ignore microseconds)
                    source_truncated = source_dt.replace(microsecond=0)
                    target_truncated = target_dt.replace(microsecond=0)
                    modtime_different = source_truncated != target_truncated
                except (ValueError, TypeError):
                    # Fallback to string comparison if parsing fails
                    modtime_different = source_item['mod_time'] != target_item['mod_time']
                
                if size_different or modtime_different:
                    differences.append({
                        'type': 'DIFFERENT',
                        'path': path,
                        'source_size': source_item['size'],
                        'source_modtime': source_item['mod_time'],
                        'target_size': target_item['size'],
                        'target_modtime': target_item['mod_time'],
                        'size_different': size_different,
                        'modtime_different': modtime_different
                    })
        
        # Log differences
        logging.info(f"Found {len(differences)} differences between source and target:")
        logging.info("-" * 100)
        
        if differences:
            for diff in differences:
                if diff['type'] == 'MISSING_IN_TARGET':
                    logging.info(f"NEW FILE: {diff['path']} (Size: {diff['source_size']}, ModTime: {diff['source_modtime']})")
                elif diff['type'] == 'MISSING_IN_SOURCE':
                    logging.info(f"DELETED: {diff['path']} (Size: {diff['target_size']}, ModTime: {diff['target_modtime']})")
                elif diff['type'] == 'DIFFERENT':
                    changes = []
                    if diff['size_different']:
                        changes.append(f"Size: {diff['source_size']} -> {diff['target_size']}")
                    if diff['modtime_different']:
                        changes.append(f"ModTime: {diff['source_modtime']} -> {diff['target_modtime']}")
                    logging.info(f"CHANGED: {diff['path']} ({', '.join(changes)})")
        else:
            logging.info("No differences found - source and target are identical!")
        
        logging.info("-" * 100)
        
        # Save differences to file
        with open(f'{log_folder}/{start_datetime}_st_check_difflist.txt', 'w', encoding='utf-8') as f:
            f.write("TYPE,PATH,SOURCE_SIZE,SOURCE_MODTIME,TARGET_SIZE,TARGET_MODTIME,SIZE_DIFF,MODTIME_DIFF\n")
            for diff in differences:
                size_diff = diff.get('size_different', False)
                modtime_diff = diff.get('modtime_different', False)
                f.write(f"{diff['type']},{diff['path']},{diff['source_size']},{diff['source_modtime']},{diff['target_size']},{diff['target_modtime']},{size_diff},{modtime_diff}\n")

        logging.info(f"Saved differences to {log_folder}/{start_datetime}_st_check_difflist.txt")


    except Exception as e:
        logging.error(f"Error in complete_list_check: {str(e)}")
        raise




if __name__ == "__main__":
    logging.info(f"-" * 300)
    logging.info(f"START SYNCING FOLDERS FROM CSV FILE: {csv_file}")
    logging.info(f"-" * 300)
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
                logging.info(f"-" * 300)
                logging.info(f"Processing source: {src} -> destination: {dst}")
                complete_file_list_check(src, dst, dry_run=False)  # Set dry_run=False to perform actions

    except Exception as e:
        logging.error(f"Error reading or processing CSV file: {str(e)}")
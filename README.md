# rclone_papi
Backup control scripts using the rclone-api library

The purpose of the scripts is to provide control over backups to backup drives such as OneDrive, AWS and etc.

### Terminology
source - folder to be backed up
target = backup folder


### Scripts:
- Compare source and target folders.
- Generate csv of differences between source and target folders on the file level.
- Run backup to the target.
- Perform backup on many pairs: source=:target as needed, (defined in the CSV file).
- Log all changes to be performed on the target.
- Purge folders on the target even if they contain many files.
- List currently handled files by backup.

Python scripts are using the rclone-api library https://pypi.org/project/rclone-api/

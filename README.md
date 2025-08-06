# rclone_papi - rclone python backup scripts
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



Scripts were tested on Windows11 backuping up files to OneDrive personal and business


# Need a Custom Feature or Special Version?

We’re always open to collaboration and new ideas! If you need a custom version, a specific feature, or a similar solution tailored to your requirements, please don’t hesitate to reach out.
Simply send an email to hello@inforexx.com with your request, including as much detail as possible. We’ll be happy to discuss how we can help!



Python scripts are using the rclone-api library https://pypi.org/project/rclone-api/


Powered by [INFOREXX](https:www.inforexx.com)
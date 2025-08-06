REM prerequisit: you need to have created rclone OnDrive OneDrive_T1 already

rclone config delete OneDrive_T1_crypt --progress --verbose

rclone config create OneDrive_T1_crypt crypt remote=OneDrive_T1: filename_encryption=off directory_name_encryption=false suffix=. password="YOUR_STRONG_PASSOWORD_1" password2="YOUR_STRONG_PASSOWORD_2"

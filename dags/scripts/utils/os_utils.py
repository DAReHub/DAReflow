import os
import shutil
from datetime import datetime, timezone
import re
import stat


def remove_cached_data(base_dir, days_to_keep):
    # Updated regex pattern to match the new directory name format
    DIR_NAME_PATTERN = re.compile(r'(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\.\d{3})')

    # Get the current time in UTC (timezone-aware)
    now = datetime.now(timezone.utc)

    for root, dirs, files in os.walk(base_dir):
        for dir_name in dirs:
            # Check if the directory name matches the desired pattern
            match = DIR_NAME_PATTERN.match(dir_name)
            if not match:
                continue

            # Extract timestamp from directory name
            dir_timestamp_str = match.group(1)  # The first capturing group is the timestamp
            dir_timestamp = datetime.strptime(dir_timestamp_str, '%Y-%m-%d_%H-%M-%S.%f')

            # If dir_timestamp is offset-naive (without timezone), make it UTC
            if dir_timestamp.tzinfo is None:
                dir_timestamp = dir_timestamp.replace(tzinfo=timezone.utc)

            # Check if the directory is older than the retention period
            if (now - dir_timestamp).days > days_to_keep:
                dir_path = os.path.join(root, dir_name)

                print(f"Changing permissions for directory: {dir_path}")
                # Change permissions for all files and subdirectories
                for root_dir, subdirs, subfiles in os.walk(dir_path):
                    for file_name in subfiles:
                        file_path = os.path.join(root_dir, file_name)
                        # Make file writable by the user
                        os.chmod(file_path, stat.S_IWUSR | stat.S_IREAD)

                    for sub_dir_name in subdirs:
                        sub_dir_path = os.path.join(root_dir, sub_dir_name)
                        # Writable and executable
                        os.chmod(sub_dir_path, stat.S_IWUSR | stat.S_IREAD | stat.S_IEXEC)

                print(f"Removing directory: {dir_path}")
                shutil.rmtree(dir_path)
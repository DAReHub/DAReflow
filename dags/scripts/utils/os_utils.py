import os
import shutil
from datetime import datetime, timezone
import re
from dateutil import parser


# def remove_cached_data(base_dir, days_to_keep):
#     DIR_NAME_PATTERN = re.compile(r'(.+)__([0-9T:+-]+)')
#
#     # Get the current time in UTC (timezone-aware)
#     now = datetime.now(timezone.utc)
#
#     for root, dirs, files in os.walk(base_dir):
#         for dir_name in dirs:
#
#             if not any(s in dir_name for s in ['manual__', 'scheduled__', 'dataset_triggered__', 'backfill__']):
#                 continue
#
#             match = DIR_NAME_PATTERN.match(dir_name)
#             if not match:
#                 continue
#
#             # Extract timestamp from directory name
#             dir_timestamp_str = match.group(2)
#             dir_timestamp = parser.parse(dir_timestamp_str)
#
#             # If dir_timestamp is offset-naive (without timezone), make it UTC
#             if dir_timestamp.tzinfo is None:
#                 dir_timestamp = dir_timestamp.replace(tzinfo=timezone.utc)
#
#             # Check if the directory is older than the retention period
#             if (now - dir_timestamp).days > days_to_keep:
#                 dir_path = os.path.join(root, dir_name)
#                 print(f"Removing directory: {dir_path}")
#                 shutil.rmtree(dir_path)

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
                print(f"Removing directory: {dir_path}")
                shutil.rmtree(dir_path)
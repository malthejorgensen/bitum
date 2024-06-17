#!/usr/bin/env python
import argparse
from collections import defaultdict
import os
from pathlib import Path
import re
import secrets
import sqlite3
import string
import tempfile

from constants import DATABASE_FILENAME
from debug_cli import (
    build,
    check_sizes,
    diff_local,
    download_all,
    extract_single_file,
    integrity,
    upload_all,
)
from utils import (
    DirEntry,
    TimedMessage,
    build_bucket,
    chunks,
    dirtree_from_db,
    dirtree_from_disk,
    get_s3_client,
    pp_file_size,
)

"""
bitum
-----

It works like this:

1. Split files into groups by size

2. Write file which is a concatenation of all files in each group
   - possibly compressed (ideally across the whole group)
   - possibly spaced out at equidistant parts (with padding in between)

3. Write to database (e.g. SQLite) where each file is stored along with metadata (modified date, MD5/SHA256)
"""


def _bucket_name():
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for i in range(8))


def _build_buckets(target_dir, source_dir, files):
    BUCKET_SIZE = 100 * 2**20  # 100 MiB

    buckets = []
    with TimedMessage('Building buckets...'):
        # for (file_path, file_type, file_hash, file_size, file_perms) in set_tree1:
        current_bucket_size = 0
        current_bucket = []
        for file_props in files:
            if file_props.file_size is None:
                continue

            if current_bucket_size + file_props.file_size > BUCKET_SIZE:
                current_bucket_size = 0
                buckets.append((_bucket_name(), current_bucket, current_bucket_size))

            current_bucket.append(file_props)
            current_bucket_size += file_props.file_size

        buckets.append((_bucket_name(), current_bucket, current_bucket_size))

    num_files = 0
    total_size = 0
    for bucket_name, bucket_file_list, bucket_size in buckets:
        print(
            f'{bucket_name}: {len(bucket_file_list)} files ({pp_file_size(bucket_size)})'
        )

        num_files += len(bucket_file_list)
        total_size += bucket_size
    print(f'Total: {num_files} files ({pp_file_size(total_size)})')

    #######################
    # Build bitumen files #
    #######################
    db_entries = []
    with TimedMessage('Building bitumen files...'):
        print()
        for bucket_name, bucket_file_list, bucket_size in buckets:
            db_entries += build_bucket(dir, bucket_name, bucket_file_list)
        print()

    with TimedMessage('Building bitumen database...'):
        con = sqlite3.connect(DATABASE_FILENAME)
        cur = con.cursor()
        cur.execute(
            'CREATE TABLE IF NOT EXISTS files(bucket, file_path PRIMARY KEY, byte_index, file_size, file_hash, file_perms)'
        )
        cur.executemany(
            'INSERT OR REPLACE INTO files VALUES(?, ?, ?, ?, ?, ?)', db_entries
        )
        con.commit()  # Remember to commit the transaction after executing INSERT.
        con.close()

    return buckets


def download_backup_file(args, db_filepath, filepath):
    # type: (None, str, str) -> None
    "Downloads a file from inside a .bitumen-file by doing an HTTP Range request"
    s3_client = get_s3_client(args.endpoint_url)

    prefix = args.prefix
    if prefix and not prefix.endswith('/'):
        prefix = f'{prefix}/'

    con = sqlite3.connect(db_filepath)
    con.row_factory = sqlite3.Row  # Allow accessing results by column name
    cur = con.cursor()
    cur.execute(
        'SELECT bucket, file_path, byte_index, file_size, file_hash, file_perms FROM files WHERE file_path = ?',
        [filepath],
    )
    row = cur.fetchone()
    con.close()

    # All methods below are from: https://stackoverflow.com/questions/30075978/reading-part-of-a-file-in-s3-using-boto
    # Method 1:
    #
    #     obj = boto3.resource('s3').Object('mybucket', 'mykey')
    #     stream = obj.get(Range='bytes=32-64')['Body']
    #     print(stream.read())
    #
    # Method 2:
    #
    #     s3 = boto.connect_s3()
    #     bucket = s3.lookup('mybucket')
    #     key = bucket.lookup('mykey')
    #     your_bytes = key.get_contents_as_string(headers={'Range': 'bytes=73-1024'})
    #
    # Method 3:
    byte_start = row['byte_index']
    byte_end = byte_start + row['file_size'] - 1
    bytes_range = f'bytes={byte_start}-{byte_end}'
    s3_path = f'{prefix}{row["bucket"]}.bitumen'

    # `filepath` can start with a `/`. When `os.path.join()`
    # sees this, it ignores all preceding arguments and just starts the
    # path there, which is not what we want. Therefore the `.lstrip()`.
    with open(os.path.join(args.dir, filepath.lstrip('/')), 'wb') as f_disk:
        response = s3_client.get_object(
            Bucket=args.bucket, Key=s3_path, Range=bytes_range
        )
        body = response['Body']
        # The simplest solution can incur large memory usage for multi GiB-files:
        #
        #     f_disk.write(body.read())
        #
        # Instead we do a chunked read and write:
        while True:
            data = body.read(2**14)  # Read 16 KiB
            if not data:
                break
            f_disk.write(data)


def set_disk_file_perms(args, db_filepath, filepath):
    # type: (None, str, str) -> None

    con = sqlite3.connect(db_filepath)
    con.row_factory = sqlite3.Row  # Allow accessing results by column name
    cur = con.cursor()
    cur.execute(
        'SELECT bucket, file_path, byte_index, file_size, file_hash, file_perms FROM files WHERE file_path = ?',
        [filepath],
    )
    row = cur.fetchone()
    con.close()

    disk_filepath = os.path.join(args.dir, filepath.lstrip('/'))
    os.chmod(disk_filepath, row['file_perms'])


def download(args, tempdir_path):
    "Diffs the remote and local tree and downloads files that have changed in remote"
    s3_client = get_s3_client(args.endpoint_url)

    prefix = args.prefix
    if prefix and not prefix.endswith('/'):
        prefix = f'{prefix}/'

    s3_db_filepath = f'{prefix}{DATABASE_FILENAME}'
    db_filepath = os.path.join(tempdir_path, DATABASE_FILENAME)
    with open(db_filepath, 'wb') as f_db:
        s3_client.download_fileobj(args.bucket, s3_db_filepath, f_db)

    set_tree_disk, tree_disk = dirtree_from_disk(
        args.dir,
        return_sizes=True,  # not args.skip_sizes,
        return_perms=True,  # not args.skip_perms,
        return_hashes=True,  # not args.skip_hashes,
        # exclude_pattern=args.re_exclude,
    )
    set_tree_backup, tree_backup = dirtree_from_db(
        db_filepath,
        return_sizes=True,  # not args.skip_sizes,
        return_perms=True,  # not args.skip_perms,
        return_hashes=True,  # not args.skip_hashes,
    )

    diff = set_tree_disk.symmetric_difference(set_tree_backup)

    if len(set_tree_disk) == 0 and len(set_tree_backup) == 0:
        print('Both DISK and BACKUP are empty')
        return
    elif len(diff) == 0:
        print('No changes! Backup is up-to-date')
        return
    else:
        print(f'{len(diff)} files changed.')

    visited = set()
    for dir_entry in sorted(
        diff,
        key=lambda e: e.file_path + '/' if e.file_type == 'D' else e.file_path,
    ):
        path = dir_entry.file_path
        if path in visited:
            continue
        else:
            visited.add(path)

        if path in tree_disk and path not in tree_backup:
            # remove_disk_file()
            continue
        elif path not in tree_disk and path in tree_backup:
            download_backup_file(args, db_filepath, path)
        elif tree_disk[path].file_size != tree_backup[path].file_size:
            download_backup_file(args, db_filepath, path)
        elif tree_disk[path].file_hash != tree_backup[path].file_hash:
            download_backup_file(args, db_filepath, path)
        elif tree_disk[path].file_perms != tree_backup[path].file_perms:
            # Always change file perms (see below)
            pass

        # Always change file perms
        if tree_backup[path].file_perms is not None:
            set_disk_file_perms(args, db_filepath, path)


def upload(args):
    s3_client = get_s3_client(args.endpoint_url)

    prefix = args.prefix
    if prefix and not prefix.endswith('/'):
        prefix = f'{prefix}/'

    s3_db_filepath = f'{prefix}{DATABASE_FILENAME}'

    local_db_filepath = DATABASE_FILENAME
    try:
        s3_client.head_object(Bucket=args.bucket, Key=s3_db_filepath)
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            # No DB in S3 -- create an empty DB
            if not args.create:
                print(
                    f'No bitum DB was found at "s3://{args.bucket}/{s3_db_filepath}" -- do you want to continue? (Y/n) ',
                    end='',
                    flush=True,
                )
                if input().lower()[0] != 'y':
                    return
            set_tree_backup, tree_backup = (set(), {})
            con = sqlite3.connect(local_db_filepath)
            con.row_factory = sqlite3.Row  # Allow accessing results by column name
            cur = con.cursor()
            cur.execute(
                'CREATE TABLE IF NOT EXISTS files(bucket, file_path PRIMARY KEY, byte_index, file_size, file_hash, file_perms)'
            )
            con.commit()
            con.close()
        else:
            raise
    else:
        # Use DB in S3
        with open(local_db_filepath, 'wb') as f_db:
            s3_client.download_fileobj(args.bucket, s3_db_filepath, f_db)
        set_tree_backup, tree_backup = dirtree_from_db(
            local_db_filepath,
            return_sizes=True,  # not args.skip_sizes,
            # Ignore changes in permissions for now
            return_perms=False,  # not args.skip_perms,
            return_hashes=True,  # not args.skip_hashes,
        )

    re_exclude = re.compile(args.exclude) if args.exclude else None
    set_tree_disk, tree_disk = dirtree_from_disk(
        args.dir,
        return_sizes=True,  # not args.skip_sizes,
        # Ignore changes in permissions for now
        return_perms=False,  # not args.skip_perms,
        return_hashes=True,  # not args.skip_hashes,
        exclude_pattern=re_exclude,
    )

    diff = set_tree_disk - set_tree_backup

    if len(set_tree_disk) == 0 and len(set_tree_backup) == 0:
        print('Both DISK and BACKUP are empty')
        return
    elif len(diff) == 0:
        print('No changes! Backup is up-to-date')
        return

    dir_disk = 'DISK'
    dir_backup = 'BACKUP'

    files_to_upload = set_tree_disk - set_tree_backup

    prefix = args.prefix
    if prefix and not prefix.endswith('/'):
        prefix = f'{prefix}/'

    con = sqlite3.connect(local_db_filepath)
    con.row_factory = sqlite3.Row  # Allow accessing results by column name
    cur = con.cursor()
    # SQLite IN query parameters: https://stackoverflow.com/questions/1309989/parameter-substitution-for-a-sqlite-in-clause
    # - Limit of 999 "?"
    # - You have to make the string of "?, ?, ?, ?, ?" yourself
    affected_buckets = set()
    existing_files = set()
    for files_to_upload_part in chunks(list(files_to_upload), 999):
        questionmarks = '?,' * (len(files_to_upload_part) - 1) + '?'
        cur.execute(
            f'SELECT bucket, file_path, byte_index, file_size, file_hash, file_perms FROM files WHERE file_path IN ({questionmarks})',
            [e.file_path for e in files_to_upload_part],
        )
        rows_buckets = cur.fetchall()

        affected_buckets |= set(row['bucket'] for row in rows_buckets)
        existing_files |= set(row['bucket'] for row in rows_buckets)

    new_files = [e for e in files_to_upload if e.file_path not in existing_files]

    db_entries = []
    for bucket in affected_buckets:
        # Get all files in bucket
        cur.execute(
            'SELECT bucket, file_path, byte_index, file_size, file_hash, file_perms FROM files WHERE bucket = ?',
            [bucket],
        )
        rows = cur.fetchall()

        bucket_file_list = [
            DirEntry(
                file_path=row['file_path'],
                file_type='F',
                file_hash=row['file_hash'],
                file_size=row['file_size'],
                file_perms=row['file_perms'],
            )
            for row in rows
        ]

        db_entries += build_bucket(args.dir, bucket, bucket_file_list)

    cur.executemany('INSERT OR REPLACE INTO files VALUES(?, ?, ?, ?, ?, ?)', db_entries)
    con.commit()  # Remember to commit the transaction after executing INSERT.
    con.close()

    # Handle new files and insert them into the DB
    new_buckets = _build_buckets(args.dir, new_files)

    ################
    # UPLOAD FILES #
    ################
    bucket_files_to_upload = []
    for bucket_name in affected_buckets | set(b[0] for b in new_buckets):
        filename = f'{bucket_name}.bitumen'
        bucket_files_to_upload.append(filename)

    # Always upload DB
    bucket_files_to_upload.append(local_db_filepath)

    for filename in bucket_files_to_upload:
        total_bytes = os.stat(filename).st_size
        s3_path = f'{prefix}{filename}'

        # FROM: https://stackoverflow.com/a/70263266
        with open(filename, 'rb') as f_bitumen:
            with TimedMessage(f'Uploading "{filename}"...'):
                s3_client.upload_fileobj(f_bitumen, args.bucket, s3_path)


def extract(args):
    ###################
    # Build file list #
    ###################
    buckets = defaultdict(list)
    with TimedMessage('Building file list from backup...'):
        # set_tree_backup = set()
        # tree_backup = {}
        con = sqlite3.connect(DATABASE_FILENAME)
        cur = con.cursor()
        cur.execute(
            'SELECT bucket, file_path, byte_index, file_size, file_hash, file_perms FROM files ORDER BY bucket ASC, byte_index ASC'
        )
        rows = cur.fetchall()
        con.close()
        for (
            bucket_name,
            file_path,
            byte_index,
            file_size,
            file_hash,
            file_perms,
        ) in rows:
            buckets[bucket_name].append((byte_index, file_path, file_size, file_perms))

    with TimedMessage('Extracting buckets...'):
        print()
        # for (file_path, file_type, file_hash, file_size, file_perms) in set_tree1:
        for bucket_name, files in buckets.items():
            progress_str = ''
            bytes_written = 0
            current_seek = 0
            with open(f'{bucket_name}.bitumen', 'rb') as f_bitumen:
                for i, (byte_index, file_path, file_size, file_perms) in enumerate(
                    files
                ):
                    if current_seek != byte_index:
                        breakpoint()

                    if i % 1000 == 0:
                        progress_str = f'{i}/{len(files)}\r'
                        print(progress_str, end='', flush=True)

                    # `file_props.file_path` starts with a `/`. When `os.path.join()`
                    # sees this, it ignores all preceding arguments and just starts the
                    # path there, which is not what we want.
                    full_path = Path(args.dir).joinpath(file_path[1:])
                    # Ensure that directory exists
                    parent_dir = full_path.parent
                    parent_dir.mkdir(parents=True, exist_ok=True)
                    # Write file
                    with open(full_path, 'wb') as f_output:
                        current_seek += file_size
                        bytes_written += f_output.write(f_bitumen.read(file_size))

                    # Set file permissions
                    os.chmod(full_path, file_perms)

            print(' ' * len(progress_str) + '\r', end='', flush=True)
        print()


def entry():
    argparser = argparse.ArgumentParser(
        description='Quickly send your files to cloud storage'
    )
    subparsers = argparser.add_subparsers(
        title='command', dest='command', required=True
    )

    upload_cmd = subparsers.add_parser(
        'upload',
        help='Upload changed files to the bucket (overwriting files in the bucket)',
    )
    upload_cmd.add_argument(
        '-e',
        '--exclude',
        help='Exclude files matching this regex',
        metavar='exclude_regex',
    )
    upload_cmd.add_argument(
        '--create',
        action='store_true',
        help="Create `bitumen.sqlite3` if it doesn't exist (bypasses question)",
    )
    download_cmd = subparsers.add_parser(
        'download',
        description='Download changed files from the bucket (overwrite local files)',
    )
    for cmd in [download_cmd, upload_cmd]:
        cmd.add_argument(
            'dir',
            type=str,
            help='Which local directory to upload/download files from/to',
        )

    debug_cmd = subparsers.add_parser(
        'debug',
        description='Access debug commands',
    )
    debug_subcommands = debug_cmd.add_subparsers(
        title='debug_command', dest='debug_command', required=True
    )
    build_cmd = debug_subcommands.add_parser('build')
    build_cmd.add_argument(
        '--dry-run',
        action='store_true',
        help='Only list number of files in buckets. Do not build .bitumen-files.',
    )
    diff_local_cmd = debug_subcommands.add_parser(
        'diff-local', help=f'Diff tree in local {DATABASE_FILENAME} against local files'
    )
    check_sizes_cmd = debug_subcommands.add_parser(
        'check-sizes',
        description='Checks sizes of .bitumen-files in the current folder against the ones at the given prefix in the bucket',
    )
    integrity_cmd = debug_subcommands.add_parser(
        'integrity',
        help='Check integrity between any of "local-files", "local-db", "remote-db", "remote-files"',
    )
    integrity_cmd.add_argument(
        'arg1', choices=['local-files', 'local-db', 'remote-db', 'remote-files']
    )
    integrity_cmd.add_argument(
        'arg2', choices=['local-files', 'local-db', 'remote-db', 'remote-files']
    )
    extract_single_file_cmd = debug_subcommands.add_parser(
        'extract-single-file',
        help='Extracts a single file from .bitumen-files in the current folder',
    )
    extract_single_file_cmd.add_argument('filepath', help='Path of the file to extract')
    upload_all_cmd = debug_subcommands.add_parser(
        'upload-all',
        description=f'Uploads all .bitumen-files in the current folder to the given prefix in the bucket as well as the database file ({DATABASE_FILENAME})',
    )
    download_all_cmd = debug_subcommands.add_parser(
        'download-all',
        description=f'Downloads all .bitumen-files at the given prefix in the bucket as well as the the database file ({DATABASE_FILENAME})',
    )
    for cmd in [upload_all_cmd, download_all_cmd]:
        pass

    for cmd in [
        download_cmd,
        upload_cmd,
        check_sizes_cmd,
        integrity_cmd,
        upload_all_cmd,
        download_all_cmd,
    ]:
        cmd.add_argument(
            '--bucket',
            required=True,
            type=str,
            help='S3-compatible bucket to upload to',
        )
        cmd.add_argument(
            '--prefix',
            type=str,
            help='Prefix inside the bucket to upload or download the files from to',
            default='',
        )
        cmd.add_argument(
            '--endpoint-url',
            type=str,
            help='S3-compatible endpoint URL (e.g. Backblaze "s3.eu-central-003.backblazeb2.com")',
        )

    extract_cmd = subparsers.add_parser('extract')
    extract_cmd.add_argument('dir')

    for cmd in [build_cmd, diff_local_cmd, integrity_cmd]:
        # fmt: off
        cmd.add_argument('dir')
        cmd.add_argument('-s', '--skip-sizes', action='store_true', help='Don\'t store and check file sizes -- this means only checking whether each file exists')
        cmd.add_argument('-p', '--skip-perms', action='store_true', help='Don\'t store and check file permissions')
        cmd.add_argument('-z', '--skip-hashes', action='store_true', help='Don\'t store and check file hashes')
        cmd.add_argument('-d', '--dir-norecurse', action='store_true', help='Show missing directories as a single entry (don\'t show files in the directory)') # noqa: E501
        cmd.add_argument('-e', '--exclude', help='Exclude files matching this regex', metavar='exclude_regex')
        # fmt: on

    args = argparser.parse_args()

    if 'dir' in args and not os.path.exists(args.dir):
        print(f'"{args.dir}" does not exist')
        return

    if args.command == 'debug':
        if args.debug_command == 'build':
            build(args)
        elif args.debug_command == 'diff-local':
            diff_local(args)
        elif args.debug_command == 'check-sizes':
            check_sizes(args)
        elif args.debug_command == 'integrity':
            integrity(args)
        elif args.debug_command == 'extract-single-file':
            extract_single_file(args)
        elif args.debug_command == 'upload-all':
            upload_all(args)
        elif args.debug_command == 'download-all':
            download_all(args)
        else:
            print(f'Unknown debug subcommand {args.debug_command}')
            exit(1)
    elif args.command == 'upload':
        upload(args)
    elif args.command == 'download':
        with tempfile.TemporaryDirectory('wb') as tempdir_path:
            download(args, tempdir_path)
    elif args.command == 'extract':
        extract(args)
    else:
        print(f'Unknown command {args.command}')
        exit(1)


if __name__ == '__main__':
    entry()

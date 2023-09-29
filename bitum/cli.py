#!/usr/bin/env python
import argparse
import configparser
import hashlib
import os
import re
import sqlite3
import time
from collections import defaultdict, namedtuple

import boto3
from tqdm import tqdm

from utils import pp_file_size, print_tree_diff

'''
bitum
-----

It works like this:

1. Split files into groups by size

2. Write file which is a concatenation of all files in each group
   - possibly compressed (ideally across the whole group)
   - possibly spaced out at equidistant parts (with padding in between)

3. Write to database (e.g. SQLite) where each file is stored along with metadata (modified date, MD5/SHA256)
'''

CONFIG_PATH = '~/.config/bitum/config.ini'
DATABASE_FILENAME = 'bitumen.sqlite3'


DirEntry = namedtuple(
    'DirEntry', ['file_path', 'file_type', 'file_hash', 'file_size', 'file_perms']
)
DirEntryProps = namedtuple(
    'DirEntryProps', ['file_type', 'file_hash', 'file_size', 'file_perms']
)


# hash_func=hashlib.md5, block_size=2 ** 20
def file_hash(path, hash_func=hashlib.blake2b, block_size=8192):
    with open(path, 'rb') as f:
        hash_sum = hash_func()
        while True:
            chunk = f.read(block_size)
            if not chunk:
                break
            hash_sum.update(chunk)
    return hash_sum.hexdigest()
    # return hash_sum.digest()


def build_dirtree(
    base_path,
    return_hashes=False,
    return_sizes=False,
    return_perms=False,
    exclude_pattern=None,
):
    '''Build a `set` of tuples for each file under the given filepath

    The tuples are of the form

        (file_path, file_type, file_hash, file_size, file_perms)

    For directories `file_hash` is always `None`.

    From: github.com/malthejorgensen/difftree.
    '''
    tree = dict()
    set_dirtree = set()
    for dirpath, dirnames, filenames in os.walk(base_path):
        dir_entries = [(f, 'F') for f in filenames] + [(d, 'D') for d in dirnames]

        for entry, entry_type in dir_entries:
            abs_path = os.path.join(dirpath, entry)
            rel_path = abs_path[len(base_path) :]

            if exclude_pattern and exclude_pattern.match(rel_path):
                continue

            if entry_type == 'D':
                continue

            stat = os.stat(abs_path)
            file_props = {
                'file_type': entry_type,
                'file_hash': file_hash(abs_path) if return_hashes else None,
                # 'file_size': os.path.getsize(filepath),
                'file_size': stat.st_size
                if return_sizes and entry_type == 'F'
                else None,
                'file_perms': stat.st_mode if return_perms else None,
            }
            dir_entry = DirEntry(
                file_path=rel_path,
                **file_props,
            )
            set_dirtree.add(dir_entry)
            tree[rel_path] = DirEntryProps(**file_props)

    return set_dirtree, tree


BUCKETS = [
    ('256 bytes', 256, [], [0]),
    ('1 KiB', 1024, [], [0]),
    ('4 KiB', 4096, [], [0]),
    ('16 KiB', 2 ** 14, [], [0]),
    ('64 KiB', 2 ** 16, [], [0]),
    ('128 KiB', 2 ** 17, [], [0]),
    ('256 KiB', 2 ** 18, [], [0]),
    ('512 KiB', 2 ** 19, [], [0]),
    ('1 MiB', 2 ** 20, [], [0]),
    ('4 MiB', 2 ** 22, [], [0]),
    ('rest', 2 ** 40, [], [0]),  # 1 TiB
    # ('16 MiB', 2**24, []),
    # ('16 MiB', 2**24, []),
]


class TimedMessage:
    def __init__(self, message):
        self.message = message

    def __enter__(self):
        print(f'{self.message}', end=' ', flush=True)
        self.t_begin = time.time()

    def __exit__(self, *exc_details):
        self.t_end = time.time()
        self.duration = self.t_end - self.t_begin
        print(f'Done ({self.duration:.2f}s)')


def build(args):
    re_exclude = re.compile(args.exclude) if args.exclude else None

    ###################
    # Build file list #
    ###################
    with TimedMessage('Building file list...'):
        set_tree1, tree1 = build_dirtree(
            args.dir,
            return_sizes=not args.skip_sizes,
            return_perms=not args.skip_perms,
            return_hashes=not args.skip_hashes,
            exclude_pattern=re_exclude,
        )

    with TimedMessage('Building buckets...'):
        # for (file_path, file_type, file_hash, file_size, file_perms) in set_tree1:
        for file_props in set_tree1:
            if file_props.file_size is None:
                continue
            for _, bucket_max_size, bucket_file_list, bucket_size in BUCKETS:
                if file_props.file_size <= bucket_max_size:
                    bucket_file_list.append(file_props)
                    bucket_size[0] += file_props.file_size
                    break

    num_files = 0
    total_size = 0
    for bucket_name, bucket_max_size, bucket_file_list, bucket_size in BUCKETS:
        print(
            f'{bucket_name}: {len(bucket_file_list)} files ({pp_file_size(bucket_size[0])})'
        )

        num_files += len(bucket_file_list)
        total_size += bucket_size[0]
    print(f'Total: {num_files} files ({pp_file_size(total_size)})')

    if args.dry_run:
        return 0

    #######################
    # Build bitumen files #
    #######################
    db_entries = []
    with TimedMessage('Building bitumen files...'):
        print()
        for bucket_name, bucket_max_size, bucket_file_list, bucket_size in BUCKETS:
            progress_str = ''
            bytes_written = 0
            with open(f'{bucket_name}.bitumen', 'wb') as f_bitumen:
                for i, file_props in enumerate(bucket_file_list):
                    if i % 1000 == 0:
                        progress_str = f'{i}/{len(bucket_file_list)}\r'
                        print(progress_str, end='', flush=True)
                    # `file_props.file_path` starts with a `/`. When `os.path.join()`
                    # sees this, it ignores all preceding arguments and just starts the
                    # path there, which is not what we want.
                    with open(
                        os.path.join(args.dir, file_props.file_path[1:]), 'rb'
                    ) as f_input:
                        db_entries.append(
                            (
                                bucket_name,
                                file_props.file_path,
                                bytes_written,
                                file_props.file_size,
                                file_props.file_hash,
                                file_props.file_perms,
                            )
                        )
                        bytes_written += f_bitumen.write(f_input.read())
            print(' ' * len(progress_str) + '\r', end='', flush=True)
        print()

    with TimedMessage('Building bitumen database...'):
        con = sqlite3.connect(DATABASE_FILENAME)
        cur = con.cursor()
        cur.execute('DROP TABLE IF EXISTS files')
        cur.execute(
            'CREATE TABLE files(bucket, file_path, byte_index, file_size, file_hash, file_perms)'
        )
        cur.executemany('INSERT INTO files VALUES(?, ?, ?, ?, ?, ?)', db_entries)
        con.commit()  # Remember to commit the transaction after executing INSERT.
        con.close()


def get_s3_client(endpoint_url=None):
    config = configparser.ConfigParser()
    config.read(os.path.expanduser(CONFIG_PATH))

    if not endpoint_url and not config['default'].get('endpoint_url'):
        print(
            f'Must pass either `--endpoint-url` or set `endpoint_url` in {CONFIG_PATH}'
        )
        exit(1)

    session = boto3.session.Session(
        aws_access_key_id=config['default']['access_key_id'],
        aws_secret_access_key=config['default']['secret_access_key'],
        region_name=config['default'].get('region_name', None),
        profile_name=config['default'].get('profile_name', None),
    )
    s3_client = session.client(
        's3',
        endpoint_url=endpoint_url or config['default']['endpoint_url'],
    )

    return s3_client


def upload_all(args):
    s3_client = get_s3_client(args.endpoint_url)

    prefix = args.prefix
    if prefix and not prefix.endswith('/'):
        prefix = f'{prefix}/'

    files = []
    for bucket_name, _, _, _ in BUCKETS:
        filename = f'{bucket_name}.bitumen'
        files.append(filename)

    if args.include_db:
        files.append(DATABASE_FILENAME)

    for filename in files:
        total_bytes = os.stat(filename).st_size
        s3_path = f'{prefix}{filename}'

        # FROM: https://stackoverflow.com/a/70263266
        with open(filename, 'rb') as f_bitumen:
            with tqdm(
                total=total_bytes,
                desc=f'source: s3://{args.bucket}/{s3_path}',
                bar_format="{percentage:.1f}%|{bar:25} | {rate_fmt} | {desc}",
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
            ) as pbar:
                s3_client.upload_fileobj(
                    f_bitumen, args.bucket, s3_path, Callback=pbar.update
                )


def download_all(args):
    s3_client = get_s3_client(args.endpoint_url)

    prefix = args.prefix
    if prefix and not prefix.endswith('/'):
        prefix = f'{prefix}/'

    files = []
    for bucket_name, _, _, _ in BUCKETS:
        filename = f'{bucket_name}.bitumen'
        files.append(filename)

    if args.include_db:
        files.append(DATABASE_FILENAME)

    for filename in files:
        s3_path = f'{prefix}{filename}'

        meta_data = s3_client.head_object(Bucket=args.bucket, Key=s3_path)
        total_bytes = int(meta_data.get('ContentLength', 0))
        with tqdm(
            total=total_bytes,
            desc=f'source: s3://{args.bucket}/{s3_path}',
            bar_format="{percentage:.1f}%|{bar:25} | {rate_fmt} | {desc}",
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
        ) as pbar:
            with open(filename, 'wb') as f_bitumen:
                s3_client.download_fileobj(
                    args.bucket, s3_path, f_bitumen, Callback=pbar.update
                )


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
            buckets[bucket_name].append((byte_index, file_path, file_size))

    with TimedMessage('Extracting buckets...'):
        print()
        # for (file_path, file_type, file_hash, file_size, file_perms) in set_tree1:
        for bucket_name, files in buckets.items():
            progress_str = ''
            bytes_written = 0
            current_seek = 0
            with open(f'{bucket_name}.bitumen', 'rb') as f_bitumen:
                for i, (byte_index, file_path, file_size) in enumerate(files):
                    if current_seek != byte_index:
                        breakpoint()

                    if i % 1000 == 0:
                        progress_str = f'{i}/{len(files)}\r'
                        print(progress_str, end='', flush=True)

                    # `file_props.file_path` starts with a `/`. When `os.path.join()`
                    # sees this, it ignores all preceding arguments and just starts the
                    # path there, which is not what we want.
                    with open(os.path.join(args.dir, file_path[1:]), 'wb') as f_output:
                        current_seek += file_size
                        bytes_written += f_output.write(f_bitumen.read(file_size))

            print(' ' * len(progress_str) + '\r', end='', flush=True)
        print()


def check(args):
    re_exclude = re.compile(args.exclude) if args.exclude else None

    ###################
    # Build file list #
    ###################
    with TimedMessage('Building file list from disk...'):
        set_tree_disk, tree_disk = build_dirtree(
            args.dir,
            return_sizes=not args.skip_sizes,
            return_perms=not args.skip_perms,
            return_hashes=not args.skip_hashes,
            exclude_pattern=re_exclude,
        )

    with TimedMessage('Building file list from backup...'):
        set_tree_backup = set()
        tree_backup = {}
        con = sqlite3.connect(DATABASE_FILENAME)
        cur = con.cursor()
        cur.execute(
            'SELECT bucket, file_path, byte_index, file_size, file_hash, file_perms FROM files'
        )
        rows = cur.fetchall()
        con.close()
        for bucket, file_path, byte_index, file_size, file_hash, file_perms in rows:
            file_props = {
                # fmt: off
                'file_type': 'F',
                'file_hash': file_hash if not args.skip_hashes else None,
                'file_size': file_size if not args.skip_sizes else None, # and entry_type == 'F'
                'file_perms': file_perms if not args.skip_perms else None,
                # fmt: on
            }
            dir_entry = DirEntry(
                file_path=file_path,
                **file_props,
            )
            set_tree_backup.add(dir_entry)
            tree_backup[file_path] = DirEntryProps(**file_props)

    print_tree_diff(args, set_tree_disk, tree_disk, set_tree_backup, tree_backup)


def entry():
    argparser = argparse.ArgumentParser(
        description='Quickly send your files to cloud storage'
    )
    subparsers = argparser.add_subparsers(
        title='command', dest='command', required=True
    )
    build_cmd = subparsers.add_parser('build')
    build_cmd.add_argument(
        '--dry-run',
        action='store_true',
        help='Only list number of files in buckets. Do not build .bitumen-files.',
    )

    upload_all_cmd = subparsers.add_parser(
        'upload-all', description='Upload all .bitumen-files in the current folder'
    )
    download_all_cmd = subparsers.add_parser(
        'download-all',
        description='Download all .bitumen-files at the given prefix in the bucket, or at the root if no prefix is given',
    )
    for cmd in [upload_all_cmd, download_all_cmd]:
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
        cmd.add_argument(
            '--include-db',
            action='store_true',
            help=f'Include file-database ({DATABASE_FILENAME})',
        )

    extract_cmd = subparsers.add_parser('extract')
    extract_cmd.add_argument('dir')
    check_cmd = subparsers.add_parser('check')

    for cmd in [build_cmd, check_cmd]:
        # fmt: off
        cmd.add_argument('dir')
        cmd.add_argument('-s', '--skip-sizes', action='store_true', help='Don\'t store and check file sizes -- this means only checking whether each file exists')
        cmd.add_argument('-p', '--skip-perms', action='store_true', help='Don\'t store and check file permissions')
        cmd.add_argument('-z', '--skip-hashes', action='store_true', help='Don\'t store and check file hashes')
        cmd.add_argument('-d', '--dir-norecurse', action='store_true', help='Show missing directories as a single entry (don\'t show files in the directory)') # noqa: E501
        cmd.add_argument('-e', '--exclude', help='Exclude files matching this regex', metavar='exclude_regex')
        # fmt: on

    args = argparser.parse_args()

    if not os.path.exists(args.dir):
        print(f'"{args.dir}" does not exist')
        return

    if args.command == 'build':
        build(args)
    elif args.command == 'upload-all':
        upload_all(args)
    elif args.command == 'download-all':
        download_all(args)
    elif args.command == 'check':
        check(args)
    elif args.command == 'extract':
        extract(args)
    else:
        print(f'Unknown command {args.command}')
        exit(1)


if __name__ == '__main__':
    entry()

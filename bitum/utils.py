from collections import namedtuple
import configparser
import hashlib
from itertools import cycle
import os
import shutil
import sqlite3
import stat
import time

import boto3

from constants import CONFIG_PATH

DirEntry = namedtuple(
    'DirEntry', ['file_path', 'file_type', 'file_hash', 'file_size', 'file_perms']
)
DirEntryProps = namedtuple(
    'DirEntryProps', ['file_type', 'file_hash', 'file_size', 'file_perms']
)


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


def chunks(l, size):
    # type: (list[T], int) -> list[T]
    i = 0
    while i < len(l):
        yield l[i : i + size]
        i += size


def build_bucket(target_dir, dir, bucket_name, bucket_file_list):
    # type: (str, str, str, list[DirEntry]) -> list[tuple[str, str, int, int, str, str]]
    db_entries = []

    filename = f'{bucket_name}.bitumen'
    full_path = os.path.join(target_dir, filename)

    progress_str = ''
    bytes_written = 0
    with open(full_path, 'wb') as f_bitumen:
        for i, file_props in enumerate(bucket_file_list):
            if i % 1000 == 0:
                progress_str = f'{i}/{len(bucket_file_list)}\r'
                print(progress_str, end='', flush=True)
            # `file_props.file_path` starts with a `/`. When `os.path.join()`
            # sees this, it ignores all preceding arguments and just starts the
            # path there, which is not what we want. Therefore the `.lstrip()`.
            with open(
                os.path.join(dir, file_props.file_path.lstrip('/')), 'rb'
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

    return db_entries


def get_s3_client(endpoint_url=None):
    config = configparser.ConfigParser()
    config.read(os.path.expanduser(CONFIG_PATH))

    if not endpoint_url and not config['default'].get('endpoint_url'):
        print(
            f'Must pass either `--endpoint-url` or set `endpoint_url` in {CONFIG_PATH}'
        )
        exit(1)

    config_dict = config['default'] if 'default' in config else {}

    # Environment variables override config
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID') or config_dict.get(
        'access_key_id'
    )
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY') or config_dict.get(
        'secret_access_key'
    )
    region_name = (
        os.getenv('AWS_REGION')
        or os.getenv('AWS_DEFAULT_REGION')
        or config_dict.get('region_name')
        or None
    )
    profile_name = os.getenv('AWS_PROFILE') or config_dict.get('profile_name') or None

    session = boto3.session.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
        profile_name=profile_name,
    )
    s3_client = session.client(
        's3',
        endpoint_url=endpoint_url or config['default']['endpoint_url'],
    )

    return s3_client


def download_s3_file(s3_client, bucket, s3_path, target_path):
    try:
        from tqdm import tqdm

        has_tqdm = True
    except ImportError:
        has_tqdm = False

    meta_data = s3_client.head_object(Bucket=bucket, Key=s3_path)
    total_bytes = int(meta_data.get('ContentLength', 0))

    with open(target_path, 'wb') as f:
        if has_tqdm:
            with tqdm(
                total=total_bytes,
                desc=f'source: s3://{bucket}/{s3_path}',
                bar_format='{percentage:.1f}%|{bar:25} | {rate_fmt} | {desc}',
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
            ) as pbar:
                s3_client.download_fileobj(bucket, s3_path, f, Callback=pbar.update)
        else:
            s3_client.download_fileobj(bucket, s3_path, f)


def upload_s3_file(s3_client, bucket, s3_path, source_path):
    try:
        from tqdm import tqdm

        has_tqdm = True
    except ImportError:
        has_tqdm = False

    total_bytes = os.stat(source_path).st_size

    with open(source_path, 'rb') as f:
        if has_tqdm:
            # FROM: https://stackoverflow.com/a/70263266
            with tqdm(
                total=total_bytes,
                desc=f'source: s3://{bucket}/{s3_path}',
                bar_format='{percentage:.1f}%|{bar:25} | {rate_fmt} | {desc}',
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
            ) as pbar:
                s3_client.upload_fileobj(f, bucket, s3_path, Callback=pbar.update)
        else:
            s3_client.upload_fileobj(f, bucket, s3_path)


def pp_file_size(size_bytes):
    if size_bytes < 2**10:
        value = size_bytes
        unit = 'bytes'
    elif size_bytes < 2**20:
        value = size_bytes / 2**10
        unit = 'KiB'
    elif size_bytes < 2**30:
        value = size_bytes / 2**20
        unit = 'MiB'
    else:
        value = size_bytes / 2**30
        unit = 'GiB'

    if unit == 'bytes':
        return f'{value} {unit}'
    else:
        return f'{value:.2f} {unit}'


def pp_file_perms(perms):
    CONST_FILE_PERMS = [
        stat.S_IRUSR,
        stat.S_IWUSR,
        stat.S_IXUSR,
        stat.S_IRGRP,
        stat.S_IWGRP,
        stat.S_IXGRP,
        stat.S_IROTH,
        stat.S_IWOTH,
        stat.S_IXOTH,
    ]
    result = ''
    for char, stat_const in zip(cycle(['r', 'w', 'x']), CONST_FILE_PERMS):
        if perms & stat_const != 0:
            result += char
        else:
            result += '-'
    return result


def print_file_diff(path1, op, path2, width, extras1=None, extras2=None):
    if extras1:
        path1 = f'{path1} ({extras1})'
    if extras2:
        path2 = f'{path2} ({extras2})'
        # path2 = path2.ljust(width)

    path1 = path1.ljust(width)
    print(f'{path1} {op} {path2}')


def print_tree_diff(args, set_tree1, tree1, set_tree2, tree2):
    diff = set_tree1 - set_tree2

    if len(set_tree1) == 0 and len(set_tree2) == 0:
        print('Both DISK and BACKUP are empty')
        return
    elif len(diff) == 0:
        print('No changes! Backup is up-to-date')
        return

    dir1 = 'DISK'
    dir2 = 'BACKUP'

    width = max(max(len(e.file_path) for e in diff), len(dir1))
    max_path_length = max(len(e.file_path) for e in diff)
    if not args.skip_perms:
        # Include permissions in width
        max_path_length += len(' (xxxxxxxxx)')
    if not args.skip_hashes:
        # Include hash in width
        max_path_length += len(' (xxxxxx)')
    width = max(max_path_length, len(dir1))
    # Don't go beyond half the width of the terminal
    width = min(width, shutil.get_terminal_size().columns // 2)
    print_file_diff(dir1, '<->', dir2, width)
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

        if path in tree1 and path not in tree2:
            if dir_entry.file_type == 'D':
                path += '/'
            print_file_diff(path, ' ->', '', width)
        elif path not in tree1 and path in tree2:
            if dir_entry.file_type == 'D':
                path += '/'
            print_file_diff('', '<- ', path, width)
        elif tree1[path].file_type != tree2[path].file_type:
            path1 = path
            path2 = path
            if tree1[path].file_type == 'D':
                path1 += '/'
            if tree2[path].file_type == 'D':
                path2 += '/'
            file_type1 = tree1[path].file_type
            file_type2 = tree2[path].file_type
            print_file_diff(
                path1, '<->', path2, width, extras1=file_type1, extras2=file_type2
            )
        elif tree1[path].file_size != tree2[path].file_size:
            file_size1 = pp_file_size(tree1[path].file_size)
            file_size2 = pp_file_size(tree2[path].file_size)
            print_file_diff(
                path, '<->', path, width, extras1=file_size1, extras2=file_size2
            )
        elif tree1[path].file_hash != tree2[path].file_hash:
            file_hash1 = tree1[path].file_hash
            file_hash2 = tree2[path].file_hash
            print_file_diff(
                path, '<->', path, width, extras1=file_hash1[:6], extras2=file_hash2[:6]
            )
        elif tree1[path].file_perms != tree2[path].file_perms:
            file_perms1 = pp_file_perms(tree1[path].file_perms)
            file_perms2 = pp_file_perms(tree2[path].file_perms)
            print_file_diff(
                path, '<->', path, width, extras1=file_perms1, extras2=file_perms2
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


def dirtree_from_disk(
    base_path,
    return_hashes=False,
    return_sizes=False,
    return_perms=False,
    exclude_pattern=None,
):
    # type: (str, bool, bool, bool, re.Pattern | None) -> tuple[set[DirEntry], dict[str, DirEntryProps]]
    """Build a `set` of tuples for each file under the given filepath

    The tuples are of the form

        (file_path, file_type, file_hash, file_size, file_perms)

    For directories `file_hash` is always `None`.

    From: github.com/malthejorgensen/difftree.
    """
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

            try:
                stat = os.stat(abs_path)
            except FileNotFoundError:
                # When symlink points to a directory or file that does not exist
                continue
            except OSError as err:
                if err.errno == 62:
                    # Too many levels of symlinking
                    continue
                else:
                    raise

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


def dirtree_from_db(
    db_filepath,
    return_hashes=False,
    return_sizes=False,
    return_perms=False,
):
    # type: (str, bool, bool, bool) -> tuple[set[DirEntry], dict[str, DirEntryProps]]
    set_tree_backup = set()
    tree_backup = {}
    con = sqlite3.connect(db_filepath)
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
            'file_hash': file_hash if return_hashes else None,
            'file_size': file_size if return_sizes else None,  # and entry_type == 'F'
            'file_perms': file_perms if return_perms else None,
            # fmt: on
        }
        dir_entry = DirEntry(
            file_path=file_path,
            **file_props,
        )
        set_tree_backup.add(dir_entry)
        tree_backup[file_path] = DirEntryProps(**file_props)

    return set_tree_backup, tree_backup

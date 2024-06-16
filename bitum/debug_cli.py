import os
from pathlib import Path
import re
import sqlite3

from constants import BUCKETS, DATABASE_FILENAME
from utils import (
    TimedMessage,
    dirtree_from_db,
    dirtree_from_disk,
    download_s3_file,
    get_s3_client,
    print_tree_diff,
    upload_s3_file,
)


def diff_local(args):
    re_exclude = re.compile(args.exclude) if args.exclude else None

    ###################
    # Build file list #
    ###################
    with TimedMessage('Building file list from disk...'):
        set_tree_disk, tree_disk = dirtree_from_disk(
            args.dir,
            return_sizes=not args.skip_sizes,
            return_perms=not args.skip_perms,
            return_hashes=not args.skip_hashes,
            exclude_pattern=re_exclude,
        )

    with TimedMessage('Building file list from DB...'):
        set_tree_backup, tree_backup = dirtree_from_db(
            DATABASE_FILENAME,
            return_sizes=not args.skip_sizes,
            return_perms=not args.skip_perms,
            return_hashes=not args.skip_hashes,
        )

    print_tree_diff(args, set_tree_disk, tree_disk, set_tree_backup, tree_backup)


def _tree_from_arg(arg, args):
    if arg == 'local-files':
        re_exclude = re.compile(args.exclude) if args.exclude else None

        with TimedMessage('Building file list from disk...'):
            set_tree, tree = dirtree_from_disk(
                args.dir,
                return_sizes=not args.skip_sizes,
                return_perms=not args.skip_perms,
                return_hashes=not args.skip_hashes,
                exclude_pattern=re_exclude,
            )
    elif arg == 'local-db':
        with TimedMessage('Building file list from local DB...'):
            set_tree, tree = dirtree_from_db(
                DATABASE_FILENAME,
                return_sizes=not args.skip_sizes,
                return_perms=not args.skip_perms,
                return_hashes=not args.skip_hashes,
            )
    elif arg == 'remote-db':
        s3_client = get_s3_client(args.endpoint_url)

        prefix = args.prefix
        if prefix and not prefix.endswith('/'):
            prefix = f'{prefix}/'

        s3_path = f'{prefix}{DATABASE_FILENAME}'

        db_filepath = DATABASE_FILENAME
        with open(db_filepath, 'wb') as f_db:
            s3_client.download_fileobj(
                args.bucket, s3_path, f_db
            )  # , Callback=pbar.update

        with TimedMessage('Building file list from remote DB...'):
            set_tree, tree = dirtree_from_db(
                db_filepath,
                return_sizes=not args.skip_sizes,
                return_perms=not args.skip_perms,
                return_hashes=not args.skip_hashes,
            )
    elif arg == 'remote-files':
        raise ValueError('Integrity for `remote-files` not currently supported')

    return set_tree, tree


def integrity(args):
    'Check integrity between any of "local-files", "local-db", "remote-db", "remote-files"'

    set_tree_arg1, tree_arg1 = _tree_from_arg(args.arg1, args)
    set_tree_arg2, tree_arg2 = _tree_from_arg(args.arg2, args)

    print_tree_diff(args, set_tree_arg1, tree_arg1, set_tree_arg2, tree_arg2)


def upload_all(args):
    s3_client = get_s3_client(args.endpoint_url)

    prefix = args.prefix
    if prefix and not prefix.endswith('/'):
        prefix = f'{prefix}/'

    files = []
    for bucket_name, _, _, _ in BUCKETS:
        filename = f'{bucket_name}.bitumen'
        files.append(filename)

    # Always download DB
    files.append(DATABASE_FILENAME)

    for filename in files:
        s3_path = f'{prefix}{filename}'

        upload_s3_file(s3_client, args.bucket, s3_path, filename)


def download_all(args):
    s3_client = get_s3_client(args.endpoint_url)

    prefix = args.prefix
    if prefix and not prefix.endswith('/'):
        prefix = f'{prefix}/'

    files = []
    for bucket_name, _, _, _ in BUCKETS:
        filename = f'{bucket_name}.bitumen'
        files.append(filename)

    # Always download DB
    files.append(DATABASE_FILENAME)

    for filename in files:
        s3_path = f'{prefix}{filename}'

        download_s3_file(s3_client, args.bucket, s3_path, filename)


def check_sizes(args):
    s3_client = get_s3_client(args.endpoint_url)

    prefix = args.prefix
    if prefix and not prefix.endswith('/'):
        prefix = f'{prefix}/'

    files = []
    for bucket_name, _, _, _ in BUCKETS:
        filename = f'{bucket_name}.bitumen'
        files.append(filename)

    # Always download
    files.append(DATABASE_FILENAME)

    for filename in files:
        s3_path = f'{prefix}{filename}'

        try:
            meta_data = s3_client.head_object(Bucket=args.bucket, Key=s3_path)
        except:
            print(f'"{filename}" not found in bucket')
            continue
        remote_filesize = int(meta_data.get('ContentLength'))

        stat = os.stat(os.getcwd() + '/' + filename)
        local_filesize = stat.st_size

        if local_filesize != remote_filesize:
            print(
                f'{filename} {local_filesize} bytes != {filename} {remote_filesize} bytes'
            )


def extract_single_file(args):
    # Ensure `/` at beginning of string
    filepath = '/' + args.filepath.lstrip('/')

    con = sqlite3.connect(DATABASE_FILENAME)
    cur = con.cursor()
    cur.execute(
        """
        SELECT bucket, file_path, byte_index, file_size, file_hash, file_perms
        FROM files
        WHERE file_path = ?
    """,
        [filepath],
    )
    (
        bucket_name,
        file_path,
        byte_index,
        file_size,
        file_hash,
        file_perms,
    ) = cur.fetchone()
    con.close()

    print(
        f'Extracting "{args.filepath}" from {bucket_name}.bitumen at byte index {byte_index}'
    )
    with open(f'{bucket_name}.bitumen', 'rb') as f_bitumen:
        f_bitumen.seek(byte_index)

        # `file_props.file_path` starts with a `/`. When `os.path.join()`
        # sees this, it ignores all preceding arguments and just starts the
        # path there, which is not what we want.
        full_path = Path(args.filepath)
        filename = full_path.name
        # Write file
        with open(filename, 'wb') as f_output:
            bytes_written = f_output.write(f_bitumen.read(file_size))

    assert bytes_written == file_size


print()

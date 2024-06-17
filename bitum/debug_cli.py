import os
from pathlib import Path
import re
import sqlite3

from constants import BUCKETS, DATABASE_FILENAME
from utils import (
    TimedMessage,
    build_bucket,
    dirtree_from_db,
    dirtree_from_disk,
    download_s3_file,
    get_s3_client,
    pp_file_size,
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


def build(args):
    re_exclude = re.compile(args.exclude) if args.exclude else None
    target_dir = '.'

    ###################
    # Build file list #
    ###################
    with TimedMessage('Building file list...'):
        set_tree1, tree1 = dirtree_from_disk(
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
            db_entries += build_bucket(
                target_dir, args.dir, bucket_name, bucket_file_list
            )
        print()

    with TimedMessage('Building bitumen database...'):
        con = sqlite3.connect(DATABASE_FILENAME)
        cur = con.cursor()
        cur.execute('DROP TABLE IF EXISTS files')
        cur.execute(
            'CREATE TABLE files(bucket, file_path PRIMARY KEY, byte_index, file_size, file_hash, file_perms)'
        )
        cur.executemany('INSERT INTO files VALUES(?, ?, ?, ?, ?, ?)', db_entries)
        con.commit()  # Remember to commit the transaction after executing INSERT.
        con.close()


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

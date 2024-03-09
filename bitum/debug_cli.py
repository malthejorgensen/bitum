import os
import re

from tqdm import tqdm

from constants import BUCKETS, DATABASE_FILENAME
from utils import (
    TimedMessage,
    dirtree_from_db,
    dirtree_from_disk,
    get_s3_client,
    print_tree_diff,
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

    set_tree_backup, tree_backup = dirtree_from_db(
        DATABASE_FILENAME,
        return_sizes=not args.skip_sizes,
        return_perms=not args.skip_perms,
        return_hashes=not args.skip_hashes,
    )

    print_tree_diff(args, set_tree_disk, tree_disk, set_tree_backup, tree_backup)


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
        total_bytes = os.stat(filename).st_size
        s3_path = f'{prefix}{filename}'

        # FROM: https://stackoverflow.com/a/70263266
        with open(filename, 'rb') as f_bitumen:
            with tqdm(
                total=total_bytes,
                desc=f'source: s3://{args.bucket}/{s3_path}',
                bar_format='{percentage:.1f}%|{bar:25} | {rate_fmt} | {desc}',
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

    # Always download DB
    files.append(DATABASE_FILENAME)

    for filename in files:
        s3_path = f'{prefix}{filename}'

        meta_data = s3_client.head_object(Bucket=args.bucket, Key=s3_path)
        total_bytes = int(meta_data.get('ContentLength', 0))
        with tqdm(
            total=total_bytes,
            desc=f'source: s3://{args.bucket}/{s3_path}',
            bar_format='{percentage:.1f}%|{bar:25} | {rate_fmt} | {desc}',
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
        ) as pbar:
            with open(filename, 'wb') as f_bitumen:
                s3_client.download_fileobj(
                    args.bucket, s3_path, f_bitumen, Callback=pbar.update
                )

#!/usr/bin/env python
import argparse
import os
import itertools
import time


def dirtree_from_disk(base_path):
    # type: (str) -> tuple[list[str], int]
    total_size = 0
    list_dirtree = list()

    for dirpath, directories, filenames in os.walk(base_path):
        # for entry in directories:
        for entry in itertools.chain(directories, filenames):
            abs_path = os.path.join(dirpath, entry)
            # rel_path = abs_path[len(base_path) :]
            # list_dirtree.append(abs_path)
            list_dirtree.append(entry)

            stat = os.stat(abs_path)
            file_size = stat.st_size
            total_size += file_size
            # file_perms = stat.st_mode if return_perms else None,

        # for entry in filenames:
        #     abs_path = os.path.join(dirpath, entry)
        #     # rel_path = abs_path[len(base_path) :]
        #     # list_dirtree.append(abs_path)
        #     list_dirtree.append(entry)

        #     stat = os.stat(abs_path)
        #     file_size = stat.st_size
        #     total_size += file_size
        #     # file_perms = stat.st_mode if return_perms else None,

    return list_dirtree, total_size


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


def entry():
    argparser = argparse.ArgumentParser(
        description='How fast can Python list recursively all files in?'
    )

    argparser.add_argument('dir')
    args = argparser.parse_args()

    if not os.path.exists(args.dir):
        print(f'"{args.dir}" does not exist')
        return

    file_list, total_size = dirtree_from_disk(args.dir)

    print(len(file_list))
    print(total_size)


if __name__ == '__main__':
    entry()

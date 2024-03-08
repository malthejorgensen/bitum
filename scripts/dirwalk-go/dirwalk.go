package main

import (
    "fmt"
    "os"
    "io/fs"
    "path/filepath"
)


var files = []string{}
var total_size int64 = 0

func visit(path string, di fs.DirEntry, err error) error {
    // fmt.Printf("Visited: %s\n", path)
    files = append(files, path)
    return nil
}

func visit_size(path string, di fs.DirEntry, err error) error {
    // fmt.Printf("Visited: %s\n", path)
    file_info, err := os.Stat(path)
    total_size += file_info.Size()
    files = append(files, path)
    return nil
}


func main() {
    // flag.Parse()
    // root := flag.Arg(0)
	path_dir := os.Args[1]
    fmt.Println(path_dir)
    err := filepath.WalkDir(path_dir, visit_size)
    if (err != nil) {
    	return
    }
    // fmt.Println(len(os.Args), os.Args, path_dir)
    fmt.Println(len(files))
    fmt.Println(total_size)
}


// #!/usr/bin/env python
// import argparse
// import hashlib
// import os
// import re
// import time
// from collections import defaultdict, namedtuple


// '''
// bitum
// -----

// It works like this:

// 1. Split files into groups by size

// 2. Write file which is a concatenation of all files in each group
//    - possibly compressed (ideally across the whole group)
//    - possibly spaced out at equidistant parts (with padding in between)

// 3. Write to database (e.g. SQLite) where each file is stored along with metadata (modified date, MD5/SHA256)
// '''

// DirEntry = namedtuple(
//     'DirEntry', ['file_path', 'file_type', 'file_hash', 'file_size', 'file_perms']
// )
// DirEntryProps = namedtuple(
//     'DirEntryProps', ['file_type', 'file_hash', 'file_size', 'file_perms']
// )


// # hash_func=hashlib.md5, block_size=2 ** 20
// # def file_hash(path, hash_func=hashlib.blake2b, block_size=8192):
// def file_hash(path, hash_func=hashlib.md5, block_size=2 ** 20):
//     with open(path, 'rb') as f:
//         hash_sum = hash_func()
//         while True:
//             chunk = f.read(block_size)
//             if not chunk:
//                 break
//             hash_sum.update(chunk)
//     return hash_sum.hexdigest()
//     # return hash_sum.digest()


// def dirtree_from_disk(
//     base_path,
//     return_hashes=False,
//     return_sizes=False,
//     return_perms=False,
//     exclude_pattern=None,
// ):
//     # type: (str, bool, bool, bool, re.Pattern | None) -> tuple[set[DirEntry], dict[str, DirEntryProps]]
//     '''Build a `set` of tuples for each file under the given filepath

//     The tuples are of the form

//         (file_path, file_type, file_hash, file_size, file_perms)

//     For directories `file_hash` is always `None`.

//     From: github.com/malthejorgensen/difftree.
//     '''
//     tree = dict()
//     set_dirtree = set()
//     for dirpath, dirnames, filenames in os.walk(base_path):
//         dir_entries = [(f, 'F') for f in filenames] + [(d, 'D') for d in dirnames]

//         for entry, entry_type in dir_entries:
//             abs_path = os.path.join(dirpath, entry)
//             rel_path = abs_path[len(base_path) :]

//             # if exclude_pattern and exclude_pattern.match(rel_path):
//             #     continue

//             # if entry_type == 'D':
//             #     continue

//             # stat = os.stat(abs_path)
//             # file_props = {
//             #     'file_type': entry_type,
//             #     'file_hash': file_hash(abs_path) if return_hashes else None,
//             #     # 'file_size': os.path.getsize(filepath),
//             #     'file_size': stat.st_size
//             #     if return_sizes and entry_type == 'F'
//             #     else None,
//             #     'file_perms': stat.st_mode if return_perms else None,
//             # }
//             # dir_entry = DirEntry(
//             #     file_path=rel_path,
//             #     **file_props,
//             # )
//             # set_dirtree.add(dir_entry)
//             # tree[rel_path] = DirEntryProps(**file_props)

//     # return set_dirtree, tree
//     return (None, None)


// class TimedMessage:
//     def __init__(self, message):
//         self.message = message

//     def __enter__(self):
//         print(f'{self.message}', end=' ', flush=True)
//         self.t_begin = time.time()

//     def __exit__(self, *exc_details):
//         self.t_end = time.time()
//         self.duration = self.t_end - self.t_begin
//         print(f'Done ({self.duration:.2f}s)')


// def build(dir):
//     # re_exclude = re.compile(args.exclude) if args.exclude else None

//     ###################
//     # Build file list #
//     ###################
//     with TimedMessage('Building file list...'):
//         set_tree1, tree1 = dirtree_from_disk(
//             dir,
//             # return_sizes=not args.skip_sizes,
//             # return_perms=not args.skip_perms,
//             # return_hashes=not args.skip_hashes,
//             # exclude_pattern=re_exclude,
//         )


// def entry():
//     argparser = argparse.ArgumentParser(
//         description='How fast can Python list recursively all files in?'
//     )
//     # subparsers = argparser.add_subparsers(
//     #     title='command', dest='command', required=True
//     # )

//     # if not os.path.exists(args.dir):
//     #     print(f'"{args.dir}" does not exist')
//     #     return

//     argparser.add_argument('dir')
//     args = argparser.parse_args()

//     build(args.dir)

//     # elif args.command == 'check':
//     #     check(args)
//     # elif args.command == 'extract':
//     #     extract(args)
//     # elif args.command == '_idempotency-check':
//     #     idempotency_check(args)
//     # elif args.command == 'download':
//     #     pass
//     # else:
//     #     print(f'Unknown command {args.command}')
//     #     exit(1)
//     # elif args.command == 'sync':
//     #     pass


// if __name__ == '__main__':
//     entry()

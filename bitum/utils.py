from itertools import cycle
import shutil
import stat


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

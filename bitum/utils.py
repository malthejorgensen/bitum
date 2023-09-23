def pp_file_size(size_bytes):
    if size_bytes < 2 ** 10:
        value = size_bytes
        unit = 'bytes'
    elif size_bytes < 2 ** 20:
        value = size_bytes / 2 ** 10
        unit = 'KiB'
    elif size_bytes < 2 ** 30:
        value = size_bytes / 2 ** 20
        unit = 'MiB'
    else:
        value = size_bytes / 2 ** 30
        unit = 'GiB'

    if unit == 'bytes':
        return f'{value} {unit}'
    else:
        return f'{value:.2f} {unit}'


AWS CLI: Using a different storage provider than S3
---------------------------------------------------
The AWS CLI can be used with other S3-compatible storage providers like
Backblaze's B2 or Cloudflare's R2 by using the `--endpoint-url` argument
and setting `~/.aws/credentials` to the credentials for that provider:

    aws s3 --endpoint-url https://s3.eu-central-003.backblazeb2.com ls malthe/bitum_test/

Alternately, you can set `endpoint_url = <ENDPOINT_URL>` directly in `~/.aws/credentials`
in which case you don't have to pass the argument on the command line.

Joining paths quickly
---------------------
`os.path.join()` is 3 times faster than `pathlib.Path.joinpath()`:

    > python -m timeit --setup 'import os' "os.path.join('/Users/malthe/Programming', 'a/b/c/d')"
    500000 loops, best of 5: 950 nsec per loop
    > python -m timeit --setup 'from pathlib import Path' "Path('/Users/malthe/Programming').joinpath('a/b/c/d')"
    100000 loops, best of 5: 3.2 usec per loop


Creating directories quickly
----------------------------
`os.makedirs()` is slightly faster than `Path.mkdir(parents=True)` but not by a lot (33%).

    > python -m timeit --setup 'import os' "os.makedirs('/Users/malthe/Programming/a/b/c/d', exist_ok=True)"
    20000 loops, best of 5: 12.1 usec per loop
    > python -m timeit --setup 'from pathlib import Path' "Path('/Users/malthe/Programming/a/b/c/d').mkdir(parents=True, exist_ok=True)"
    20000 loops, best of 5: 18.2 usec per loop

bitum
=====
**`bitum` is experimental software, and may inadventently delete or corrupt
your files. Do not use it to back up your Grandma Satoshi's private keys or
those important holiday photos from yesteryear.**

bitum is a command-line tool that allows backing up files to cloud faster,
when you're dealing with many small files (e.g. git repositories, virtual
environments).

"bitum" is short for bitumen, which is a viscous material quite similar to
tar. The name  a reference to the `tar` UNIX tool that bundles up a
directory of files by sticking them together one-after-another in one big
file. tarballs (`.tar`-files) are great for snapshots, but don't lend
themselves well to continually updated backups (e.g. similar Dropbox or
Backblaze) as the metadata for each files is stored right before the file
content itself in the tarball. So when trying to check for changes between
the current files and the backup, you have to look through the whole tarball
to get to each file header and thus the file metadata.

bitum basically creates tarballs (`.bitum`-files) but keeps an index of the files
and their metadata in a separate file. This allows quickly downloading the index
from a cloud service, and then scanning for changes by using the metadata.

I think this is similar to `tarsnap`, but I don't actually know how `tarsnap` works.


Developing
----------
If you want to test `bitum` while developing you can do:

    python bitum/cli.py [...]


Or you can run any of the following commands:

    poetry build
    pipx install --force .
    bitum [...]


Alternative names
-----------------

- bitumen / bitum
- bucketeer / bkteer / bktier
- tarbox (Dropbox)
- tars3n (S3)
- tarz4n (Monkey-man)

Related work
------------

- [tarsnap](https://github.com/Tarsnap/tarsnap)
- [tarsync #1](https://github.com/zmedico/tarsync)
- [tarsync #2](https://github.com/carlba/tarsync)


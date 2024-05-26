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

Why not just use `.tar`-files?
------------------------------
Maybe? However, you'd still need the index file as `.tar`
files store metadata right before the content:

tar-file = FILE METADATA | FILE CONTENT | FILE METADATA | FILE CONTENT

So in order to sync "sparsely" (and not download the full .tar-file)
you need to look in the index to be able to grab just the file you want
(when few files have been updated on the remote).

This also means you're storing the file metadata twice, once in the index file
and once in the `.tar`-file so it has a slight storage overhead.


Why not just use `.zip`-files?
------------------------------
I don't disagree, but `.zip`-files have lots of compatibility issues.
A file compressed on macOS might not work on Windows, and there are
path length issues as well as file name encoding issues.

Lastly, `.zip`-files store their index of files at the end of the file

(Yes, you could an HTTP HEAD request, followed by some clever HTTP Range
requests but it's just not worth it given the compatibility issues)

Zip does do a couple of cool things by default. It stores the index at
the end of the zip file, so you could potentially use that index instead
of SQLite index file. It also compresses single files, which means you
could extract single files even under compression. But that also makes
for worse compressability than cross-file compression.

Why not compress the files by default?
--------------------------------------
Since bitum is a syncing tool, we need to be able to extract individual file
contents -- so that we can sync just that file. We do this using HTTP Range
requests. If we compressed the files, we'd need the compression dictionary to
live at some known place in the file (the beginning or end).

Even if we placed the dictionary somewhere, `gzip` is a streaming compression
so the result of decompressing some file in the middle requires reading all
of the contents of the files that came before.

Then you could compress the files individually, which isn't as effective
but doesn't have the dictionary problem.

See `COMPRESSION.md` in this directory for more info.

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


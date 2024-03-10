
- [ ] Add table `bitumen` that lists `.bitumen`-files -- their filenames and their sizes
  - [ ] Change first column in `files`-table to point to  `bitumen`-table instead of writing out filename
- [ ] Compress `bitumen.sqlite3` with e.g. gzip (currently 800K files takes up 115MiB)
- [ ] Store hash function either directly in hash as `sha256:<hash>` or in a `metadata`-table
  - [ ] Check remote hash algorithm and use that for the local filetree, to ensure sensible comparison

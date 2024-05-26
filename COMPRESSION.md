The bitum files (`*.bitum`)
---------------------------

Larger files are often compressed in their own right and stand little to gain from 
lossless compression, this obviously is the case for audio, images and video, but also
Docker images.

But smaller files often contain text and thus compress well.

However, when

The index file (bitumen.sqlite3)
--------------------------------
The bitum index file `bitumen.sqlite3` is highly compressible as usually a large
number of paths will have the same prefix, and normally only use 30-50% of the
full ASCII characters set.

An example using my own "Programming" directory using `gzip` with the standard
level compression 6 yields:

    Uncompressed size   115.2 MiB
    Compressed size      42.2 MiB


Random notes on compression
---------------------------
Compressing the macOS app `Google Chrome.app` using `tar -cz` yields:

    Uncompressed size   1021.5 MiB
    Compressed size      470.4 MiB

Surprisingly good results!
Given that result, the good thing is that macOS apps are considered
directores by command-line tools (hence the use of `tar`) and thus
bitum will apply compression under the "don't compress large files"
scheme.
However, of rougly 1 GiB that Google Chrome takes up, these are split
into two versions, each containing a 355 MiB binary called
"Google Chrome Framework" which is gzip-compressible down to 162 MiB.
So that would not be "caught" under the large-file scheme.
This document is intended for people wishing to modify or understand the
implementation of slime. It should help you figure out wheree you might want
to start; it is not intended as a complete reference.

Parts of The Proxy Server
=========================

The proxy server handles redundancy (erasure correction), keeps track of
which chunk servers are hosting which locations, and keeps track of files
stored. It talks to the PostgreSQL database.

    `func proxyServer()` in `main.go`:
        Connects to PostgreSQL database through `git.encryptio.com/kvl`
        Serves `lib/proxyserver.Handler` on `/`

    `lib/proxyserver.Handler`:
        serves special endpoints `/redundancy` and `/stores` specially
        sends requests inside `/data/` to the `lib/store/multi.Multi` via
            `lib/store/storehttp`

    `lib/store/multi.Multi`:
        handles erasure coding with `lib/rs` by splitting up user-given files
        maps user-given names to internal names ("prefix ids") and stores the
            mapping in the database with `lib/meta.File`
        works in concert with `lib/store/multi.Finder` to maintain a list of
            known locations served by chunk servers (mapped using
            `lib/meta.Location` to the database)
        in the background, looks for files which are on locations marked dead
            or with the incorrect redundancy values and rebuilds them
            (`multi_scrubfiles.go`)
        in the background, looks for locations which have extraneous chunks and
            deletes them, and for locations which have missing chunks and
            rebuilds them (`multi_scrublocations.go`)
        in the background, looks for an imbalance in free space between
            locations and moves chunks from one location to another to try to
            equalize the free space (`multi_rebalance.go`)
        talks to locations on read/write operations via
            `lib/store/storehttp.Client` (`multi_store.go`)

    `lib/store/multi.Finder`:
        keeps track of which locations are currently reachable
        on request (by `proxyserver.Handler`), scans new URLs for locations

    `lib/meta`:
        maps slime types to key-value pairs and passes those on to
            `git.encryptio.com/kvl`
        maintains indexes for fast lookup of data

Parts of The Chunk Server
=========================

The chunk server serves multiple locations, each of which is a single directory
(supposedly on different filesystems.) It verifies that the chunks it stores
are the chunks it sends with checksums. It does not deal with redundancy or the
PostgreSQL database.

    `func chunkServer()` in `main.go`:
        Opens stores (with `lib/store/storedir`) with retries
            (`lib/store/retry.go`)
        Passes the retry stores to `lib/chunkserver.Handler` and serves on `/`

    `lib/chunkserver`:
        serves special endpoint `/uuids`
        passes on requests starting with `/SOMEUUID-GOES-HERE-ffff-ffffffffffff/`
            to a `lib/store/storehttp.Server` pointing at the retry store
            pointing at the `lib/store/storedir` that was passed in from main

    `lib/store/storedir`:
        stores values in a directory on a filesystem, including their sha256 and
            fnv-1a hash.
        on get request, verifies that the fnv-1a hash of all the data is what it
            was when the file was originally written. if that fails, it
            quarantines the file.
        in the background, reads all data slowly to check for bit rot/bad blocks
            and quarantines bad files (`hashcheck.go`)
        splits data into multiple subdirectories to avoid having a large number
            of files in a single dir (`resplit.go`)

Committing
==========

Ensure that your code is `go fmt`ed, that all tests pass, and that new tests are
written as applicable.

Send in a patch to the maintainer at `encryptio@gmail.com` using
`git format-patch`.

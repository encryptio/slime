Slime is a distributed, consistent object store that uses erasure coding to
store data extremely efficiently while keeping a configurable amount of
redundancy.

*WARNING*: It is not ready for large scale production use. Play with it, but
be prepared to run into bugs that may destroy data in slime.

Dependencies
------------

slime requires Go 1.5 to build and test.

PostgreSQL MUST be 9.1 or later; true serializability was implemented in that
version.

Quickstart
----------

This is how you can get a slime instance running locally to try it out. If you
want to do a more production-style deployment, see PRODUCTION.md for details.

Compile the slime daemon and control tool:

    $ go install github.com/encryptio/slime github.com/encryptio/slime/slimectl

Create a PostgreSQL database and user:

    $ psql
    postgres=# CREATE DATABASE slime;
    postgres=# CREATE USER slime WITH PASSWORD 'secret';
    postgres=# GRANT ALL ON DATABASE slime TO slime;

Create a few storage directories:

    $ slime fmt-dir /tmp/slime-1
    $ slime fmt-dir /tmp/slime-2
    $ slime fmt-dir /tmp/slime-3

Create a config file:

    $ cat > server.toml
    [proxy.database]
    type = "postgresql"
    dsn = "user=slime password=secret sslmode=disable"
    [chunk]
    dirs = ["/tmp/slime-1", "/tmp/slime-2", "/tmp/slime-3"]

Start the chunk server (it will listen on port 17941 by default):

    $ slime chunk-server server.toml
    <command will not return>

Start the proxy server (it will listen on port 17942 by default):

    $ slime proxy-server server.toml
    <command will not return>

Tell the proxy server about the chunk server:

    $ slimectl store scan http://127.0.0.1:17941

Make sure it actually found what you wanted it to find:

    $ slimectl store list
    Name             UUID                                 Status    Free
    box:/tmp/slime-1 cfe68a68-a841-4a4c-4e11-dac8db89167e connected 649.3 GiB
    box:/tmp/slime-2 d75eac08-4005-47a8-6984-d2cd2d9b52e0 connected 649.3 GiB
    box:/tmp/slime-3 52e43f88-a619-43b1-7ebd-7a6a8016985c connected 649.3 GiB

Set the redundancy level:

    $ slimectl redundancy set 2 3
    Redundancy sucessfully changed to 2 of 3

Save a file:

    $ curl -v -XPUT --data-binary @somefilename http://127.0.0.1:17942/data/file

Corrupt one of the data stores:

    $ rm -f /tmp/slime-1/data/*/*

Download the file back, despite the corruption:

    $ curl -v -o thefile http://127.0.0.1:17942/data/file

Redundancy Settings
-------------------

Erasure coding with checksums allows slime to efficiently store data. It splits
each file store on it into a configurable number of equally sized pieces
(the redundancy's "need" option) and adds parity chunks to get it to a specified
redundancy level (the redundancy's "total" option.)

You can then have _any_ "need" of the chunks, parity or data, to reconstruct the
file.

For example, if you have your redundancy set to 3 of 5 and you lose a drive,
all files can still be recovered, even if there's a drive with an unexpected
bad or bitrotted block.

The storage required for data is `total/need` times that of the actual data
(plus a few bytes per file for tracking.)

Run `slimectl redundancy` to get the current redundancy level, and run
`slimectl redundancy set NEED TOTAL` to adjust the redundancy level.

Store Discovery
---------------

The proxy servers store a list of chunk directories (stores) that they think
exist in the database, and on startup, will scan all of the URLs for chunk
servers to figure out which ones are currently connectable. From then on, they
will periodically scan the chunk servers to look for new and no-longer-available
stores.

`slimectl store list` will show you a list of the known stores.

You must tell the proxy server explicitly to scan a chunk server if it's on a
new URL; `slimectl store scan http://chunkserver` to scan a new chunk server (or
an old one at a new URL.)

Each directory has a UUID file in it; if you move drives between servers, then
the URL stored in the database will be updated on the first successful scan of
the chunk server containing it.

Recovery from loss of a drive
-----------------------------

If a drive is no longer served by a chunk server, slime will stop writing new
data to it, but in the hopes that it might come back, will not start rebuilding
the data on the drives it can access until you tell it to do so.

To notify slime that a drive really is dead, `slimectl store dead STORE`.

Slime will gradually rewrite the data that was stored in the dead drive to other
drives.

Note that you can even mark a drive that's still connected "dead", and slime
will try to read data from it if it needs it, but will still rewrite the data to
no longer depend on that drive. This can be useful because you don't lose
failure tolerance while rebalancing.

Recovery from loss of files on a drive
--------------------------------------

Drives sometimes gain bad blocks, and more rarely have uncaught bit errors on
read. Slime protects against both of these. There is a hash stored with every
chunk that protects from uncaught bit errors; it is checked on every read. If
the data read from disk doesn't match the hash or the drive returns a read
error, the file will be moved into the "quarantine" subdirectory for later
inspection, and the proxy will reconstruct the data out of other chunks (if
possible.)

No manual intervention is required in this case; the proxy periodically scans
all stores to see if they are missing files (for any reason, quarantined,
deleted, or other); if it notices something missing, it will attempt to
reconstruct and rewrite the data into new chunks, possibly on different stores,
possibly on the same ones.

Metadata Backups
----------------

It's extremely important to back up your metadata database; the loss of it means
the loss of ALL your data.

Use the standard PostgreSQL utilties to do this, but be sure to specify
--serializable-deferrable to ensure serializability of the dump with relation to
other transactions the proxy is currently running:

    $ pg_dump -Fc --serializable-deferrable -f slime.pgdump slime

Consistency and Availability Model
----------------------------------

Slime's metadata and data are highly consistent. If an operation cannot be
guaranteed consistent, then an error is returned instead.

A proxy's cache is kept consistent with the data underneath it; no read will
return stale data, even if a proxy's in-memory cache is involved.

Slime requires a serializable metadata database, but inherits that database's
availability model for metadata, meaning it can only support CP distributed
databases in terms of the CAP theorem. Non-distributed serializable databases
such as (vanilla) PostgreSQL work, but are, of course, not partition tolerant.

Data (contents) stored by slime follows a very different model. Assuming the
metadata server is accessible by a particular proxy, then:

- Reads are possible on an item when the required number of chunk servers
  containing data chunks on each item is greater than or equal to the configured
  redundancy's "need" level.
- Writes are possible when the number of chunk servers connected (and with
  enough free space) is greater than or equal to the configured redundancy's
  "need" level.
- Deletes are always possible.

Note that writes are not "sticky"; writing data to a pre-existing item does not
mean the data is stored in the same location. Even if a particular proxy can't
read data at an item, that does not mean it can't serve highly-consistent writes
to that item.

One possible consequence of the differing availability models for data and
metadata is that two proxy servers with different connections to chunk servers
may be able to successfully write to enough chunk servers _and_ successfully
update the metadata store, and thus will see each other's writes in metadata,
but may not be able to actually read the contents that the other proxy writes
if they can't talk to the same chunk servers that the data was written to.

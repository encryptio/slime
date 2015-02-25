Slime is a distributed, consistent object store that uses erasure coding to
store data extremely efficiently while keeping a configurable amount of
redundancy.

*WARNING*: It is not ready for large scale production use. Play with it, but
be prepared to run into bugs that may destroy data in slime.

Dependencies
------------

slime requires Go 1.4 to build, and Go tip to run its test suite.

PostgreSQL must be 9.1 or later; true serializability was implemented in that
version.

Quickstart
----------

Compile the slime tool (which includes the daemons in the same binary):

    $ go install git.encryptio.com/slime

Create a PostgreSQL database and user:

    $ psql
    postgres=# CREATE DATABASE slime;
    postgres=# CREATE USER slime WITH PASSWORD 'secret';
    postgres=# GRANT ALL ON DATABASE slime TO slime;

Create a few storage directories:

    $ slime fmt-dir /tmp/slime-1
    $ slime fmt-dir /tmp/slime-2
    $ slime fmt-dir /tmp/slime-3

Start the chunk server (it will listen on port 17941 by default):

    $ slime chunk-server /tmp/slime-{1,2,3}
    <command will not return>

Start the proxy server (it will listen on port 17942 by default):

    $ export SLIME_PGDSN="user=slime dbname=slime password=secret sslmode=disable"
    $ slime proxy-server
    <command will not return>

Tell the proxy server about the chunk server:

    $ curl -v -XPOST -d '{"operation":"scan","url":"http://127.0.0.1:17941"}' http://127.0.0.1:17942/stores

Set the redundancy level:

    $ curl -v -XPOST -d '{"need":2,"total":3}' http://127.0.0.1:17942/redundancy

Save a file:

    $ curl -v -XPUT --data-binary @somefilename http://127.0.0.1:17942/data/file

Download it back:

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

GET /redundancy on the proxy to get the current redundancy level (JSON
formatted), and POST to the same path with a similar object to adjust the
redundancy levels.

Store Discovery
---------------

The proxy servers store a list of chunk directories (stores) that they think
exist in the database, and on startup, will scan all of the URLs for chunk
servers to figure out which ones are currently connectable. From then on, they
will periodically scan the chunk servers to look for new and no-longer-available
stores.

GET /stores on the proxy server to get a json-formatted list of stores.

You must tell the proxy server explicitly to scan a chunk server if it's on a
new or different URL; POST {"operation":"scan","url":"http://host:port"} to
/stores on a proxy to scan a new chunk server (or an old one at a new URL.)

Each directory has a UUID file in it; if you move drives between servers, then
the URL stored in the database will be updated on the first successful scan of
the chunk server containing it.

Recovery from loss of a drive
-----------------------------

If a drive is no longer served by a chunk server, slime will stop writing new
data to it, but in the hopes that it might come back, will not start rebuilding
the data on the drives it can access until you tell it to do so.

To notify slime that a drive really is dead, POST
    {
        "operation": "dead",
        "uuid": "...drive uuid..."
    }
to /stores on a proxy. (GET /stores to see a list of stores; any dead ones will
likely show up as connected: false, and you can use the name field to verify
that the UUID you're marking is actually the dead drive.)

Slime will gradually rewrite the data that was stored in the dead drive to other
drives.

Note that you can even mark a drive that's still connected "dead", and slime
will try to read data from it if it needs it, but will still rewrite the data to
no longer depend on that drive. This can be useful if you're replacing a drive
for upgrades or because you suspect it will fail soon.

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

Consistency Model
-----------------

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
may be able to successfully write to the chunk servers _and_ successfully update
the metadata store, and thus will see each other's writes in metadata, but may
not be able to actually read the contents that the other proxy writes.

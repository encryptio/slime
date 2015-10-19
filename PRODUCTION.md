*WARNING: Slime is not yet safe enough to trust! Don't use it for data you
can't replace!*

This document gives some advice about how to set up a production slime cluster.
This document is just advice, not a set of hard rules; if you understand what
consequences you'll see, feel free to set it up in another way.

Operating System
================

Slime is thoroughly tested on Linux. Other UNIX-like OSes like FreeBSD, OpenBSD,
NetBSD, and Solaris should work fine, but are not thoroughly tested.

Windows is not recommended; there have been race conditions due to differences
between the UNIX and NT APIs that have caused corruption and weird errors in the
past, and it's not clear that they're all fixed.

Database
========

Currently the only stable option for the backend storage system is PostgreSQL
9.1 and above.

If you run an older version of PostgreSQL, it will not be able to handle any
serialization issues and *may corrupt your data*. Run a recent version.

Your PostgreSQL database is the core of the system; be paranoid about what
processes run near it. If you can afford the hardware cost, run it on a
dedicated box. If you can't do that, look into your kernel's options for
managing system resources and try to make sure some of them are always given to
PostgreSQL.

Set up regular backups, and make sure they're done with the SERIALIZABLE
isolation level, and ideally SERIALIZABLE DEFERRABLE to avoid extraneous
transaction conflicts. With `pg_dump`, the `--serializable-deferrable` option
does this.

Backups should be done very often; if you lose your database and recover from a
copy that's too old, slime may not be able to find data that it's moved around.

Expect to see several serializability errors per day in the PostgreSQL logs.
They'll look something like this:

    STATEMENT:  WITH upsert AS (    UPDATE data SET value = $2 WHERE key = $1
        RETURNING *) INSERT INTO data (key, value) SELECT $1, $2    
    ERROR:  could not serialize access due to read/write dependencies among
        transactions
    DETAIL:  Reason code: Canceled on identification as a pivot, during conflict
        out checking.
    HINT:  The transaction might succeed if retried.

This is normal, and slime will automatically retry transactions which fail this
way; this error will not be exposed to the user.

Chunk Servers
=============

You'll want to run one chunk server instance per physical machine.

Use the filesystem you understand the best that has good journaling; XFS is a
great option, and ext3/ext4 is acceptable.

Do not use the "nobarrier"/"async" or other safety-destroying options when
mounting filesystems to be used with slime. The performance benefit is nearly
nothing, even if you don't have a battery-backed cache card (which will quickly
respond to barrier requests internally.)

Each drive you want to manage should NOT be done through RAID, and should be
used with a single filesystem per physical drive. Slime will handle the
redundancy between them more efficiently than RAID.

Tell the chunk server about each of the drives' mountpoints explicitly in your
run script; don't use a shell glob. This way, you can unmount and remount
drives on the fly, and if the chunk server starts before the drives are
mounted, you'll still serve it when it gets mounted. The chunk server will
notice when the "uuid" files disappear and reappear and will expose that data
to the proxy servers as appropriate.

Feel free to use drives of varying sizes; slime's balancing will try to keep the
free space nearly equal on all of the drives, and its allocation is intended to
handle an out-of-balance cluster cleanly.

You should be watching for files in the quarantine directories; if you see any,
that means that the chunk server found corrupt or unreadable files on that
filesystem. If this makes you weary of trusting a drive, mark the location
"dead", wait for the proxies to move the data off it, and remove or replace the
drive.

Additionally, you should watch the kernel logs for drive errors. For any drives
that show up, you should mark them dead, then remove/replace them when
possible.

Proxy Servers
=============

Run one proxy server per physical machine you want proxying. These may be on
the chunk servers themselves, or may be separate hardware.

Ensure they have good connectivity to the database; good connectivity to the
chunk servers is less important.

All chunk data is held in memory while it is being reconstructed. Limit the
number of parallel http requests handled with the parallel-requests config
option. A conservative estimate of RAM usage would be 4x the size of the file
being requested in RAM.

Getting data from the proxy servers is relatively expensive, but getting
metadata (file listings, HEAD requests, and matching If-None-Match GETs) is very
cheap.

Enable the largest cache you can fit in memory using the cache-size setting to
reduce the cost of repeated reads to the same values. Note that the cache is
highly consistent; it will never return stale data. When choosing a value, keep
the temporary memory you need for in-flight requests in mind; see above.

Removing Drives in a Running Cluster
====================================

If you want to remove a drive, mark it "dead" via the proxy API.

Then, if possible, wait for the slime proxies to move data off the drive. This
step is optional, but if you can do it, then you'll have a higher redundancy
level for all of your data.

Remove the mountpoint from the chunk server runscript and restart the chunk
server.

Any chunks that were still on the drive will be recreated elsewhere once the
scrub process gets to them.

Adding Drives in a Running Cluster
==================================

Make a new filesystem per drive, mount it, run "slime fmt-dir /mnt/newdrive",
add the new mountpoint to the chunk server config, and restart it.

If the machine wasn't running any other locations, use the proxy API to scan the
chunk server. If it is running other locations, the proxy servers will find the
new drive on their periodic scans.

A Note About Failing Drives
===========================

Data on failing and missing locations *will not be rebuilt* unless the locations
are marked dead. Monitor your cluster and mark drives dead aggressively. If you
were too aggressive, you can always change your mind and mark it undead.

Redundancy Level
================

Set your redundancy level high enough to deal with the number of concurrent
failures you want to handle. Note that this is not just about drives failing
(head crashes, bad blocks, bit rot), but also about drive controller failure,
motherboard failure, RAM failure, and network failure.

Your storage footprint will be `total/need` times the size of the data you
store; for example, storing 1 GB of data at `{"need":5,"total":8}` will require
1.6 GB of space. You can make total and need larger to reduce this cost; for
example, using `{"need":12,"total":15"}` will reduce the space needed to
1.25 GB, while still retaining the ability to handle 3 concurrent location
failures.

If you need your data to be online all the time, you'll want to make your
redundancy ratio (total/need) fairly high and the "need" count fairly high as
well. This way you can handle a failure of a few machines (even if only
temporary) and still read your data.

Also note that higher "need" values will increase the amount of work the proxy
needs to do to reconstruct the data, so high values will be more CPU-intensive
and may cause more latency.

Consider your network topology and how that may cause temporary failures.

In the future, slime will have tools to help run with a tighter reundancy ratio
(closer to 1) and a smaller "total" count while still avoiding correlated
failures (for example, an entire machine failure) by allowing you to customize
the allocation strategy (for example, if you have 3/5 redundancy, you can ensure
all 5 chunks are stored on different machines, not just different locations.)

See "Consistency and Availability Model" in README.md for some more information
on when data is readable at a given redundancy level.

Nagios Checks
=============

There is a nagios plugin for slime-proxy at nagios-check-slime-proxy, which
tests connectivity to the proxy as well as its connectivity to all of the chunk
servers.

Supervision
===========

Both the proxy server and the chunk server crash on major failures, and should
be run in a process supervisor, such as runit or systemd.

Logging
=======

Both the proxy server and the chunk server log to stdout. Panic messages are
sent to stderr.

Your supervisor should be able to redirect these to a logging system of your
choice; for example, you can add a log service to a runit service that runs
logger(1), svlogd(8), or anything else.

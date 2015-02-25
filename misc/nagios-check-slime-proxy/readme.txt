This program checks to see if a slime proxy is connected to the non-dead stores
in the database and exits with nagios plugin status codes if the number of
non-connected non-dead stores exceeds the specified thresholds.

It also checks the UUID of the proxy to make sure you're checking the right
cluster.

Example invocation:
    nagios-check-slime-proxy -addr=slime.int.company.com:17942 \
        -crit=2 -warn=1 -uuid=c2d7e7f0-e259-42c1-6f8c-3cf9f4d526fc

You can get the UUID for the metadata database of your slime cluster by running:
    curl -s 'http://HOST:PORT/data/?mode=uuid'


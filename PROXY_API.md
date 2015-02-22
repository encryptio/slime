# The Public (Proxy) API

## Metadata

### GET /redundancy

Get the redundancy level. Response body is a JSON-encoded object of the form:

```
{
    "want": 3,
    "total": 5
}
```

### POST /redundancy

Set the redundancy level. Request body is a JSON-encoded object of the same form
as the response to GET /redundancy.

### GET /stores

Get meta-info on the stores available. Response body is a JSON-encoded array of
the form:

```
[
    {
        "connected": true,
        "dead": false,
        "free": 900190019001, // number of bytes free OR an error string
        "last_seen": "2015-02-21T16:37:53Z",
        "name": "host:/path/to/dir", # opaque string, for human use only
        "url": "http://host:17941", // whatever was sent in the last successful
                                    // scan operation for this store
        "uuid": "8028e680-6e4d-4a02-4261-828c5fb5699d"
    },
    ...
]
```

### POST /stores

Do an operation on the stores. Request body is a JSON-encoded object. Always
responds with the same data that a GET /stores would respond after the operation
completes.

Operations:

- scan: `{"operation": "scan", "url": "http://somehost:17941"}` scan an object
  store for UUIDs.
- dead: `{"operation": "dead", "uuid": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"}`
  Mark the store with the given UUID "dead"; slime will no longer allocate
  data on this store and it will rewrite any data that is contained in the store
  to not be dependent on it anymore. Being "dead" does not imply that the store
  is accessible or inaccessible, just that we don't want to use it in the
  future.
- delete: `{"operation": "delete", "uuid": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"}`
  Remove knowledge of a store from slime. It must not be connected (which may
  take a few minutes for the proxy to realize) and must already be marked dead.

### GET /data/?mode=free

Get the number of bytes expected to be usable, given the current redundancy
level. Response body is an ASCII decimal integer.

### GET /data/?mode=uuid

Get the UUID of the storage system as a whole; this is an identifier for the
database, not any individual store that backs its data.

### GET /data/?mode=list&limit=1000&after=

Get a partial list of keys in sorted order, separated by newline characters. The
"limit" parameter specifies a maximum number of keys to return, and must be less
than or equal to 10000. The "after" parameter specifies a lower limit on the
keys returned (by lexicographic comparison.)

For example, to read the entire key space, send a ?mode=list&limit=100 request.
If you got exactly 100 keys, set the "after" parameter to the last key you got
and repeat. (Note that the list you have after finishing all requests may not
match the actual total list of keys if other processes are writing data to
slime during this time.)

## Data

### GET/HEAD /data/key

Get data for a key. GET may be optionally conditional on the If-None-Match
header.

Expected responses:

- 200 OK: Data was found for this key. Contains ETag and X-Content-SHA256
  headers. If the method is GET, the data is returned as the content body.
- 304 Not Modified: The ETag given in the If-None-Match request header matches
  the current data.
- 404 Not Found: Data was not found for this key.

### PUT /data/key

Set a key to some data. Optionally conditional on the If-Match header.

Expected responses:

- 204 No Content: The data was successfully written.
- 412 Precondition Failed: The data currently at this location does not match
  the request's If-Match header.

### DELETE /data/key

Remove a key. Optionally conditional on the If-Match header.

Expected responses:

- 204 No Content: The data was successfully removed.
- 404 Not Found: No data for that key existed.
- 412 Precondition Failed: The data currently at this location does not match
  the request's If-Match header.

## Preconditions

ETag values in slime are of the form `"XXXXXXX..."`, where the string of Xs
represent a hex-encoded sha256 of the content (the specific form may change,
do not depend on it; use X-Content-SHA256 for transit verification) or of the
special form `"nonexistent"` which represents that no content exists at the key.

You can pass these values in If-None-Match and If-Match (appropriate to the
method you're using) to conditionally do an operation if the content at that key
matches what you send. Operations involving these headers are guaranteed to be
atomic and highly consistent across the slime cluster.

For example, if you want to delete a value, but only if it hasn't changed since
your last GET request, you can set the If-Match header on your DELETE request to
the ETag that you got from the GET request. If the value has changed, then the
response to the DELETE request will be 412 Precondition Failed, at which point
your code can respond appropriately (e.g. by aborting or retrying from the GET.)

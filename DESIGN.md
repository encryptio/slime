Slime stores small files (<64MB) redundantly over a set of filesystems and
exposes them over a RESTful API.

It is built out of 3 main pieces visible to the system administrator:

    - The proxy server, which handles redundancy and error recovery
    - The chunk servers, which handle storing data and checking for bitrot
    - The metadata database, which stores information about what files exist
      and where their chunks can be found.

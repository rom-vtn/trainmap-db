# `trainmapdb` module
> Note: still in development, expect breaking changes

Go Module to:
- Import GTFS feeds and parse them into a database
- Use that database to calculate train sights at a given geographical point in a given timespan

# Inspiration
- Inspiration for this module was taken from the Python implementation `pygtfs`, which was too slow for me so I decided to rewrite it in Golang so it could be faster
  - Note: I'm not parsing _all_ of the GTFS feed, infos such as `shapes.txt` are left out, check `model.go` to see what's stored in the DB

# Known issues / TODO
- Route IDs not parsing correctly on some feeds (looking at you ÖBB)
- Refine sights criteria and parse paths from existing paths
- Update the `route_type` criteria to allow for other rail types (monorail and stuffies)
- Write tests

# Examples
- https://github.com/rom-vtn/trainmap_api (an API using this package to answer queries using a given database) [TODO: not actually published, gotta fix]
- https://github.com/rom-vtn/trainmap_loader (a loader which generates a database using a config file containing URLs to GTFS feeds) [TODO: not actually published either]

# See also
- https://github.com/jarondl/pygtfs (inspiration for the package)
- https://gtfs.org/schedule/reference/ (GTFS Spec)
# DPN Test Bags

The bags in the directory that begin with 00000000 are used in
automated unit and integration tests.

The other bags, whose names consist of real UUIDs, are for testing
replication transfers. We upload these to the test server, and other
nodes replicate them. They're in source control so we don't lose them.

The bag_data.json file contains bag and fixity entries matching the
six bags with real UUIDs. You can load these into the REST service
using:

```
python manage.py loaddata <path/to/bag_data.json>
```

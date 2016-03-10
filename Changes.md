# Changes

## March 10, 2016 (c7a0cae)

The Go services now send a copy of the processing state JSON for each
bag to Fluctus. This is in addition to the JSON state data in
NSQ. Currently, the state info being saved to Fluctus is for
diagnostics and recovery only (i.e. requeueing failed or stalled
requests). As the system evolves, we'll remove the JSON state data
from NSQ.

Removed bagdeleter, cleanup_reader, cleanup_file, cleanup_result and
related config settings because we weren't using them.

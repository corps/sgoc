sgoc
====

A relational database / log-sequenced synchrobnization protocol, built on top of protocol buffers, inspired by work done at Square.

Idea is that one can define a schema of related objects and indexes in protobufs by using extensions from [schema](src/main/proto/corps/sgoc/schema.proto), and then be able to "synchronize" writes and reads to that dataset from multiple different sources via the [sync protocol](https://github.com/corps/sgoc/blob/master/src/main/proto/corps/sgoc/sync.proto).  Deleted objects are considered "terminal" versions, and collisions are handled simply via optimistic locking (accepting the highest novel version).

Pretty simple project that gave me deeper insight into various issues of database and synchronization protocols.

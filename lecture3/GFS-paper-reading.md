# The Google File System

## Outline
- reexamining traditional file system assumptions in light of our current and anticipated application
workloads and technological environment.
- We treat component failures as the norm rather than the
exception
- Our system provides fault tolerance by constant monitoring, replicating crucial data, and fast and automatic recovery
- Our design delivers high aggregate throughput to many
concurrent readers and writers performing a variety of tasks.
- 
## Abstract
- GFS: a scalable distributed file system for large distributed
data-intensive applications.
    - fault tolerance
    - high aggregate performance

## introduction
-  GFS is driven by key observation of application workloads and technological environment

- Reexamination
    - First, component failures are the norm(标准) rather than the
    exception.
    - Second, files are huge by traditional standards. 
    - Third, most files are mutated by appending new data
    rather than overwriting existing data.
    - Fourth, co-designing the applications and the file system API benefits the overall system by increasing our flexibility.

## Design Overview

### Assuptions

- built on commodity components that may fail
- The system stores a modest number of large files
- Reads:
    1. large streaming read
    2. small random read
- Writes:
    - large append
    - random and small write 
- The system must efficiently implement well-defined semantics
for multiple clients that concurrently append
to the same file.
    - Our files are often used as producerconsumer
queues or for many-way merging
- High sustained bandwidth is more important than low
latency.

### Interface
- does not implement a standard API like POSIX
- but support usual operations like create, delete, open...
- Moreover, GFS support *snapshot*, **record append** which is useful for multi-way merge and producer-consumer queues that many clients can simultaneously append
to without additional locking.

### Architecture

![GFS](GFS.png)

- GFS consists of a single master and multiple *chunkservers* and is accessed by multiple *clients* 
- Files are divided into fixed-size chunks.
    - Each chunk is identified by an immutable and globally unique 64 bit chunk handle assigned by the master at the time of chunk creation.
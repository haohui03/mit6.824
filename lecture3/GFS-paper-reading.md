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
    - Second, files are huge by traditional standards. Multi-GB
    files are common.
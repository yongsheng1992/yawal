# yawal

Yet another write ahead log(yawal) is a simple write ahead log library. It consists of four components: Log, Segment, Index and Store.

* **Log** is a sequence of logs, and it has only two public method `Append(record)` and `Read(offset)`. `Append` appends record at the end of the sequence. `Read` reads the log at the given position.
* **Segment** is a smaller log sequence.
* **Index** is the offset of a log in the segment, which stores offset and position of a log.
* **Store** store logs in a file.
* **Offset** is the position of a record in the segment.
* **Position** is the offset of a record in the file.

```mermaid
---
title: WAL data strucrture
---
classDiagram
    class Log {
        + Append(record *api.Record) (uint64, error)
        + Read(offset uint64) (*api.Record, error)
    }

    class Segment {
        + Append(record *api.Record) (uint64, error)
        + Read(offset uint64) (*api.Record, error)
    }

    class Index {
        + Read(offset uint64) (uint64, uint64, error)
        + Write(offset uint64, position uint64) error
    }

    class Store {
        + Append(data []byte) (uint64, uint64, error)
        + Read(pos uint64) ([]byte, error)
    }

    Log "1" *-- "n" Segment
    Segment *-- Index
    Segment *-- Store
```

# Performance Concerns

* Use `truncate` and `fdatasync` instead of `fsync`.

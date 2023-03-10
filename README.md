# yawal

Yet another write ahead log(yawal) is a simple read or append log library. It consists of four components: Log, Segment, Index and Store.

* **Log** is an abstract concept which only has two public method `Append(record)` and `Read(offset)`. `Append` appends record at the end of a file. `Read` reads the record at the offset postion.
* **Segment** is a collection of records.

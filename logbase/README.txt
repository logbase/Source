LogBase [1] is an open-source, scalable log-structured database system 
that adopts log-only storage structure for removing the write bottleneck observed 
in write-heavy environments, e.g., continuous stream processing. 

LogBase leverages the Hadoop Distributed File System (HDFS) [2] to maintain 
log files, which constitute the only data repository in the system. 
LogBase is implemented in Java, inherits basic infrastructures from the open-source HBase [3], 
and adds new features for log-structured storages including access to log files and in-memory indexes.

The source code, executable, getting started documents of LogBase can be found at [1].

1. http://www.comp.nus.edu.sg/~logbase/
2. http://hadoop.apache.org
3. http://hbase.apache.org/

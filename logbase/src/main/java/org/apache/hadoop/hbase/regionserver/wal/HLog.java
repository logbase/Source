/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Arrays;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.MemIndex;
import org.apache.hadoop.hbase.regionserver.wal.LogScanner.ScannerType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

import sg.edu.nus.harmonious.io.HSequenceFile;
import sg.edu.nus.harmonious.utils.HUtils;
import sg.edu.nus.test.Debug;

/**
 * HLog stores all the edits to the HStore.  It's the hbase write-ahead-log
 * implementation. It's also the log-file in the log-only store implementation.
 * Only the Log is store, and the HLog take care of all the log file in the 
 * its directory. 
 * 
 * It performs logfile-rolling, so external callers are not aware that the
 * underlying file is being rolled.
 *
 * <p>
 * There is one HLog per RegionServer.  All edits for all Regions carried by
 * a particular RegionServer are entered first in the HLog.
 *
 * <p>
 * Each HRegion is identified by a unique long <code>int</code>. HRegions do
 * not need to declare themselves before using the HLog; they simply include
 * their HRegion-id in the <code>append</code> or
 * <code>completeCacheFlush</code> calls.
 *
 * <p>
 * An HLog consists of multiple on-disk files, which have a chronological order.
 * As data is flushed to other (better) on-disk structures, the log becomes
 * obsolete. We can destroy all the log messages for a given HRegion-id up to
 * the most-recent CACHEFLUSH message from that HRegion.
 *
 * <p>
 * It's only practical to delete entire files. Thus, we delete an entire on-disk
 * file F when all of the messages in F have a log-sequence-id that's older
 * (smaller) than the most-recent CACHEFLUSH message for every HRegion that has
 * a message in F.
 *
 * <p>
 * Synchronized methods can never execute in parallel. However, between the
 * start of a cache flush and the completion point, appends are allowed but log
 * rolling is not. To prevent log rolling taking place during this period, a
 * separate reentrant lock is used.
 *
 * <p>To read an HLog, call {@link #getReader(org.apache.hadoop.fs.FileSystem,
 * org.apache.hadoop.fs.Path, org.apache.hadoop.conf.Configuration)}.
 *
 */
public class HLog implements Syncable {
  static final Log LOG = LogFactory.getLog(HLog.class);
  public static final byte [] METAFAMILY = Bytes.toBytes("METAFAMILY");
  static final byte [] METAROW = Bytes.toBytes("METAROW");

  /*
   * Name of directory that holds recovered edits written by the wal log
   * splitting code, one per region
   */
  private static final String RECOVERED_EDITS_DIR = "recovered.edits";
  private static final Pattern EDITFILES_NAME_PATTERN =
    Pattern.compile("-?[0-9]+");
  
  private final FileSystem fs;
  private final Path dir;
  private final Path tmpDir;
  private final Configuration conf;
  // Listeners that are called on WAL events.
  private List<WALObserver> listeners =
    new CopyOnWriteArrayList<WALObserver>();
  private final long optionalFlushInterval;
  private final long blocksize;
  private final String prefix;
  private final Path oldLogDir;
  private boolean logRollRequested;

  private static Class<? extends Writer> logWriterClass;
  private static Class<? extends Reader> logReaderClass;
  
  //WangSheng
  public LogScanner createLogScanner(MemIndex index, LogScanner.ScannerType type, LogStoreCache cache)throws IOException{
	  	LogScanner scanner = LogScannerFactory.getScanner(type);
	  	//scanner.init(this.outputfiles, this.fs, this.conf, index);
	  	scanner.init(this.outputfiles, this.fs, this.conf, index, cache);
	  	return scanner;
	  }
  
  public LogScanner createLogScanner(MemIndex index, LogScanner.ScannerType type)throws IOException{
  	LogScanner scanner = LogScannerFactory.getScanner(type);
  	scanner.init(this.outputfiles, this.fs, this.conf, index);
  	return scanner;
  }

  public LogScanner createLogScannerByKey(MemIndex index)throws IOException{
  	return this.createLogScanner(index, LogScanner.ScannerType.ByKey);
  }

  public LogScanner createLogScannerByFile(MemIndex index)throws IOException{
  	return this.createLogScanner(index, LogScanner.ScannerType.ByFile);
  }
  
  static void resetLogReaderClass() {
    HLog.logReaderClass = null;
  }

  private OutputStream hdfs_out;     // OutputStream associated with the current SequenceFile.writer
  private int initialReplication;    // initial replication factor of SequenceFile.writer
  private Method getNumCurrentReplicas; // refers to DFSOutputStream.getNumCurrentReplicas
  final static Object [] NO_ARGS = new Object []{};

  // used to indirectly tell syncFs to force the sync
  private boolean forceSync = false;

  public interface Reader {
    void init(FileSystem fs, Path path, Configuration c) throws IOException;
    void close() throws IOException;
    Entry next() throws IOException;
    Entry next(Entry reuse) throws IOException;
    void seek(long pos) throws IOException;
    long getPosition() throws IOException;
  }

  public interface Writer {
    void init(FileSystem fs, Path path, Configuration c) throws IOException;
    void close() throws IOException;
    void sync() throws IOException;
    void append(Entry entry) throws IOException;
    long getLength() throws IOException;
  }

  /*
   * Current log file.
   */
  Writer writer;

  /*
   * Map of all log files but the current one.
   */
  final public SortedMap<LongWritable, Path> outputfiles =
    Collections.synchronizedSortedMap(new TreeMap<LongWritable, Path>());

  /*
   * Map of regions to most recent sequence/edit id in their memstore.
   * Key is encoded region name.
   */
  private final ConcurrentSkipListMap<byte [], LongWritable> lastSeqWritten =
    new ConcurrentSkipListMap<byte [], LongWritable>(Bytes.BYTES_COMPARATOR);

  private volatile boolean closed = false;

  private final AtomicLong logSeqNum = new AtomicLong(0);

  // The timestamp (in second) when the log file was created.
  private volatile int filenum = -1;

  //number of transactions in the current Hlog.
  private final AtomicInteger numEntries = new AtomicInteger(0);

  // If > than this size, roll the log. This is typically 0.95 times the size
  // of the default Hdfs block size.
  private final long logrollsize;

  // This lock prevents starting a log roll during a cache flush.
  // synchronized is insufficient because a cache flush spans two method calls.
  private final Lock cacheFlushLock = new ReentrantLock();

  // We synchronize on updateLock to prevent updates and to prevent a log roll
  // during an update
  // locked during appends
  private final Object updateLock = new Object();

  private final boolean enabled;

  /*
   * If more than this many logs, force flush of oldest region to oldest edit
   * goes to disk.  If too many and we crash, then will take forever replaying.
   * Keep the number of logs tidy.
   */
  private final int maxLogs;

  /**
   * Thread that handles optional sync'ing
   */
  private final LogSyncer logSyncerThread;

  /**
   * Pattern used to validate a HLog file name
   */
  private static final Pattern pattern = Pattern.compile(".*\\.\\d*");

  static byte [] COMPLETE_CACHE_FLUSH;
  static {
    try {
      COMPLETE_CACHE_FLUSH =
        "HBASE::CACHEFLUSH".getBytes(HConstants.UTF8_ENCODING);
    } catch (UnsupportedEncodingException e) {
      assert(false);
    }
  }

  // For measuring latency of writes
  private static volatile long writeOps;
  private static volatile long writeTime;
  // For measuring latency of syncs
  private static volatile long syncOps;
  private static volatile long syncTime;
  
  public static long getWriteOps() {
    long ret = writeOps;
    writeOps = 0;
    return ret;
  }

  public static long getWriteTime() {
    long ret = writeTime;
    writeTime = 0;
    return ret;
  }

  public static long getSyncOps() {
    long ret = syncOps;
    syncOps = 0;
    return ret;
  }

  public static long getSyncTime() {
    long ret = syncTime;
    syncTime = 0;
    return ret;
  }

  /**
   * Constructor.
   *
   * @param fs filesystem handle
   * @param dir path to where hlogs are stored
   * @param oldLogDir path to where hlogs are archived
   * @param conf configuration to use
   * @throws IOException
   */
  public HLog(final FileSystem fs, final Path dir, final Path oldLogDir,
              final Configuration conf)
  throws IOException {
    this(fs, dir, oldLogDir, conf, null, true, null);
  }

  /**
   * Create an edit log at the given <code>dir</code> location.
   *
   * You should never have to load an existing log. If there is a log at
   * startup, it should have already been processed and deleted by the time the
   * HLog object is started up.
   *
   * @param fs filesystem handle
   * @param dir path to where hlogs are stored
   * @param oldLogDir path to where hlogs are archived
   * @param conf configuration to use
   * @param listeners Listeners on WAL events. Listeners passed here will
   * be registered before we do anything else; e.g. the
   * Constructor {@link #rollWriter()}.
   * @param prefix should always be hostname and port in distributed env and
   *        it will be URL encoded before being used.
   *        If prefix is null, "hlog" will be used
   * @throws IOException
   */
  public HLog(final FileSystem fs, final Path dir, final Path oldLogDir,
      final Configuration conf, final List<WALObserver> listeners,
      final String prefix) throws IOException {
    this(fs, dir, oldLogDir, conf, listeners, true, prefix);
  }

  /**
   * Create an edit log at the given <code>dir</code> location.
   *
   * You should never have to load an existing log. If there is a log at
   * startup, it should have already been processed and deleted by the time the
   * HLog object is started up.
   *
   * @param fs filesystem handle
   * @param dir path to where hlogs are stored
   * @param oldLogDir path to where hlogs are archived
   * @param conf configuration to use
   * @param listeners Listeners on WAL events. Listeners passed here will
   * be registered before we do anything else; e.g. the
   * Constructor {@link #rollWriter()}.
   * @param failIfLogDirExists If true IOException will be thrown if dir already exists.
   * @param prefix should always be hostname and port in distributed env and
   *        it will be URL encoded before being used.
   *        If prefix is null, "hlog" will be used
   * @throws IOException
   */
  public HLog(final FileSystem fs, final Path dir,
      final Path oldLogDir,
      final Configuration conf, 
      final List<WALObserver> listeners,
      final boolean failIfLogDirExists, final String prefix)
 throws IOException {
    super();
    
    this.fs = fs;
    this.dir = dir;
    this.tmpDir = this.createTmpDir();
    this.conf = conf;
    if (listeners != null) {
      for (WALObserver i: listeners) {
        registerWALActionsListener(i);
      }
    }
    this.blocksize = conf.getLong("hbase.regionserver.hlog.blocksize",
      this.fs.getDefaultBlockSize());
    // Roll at 95% of block size.
    float multi = conf.getFloat("hbase.regionserver.logroll.multiplier", 0.95f);
    
    this.logrollsize = (long)(this.blocksize * multi);

    Debug.debug("hbase.regionserver.logroll.multiplier: " + multi + " blocksize: " + blocksize+ " rollsize: " + logrollsize);
    
    this.optionalFlushInterval =
      conf.getLong("hbase.regionserver.optionallogflushinterval", 1 * 1000);
    if (failIfLogDirExists && fs.exists(dir)) {
      throw new IOException("Target HLog directory already exists: " + dir);
    }
    
    if (!fs.mkdirs(dir)) {
      throw new IOException("Unable to mkdir " + dir);
    }
    if(!fs.mkdirs(this.tmpDir)){
      throw new IOException("Unable to create tmp dir " + tmpDir);
    }

    this.oldLogDir = oldLogDir;
    if (!fs.exists(oldLogDir)) {
      if (!fs.mkdirs(this.oldLogDir)) {
        throw new IOException("Unable to mkdir " + this.oldLogDir);
      }
    }
    LOG.debug("oldPath: " + oldLogDir + ", dir: " + dir);
    
    // initial the previous output logs:
    initialPreviousOutputLogs(); 
    
    this.maxLogs = conf.getInt("hbase.regionserver.maxlogs", 32);
    this.enabled = conf.getBoolean("hbase.regionserver.hlog.enabled", true);
    LOG.info("HLog configuration: blocksize=" +
      StringUtils.byteDesc(this.blocksize) +
      ", rollsize=" + StringUtils.byteDesc(this.logrollsize) +
      ", enabled=" + this.enabled +
      ", optionallogflushinternal=" + this.optionalFlushInterval + "ms");
    // If prefix is null||empty then just name it hlog
    this.prefix = prefix == null || prefix.isEmpty() ?
        "hlog" : URLEncoder.encode(prefix, "UTF8");
    // rollWriter sets this.hdfs_out if it can.
    
    rollWriter();

//    System.out.println("2");this.showDiskStatus(this.dir);
    // handle the reflection necessary to call getNumCurrentReplicas()
    this.getNumCurrentReplicas = null;
    Exception exception = null;
    if (this.hdfs_out != null) {
      try {
        this.getNumCurrentReplicas = this.hdfs_out.getClass().
          getMethod("getNumCurrentReplicas", new Class<?> []{});
        this.getNumCurrentReplicas.setAccessible(true);
      } catch (NoSuchMethodException e) {
        // Thrown if getNumCurrentReplicas() function isn't available
        exception = e;
      } catch (SecurityException e) {
        // Thrown if we can't get access to getNumCurrentReplicas()
        exception = e;
        this.getNumCurrentReplicas = null; // could happen on setAccessible()
      }
    }
    if (this.getNumCurrentReplicas != null) {
      LOG.info("Using getNumCurrentReplicas--HDFS-826");
    } else {
      LOG.info("getNumCurrentReplicas--HDFS-826 not available; hdfs_out=" +
        this.hdfs_out + ", exception=" + exception.getMessage());
    }
    logSyncerThread = new LogSyncer(this.optionalFlushInterval);
    Threads.setDaemonThreadRunning(logSyncerThread,
        Thread.currentThread().getName() + ".logSyncer");
  }
  
  private void initialPreviousOutputLogs()throws IOException{
    FileStatus[] status = fs.listStatus(this.dir);
    for(FileStatus s : status){
      Path path = s.getPath();
      LongWritable fileNum = new LongWritable(this.computeFilenum(path));
      this.outputfiles.put(fileNum, path);
    }
    if(this.outputfiles.size() > 0)this.logSeqNum.set(outputfiles.lastKey().get());
  }

  public void registerWALActionsListener (final WALObserver listener) {
    this.listeners.add(listener);
  }

  public boolean unregisterWALActionsListener(final WALObserver listener) {
    return this.listeners.remove(listener);
  }

  /**
   * @return Current state of the monotonically increasing file id.
   */
  public int getFilenum() {
    return this.filenum;
  }

  /**
   * Called by HRegionServer when it opens a new region to ensure that log
   * sequence numbers are always greater than the latest sequence number of the
   * region being brought on-line.
   *
   * @param newvalue We'll set log edit/sequence number to this value if it
   * is greater than the current value.
   */
  public void setSequenceNumber(final long newvalue) {
    for (long id = this.logSeqNum.get(); id < newvalue &&
        !this.logSeqNum.compareAndSet(id, newvalue); id = this.logSeqNum.get()) {
      // This could spin on occasion but better the occasional spin than locking
      // every increment of sequence number.
      LOG.debug("Changed sequenceid from " + logSeqNum + " to " + newvalue);
    }
  }

  /**
   * @return log sequence number
   */
  public long getSequenceNumber() {
    return logSeqNum.get();
  }

  // usage: see TestLogRolling.java
  OutputStream getOutputStream() {
    return this.hdfs_out;
  }

  /**
   * Roll the log writer. That is, start writing log messages to a new file.
   *
   * Because a log cannot be rolled during a cache flush, and a cache flush
   * spans two method calls, a special lock needs to be obtained so that a cache
   * flush cannot start when the log is being rolled and the log cannot be
   * rolled during a cache flush.
   *
   * <p>Note that this method cannot be synchronized because it is possible that
   * startCacheFlush runs, obtaining the cacheFlushLock, then this method could
   * start which would obtain the lock on this but block on obtaining the
   * cacheFlushLock and then completeCacheFlush could be called which would wait
   * for the lock on this and consequently never release the cacheFlushLock
   *
   * @return If lots of logs, flush the returned regions so next time through
   * we can clean logs. Returns null if nothing to flush.  Names are actual
   * region names as returned by {@link HRegionInfo#getEncodedName()}
   * @throws org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException
   * @throws IOException
   */
  public byte [][] rollWriter() throws FailedLogCloseException, IOException {
    // Return if nothing to flush.
    if (this.writer != null && this.numEntries.get() <= 0) {
      return null;
    }
    byte [][] regionsToFlush = null;
    this.cacheFlushLock.lock();
    try {
      if (closed) {
        return regionsToFlush;
      }
      // Do all the preparation outside of the updateLock to block
      // as less as possible the incoming writes
      int currentFilenum = this.filenum;
      
      this.filenum = this.createFileNum();
//      System.out.println("map: " + filenum + " vs " + )
      Path newPath = computeFilename();
      HLog.Writer nextWriter = this.createWriterInstance(fs, newPath, conf);
//      LOG.debug("create log file: " + newPath);
      int nextInitialReplication = fs.getFileStatus(newPath).getReplication();
      // Can we get at the dfsclient outputstream?  If an instance of
      // SFLW, it'll have done the necessary reflection to get at the
      // protected field name.
      OutputStream nextHdfsOut = null;
      if (nextWriter instanceof SequenceFileLogWriter) {
        nextHdfsOut =
          ((SequenceFileLogWriter)nextWriter).getDFSCOutputStream();
      }
      // Tell our listeners that a new log was created
      if (!this.listeners.isEmpty()) {
        for (WALObserver i : this.listeners) {
          i.logRolled(newPath);
        }
      }

      synchronized (updateLock) {
        // Clean up current writer.
        Path oldFile = cleanupCurrentWriter(currentFilenum);
        this.writer = nextWriter;
        this.initialReplication = nextInitialReplication;
        this.hdfs_out = nextHdfsOut;

        LOG.info((oldFile != null?
            "Roll " + FSUtils.getPath(oldFile) + ", entries=" +
            this.numEntries.get() +
            ", filesize=" +
            this.fs.getFileStatus(oldFile).getLen() + ". ": "") +
          "New hlog " + FSUtils.getPath(newPath));
        this.numEntries.set(0);
        this.logRollRequested = false;
      }
      // Can we delete any of the old log files?
      // Yuting: this should be removed, because we clean the log in other conditions
//      if (this.outputfiles.size() > 0) {
//        if (this.lastSeqWritten.isEmpty()) {
//          LOG.debug("Last sequenceid written is empty. Deleting all old hlogs");
//          // If so, then no new writes have come in since all regions were
//          // flushed (and removed from the lastSeqWritten map). Means can
//          // remove all but currently open log file.
//          for (Map.Entry<Long, Path> e : this.outputfiles.entrySet()) {
//            archiveLogFile(e.getValue(), e.getKey());
//          }
//          this.outputfiles.clear();
//        } else {
//          regionsToFlush = cleanOldLogs();
//        }
//      }
      if(needToCleanLog() == true){
      	return cleanLog();
      }
    } finally {
      this.cacheFlushLock.unlock();
    }
    return regionsToFlush;
  }

  // TODO: set the log to remove
  boolean needToCleanLog(){
  	return false;
  }
  byte[][] cleanLog(){
  	return null;
  }

//  private int createFileNum(){
//    return (int) System.currentTimeMillis() / 100;
//  }

  // Here the log file number should be in order, 
  // so, we just need to scan the file in reverse-order
  // and write the new record in the new log
  void compact(MemIndex index) throws IOException{
  	// create snapshot for the memIndex
  }
  
  /**
   * compact the 
   * @param memIndex
   * @param logFilesToCompact
   * @param majorCompact
   * @throws IOException
   */
  void compact(MemIndex memIndex, final SortedMap<Long, Path> logFilesToCompact,
            boolean majorCompact)throws IOException{
    int compactFileNum = this.createFileNum();
    Path newPath = createTmpFile(compactFileNum);
    long newLogOffset;
    HLog.Writer nextWriter = this.createWriterInstance(fs, newPath, conf);
    
    // don't need to care the order to scan the HLog
    Set<Map.Entry<Long, Path>> set = logFilesToCompact.entrySet();
    HLog.Entry entry = new HLog.Entry();
    
    KeyValue.Key key = null;
    int numUpdateEntries = 0;
    
    LogScanner scanner = LogScannerFactory.getScanner(ScannerType.ByFile);
    scanner.init(outputfiles, this.fs, this.conf, memIndex);
    
    KeyValue kv = null;
    long seqNum = obtainSeqNum();
    long now = System.currentTimeMillis();
    
    HLogKey logKey = makeKey(Bytes.EMPTY_BYTES, Bytes.EMPTY_BYTES, seqNum, now);
    WALEdit edit = new WALEdit();
    List<LogEntryOffset> offsets = null;
    long beginOffset;
    while((kv = scanner.next()) != null){
    	edit.clear();
    	edit.add(kv);
      key = kv.getKeyForLog();
    	entry.setEntry(logKey, edit);
    	beginOffset = nextWriter.getLength();
    	nextWriter.append(entry);
      numUpdateEntries ++;
      
    	// update index
    	offsets = entry.getKeyValueOffset(beginOffset, compactFileNum, offsets);
    	memIndex.add(key, offsets.get(0));
    }
    nextWriter.close();

    if(numUpdateEntries != memIndex.getEntriesNum())
      throw new IOException("the number of updated KeyValue entries(" + numUpdateEntries + 
          ") is not equal to the number of entries in the memIndex(" + memIndex.getEntriesNum() + ")");

    return;
    
//    // write to the new file
//    for(Map.Entry<Long, Path> mapEntry: set){
//      int fileNum = mapEntry.getKey().intValue();
//      Path hlogPath = mapEntry.getValue(); 
//      HLog.Reader reader = HLog.getReader(fs, hlogPath, conf);
//      long previousLogPos;
//      
//      // TODO: compact 
//      do{
//        previousLogPos = reader.getPosition();
//        if(reader.next(entry) == null) break;
//        List<KeyValue> kvs = entry.edit.getKeyValues();
//        previousLogPos += entry.edit.prefixLength();
//        
//        newLogOffset = nextWriter.getLength() + entry.key.getLengthWithEmptyRegion();
//        
//        for(int i = 0;i < kvs.size();i ++){
//          KeyValue kv = kvs.get(i);
//          kv.getKeyForLog(key);
//          LogEntryOffset offsetEntry = memIndex.getOffset(key);
//          if(offsetEntry.filenum == fileNum && offsetEntry.offset == previousLogPos){
//            // this is the latest record, update the data in this index
//            offsetEntry.filenum = compactFileNum;
//            offsetEntry.offset = (int)newLogOffset;
//            // don't need to change the offset.size()
//            newLogOffset += kv.getSerializationLength();
//            
//            numUpdateEntries ++;
//          }else{
//            kvs.remove(i);
//            i --;
//          }
//        }
//        // write to the new log, no need to flush each time.
//        // if possible, can add some statistic info here in the future
//        nextWriter.append(entry);
//      }while(true);
//    }
//    
//    
//    if(numUpdateEntries != memIndex.getEntriesNum())
//      throw new IOException("the number of updated KeyValue entries(" + numUpdateEntries + 
//          ") is not equal to the number of entries in the memIndex(" + memIndex.getEntriesNum() + ")");
//    // TODO: if the log is larger than certain threshold, shall we roll the writer?
//    writer.close();
//    
//    updateCompactLog();
  }
  
  /**
   * TODO: remove the old log in the snapshot, and replace by the new logs 
   */
  public void updateCompactLog()throws IOException{
    // delete the old logs
    Collection<Path> oldLogs = this.outputfiles.values();
    for(Path oldLog : oldLogs){
      fs.delete(oldLog, true);
    }
    // rename the new Log
    Path[] newLogs = HUtils.listFilesPath(fs, tmpDir);
    for(Path newLog : newLogs){
      Path dest = new Path(dir, newLog.getName());
      boolean flag = fs.rename(newLog, dest);
      if(!flag) throw new IOException("fail to rename: " + newLog + " to " + dest);
    }
  }
  
  private Path createTmpDir()throws IOException{
    Path tmpDir = dir.suffix("-tmp");
    fs.delete(tmpDir, true);          // remove the previous 
    return tmpDir;
  }
  
  protected Path createTmpFile(long filenum)throws IOException{
    if (filenum < 0) {
      throw new RuntimeException("hlog file number can't be < 0");
    }
    return new Path(this.tmpDir, prefix + "." + filenum);
  }
  
  /**
   * This method allows subclasses to inject different writers without having to
   * extend other methods like rollWriter().
   * 
   * @param fs
   * @param path
   * @param conf
   * @return Writer instance
   * @throws IOException
   */
  protected Writer createWriterInstance(final FileSystem fs, final Path path,
      final Configuration conf) throws IOException {
    return createWriter(fs, path, conf);
  }

  /**
   * Get a reader for the WAL.
   * @param fs
   * @param path
   * @param conf
   * @return A WAL reader.  Close when done with it.
   * @throws IOException
   */
  public static Reader getReader(final FileSystem fs,
    final Path path, Configuration conf)
  throws IOException {
    try {
      if (logReaderClass == null) {
        logReaderClass = conf.getClass("hbase.regionserver.hlog.reader.impl",
            SequenceFileLogReader.class, Reader.class);
      }

      HLog.Reader reader = logReaderClass.newInstance();
      reader.init(fs, path, conf);
      return reader;
    } catch (IOException e) {
      throw e;
    }
    catch (Exception e) {
      throw new IOException("Cannot get log reader", e);
    }
  }

  /**
   * Get a writer for the WAL.
   * @param path
   * @param conf
   * @return A WAL writer.  Close when done with it.
   * @throws IOException
   */
  public static Writer createWriter(final FileSystem fs,
      final Path path, Configuration conf)
  throws IOException {
    try {
      if (logWriterClass == null) {
        logWriterClass = conf.getClass("hbase.regionserver.hlog.writer.impl",
            SequenceFileLogWriter.class, Writer.class);
      }
      HLog.Writer writer = (HLog.Writer) logWriterClass.newInstance();
      writer.init(fs, path, conf);
      return writer;
    } catch (Exception e) {
      System.out.println("the reason is " + e);
      IOException ie = new IOException("cannot get log writer");
      ie.initCause(e);
      throw ie;
    }
  }

  /*
   * Clean up old commit logs.
   * @return If lots of logs, flush the returned region so next time through
   * we can clean logs. Returns null if nothing to flush.  Returns array of
   * encoded region names to flush.
   * @throws IOException
   */
  private byte [][] cleanOldLogs() throws IOException {
    LongWritable oldestOutstandingSeqNum = getOldestOutstandingSeqNum();
    // Get the set of all log files whose last sequence number is smaller than
    // the oldest edit's sequence number.
    TreeSet<LongWritable> sequenceNumbers =
      new TreeSet<LongWritable>(this.outputfiles.headMap(
        (new LongWritable(oldestOutstandingSeqNum.get()) )).keySet());
    // Now remove old log files (if any)
    int logsToRemove = sequenceNumbers.size();
    if (logsToRemove > 0) {
      if (LOG.isDebugEnabled()) {
        // Find associated region; helps debugging.
        byte [] oldestRegion = getOldestRegion(oldestOutstandingSeqNum);
        LOG.debug("Found " + logsToRemove + " hlogs to remove" +
          " out of total " + this.outputfiles.size() + ";" +
          " oldest outstanding sequenceid is " + oldestOutstandingSeqNum +
          " from region " + Bytes.toStringBinary(oldestRegion));
      }
      for (LongWritable seq : sequenceNumbers) {
        archiveLogFile(this.outputfiles.remove(seq), seq);
      }
    }

    // If too many log files, figure which regions we need to flush.
    // Array is an array of encoded region names.
    byte [][] regions = null;
    int logCount = this.outputfiles.size();
    if (logCount > this.maxLogs && this.outputfiles != null &&
        this.outputfiles.size() > 0) {
      // This is an array of encoded region names.
      regions = findMemstoresWithEditsEqualOrOlderThan(this.outputfiles.firstKey().get(),
        this.lastSeqWritten);
      if (regions != null) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < regions.length; i++) {
          if (i > 0) sb.append(", ");
          sb.append(Bytes.toStringBinary(regions[i]));
        }
        LOG.info("Too many hlogs: logs=" + logCount + ", maxlogs=" +
           this.maxLogs + "; forcing flush of " + regions.length + " regions(s): " +
           sb.toString());
      }
    }
    return regions;
  }

  /**
   * Return regions (memstores) that have edits that are equal or less than
   * the passed <code>oldestWALseqid</code>.
   * @param oldestWALseqid
   * @param regionsToSeqids
   * @return All regions whose seqid is < than <code>oldestWALseqid</code> (Not
   * necessarily in order).  Null if no regions found.
   */
  static byte [][] findMemstoresWithEditsEqualOrOlderThan(final long oldestWALseqid,
      final Map<byte [], LongWritable> regionsToSeqids) {
    //  This method is static so it can be unit tested the easier.
    List<byte []> regions = null;
    for (Map.Entry<byte [], LongWritable> e: regionsToSeqids.entrySet()) {
      if (e.getValue().get() <= oldestWALseqid) {
        if (regions == null) regions = new ArrayList<byte []>();
        regions.add(e.getKey());
      }
    }
    return regions == null?
      null: regions.toArray(new byte [][] {HConstants.EMPTY_BYTE_ARRAY});
  }

  /*
   * @return Logs older than this id are safe to remove.
   */
  private LongWritable getOldestOutstandingSeqNum() {
    return Collections.min(this.lastSeqWritten.values());
  }

  /**
   * @param oldestOutstandingSeqNum
   * @return (Encoded) name of oldest outstanding region.
   */
  private byte [] getOldestRegion(final LongWritable oldestOutstandingSeqNum) {
    byte [] oldestRegion = null;
    for (Map.Entry<byte [], LongWritable> e: this.lastSeqWritten.entrySet()) {
      if (e.getValue().get() == oldestOutstandingSeqNum.get()) {
        oldestRegion = e.getKey();
        break;
      }
    }
    return oldestRegion;
  }

  /*
   * Cleans up current writer closing and adding to outputfiles.
   * Presumes we're operating inside an updateLock scope.
   * @return Path to current writer or null if none.
   * @throws IOException
   */
  private Path cleanupCurrentWriter(final int currentfilenum)
  throws IOException {
    Path oldFile = null;
    if (this.writer != null) {
      // Close the current writer, get a new one.
      try {
        this.writer.close();
      } catch (IOException e) {
        // Failed close of log file.  Means we're losing edits.  For now,
        // shut ourselves down to minimize loss.  Alternative is to try and
        // keep going.  See HBASE-930.
        FailedLogCloseException flce =
          new FailedLogCloseException("#" + currentfilenum);
        flce.initCause(e);
        throw e;
      }
      if (currentfilenum >= 0) {
        oldFile = computeFilename(currentfilenum);
        //this.outputfiles.put(new LongWritable(this.logSeqNum.get()), oldFile);
        this.outputfiles.put(new LongWritable(currentfilenum), oldFile);
        //System.out.println("DEBUG: new log files: file num " + currentfilenum + " LSN: " + this.logSeqNum.get());
      }
    }
    return oldFile;
  }
  
  public int getCurrentFileNumber(){
	  return this.filenum;
  }
  
  private void archiveLogFile(final Path p, final LongWritable seqno) throws IOException {
    Path newPath = getHLogArchivePath(this.oldLogDir, p);
    LOG.info("moving old hlog file " + FSUtils.getPath(p) +
      " whose highest sequenceid is " + seqno + " to " +
      FSUtils.getPath(newPath));
    if (!this.fs.rename(p, newPath)) {
      throw new IOException("Unable to rename " + p + " to " + newPath);
    }
  }

  /**
   * This is a convenience method that computes a new filename with a given
   * using the current HLog file-number
   * @return Path
   */
  protected Path computeFilename() {
    return computeFilename(this.filenum);
  }

  /**
   * This is a convenience method that computes a new filename with a given
   * file-number.
   * @param filenum to use
   * @return Path
   */
  protected Path computeFilename(int filenum) {
    if (filenum < 0) {
      throw new RuntimeException("hlog file number can't be < 0");
    }
    return new Path(dir, prefix + "." + filenum);
  }

  protected int computeFilenum(Path path){
    if(path == null) return -1;
    String fileName = path.getName();
//    System.out.println("HLog.computeFilenum(): " + fileName.substring(fileName.indexOf(".") + 1));
    return Integer.parseInt(fileName.substring(fileName.indexOf(".") + 1));
  }
  
  /**
   * Shut down the log and delete the log directory
   *
   * @throws IOException
   */
  public void closeAndDelete() throws IOException {
    close();
    FileStatus[] files = fs.listStatus(this.dir);
    for(FileStatus file : files) {
      Path p = getHLogArchivePath(this.oldLogDir, file.getPath());
      if (!fs.rename(file.getPath(),p)) {
        throw new IOException("Unable to rename " + file.getPath() + " to " + p);
      }
    }
    LOG.debug("Moved " + files.length + " log files to " +
        FSUtils.getPath(this.oldLogDir));
    if (!fs.delete(dir, true)) {
      LOG.info("Unable to delete " + dir);
    }else{
      LOG.info("delete log: " + dir);
    }
  }

  /**
   * Shut down the log.
   *
   * @throws IOException
   */
  public void close() throws IOException {
    try {
      logSyncerThread.interrupt();
      // Make sure we synced everything
      logSyncerThread.join(this.optionalFlushInterval*2);
    } catch (InterruptedException e) {
      LOG.error("Exception while waiting for syncer thread to die", e);
    }

    cacheFlushLock.lock();
    try {
      // Tell our listeners that the log is closing
      if (!this.listeners.isEmpty()) {
        for (WALObserver i : this.listeners) {
          i.logCloseRequested();
        }
      }
      synchronized (updateLock) {
        this.closed = true;
        if (LOG.isDebugEnabled()) {
          LOG.debug("closing hlog writer in " + this.dir.toString());
        }
        this.writer.close();
        
        //commented out by huanghao (no delete)
//        if(this.numEntries.get() <= 0){ 
//          fs.delete(this.computeFilename(this.filenum), true);
//          System.out.println("delete " + this.computeFilename(this.filenum));
//        }
//        System.out.println("close the hlog"); this.showDiskStatus(this.dir);
      }
    } finally {
      cacheFlushLock.unlock();
    }
  }

   /** Append an entry to the log.
   *
   * @param regionInfo
   * @param logEdit
   * @param now Time of this edit write.
   * @throws IOException
   */
  public void append(HRegionInfo regionInfo, WALEdit logEdit,
    final long now,
    final boolean isMetaRegion)
  throws IOException {
    byte [] regionName = regionInfo.getEncodedNameAsBytes();
    byte [] tableName = regionInfo.getTableDesc().getName();
    this.append(regionInfo, makeKey(regionName, tableName, -1, now), logEdit);
  }

  /**
   * @param now
   * @param regionName
   * @param tableName
   * @return New log key.
   */
  protected HLogKey makeKey(byte[] regionName, byte[] tableName, long seqnum, long now) {
    return new HLogKey(regionName, tableName, seqnum, now);
  }



  /** Append an entry to the log.
   *
   * @param regionInfo
   * @param logEdit
   * @param logKey
   * @throws IOException
   */
  public void append(HRegionInfo regionInfo, HLogKey logKey, WALEdit logEdit)
  throws IOException {
    if (this.closed) {
      throw new IOException("Cannot append; log is closed");
    }
    synchronized (updateLock) {
      long seqNum = obtainSeqNum();
      logKey.setLogSeqNum(seqNum);
      // The 'lastSeqWritten' map holds the sequence number of the oldest
      // write for each region (i.e. the first edit added to the particular
      // memstore). When the cache is flushed, the entry for the
      // region being flushed is removed if the sequence number of the flush
      // is greater than or equal to the value in lastSeqWritten.
      this.lastSeqWritten.putIfAbsent(regionInfo.getEncodedNameAsBytes(), new LongWritable(seqNum));
      doWrite(regionInfo, logKey, logEdit);
      this.numEntries.incrementAndGet();
    }

    // Sync if catalog region, and if not then check if that table supports
    // deferred log flushing
    if (regionInfo.isMetaRegion() ||
        !regionInfo.getTableDesc().isDeferredLogFlush()) {
      // sync txn to file system
      this.sync();
    }
  }

  /**
   * Append a set of edits to the log. Log edits are keyed by (encoded)
   * regionName, rowname, and log-sequence-id.
   *
   * Later, if we sort by these keys, we obtain all the relevant edits for a
   * given key-range of the HRegion (TODO). Any edits that do not have a
   * matching COMPLETE_CACHEFLUSH message can be discarded.
   *
   * <p>
   * Logs cannot be restarted once closed, or once the HLog process dies. Each
   * time the HLog starts, it must create a new log. This means that other
   * systems should process the log appropriately upon each startup (and prior
   * to initializing HLog).
   *
   * synchronized prevents appends during the completion of a cache flush or for
   * the duration of a log roll.
   *
   * @param info
   * @param tableName
   * @param edits
   * @param now
   * @throws IOException
   */
  public void append(HRegionInfo info, byte [] tableName, WALEdit edits,
    final long now)
  throws IOException {
    if (edits.isEmpty()) return;
    if (this.closed) {
      throw new IOException("Cannot append; log is closed");
    }
    synchronized (this.updateLock) {
      long seqNum = obtainSeqNum();
      // The 'lastSeqWritten' map holds the sequence number of the oldest
      // write for each region (i.e. the first edit added to the particular
      // memstore). . When the cache is flushed, the entry for the
      // region being flushed is removed if the sequence number of the flush
      // is greater than or equal to the value in lastSeqWritten.
      // Use encoded name.  Its shorter, guaranteed unique and a subset of
      // actual  name.
      byte [] hriKey = info.getEncodedNameAsBytes();
      this.lastSeqWritten.putIfAbsent(hriKey, new LongWritable(seqNum));
      HLogKey logKey = makeKey(hriKey, tableName, seqNum, now);
      doWrite(info, logKey, edits);
      this.numEntries.incrementAndGet();
    }
    // Sync if catalog region, and if not then check if that table supports
    // deferred log flushing
    if (info.isMetaRegion() ||
        !info.getTableDesc().isDeferredLogFlush()) {
      // sync txn to file system
      this.sync();
    }
  }
  
  List<LogEntryOffset> offsets = new ArrayList<LogEntryOffset>();
  /**
   * Yuting: This append() is used for the log-only store.
   * Since each store manages its own log, no regionInfo and the tableName
   * is required in this function.
   *  
   * @param tableName
   * @param edits
   * @param now
   * @throws IOException
   * @author 
   */
  public List<LogEntryOffset> append(WALEdit edits, final long now) throws IOException{
  	// TODO: return the log offset
    if (edits.isEmpty()) return null;
    if (this.closed) {
      throw new IOException("Cannot append; log is closed");
    }
    HLogKey logKey ;
    synchronized (this.updateLock) {
      long seqNum = obtainSeqNum();
      // The 'lastSeqWritten' map holds the sequence number of the oldest
      // write for each region (i.e. the first edit added to the particular
      // memstore). . When the cache is flushed, the entry for the
      // region being flushed is removed if the sequence number of the flush
      // is greater than or equal to the value in lastSeqWritten.
      // Use encoded name.  Its shorter, guaranteed unique and a subset of
      // actual  name.
      long beginOffset = writer.getLength();
      logKey = makeKey(Bytes.EMPTY_BYTES, Bytes.EMPTY_BYTES, seqNum, now);
      HLog.Entry entry = new HLog.Entry(logKey, edits);
      doWrite(null, entry);
      offsets = entry.getKeyValueOffset(beginOffset, filenum, offsets);
      // TODO: set the RegionInfo as null. It incurs exception when the
      // Replication is used as the WALObserver 
      this.numEntries.incrementAndGet();
      
      
      // now we choose not to defer log flush, so, flush as soon as possible
      // even the performance maybe bad
      this.sync();
      
      
      // VHTam
      // Rolling out the log
      if (this.writer.getLength() > this.logrollsize){
    	  //Debug.debug("Rolling out!!! Inserted " + this.numEntries);
    	  rollWriter();
    	
      }
    
    }
    
    
    return offsets;
  }
  
  /**
   * This thread is responsible to call syncFs and buffer up the writers while
   * it happens.
   */
   class LogSyncer extends Thread {

    private final long optionalFlushInterval;

    LogSyncer(long optionalFlushInterval) {
      this.optionalFlushInterval = optionalFlushInterval;
    }

    @Override
    public void run() {
      try {
        // awaiting with a timeout doesn't always
        // throw exceptions on interrupt
        while(!this.isInterrupted()) {

          Thread.sleep(this.optionalFlushInterval);
          sync();
        }
      } catch (IOException e) {
        LOG.error("Error while syncing, requesting close of hlog ", e);
        requestLogRoll();
      } catch (InterruptedException e) {
        LOG.debug(getName() + " interrupted while waiting for sync requests");
      } finally {
        LOG.info(getName() + " exiting");
      }
    }
  }

  @Override
  public void sync() throws IOException {
    synchronized (this.updateLock) {
      if (this.closed) {
        return;
      }
    }
    try {
      long now = System.currentTimeMillis();
      // Done in parallel for all writer threads, thanks to HDFS-895
      this.writer.sync();
      synchronized (this.updateLock) {
        syncTime += System.currentTimeMillis() - now;
        syncOps++;
        if (!logRollRequested) {
          checkLowReplication();
          if (this.writer.getLength() > this.logrollsize) {
            requestLogRoll();
          }
        }
      }

    } catch (IOException e) {
      LOG.fatal("Could not append. Requesting close of hlog", e);
      requestLogRoll();
      throw e;
    }
  }

  private void checkLowReplication() {
    // if the number of replicas in HDFS has fallen below the initial
    // value, then roll logs.
    try {
      int numCurrentReplicas = getLogReplication();
      if (numCurrentReplicas != 0 &&
          numCurrentReplicas < this.initialReplication) {
        LOG.warn("HDFS pipeline error detected. " +
            "Found " + numCurrentReplicas + " replicas but expecting " +
            this.initialReplication + " replicas. " +
            " Requesting close of hlog.");
        requestLogRoll();
        logRollRequested = true;
      }
    } catch (Exception e) {
      LOG.warn("Unable to invoke DFSOutputStream.getNumCurrentReplicas" + e +
          " still proceeding ahead...");
    }
  }

  /**
   * This method gets the datanode replication count for the current HLog.
   *
   * If the pipeline isn't started yet or is empty, you will get the default
   * replication factor.  Therefore, if this function returns 0, it means you
   * are not properly running with the HDFS-826 patch.
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   *
   * @throws Exception
   */
  int getLogReplication() throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
    if(this.getNumCurrentReplicas != null && this.hdfs_out != null) {
      Object repl = this.getNumCurrentReplicas.invoke(this.hdfs_out, NO_ARGS);
      if (repl instanceof Integer) {
        return ((Integer)repl).intValue();
      }
    }
    return 0;
  }

  boolean canGetCurReplicas() {
    return this.getNumCurrentReplicas != null;
  }

  public void hsync() throws IOException {
    // Not yet implemented up in hdfs so just call hflush.
    sync();
  }

  private void requestLogRoll() {
    if (!this.listeners.isEmpty()) {
      for (WALObserver i: this.listeners) {
        i.logRollRequested();
      }
    }
  }

  protected void doWrite(HRegionInfo info, HLog.Entry entry)throws IOException{
    if (!this.enabled) {
      return;
    }
    if (!this.listeners.isEmpty()) {
      for (WALObserver i: this.listeners) {
        i.visitLogEntryBeforeWrite(info, entry.key, entry.edit);
      }
    }
    try {
      long now = System.currentTimeMillis();
      this.writer.append(entry);
      long took = System.currentTimeMillis() - now;
      writeTime += took;
      writeOps++;
      if (took > 1000) {
        long len = 0;
        for(KeyValue kv : entry.edit.getKeyValues()) { 
          len += kv.getLength(); 
        }
        LOG.warn(String.format(
          "%s took %d ms appending an edit to hlog; editcount=%d, len~=%s",
          Thread.currentThread().getName(), took, this.numEntries.get(), 
          StringUtils.humanReadableInt(len)));
      }
    } catch (IOException e) {
      LOG.fatal("Could not append. Requesting close of hlog", e);
      requestLogRoll();
      throw e;
    }
  }
  
  protected void doWrite(HRegionInfo info, HLogKey logKey, WALEdit logEdit)
  throws IOException {
  	doWrite(info, new HLog.Entry(logKey, logEdit));
  }

  /** @return How many items have been added to the log */
  int getNumEntries() {
    return numEntries.get();
  }

  /**
   * Obtain a log sequence number.
   */
  private long obtainSeqNum() {
    return this.logSeqNum.incrementAndGet();
  }

  /** @return the number of log files in use */
  int getNumLogFiles() {
    return outputfiles.size();
  }

  /**
   * By acquiring a log sequence ID, we can allow log messages to continue while
   * we flush the cache.
   *
   * Acquire a lock so that we do not roll the log between the start and
   * completion of a cache-flush. Otherwise the log-seq-id for the flush will
   * not appear in the correct logfile.
   *
   * @return sequence ID to pass {@link #completeCacheFlush(byte[], byte[], long, boolean)}
   * (byte[], byte[], long)}
   * @see #completeCacheFlush(byte[], byte[], long, boolean)
   * @see #abortCacheFlush()
   */
  public long startCacheFlush() {
    this.cacheFlushLock.lock();
    return obtainSeqNum();
  }

  /**
   * Complete the cache flush
   *
   * Protected by cacheFlushLock
   *
   * @param encodedRegionName
   * @param tableName
   * @param logSeqId
   * @throws IOException
   */
  public void completeCacheFlush(final byte [] encodedRegionName,
      final byte [] tableName, final long logSeqId, final boolean isMetaRegion)
  throws IOException {
    try {
      if (this.closed) {
        return;
      }
      synchronized (updateLock) {
        long now = System.currentTimeMillis();
        WALEdit edit = completeCacheFlushLogEdit();
        HLogKey key = makeKey(encodedRegionName, tableName, logSeqId,
            System.currentTimeMillis());
        this.writer.append(new Entry(key, edit));
        writeTime += System.currentTimeMillis() - now;
        writeOps++;
        this.numEntries.incrementAndGet();
        LongWritable seq = this.lastSeqWritten.get(encodedRegionName);
        if (seq != null && logSeqId >= seq.get()) {
          this.lastSeqWritten.remove(encodedRegionName);
        }
      }
      // sync txn to file system
      this.sync();

    } finally {
      this.cacheFlushLock.unlock();
    }
  }

  private WALEdit completeCacheFlushLogEdit() {
    KeyValue kv = new KeyValue(METAROW, METAFAMILY, null,
      System.currentTimeMillis(), COMPLETE_CACHE_FLUSH);
    WALEdit e = new WALEdit();
    e.add(kv);
    return e;
  }

  /**
   * Abort a cache flush.
   * Call if the flush fails. Note that the only recovery for an aborted flush
   * currently is a restart of the regionserver so the snapshot content dropped
   * by the failure gets restored to the memstore.
   */
  public void abortCacheFlush() {
    this.cacheFlushLock.unlock();
  }

  /**
   * @param family
   * @return true if the column is a meta column
   */
  public static boolean isMetaFamily(byte [] family) {
    return Bytes.equals(METAFAMILY, family);
  }

  @SuppressWarnings("unchecked")
  public static Class<? extends HLogKey> getKeyClass(Configuration conf) {
     return (Class<? extends HLogKey>)
       conf.getClass("hbase.regionserver.hlog.keyclass", HLogKey.class);
  }

  public static HLogKey newKey(Configuration conf) throws IOException {
    Class<? extends HLogKey> keyClass = getKeyClass(conf);
    try {
      return keyClass.newInstance();
    } catch (InstantiationException e) {
      throw new IOException("cannot create hlog key");
    } catch (IllegalAccessException e) {
      throw new IOException("cannot create hlog key");
    }
  }

  /**
   * Utility class that lets us keep track of the edit with it's key
   * Only used when splitting logs
   */
  public static class Entry implements Writable {
    private WALEdit edit;
    private HLogKey key;

    public Entry() {
      edit = new WALEdit();
      key = new HLogKey();
    }

    /**
     * Constructor for both params
     * @param edit log's edit
     * @param key log's key
     */
    public Entry(HLogKey key, WALEdit edit) {
      super();
      this.key = key;
      this.edit = edit;
    }
    
    public void setEntry(HLogKey key, WALEdit edit){
    	this.key = key;
    	this.edit = edit;
    }
    /**
     * Gets the edit
     * @return edit
     */
    public WALEdit getEdit() {
      return edit;
    }
    /**
     * Gets the key
     * @return key
     */
    public HLogKey getKey() {
      return key;
    }

    @Override
    public String toString() {
      return this.key + "=" + this.edit;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      this.key.write(dataOutput);
      this.edit.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      this.key.readFields(dataInput);
      this.edit.readFields(dataInput);
    }
    
    /**
     * 
     * @param beginOffset the beginning offset of this entry
     * @param filenum 
     * @param offsets to be reuse 
     * @return the offset of each KeyValue pair
     */
    public List<LogEntryOffset> getKeyValueOffset(long beginOffset, int filenum, List<LogEntryOffset> reuse){
    	if(reuse == null) reuse = new ArrayList<LogEntryOffset>();
    	reuse.clear();
    	long valueOffset = beginOffset + HSequenceFile.prefixSizeForKeyValuePair() + this.key.getSerializationLength();
    	return this.edit.calculateOffset(filenum, valueOffset, reuse);
    }
  }

  /**
   * Construct the HLog directory name
   *
   * @param info HServerInfo for server
   * @return the HLog directory name
   */
  public static String getHLogDirectoryName(HServerInfo info) {
    return getHLogDirectoryName(info.getServerName());
  }

  /**
   * Construct the HLog directory name
   *
   * @param serverAddress
   * @param startCode
   * @return the HLog directory name
   */
  public static String getHLogDirectoryName(String serverAddress,
      long startCode) {
    if (serverAddress == null || serverAddress.length() == 0) {
      return null;
    }
    return getHLogDirectoryName(
        HServerInfo.getServerName(serverAddress, startCode));
  }

  /**
   * Construct the HLog directory name
   *
   * @param serverName
   * @return the HLog directory name
   */
  public static String getHLogDirectoryName(String serverName) {
    StringBuilder dirName = new StringBuilder(HConstants.HREGION_LOGDIR_NAME);
    dirName.append("/");
    dirName.append(serverName);
    return dirName.toString();
  }

  /**
   * Get the directory we are making logs in.
   * 
   * @return dir
   */
  protected Path getDir() {
    return dir;
  }
  
  public static boolean validateHLogFilename(String filename) {
    return pattern.matcher(filename).matches();
  }

  static Path getHLogArchivePath(Path oldLogDir, Path p) {
    return new Path(oldLogDir, p.getName());
  }

  static String formatRecoveredEditsFileName(final long seqid) {
    return String.format("%019d", seqid);
  }

  /**
   * Returns sorted set of edit files made by wal-log splitter.
   * @param fs
   * @param regiondir
   * @return Files in passed <code>regiondir</code> as a sorted set.
   * @throws IOException
   */
  public static NavigableSet<Path> getSplitEditFilesSorted(final FileSystem fs,
      final Path regiondir)
  throws IOException {
    Path editsdir = getRegionDirRecoveredEditsDir(regiondir);
    FileStatus[] files = fs.listStatus(editsdir, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        boolean result = false;
        try {
          // Return files and only files that match the editfile names pattern.
          // There can be other files in this directory other than edit files.
          // In particular, on error, we'll move aside the bad edit file giving
          // it a timestamp suffix.  See moveAsideBadEditsFile.
          Matcher m = EDITFILES_NAME_PATTERN.matcher(p.getName());
          result = fs.isFile(p) && m.matches();
        } catch (IOException e) {
          LOG.warn("Failed isFile check on " + p);
        }
        return result;
      }
    });
    NavigableSet<Path> filesSorted = new TreeSet<Path>();
    if (files == null) return filesSorted;
    for (FileStatus status: files) {
      filesSorted.add(status.getPath());
    }
    return filesSorted;
  }

  /**
   * Move aside a bad edits file.
   * @param fs
   * @param edits Edits file to move aside.
   * @return The name of the moved aside file.
   * @throws IOException
   */
  public static Path moveAsideBadEditsFile(final FileSystem fs,
      final Path edits)
  throws IOException {
    Path moveAsideName = new Path(edits.getParent(), edits.getName() + "." +
      System.currentTimeMillis());
    if (!fs.rename(edits, moveAsideName)) {
      LOG.warn("Rename failed from " + edits + " to " + moveAsideName);
    }
    return moveAsideName;
  }

  /**
   * @param regiondir This regions directory in the filesystem.
   * @return The directory that holds recovered edits files for the region
   * <code>regiondir</code>
   */
  public static Path getRegionDirRecoveredEditsDir(final Path regiondir) {
    return new Path(regiondir, RECOVERED_EDITS_DIR);
  }

  public static final long FIXED_OVERHEAD = ClassSize.align(
    ClassSize.OBJECT + (5 * ClassSize.REFERENCE) +
    ClassSize.ATOMIC_INTEGER + Bytes.SIZEOF_INT + (3 * Bytes.SIZEOF_LONG));

  private static void usage() {
    System.err.println("Usage: HLog <ARGS>");
    System.err.println("Arguments:");
    System.err.println(" --dump  Dump textual representation of passed one or more files");
    System.err.println("         For example: HLog --dump hdfs://example.com:9000/hbase/.logs/MACHINE/LOGFILE");
    System.err.println(" --split Split the passed directory of WAL logs");
    System.err.println("         For example: HLog --split hdfs://example.com:9000/hbase/.logs/DIR");
  }

  private static void dump(final Configuration conf, final Path p)
  throws IOException {
    FileSystem fs = FileSystem.get(conf);
    if (!fs.exists(p)) {
      throw new FileNotFoundException(p.toString());
    }
    if (!fs.isFile(p)) {
      throw new IOException(p + " is not a file");
    }
    Reader log = getReader(fs, p, conf);
    try {
      int count = 0;
      HLog.Entry entry;
      while ((entry = log.next()) != null) {
        System.out.println("#" + count + ", pos=" + log.getPosition() + " " +
          entry.toString());
        count++;
      }
    } finally {
      log.close();
    }
  }

  private static void split(final Configuration conf, final Path p)
  throws IOException {
    FileSystem fs = FileSystem.get(conf);
    if (!fs.exists(p)) {
      throw new FileNotFoundException(p.toString());
    }
    final Path baseDir = new Path(conf.get(HConstants.HBASE_DIR));
    final Path oldLogDir = new Path(baseDir, HConstants.HREGION_OLDLOGDIR_NAME);
    if (!fs.getFileStatus(p).isDir()) {
      throw new IOException(p + " is not a directory");
    }

    HLogSplitter logSplitter = HLogSplitter.createLogSplitter(
        conf, baseDir, p, oldLogDir, fs);
    logSplitter.splitLog();
  }

  /**
   * Pass one or more log file names and it will either dump out a text version
   * on <code>stdout</code> or split the specified log files.
   *
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      usage();
      System.exit(-1);
    }
    // either dump using the HLogPrettyPrinter or split, depending on args
    if (args[0].compareTo("--dump") == 0) {
      HLogPrettyPrinter.run(Arrays.copyOfRange(args, 1, args.length));
    } else if (args[0].compareTo("--split") == 0) {
      Configuration conf = HBaseConfiguration.create();
      for (int i = 1; i < args.length; i++) {
        try {
          conf.set("fs.default.name", args[i]);
          conf.set("fs.defaultFS", args[i]);
          Path logPath = new Path(args[i]);
          split(conf, logPath);
        } catch (Throwable t) {
          t.printStackTrace(System.err);
          System.exit(-1);
        }
      }
    } else {
      usage();
      System.exit(-1);
    }
  }
  
  // Yuting: 
  // this is different with hbase, because in the index, 
  // the filenum will be included in the index
  private int createFileNum(){
    int ret = (int)(System.currentTimeMillis() / 100);
    // solve the conflict of the file num
    if(outputfiles.size() > 0){
//    	System.out.println("in HLog, key set: " + outputfiles.keySet());
      int lastKey = (int)(this.outputfiles.lastKey().get());
      if(lastKey >= ret) ret = lastKey + 1;
//    	if(this.filenum >= ret) ret = this.filenum + 1;
    }
    return ret;             //in seconds, hopefully it would be ok.
  }

  Object compactLock = new Object();
  void compact(){
    // do the compact, write the new HSequenceFiles in 
    synchronized(compactLock){
      
    }
  }

  private void showDiskStatus(Path path)throws IOException{
    LOG.debug("------------ list the file status in " + path + ": ");
    FileStatus[] status = fs.listStatus(path);
    for(FileStatus s : status){
      LOG.debug("------------ " + s.getPath() + ": " + s.getLen());
    }
  }
  
  public Configuration getConf(){
    return this.conf;
  }
  
  public FileSystem getFileSystem(){
    return this.fs;
  }
}

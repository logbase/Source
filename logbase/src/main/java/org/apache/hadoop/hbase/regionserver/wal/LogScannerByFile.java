package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.util.List;
import java.util.SortedMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.MemIndex;
import org.apache.hadoop.io.LongWritable;

public class LogScannerByFile extends LogScanner{
  HLog.Entry entry = new HLog.Entry();
  List<KeyValue> kvs = null;
  List<LogEntryOffset> keyvalueOffset = null;
  KeyValue.Key tmpKey = new KeyValue.Key();
  int kvIdx = 0;
  long beginOffset;
  
  //WangSheng
  public void init(SortedMap<LongWritable, Path> outputfiles, FileSystem fs, Configuration conf, MemIndex index, LogStoreCache cache)throws IOException{
	  	initial(outputfiles, fs, conf);
	    this.index = index;
	    this.currentFileNum = this.files.firstKey();
	    this.currentFileReader = this.getReader(this.currentFileNum);
	    this.logCache = cache;
	  }
  
  public void init(SortedMap<LongWritable, Path> outputfiles, FileSystem fs, Configuration conf, MemIndex index)throws IOException{
  	initial(outputfiles, fs, conf);
    this.index = index;
    this.currentFileNum = this.files.firstKey();
    this.currentFileReader = this.getReader(this.currentFileNum);
  }
  
	@Override
	public KeyValue next() throws IOException {
		while(true){
  		if(kvs != null && kvIdx < kvs.size()){
  			KeyValue kv = kvs.get(kvIdx);
  			kv.getKeyForLog(tmpKey);
  			LogEntryOffset memIndexOffset = this.index.getOffset(tmpKey);
  			LogEntryOffset currentOffset = this.keyvalueOffset.get(kvIdx);
  			kvIdx ++;
  			if(memIndexOffset.compareTo(currentOffset) != 0){
  				continue;
  			}else
  				return kv;
  		}
  		this.beginOffset = this.currentFileReader.getPosition();
  		HLog.Entry ret = this.currentFileReader.next(entry);
  		if(ret == null){
  			// reach the final file
  			if(this.currentFileNum.compareTo(this.files.lastKey()) == 0){
  				return null;
  			}
  			// move to another log file
  			LongWritable tmpFileNum = new LongWritable();
  			tmpFileNum.set(this.currentFileNum.get() + 1);
  			this.currentFileNum = this.files.tailMap(tmpFileNum).firstKey();
  			this.currentFileReader = this.getReader(this.currentFileNum);
  			continue;
  		}else{
  			kvs = entry.getEdit().getKeyValues();
  			keyvalueOffset = entry.getKeyValueOffset(beginOffset, (int)currentFileNum.get(), keyvalueOffset);
  			kvIdx = 0;
  			continue;
  		}
		}
  }

  public void seek(KeyValue.Key key) throws IOException{
    throw new IOException("Scanner by File do not support seek() function");
  }
}

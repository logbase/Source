package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.MemIndex;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;

public class LogBaseScanner implements KeyValueScanner, InternalScanner{
	
	
	//copyed from LogScanner.java
	
	MemIndex index = null;
	protected SortedMap<LongWritable, SequenceFileLogReader> readers = null;
	  
	protected SortedMap<LongWritable, Path> files = null;
	FileSystem fs = null;
	Configuration conf = null;
	
	LongWritable currentFileNum = new LongWritable();
	SequenceFileLogReader currentFileReader = null;
	
	//WangSheng
	LogStoreCache logCache = null;
	
	//copyed from LogScannerByKey.java
	
	HLog.Entry entry = new HLog.Entry();
	List<KeyValue> kvs = null;
	List<LogEntryOffset> keyvalueOffset = null;
	KeyValue.Key tmpKey = new KeyValue.Key();
	int kvIdx = 0;
	long beginOffset;
	
	//wangsheng 2013-4-26
	KeyValue peek = null;
	Scan scan;
	Set<byte[]> cols;
	byte[] family;

	void initial(SortedMap<LongWritable, Path> outputfiles, FileSystem fs, Configuration conf) throws IOException{
	    if(readers != null){
	      close();
	    }else {
	      readers = new TreeMap<LongWritable, SequenceFileLogReader>();
	    }
	    this.files = outputfiles;
	    this.fs = fs;
	    this.conf = conf;
	  }
	
	//WangSheng
	public LogBaseScanner(byte[] family, Scan scan, HLog currentLog, MemIndex index, LogStoreCache cache)throws IOException{
		SortedMap<LongWritable, Path> outputfiles = currentLog.outputfiles;
		FileSystem fs = currentLog.getFileSystem();
		Configuration conf = currentLog.getConf();
		initial(outputfiles, fs, conf);
		this.scan = scan;
	    this.index = index;
	    this.currentFileNum = this.files.firstKey();
	    this.currentFileReader = this.getReader(this.currentFileNum);
	    this.logCache = cache;
	    this.cols = scan.getFamilyMap().get(family);
//	    if (this.cols != null){
//	    	System.out.println("cols are not null");
//	    	for (byte[] x : cols){
//	    		System.out.println("qualifier: " + new String(x));
//	    	}
//	    }
//	    else System.out.println("cols are null");
	    
	    next();
	}
	
	protected SequenceFileLogReader getReader(LongWritable logFileNum) throws IOException{
	  	Path path = this.files.get(logFileNum);
	  	if(path == null){
	  		throw new IOException("Invalid file num: " + logFileNum);
	  	}
	  	SequenceFileLogReader ret = this.readers.get(logFileNum);
	  	if(ret == null){
	  		ret = new SequenceFileLogReader();
	  		ret.init(this.fs, path, this.conf);
	  		this.readers.put(new LongWritable(logFileNum.get()), ret);
	  	}
	  	return ret;
	  }
	  
	@Override
	public KeyValue peek() {
		
		return peek;
	}
	
	@Override
	public KeyValue next() throws IOException{
		
		KeyValue tmp = peek;
		boolean needContinue = true;
		
		do{
			peek = pre_next();
			
			//wangsheng
			//System.out.println("peek = "+ (peek == null ? "null" : peek.toString()));
			
			if (peek == null) break;
			
//			System.out.println(cols == null ? "NO" : "YES");
//			System.out.println("cols = " + cols.size());
//			System.out.println(new String(peek.getQualifier())+" x");
//			boolean x = cols.contains(peek.getQualifier());
			needContinue = false;
			//System.out.println("needContinue0 = " + needContinue);
			if (cols != null && !cols.isEmpty() && cols.contains(peek.getQualifier()) == false) needContinue = true;
//			if (cols != null){
//				System.out.println("out is not null");
//				for (byte[] x :  cols) System.out.println("cols contain = " + x);
//			}
			//System.out.println("needContinue1 = " + needContinue);
			if (Bytes.compareTo(peek.getRow(), scan.getStartRow()) < 0) needContinue = true;//wangsheng
			//System.out.println("needContinue2 = " + needContinue);
			if (Bytes.compareTo(scan.getStopRow(), Bytes.toBytes("")) != 0 && Bytes.compareTo(scan.getStopRow(), peek.getRow()) < 0) needContinue = true;//wangsheng
			//if (scan.getFamilyMap().containsKey(peek.getFamily()) == false) needContinue = true;//wangsheng
			
			
			//System.out.println("needContinue3 = " + needContinue);
			
		}while (needContinue);
		
		//wangsheng
		//System.out.println("LogBaseScanner.next() = "+ (tmp == null ? "null" : tmp.toString()));
		
		return tmp;
	}
	
	private KeyValue pre_next() throws IOException {
		while(true){
	  		if(kvs != null && kvIdx < kvs.size()){
	  			KeyValue kv = kvs.get(kvIdx);
	  			tmpKey = kv.getKeyForLogBaseMemIndex();
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

	@Override
	public boolean seek(KeyValue key) throws IOException {
		
		return true;
	}

	@Override
	public boolean reseek(KeyValue key) throws IOException {
		
		return true;
	}

	@Override
	public long getSequenceID() {
		
		return 0;
	}

	@Override
	public void close() {
		Collection<SequenceFileLogReader> rs = readers.values();
	    for(SequenceFileLogReader r : rs){
	    	try{
	    		r.close();
	    	}catch(Exception e){
	    		System.err.println(e);
	    	}
	    }
	    readers.clear();
	}

	@Override
	public boolean next(List<KeyValue> results) throws IOException {
		
		return next(results, -1);
	}

	@Override
	public boolean next(List<KeyValue> result, int limit) throws IOException {
		
		KeyValue kv = null;
		
		result.clear();
		
		while (limit != 0){
			--limit;
			kv = next();
			if (kv == null) break;
			result.add(kv);
			if (peek == null) break;
			if (Bytes.equals(peek.getRow(), kv.getRow()) == false) break;
		}
		
		return true;
	}

}

package sg.edu.nus.LogBaseAPI;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class LogAdmin {
	
	private HBaseAdmin admin;
	private Configuration conf;
	
	public LogAdmin(Configuration conf) throws MasterNotRunningException, ZooKeeperConnectionException{
		this.conf = conf;
		this.admin = new HBaseAdmin(conf);
	}
	
	public void createTable(String tableName, String[] columnName) throws IOException{
		
		HTableDescriptor desc = new HTableDescriptor(tableName);
		for (int i = 0; i < columnName.length; ++i){
			desc.addFamily(new HColumnDescriptor(getLogBaseFamily(Bytes.toBytes(columnName[i]))));
		}
		
		admin.createTable(desc);
	}
	
	public LogTable getExistingTable(String tableName) throws IOException{
		return new LogTable (new HTable(conf, tableName));
	}
	
	private byte[] getLogBaseFamily(byte[] column){
		
		//TODO: get family by the input column
		return Bytes.toBytes("LogBaseCF");
	}
	
}

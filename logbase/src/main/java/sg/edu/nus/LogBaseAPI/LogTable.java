package sg.edu.nus.LogBaseAPI;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class LogTable {
	private HTable table;
	
	public LogTable(HTable t){
		table = t;
	}
	
	public void put(byte[] row, byte[] column, byte[] value) throws IOException{
		Put p = new Put(row);
		p.add(getLogBaseFamily(column), column, value);
		table.put(p);
	}
	
	public void put(byte[] row, byte[][] columns, byte[][] values) throws IOException{
		Put p = new Put(row);
		for (int i = 0; i < columns.length; ++i){
			p.add(getLogBaseFamily(columns[i]), columns[i], values[i]);
		}
		table.put(p);
	}
	
	public Result get(byte[] row) throws IOException{
		Get g = new Get(row);
		
		return table.get(g);
	}
	
	public Result get(byte[] row, byte[] column) throws IOException{
		Get g = new Get(row);
		g.addColumn(getLogBaseFamily(column), column);
		
		return table.get(g);
	}
	
	public Result get(byte[] row, byte[][] columns) throws IOException{
		Get g = new Get(row);
		
		for (int i = 0; i < columns.length; ++i){
			g.addColumn(getLogBaseFamily(columns[i]), columns[i]);
		}
		
		return table.get(g);
	}
	
	private byte[] getLogBaseFamily(byte[] column){
		
		//TODO: get family by the input column
		return Bytes.toBytes("LogBaseCF");
	}

}

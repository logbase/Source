package sg.edu.nus.test;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import sg.edu.nus.LogBaseAPI.LogAdmin;
import sg.edu.nus.LogBaseAPI.LogTable;

public class Test {
	
	public static void main (String[] argv) throws IOException{
		LogAdmin admin = new LogAdmin(HBaseConfiguration.create());
		
		
		//Step 1. Create a table with a specified table name and a specified column name.
	    //admin.createTable("testLogBase", new String[]{"c1", "c2"});  
	    final LogTable table = admin.getExistingTable("test");
	    //Step 2 Insert a record in the table
	    byte[] row = Bytes.toBytes("r1"); //row name   
	    //byte[][] cols = new byte[][]{Bytes.toBytes("c1"), Bytes.toBytes("c2")}; //columns’ names
	    //byte[][] value = new byte[][]{Bytes.toBytes("v1"), Bytes.toBytes("v2")}; //values
	    //table.put(row, cols, value); 
	    //Step 3. Get a record with row name as the key
	    Result ret = table.get(row);  
	    for(int i=0; i<ret.size(); i++){
	      System.out.println(" table  = test row = r1 get = " + (new String (ret.raw()[i].getValue()))); 
	    }
	    //Step 4. Get a record with row name and column name as keys
	    //ret = table.get(row, Bytes.toBytes("c1"));  
	    //for(int i=0; i<ret.size(); i++){
	      //System.out.println(" get = " + new String (ret.raw()[i].getValue())); 
	    //}
	}
}

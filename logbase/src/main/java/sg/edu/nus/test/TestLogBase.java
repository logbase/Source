package sg.edu.nus.test;


import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import sg.edu.nus.LogBaseAPI.LogAdmin;
import sg.edu.nus.LogBaseAPI.LogTable;


/**
 * Class to test HBaseAdmin.
 * Spins up the minicluster once at test start and then takes it down afterward.
 * Add any testing of HBaseAdmin functionality here.
 */
public class TestLogBase {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private LogAdmin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 6);
    TEST_UTIL.startMiniCluster(3);
    System.out.println("===========================before class done. ===========================");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    this.admin = new LogAdmin(TEST_UTIL.getConfiguration());
    System.out.println("===========================setUp done. ===========================");
  }

  @Test
  public void testCreateTableAndPut() throws IOException {  
    //Step 1. Create a table with a specified table name and a specified column name.
    admin.createTable("testLogBase", new String[]{"c1", "c2"});  
    final LogTable table = admin.getExistingTable("testLogBase");
    //Step 2 Insert a record in the table
    byte[] row = Bytes.toBytes("r1"); //row name   
    byte[][] cols = new byte[][]{Bytes.toBytes("c1"), Bytes.toBytes("c2")}; //columns’ names
    byte[][] value = new byte[][]{Bytes.toBytes("v1"), Bytes.toBytes("v2")}; //values
    table.put(row, cols, value); 
    //Step 3. Get a record with row name as the key
    Result ret = table.get(row);  
    for(int i=0; i<ret.size(); i++){
      System.out.println(new String (ret.raw()[i].getValue())); 
    }
    //Step 4. Get a record with row name and column name as keys
    ret = table.get(row, Bytes.toBytes("c1"));  
    for(int i=0; i<ret.size(); i++){
      System.out.println(new String (ret.raw()[i].getValue())); 
    }
  }
  
  /*
  @Test
  public void test_LogBase_RoutineOperations() throws IOException {
    //HTableDescriptor [] tables = admin.listTables();
    //int numTables = tables.length;
    
    System.out.println("================================================================================");
    System.out.println("================================================================================");
    System.out.println("========================start testing Logbase ==================================");  
    
    //--1--create a table with a name and a col family cf1
    admin.createTable("testLogBase", new String[]{"cl1"});  
    final LogTable table = admin.getExistingTable("testLogBase");
    //--2--initialize some cols for cf1   
    byte[] r = Bytes.toBytes("r1");  //row  
//    Put put = new Put(r);
    
    System.out.println("starting put now !!!!!!!!!!");
    
    byte[] c = Bytes.toBytes("c1"); //col
    byte[] value = Bytes.toBytes("somedata"); //value
//    put.add(Bytes.toBytes("cf1"), c, value);
//    table.put(put);
    table.put(r, c, value);
     
    //use "list" to return the created tables.
    //tables = this.admin.listTables();
    
//    for (HTableDescriptor t : tables){
//    	System.out.println("table name = "+t.getNameAsString());
//    }
    
    System.out.println("starting get now !!!!!!!!!!");
    
//    Get get = new Get(r);
//    get.addColumn(Bytes.toBytes("cf1"), c);
    Result ret = table.get(r);
    System.out.println("Get result = " + ret.toString()+"!!!!!!!!!!");
    
    for(int i=0; i<ret.size(); i++)
    {
      System.out.println(new String (ret.raw()[i].getValue()));
    }
    
    //assertEquals(numTables + 1, tables.length);
    
    System.out.println("========================end testing Logbase =================================="); 
    System.out.println("==============================================================================");
    System.out.println("==============================================================================");
    
  }
  */  
  
  
  
}


  
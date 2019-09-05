package com.wxx.bigdata;

import com.wxx.bigdata.hbase.api.HBaseUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class HBaseUtilTest {

    @Test
    public void createTable(){
        HBaseUtil.createTable("filetable", new String[]{"fileinfo","saveinfo"});
    }

    @Test
    public void addFleDetails(){
        HBaseUtil.putRow("filetable","rowkey1","fileinfo","name","file1.txt");
        HBaseUtil.putRow("filetable","rowkey1","fileinfo","type","txt");
        HBaseUtil.putRow("filetable","rowkey1","fileinfo","size","1024");
        HBaseUtil.putRow("filetable","rowkey1","saveinfo","creator","tom");
        HBaseUtil.putRow("filetable","rowkey1","saveinfo","name","/home");

        HBaseUtil.putRow("filetable","rowkey2","fileinfo","name","file2.jpg");
        HBaseUtil.putRow("filetable","rowkey2","fileinfo","type","jpg");
        HBaseUtil.putRow("filetable","rowkey2","fileinfo","size","2048");
        HBaseUtil.putRow("filetable","rowkey2","saveinfo","creator","jerry");
        HBaseUtil.putRow("filetable","rowkey2","saveinfo","name","/home/pics");
    }

    @Test
    public void getFileDetails(){
        Result result = HBaseUtil.getRow("filetable", "rowkey1");
        if(result != null ){
            System.out.println("rowkey="+ Bytes.toString(result.getRow()));
            System.out.println("filename=" + Bytes.toString(result.getValue(Bytes.toBytes("fileinfo"),Bytes.toBytes("name"))));
        }
    }

    @Test
    public void scanFileDetails(){
        ResultScanner scanner = HBaseUtil.getScanner("filetable", "rowkey1","rowkey1");
        if(scanner != null){
            scanner.forEach(result -> {
                System.out.println("rowkey="+ Bytes.toString(result.getRow()));
                System.out.println("filename=" + Bytes.toString(result.getValue(Bytes.toBytes("fileinfo"),Bytes.toBytes("name"))));
            });
        }
        scanner.close();
    }

    @Test
    public void deleteRow(){
        HBaseUtil.delete("filetable", "rowkey1");
    }

    @Test
    public void deleteTable(){
        HBaseUtil.deleteTable("filetable");
    }
}

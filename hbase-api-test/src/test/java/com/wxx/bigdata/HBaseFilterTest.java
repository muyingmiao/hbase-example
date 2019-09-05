package com.wxx.bigdata;

import com.wxx.bigdata.hbase.api.HBaseUtil;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.Arrays;

public class HBaseFilterTest {
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

        HBaseUtil.putRow("filetable","rowkey3","fileinfo","name","file3.pdf");
        HBaseUtil.putRow("filetable","rowkey3","fileinfo","type","pdf");
        HBaseUtil.putRow("filetable","rowkey3","fileinfo","size","2048");
        HBaseUtil.putRow("filetable","rowkey3","saveinfo","creator","lucy");
        HBaseUtil.putRow("filetable","rowkey3","saveinfo","name","/home/pdf");
    }

    @Test
    public void rowFileterTest(){
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("rowkey1")));
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, Arrays.asList(filter));
        ResultScanner scanner = HBaseUtil.getScanner("filetable","rowkey1","rowkey3", filterList);
        if(scanner != null){
            scanner.forEach(result -> {
                System.out.println("rowkey="+ Bytes.toString(result.getRow()));
                System.out.println("filename=" + Bytes.toString(result.getValue(Bytes.toBytes("fileinfo"),Bytes.toBytes("name"))));
               ;
            });
        }
        scanner.close();
    }

    @Test
    public void prefixFilterTest(){
        Filter filter = new PrefixFilter(Bytes.toBytes("rowkey2"));
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.asList(filter));
        ResultScanner scanner = HBaseUtil.getScanner("filetable","rowkey1","rowkey3", filterList);
        if(scanner != null){
            scanner.forEach(result -> {
                System.out.println("rowkey="+ Bytes.toString(result.getRow()));
                System.out.println("filename=" + Bytes.toString(result.getValue(Bytes.toBytes("fileinfo"),Bytes.toBytes("name"))));
            });
        }
        scanner.close();
    }

    @Test
    public void keyOnlyFilterTest(){
        Filter filter = new KeyOnlyFilter(true);
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.asList(filter));
        ResultScanner scanner = HBaseUtil.getScanner("filetable","rowkey1","rowkey3", filterList);
        if(scanner != null){
            scanner.forEach(result -> {
                System.out.println("rowkey="+ Bytes.toString(result.getRow()));
                System.out.println("filename=" + Bytes.toString(result.getValue(Bytes.toBytes("fileinfo"),Bytes.toBytes("name"))));
            });
        }
        scanner.close();
    }

    @Test
    public void columnPrefixFilterTest(){
        Filter filter = new ColumnPrefixFilter(Bytes.toBytes("nam"));
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.asList(filter));
        ResultScanner scanner = HBaseUtil.getScanner("filetable","rowkey1","rowkey3", filterList);
        if(scanner != null){
            scanner.forEach(result -> {
                System.out.println("rowkey="+ Bytes.toString(result.getRow()));
                System.out.println("filename=" + Bytes.toString(result.getValue(Bytes.toBytes("fileinfo"),Bytes.toBytes("name"))));
                System.out.println("filetype=" + Bytes.toString(result.getValue(Bytes.toBytes("fileinfo"),Bytes.toBytes("type"))));
            });
        }
        scanner.close();
    }
}

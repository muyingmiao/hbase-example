package com.wxx.bigdata;

import com.wxx.bigdata.hbase.api.HBaseConn;
import javafx.scene.control.Tab;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.ipc.Server;
import org.junit.Test;

import java.io.IOException;

public class HBaseConnTest {

    @Test
    public void getConnTest(){
        Connection connection = HBaseConn.getHBaseConn();
        System.out.println(connection.isClosed());
        HBaseConn.closeConn();
        System.out.println(connection.isClosed());
    }

    @Test
    public void getTableTest(){
        Table table = null;
        try {
            table = HBaseConn.getTable("US_POPULATION");
            System.out.println(table.getName().getNameAsString());
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}

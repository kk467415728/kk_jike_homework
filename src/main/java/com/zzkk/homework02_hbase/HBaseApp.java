package com.zzkk.homework02_hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HBaseApp {
    public static void main(String[] args) throws IOException {
        String tableName = "zhangzhenkuan:student";
        String[] columnFamily = {"info","score"};
        HBaseApi hba = new HBaseApi();
//        createTable(tableName, Arrays.asList(columnFamily));
//        initData(tableName);
//        deleteTable("zhangzhenkuan:student");
//        List list = new ArrayList<Pair>();
//        Pair pair =new Pair<String,String>("student_id","G20210735010348");
//        Pair pair2 =new Pair<String,String>("class","4");
//        list.add(pair);
//        list.add(pair2);
//        List list = new ArrayList<Pair>();
//        Pair pair =new Pair<String,String>("understanding","60");
//        Pair pair2 =new Pair<String,String>("programming","60");
//        list.add(pair);
//        list.add(pair2);
//        putData(tableName,"zhangzhenkuan","score",list);

        hba.getData(tableName,"zhangzhenkuan");
        hba.getScanner(tableName);
        hba.close();
    }
}

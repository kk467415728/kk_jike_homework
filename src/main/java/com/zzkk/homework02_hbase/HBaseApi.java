package com.zzkk.homework02_hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HBaseApi {
    public static void main(String[] args) {
        System.out.println(1);
    }

    public static Configuration conf;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "47.101.216.12");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        System.out.println(2);
    }
}

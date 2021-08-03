package com.zzkk.homework02_hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HBaseApi {

    public static Connection connection;
    public static HBaseAdmin admin;
    static {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","47.101.216.12");

        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = (HBaseAdmin) connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static boolean createTable(String tableName, List<String> columnFamily) throws IOException {
        TableName table = TableName.valueOf(tableName);
            if (admin.tableExists(table)) {
                System.out.println("表已存在");
                return false;
            }
            List<ColumnFamilyDescriptor> descriptorList = new ArrayList<>();
            for (String cf : columnFamily) {
                ColumnFamilyDescriptor descriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf))
                        .setDataBlockEncoding(DataBlockEncoding.PREFIX)
                        .setBloomFilterType(BloomType.ROW)
                        .build();
                descriptorList.add(descriptor);
            }
            System.out.println(columnFamily);
            //设置表信息
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                    .setColumnFamilies(descriptorList)
                    .build();

            admin.createTable(tableDescriptor);
        System.out.println(111);
            if (admin.tableExists(TableName.valueOf(tableName))) {
                System.out.println("表创建成功");
            } else {
                System.out.println("表创建失败");
            }

            return true;
        }

        public static void deleteTable(String tableName) throws IOException {
            TableName table = TableName.valueOf(tableName);
            admin.disableTable(table);
            admin.deleteTable(table);
        }

        public static void putData(String tableName,String rowKey,String columnFamilyName,String qualifier, String value) throws IOException {
            Table table = connection.getTable(TableName.valueOf(tableName));

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            table.close();
        }

        public static void putData(String tableName,String rowKey,String columnFamilyName,List<Pair<String, String>> pairList) throws IOException {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            pairList.forEach(pair -> put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(pair.getFirst()), Bytes.toBytes(pair.getSecond())));;
            table.put(put);
            table.close();
        }

        public static void getData(String tableName,String rowKey) throws IOException {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            System.out.println(result);
        }

        public static void getScanner(String tableName) throws IOException {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner result = table.getScanner(scan);
            for (Result data: result) {
                System.out.println(data);
            }
        }

        public static void deleteRow(String tableName,String rowKey) throws IOException {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        }

        public static void initData(String tableName) throws IOException {
            Table table = connection.getTable(TableName.valueOf(tableName));
            List puts = new ArrayList();

            Put put = new Put(Bytes.toBytes("Tom"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000001"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("1"));
            put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("75"));
            put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("82"));
            puts.add(put);

            put = new Put(Bytes.toBytes("Jerry"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000002"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("1"));
            put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("85"));
            put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("67"));
            puts.add(put);

            put = new Put(Bytes.toBytes("Jack"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000003"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("2"));
            put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("80"));
            put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("80"));
            puts.add(put);

            put = new Put(Bytes.toBytes("Rose"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000004"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("2"));
            put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("60"));
            put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("61"));
            puts.add(put);

            table.put(puts);
            System.out.println("insert table Success!");

        }

        public static void close() throws IOException {
            admin.close();
            connection.close();
        }
    }

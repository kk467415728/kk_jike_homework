package com.zzkk.homework01;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private FlowBean flowBean = new FlowBean();
    private Text outKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1363157985066 	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com		24	27	2481	24681	200
        //一行数据
        String line = value.toString();

        String[] words = line.split("\t");

        //获取需要的数据
        String phone = words[1];
        String upFlow = words[words.length - 3];
        String downFlow = words[words.length - 2];

        //封装数据
        flowBean.set(Long.parseLong(upFlow), Long.parseLong(downFlow));
        outKey.set(phone);

        //写出数据
        context.write(outKey, flowBean);
    }
}

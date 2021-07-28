package com.zzkk.homework01_mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean outValue = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long totalUpFlow = 0;
        long totalDownFlow = 0;

        for (FlowBean value: values){
            totalUpFlow += value.getUpFlow();
            totalDownFlow += value.getDownFlow();
        }

        outValue.set(totalUpFlow,totalDownFlow);

        context.write(key, outValue);
    }
}

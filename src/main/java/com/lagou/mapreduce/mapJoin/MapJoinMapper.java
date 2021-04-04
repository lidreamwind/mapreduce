package com.lagou.mapreduce.mapJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class MapJoinMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    HashMap<String, String> map = new HashMap<>();
    Text k = new Text();
    /**
     * Called once at the beginning of the task.
     *
     * @param context
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream("position.txt"));
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        BufferedReader reader = new BufferedReader(inputStreamReader);
        //读取职位数据解析为kv类型(hashmap):,key：positionid,value:positionname

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            String[] fields = line.split("\t");
            map.put(fields[0], fields[1]);
        }
        reader.lines();
        super.setup(context);
    }

    /**
     * Called once for each key/value pair in the input split. Most applications
     * should override this, but the default is the identity function.
     *
     * @param key
     * @param value
     * @param context
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arr = line.split("\t");
        //都是投递行为数据
        String positionName = map.get(arr[1]);
        k.set(line + "\t" + positionName);
        context.write(k, NullWritable.get());
    }
}

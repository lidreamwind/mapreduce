package com.lagou.taskOne;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class TaskSortReducer extends Reducer<Text, IntWritable,Text, NullWritable> {
    /**
     * 思路：定义列表，用来接收所有的数据，然后在数组内使用算法排序，本次使用的是选择排序，可以选择快排、归并等
     */
    //定义属性
    Text text = new Text();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 定义排序列表
        ArrayList<Integer> arr = new ArrayList<>();
        //获取所有元素
        for (IntWritable value : values) {
            arr.add(value.get());
        }
        // 排序算法
        arr = sortArrAsc(arr);
        // 将结果进行输出
        for (int i = 0; i < arr.size(); i++) {
            this.text.set(i+1+"\t"+arr.get(i));
            context.write(text,NullWritable.get());
        }
    }
    //选择排序算法
    public ArrayList<Integer> sortArrAsc(ArrayList<Integer> arr){
        // 交换元素
        Integer temp = arr.get(0);
        //下标标记，用来标示最小元素
        Integer p = 0;
        for (int i = 0; i < arr.size()-1; i++) {
            temp = arr.get(i);
            //初始化最小元素下标
            p = i;
            for(int j=i;j<arr.size();j++){
                if(temp > arr.get(j)){
                    p = j;
                    temp = arr.get(j);
                }
            }
            // 交换元素
            temp = arr.get(p);
            arr.set(p,arr.get(i));
            arr.set(i,temp);
        }
        return arr;
    }
    // 测试排序算法
    public static void main(String[] args) {
        ArrayList<Integer> arr = new ArrayList<Integer>();
        arr.add(1);
        arr.add(52);
        arr.add(6542);
        arr.add(234);
        arr.add(42);
        arr.add(23);
        arr = new TaskSortReducer().sortArrAsc(arr);
        for (Integer integer : arr) {
            System.out.print(integer +"\t");
        }
    }
}

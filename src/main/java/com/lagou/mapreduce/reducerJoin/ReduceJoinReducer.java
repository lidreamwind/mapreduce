package com.lagou.mapreduce.reducerJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class ReduceJoinReducer extends Reducer<Text,ReduceJoinBean,ReduceJoinBean, NullWritable> {
    /**
     * This method is called once for each key. Most applications will define
     * their reduce class by overriding this method. The default implementation
     * is an identity function.
     *
     * @param key
     * @param values
     * @param context
     */
    @Override
    protected void reduce(Text key, Iterable<ReduceJoinBean> values, Context context) throws IOException, InterruptedException {
        //根据key -postitionId做key的划分，那么迭代的将是diliver和position
        // deliver列表
        ArrayList<ReduceJoinBean> deliverBeans = new ArrayList<>();
        // 职位只会有一个
        ReduceJoinBean positionBean = new ReduceJoinBean();
        for (ReduceJoinBean value : values) {
            ReduceJoinBean tempBean = new ReduceJoinBean();
            if(value.getFlag().equals("deliver")){
                //此处不能直接把bean对象添加到debeans中，需要深度拷贝才行
                try {
                    BeanUtils.copyProperties(tempBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                deliverBeans.add(tempBean);
            }else {
                try {
                    BeanUtils.copyProperties(positionBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        for (ReduceJoinBean deliverBean : deliverBeans) {
            deliverBean.setPositionName(positionBean.getPositionName());
            context.write(deliverBean,NullWritable.get());
        }
    }
}

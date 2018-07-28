package com.ljy.sessionanalyze.spark.page;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

/**
 * 组内拼接去重函数
 */
public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {

    // 指定输入数据得到字段和类型
    private StructType inputSchema = DataTypes.createStructType(
            Arrays.asList(
                    DataTypes.createStructField("cityInfo", DataTypes.StringType, true)
            )
    );
    // 指定缓冲数据的字段类型
    private StructType buffSchema = DataTypes.createStructType(
            Arrays.asList(DataTypes.createStructField("bufferCityInfo", DataTypes.StringType, true))
    );

    // 指定返回类型
    private DataType dataType = DataTypes.StringType;

    // 指定是否是确定的
    private boolean deterministic = true;

    @Override
    public StructType inputSchema() {
        return inputSchema;
    }

    @Override
    public StructType bufferSchema() {
        return buffSchema;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public boolean deterministic() {
        return deterministic;
    }

    /**
     * 初始化方法
     * 可以认为是指定一个初始值
     *
     * @param buffer
     */
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, "");
    }

    /**
     * 更新
     * 一个一个的将组内的字段值传进去
     *
     * @param buffer
     * @param input
     */
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        // 缓冲中的已经好的城市信息
        String buffCityInfo = buffer.getString(0);
        // 刚刚传进来的某个城市信息
        String cityInfo = input.getString(0);

        // 在这里要实现去重
        // 判断之前没有评结果某个城市信息,才可以拼接
        // 城市信息:0:北京  1:上海
        if (!buffCityInfo.contains(cityInfo)) {
            if ("".equals(buffCityInfo)) {
                buffCityInfo += cityInfo;
            } else {
                buffCityInfo += "," + cityInfo;
            }
        }

        buffer.update(0, buffCityInfo);
    }

    /**
     * 合并
     * update 操作,可能是对一个分组内的部分数据,在某个节点内发生的
     * 但是,可能一个分组内的数据会分布在做个节点上处理
     * 此时,就要用merge操作,将各个节点上分布式拼接好的字符串合并起来
     *
     * @param buffer1
     * @param buffer2
     */
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        StringBuilder bufferCityInfo1 = new StringBuilder(buffer1.getString(0));
        String bufferCityInfo2 = buffer2.getString(0);
        for (String cityInfo : bufferCityInfo2.split(",")) {
            if (!bufferCityInfo1.toString().contains(cityInfo)) {
                if ("".equals(bufferCityInfo2)) {
                    bufferCityInfo1.append(cityInfo);
                } else {
                    bufferCityInfo1.append(",").append(cityInfo);
                }
            }
        }

        buffer1.update(0, bufferCityInfo1.toString());
    }

    @Override
    public Object evaluate(Row buffer) {
        return buffer.getString(0);
    }
}

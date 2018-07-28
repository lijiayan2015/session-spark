package com.ljy.sessionanalyze.spark.session;

import com.ljy.sessionanalyze.constant.Constants;
import com.ljy.sessionanalyze.util.StringUtils;
import org.apache.spark.AccumulatorParam;

/**
 * 使用自定义的数据格式,比如现在要使用String格式,可以自定义model
 * 自定义类必须可序列化,可以基于这种特殊的格式来实现复杂的分布式累加计算
 * 每个task分布在集群中各个节点的Executor中运行,可以根据需求
 * task给Accumulator传入不同的值
 * 最后根据不同的值,做复杂的计算逻辑
 */
public class SessionAggStatAcc implements AccumulatorParam<String> {

    /**
     * zero方法,主要是对数据做初始化
     * 在这里返回一个值,在初始化中,所有范围之间的数量都是0
     * 各个范围之间的统计数量的拼接,还是一字符串拼接的方式,key=value|key=value
     *
     * @param initialValue
     * @return
     */
    @Override
    public String zero(String initialValue) {
        return Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0|";

    }

    /**
     * 主要实现vi舒适化的连接字符串
     * v1 就是zero中的初始值
     * v2 就是遍历Session的过程中,判断谋和Session对应的区间
     * 然后用Constants.TIME_PREIOD_13_3s
     * 实现的就是v1中v2对对应的value,然后进行累加,最后
     * 更新初始字符串
     *
     * @param t1
     * @param t2
     * @return
     */
    @Override
    public String addAccumulator(String t1, String t2) {
        return add(t1, t2);
    }

    /**
     * 可以理解为和addAccumulator一样的逻辑
     *
     * @param r1
     * @param r2
     * @return
     */
    @Override
    public String addInPlace(String r1, String r2) {
        return add(r1, r2);
    }

    private String add(String v1, String v2) {
        //判断v1为null,直接返回v2
        if (StringUtils.isEmpty(v1)) return v2;

        //判断v1不为空,从v1中提取v2对应的值,然后累加1
        final String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
        if (oldValue != null) {
            // 将范围区间原有的值转换为int类型再累加

            int newValue = Integer.valueOf(oldValue) + 1;
            //将v1中v2对应的值,更新为累加后的值
            return StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
        }
        return v1;
    }
}

package com.ljy.sessionanalyze.spark.session.ljy;

import com.ljy.sessionanalyze.constant.Constants;
import com.ljy.sessionanalyze.util.StringUtils;
import org.apache.spark.AccumulatorParam;

public class UserSessionACC implements AccumulatorParam<String> {

    /**
     * @param t1 初始值key1=value|key2=value2
     * @param t2 需要累加的key
     * @return
     */
    @Override
    public String addAccumulator(String t1, String t2) {
        return add(t1, t2);
    }

    @Override
    public String addInPlace(String r1, String r2) {
        return add(r1, r2);
    }

    @Override
    public String zero(String initialValue) {
        StringBuffer sb = new StringBuffer();
        sb.append(Constants.SESSION_COUNT).append("=").append(0).append("|")
                .append(Constants.TIME_PERIOD_1s_3s).append("=").append(0).append("|")
                .append(Constants.TIME_PERIOD_4s_6s).append("=").append(0).append("|")
                .append(Constants.TIME_PERIOD_7s_9s).append("=").append(0).append("|")
                .append(Constants.TIME_PERIOD_10s_30s).append("=").append(0).append("|")
                .append(Constants.TIME_PERIOD_30s_60s).append("=").append(0).append("|")
                .append(Constants.TIME_PERIOD_1m_3m).append("=").append(0).append("|")
                .append(Constants.TIME_PERIOD_3m_10m).append("=").append(0).append("|")
                .append(Constants.TIME_PERIOD_10m_30m).append("=").append(0).append("|")
                .append(Constants.TIME_PERIOD_30m).append("=").append(0).append("|")
                .append(Constants.STEP_PERIOD_1_3).append("=").append(0).append("|")
                .append(Constants.STEP_PERIOD_4_6).append("=").append(0).append("|")
                .append(Constants.STEP_PERIOD_7_9).append("=").append(0).append("|")
                .append(Constants.STEP_PERIOD_10_30).append("=").append(0).append("|")
                .append(Constants.STEP_PERIOD_30_60).append("=").append(0).append("|")
                .append(Constants.STEP_PERIOD_60).append("=").append(0);

        return sb.toString();
    }


    private String add(String v1, String v2) {
        if (StringUtils.isEmpty(v1)) return v2;
        String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
        if (oldValue != null) {
            return StringUtils.setFieldInConcatString(v1, "\\|", v2, (Integer.valueOf(oldValue) + 1) + "");
        }
        return v1;

    }
}

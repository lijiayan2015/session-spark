package com.ljy.sessionanalyze.spark.page;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.sql.api.java.UDF2;

/**
 * <pre>
 * UDF2<String,String,String>
 *     前连个类型是传进来值的类型
 *          第一个类型代表json格式的字符串
 *          第二个代表要获取字段子的字段名称
 *     第三个类型代表返回json串的某个字段的值得类型
 * </pre>
 */
public class GetJsonObjectUDF implements UDF2<String, String, String> {
    @Override
    public String call(String json, String field) throws Exception {
        try {
            final JSONObject jsonObject = JSONObject.parseObject(json);
            return jsonObject.getString(field);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}

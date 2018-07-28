package com.ljy.sessionanalyze.spark.session.ljy;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ljy.sessionanalyze.constant.Constants;
import com.ljy.sessionanalyze.dao.ITaskDAO;
import com.ljy.sessionanalyze.dao.factory.DAOFactory;
import com.ljy.sessionanalyze.domain.Task;
import com.ljy.sessionanalyze.util.DateUtils;
import com.ljy.sessionanalyze.util.ParamUtils;
import com.ljy.sessionanalyze.util.SparkUtils;
import com.ljy.sessionanalyze.util.StringUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

@SuppressWarnings("all")
public class UserSession {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("UserSession");
        SparkUtils.setMaster(conf);
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

        sc.checkpointFile("hdfs://h1:9000/spark/usersession" + DateUtils.getTodayDate());

        //生成模拟数据
        SparkUtils.mockData(sc, sqlContext);

        final ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);

        final Task task = taskDAO.findById(taskid);

        if (task == null) {
            throw new RuntimeException("task 获取失败");
        }
        final String taskParam = task.getTaskParam();

        final JSONObject jsonObject = JSON.parseObject(taskParam);

        final JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, jsonObject);

        JavaPairRDD<String, Row> sessionId2ActionRDD = getSessionID2ActionRDD(actionRDD);
        sessionId2ActionRDD = sessionId2ActionRDD.cache();

        JavaPairRDD<String, String> session2AggInfo = aggregateBySession(sqlContext, sessionId2ActionRDD);

        final Accumulator<String> accumulator = sc.accumulator("", new UserSessionACC());
    }


    /**
     * 对行为数据按照Session粒度进行聚合
     *
     * @param sqlContext
     * @param sessionId2ActionRDD
     * @return
     */
    private static JavaPairRDD<String, String> aggregateBySession(SQLContext sqlContext, JavaPairRDD<String, Row> sessionId2ActionRDD) {
        //根据Sessionid进行分组
        final JavaPairRDD<String, Iterable<Row>> sessionId2ActionPairRDD = sessionId2ActionRDD.groupByKey();

        JavaPairRDD<Long, String> userId2PartAggInfoRDD = sessionId2ActionPairRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tup) throws Exception {
                String sessionID = tup._1;
                final Iterator<Row> it = tup._2().iterator();
                StringBuffer searchWorldsBuff = new StringBuffer();
                StringBuffer clickCategoeyBuff = new StringBuffer();
                Long userID = null;
                Date startTime = null;
                Date endTime = null;
                int stepLen = 0;

                while (it.hasNext()) {
                    final Row row = it.next();
                    //获取UserID
                    if (userID == null) {
                        userID = row.getLong(1);
                    }

                    String searchKeyWorld = row.getString(5);

                    if (!StringUtils.isEmpty(searchKeyWorld)) {
                        if (!searchWorldsBuff.toString().contains(searchKeyWorld)) {
                            searchWorldsBuff.append(searchKeyWorld).append(",");
                        }
                    }
                    String clickCategoey = String.valueOf(row.getLong(6));
                    if (!StringUtils.isEmpty(clickCategoey)) {
                        if (!clickCategoeyBuff.toString().contains(clickCategoey)) {
                            clickCategoeyBuff.append(clickCategoey).append(",");
                        }
                    }

                    String time = row.getString(4);
                    Date actionTime = DateUtils.parseTime(time);

                    if (startTime == null) {
                        startTime = actionTime;
                    }

                    if (endTime == null) {
                        endTime = actionTime;
                    }

                    if (actionTime.before(startTime)) {
                        startTime = actionTime;
                    }

                    if (actionTime.after(endTime)) {
                        endTime = actionTime;
                    }
                    stepLen++;
                }
                //截取clickCategoeyBuff 和 searchWorldsBuff 最后添加的","
                String searchWorlds = StringUtils.trimComma(searchWorldsBuff.toString());
                String clickCategories = StringUtils.trimComma(clickCategoeyBuff.toString());
                //计算访问时长
                long visitTime = (startTime.getTime() - endTime.getTime()) / 1000;

                //集合数据,数据以key=value|key=value的方式拼接
                StringBuffer sb = new StringBuffer();
                sb.append(Constants.FIELD_SESSION_ID).append("=").append(sessionID).append("|")
                        .append(Constants.FIELD_SEARCH_KEYWORDS).append("=").append(searchWorlds).append("|")
                        .append(Constants.FIELD_CLICK_CATEGORY_IDS).append("=").append(clickCategories).append("|")
                        .append(Constants.FIELD_VISIT_LENGTH).append("=").append(visitTime).append("|")
                        .append(Constants.FIELD_STEP_LENGTH).append("=").append(stepLen).append("|")
                        .append(Constants.FIELD_START_TIME).append("=").append(startTime).append("|");
                return new Tuple2<>(userID, sb.toString());
            }
        });


        //查询所有的用户数据,映射成<userid,Row>
        String sql = "select * from user_info";
        final JavaRDD<Row> userRowRDD = sqlContext.sql(sql).javaRDD();
        JavaPairRDD<Long, Row> userID2UserInfo = userRowRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                Long userID = row.getLong(0);
                return new Tuple2<>(userID, row);
            }
        });

        //将Session信息userId2PartAggInfoRDD 与用户信息userID2UserInfo进行join,
        //生成的结果数据格式:<userid,(userID2UserInfo,userId2PartAggInfoRDD)>
        final JavaPairRDD<Long, Tuple2<Row, String>> userID2FullInfoRDD = userID2UserInfo.join(userId2PartAggInfoRDD);

        //将userId2FullInfoRDD进行拼接,返回<sessionID,fullAggInfo>
        JavaPairRDD<String, String> session2AggFullInfo = userID2FullInfoRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Row, String>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<Row, String>> tup) throws Exception {
                final String actionInfo = tup._2._2;
                final Row userInfoRow = tup._2._1;

                //获取SessionID
                final String sessionID = StringUtils.getFieldFromConcatString(actionInfo, "\\|", Constants.FIELD_SESSION_ID);

                //获取用户的年龄
                final int age = userInfoRow.getInt(3);

                //获取用户的职业
                final String professional = userInfoRow.getString(4);

                //获取用户所在的城市
                String city = userInfoRow.getString(5);

                //获取用户的性别
                String sex = userInfoRow.getString(6);

                //用户的name
                String name = userInfoRow.getString(2);

                String username = userInfoRow.getString(1);

                StringBuffer sb = new StringBuffer();

                sb.append(actionInfo).append(Constants.FIELD_AGE).append("=").append(age).append("|")
                        .append(Constants.FIELD_SEX).append("=").append(sex).append("|")
                        .append(Constants.FIELD_PROFESSIONAL).append("=").append(professional).append("|")
                        .append(Constants.FIELD_CITY).append("=").append(city);

                return new Tuple2<>(sessionID, sb.toString());
            }
        });

        return session2AggFullInfo;
    }


    private static JavaPairRDD<String, Row> getSessionID2ActionRDD(JavaRDD<Row> actionRDD) {

        final JavaPairRDD<String, Row> session2ActionInfo = actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
            @Override
            public Iterable<Tuple2<String, Row>> call(Iterator<Row> it) throws Exception {
                List<Tuple2<String, Row>> list = new ArrayList<>();
                while (it.hasNext()) {
                    final Row row = it.next();
                    String sessionId = row.getString(2);
                    list.add(new Tuple2<>(sessionId, row));
                }
                return list;
            }
        });
        return session2ActionInfo;
    }
}

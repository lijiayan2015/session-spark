package com.ljy.sessionanalyze.spark.page;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ljy.sessionanalyze.constant.Constants;
import com.ljy.sessionanalyze.dao.factory.DAOFactory;
import com.ljy.sessionanalyze.domain.PageSplitConvertRate;
import com.ljy.sessionanalyze.domain.Task;
import com.ljy.sessionanalyze.util.DateUtils;
import com.ljy.sessionanalyze.util.NumberUtils;
import com.ljy.sessionanalyze.util.ParamUtils;
import com.ljy.sessionanalyze.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.*;

/**
 * 页面转化率
 */
public class PageOnrStepConventRateSpark {
    public static void main(String[] args) {
        //模板代码
        SparkConf conf = new SparkConf();
        conf.setAppName(Constants.SPARK_APP_NAME_PAGE);
        SparkUtils.setMaster(conf);
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

        sc.setCheckpointDir("hdfs://h1:9000/spark/sessionanalyze/PageOnrStepConventRateSpark");
        //获取数据
        SparkUtils.mockData(sc, sqlContext);

        //查询任务,获取任务信息
        long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE);

        final Task task = DAOFactory.getTaskDAO().findById(taskId);

        if (task == null) throw new RuntimeException("task任务信息获取失败");

        JSONObject jsonObject = JSON.parseObject(task.getTaskParam());

        //查询指定日期范围内的用户访问行为数据
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, jsonObject);
        //对用户访问的行为数据做映射,映射为<sessionId,访问行为数据(Row)>Session粒度的数据
        //因为用户访问页面切片的生成,是基于每个Session的访问数据生成的

        JavaPairRDD<String, Row> sessionId2ActionRDD = getsession2ActionRDD(actionRDD);
        sessionId2ActionRDD = sessionId2ActionRDD.cache();

        //因为需要拿到每个SessionId对应的行为数据,蔡恒生成切片
        //所有需要对Session粒度的基础数据进行分组
        final JavaPairRDD<String, Iterable<Row>> session2ActionsRDD = sessionId2ActionRDD.groupByKey();


        //这个需求中,关键的是每个Session单跳页面切片
        //生成的页面流匹配算法
        //返回的格式为<split,1>
        JavaPairRDD<String, Integer> pageSplitRDD = generateAndMatchPageSplit(sc, session2ActionsRDD, jsonObject);


        // 获取切片的访问量
        final Map<String, Object> pageSplitPvMap = pageSplitRDD.countByKey();
        //获取起始页面的访问量
        long startPv = getStartPagePv(jsonObject, session2ActionsRDD);

        // 计算目标页面的各个页面的切片转化率
        //返回类型为Map<String,Double> key=各个页面切片,value=页面切片对应的转化率
        Map<String, Double> convertRateMap = computoePageSplitConverRate(jsonObject, pageSplitPvMap, startPv);

        persistConvertRate(taskId, convertRateMap);
    }

    /**
     * 存储页面切片转化率
     *
     * @param taskId
     * @param convertRateMap
     */
    private static void persistConvertRate(long taskId, Map<String, Double> convertRateMap) {
        // 把页面流对应的页面切片拼接到buff
        StringBuffer buffer = new StringBuffer();
        convertRateMap.entrySet().forEach(entry -> {
            //页面切片
            String pageSplit = entry.getKey();
            //转化率
            double convertRate = entry.getValue();
            buffer.append(pageSplit).append("=").append(convertRate).append("|");
        });

        //获取拼接后的切片转化率
        String convertRate = buffer.toString();
        convertRate = convertRate.substring(0, convertRate.length() - 1);
        final PageSplitConvertRate rate = new PageSplitConvertRate();
        rate.setConvertRate(convertRate);
        rate.setTaskid(taskId);

        DAOFactory.getPageSplitConvertRateDAO().insert(rate);
    }

    /**
     * 计算页面切片转化率
     *
     * @param jsonObject
     * @param pageSplitPvMap
     * @param startPv
     * @return
     */
    private static Map<String, Double> computoePageSplitConverRate(JSONObject jsonObject, Map<String, Object> pageSplitPvMap, long startPv) {
        // 用于存储页面切片对应的转化率
        Map<String, Double> convertRateMap = new HashMap<>();
        // 获取页面流
        String[] targetpages = ParamUtils.getParam(jsonObject, Constants.PARAM_TARGET_PAGE_FLOW).split(",");

        //初始化上一个页面的访问量
        long lastPageSplitPv = 0L;

        /**
         * 求转化率:
         * 如果页面流为1,2,5,8
         * 第一个页面的切片1_2
         * 第一个页面转化率是:(2的pv)/(1的pv)
         */

        // 获取目标页面流中的各个页面切片的访问量
        for (int i = 1; i < targetpages.length; i++) {
            String targetPageSplit = targetpages[i - 1] + "_" + targetpages[i];
            long targetPageSplitPv = Long.valueOf(String.valueOf(pageSplitPvMap.get(targetPageSplit)));

            // 初始化转化率
            double convertRate;
            //生成切片

            if (i == 1) {
                convertRate = NumberUtils.formatDouble(targetPageSplitPv / startPv, 2);
            } else {
                convertRate = NumberUtils.formatDouble(targetPageSplitPv / lastPageSplitPv, 2);
            }

            convertRateMap.put(targetPageSplit, convertRate);
            lastPageSplitPv = targetPageSplitPv;
        }
        return convertRateMap;
    }

    private static long getStartPagePv(JSONObject jsonObject, JavaPairRDD<String, Iterable<Row>> session2ActionsRDD) {
        // 取出使用者提供的页面流
        String targetPageFlow = ParamUtils.getParam(jsonObject, Constants.PARAM_TARGET_PAGE_FLOW);
        // 从页面流中获取起始页面id
        final long startPageId = Long.parseLong(targetPageFlow.split(",")[0]);
        JavaRDD<Long> startPageRDD = session2ActionsRDD.flatMap(tup -> {
            // 用户存储每个Session访问的起始页面ID
            List<Long> list = new ArrayList<>();
            // 获取对应的行为数据
            tup._2.forEach(row -> {
                long pageId = row.getLong(3);
                if (pageId == startPageId) {
                    list.add(pageId);
                }
            });
            return list;
        });

        return startPageRDD.count();
    }

    /**
     * 页面切片的生成和页面流匹配算法的实现
     *
     * @param sc
     * @param session2ActionsRDD
     * @param jsonObject
     * @return
     */
    private static JavaPairRDD<String, Integer> generateAndMatchPageSplit(JavaSparkContext sc, JavaPairRDD<String, Iterable<Row>> session2ActionsRDD, JSONObject jsonObject) {
        //获取页面流
        final String targetPageFlow = ParamUtils.getParam(jsonObject, Constants.PARAM_TARGET_PAGE_FLOW);

        //把目标也页面流广播到Executor端,
        final Broadcast<String> targetPageFlowBroadcast = sc.broadcast(targetPageFlow);

        return session2ActionsRDD.flatMapToPair((PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Integer>) tup -> {

            //用于存储切片格式为<split,1>
            List<Tuple2<String, Integer>> list = new ArrayList<>();

            //获取当前Session对应的行为数据
            final Iterator<Row> it = tup._2.iterator();

            //获取使用者指定的页面流
            final String[] targetPages = targetPageFlowBroadcast.value().split(",");

            /**
             * 到这里,Session粒度的访问数据已经得到,单默认情况下并没有按照时间排序
             * 在实现转化率的时候,需要把数据按照时间顺序进行排序
             */

            // 把访问行为数据放到list中,这样便于排序
            List<Row> rows = new ArrayList<>();
            while (it.hasNext()) {
                rows.add(it.next());
            }

            //开始排序,可以用自定义排序,此时用匿名内部类的方式来排序
            rows.sort((r1, r2) -> {
                String actionTime1 = r1.getString(4);
                String actionTime2 = r2.getString(4);

                Date d1 = DateUtils.parseTime(actionTime1);
                Date d2 = DateUtils.parseTime(actionTime2);

                return (int) (d1.getTime() - d2.getTime());
            });

            /**
             * 生成页面切片以及页面流的匹配
             */
            //定义一个上一个页面的id

            Long lastPageId = null;

            for (Row row : rows) {
                long pageId = row.getLong(3);
                if (lastPageId == null) {
                    lastPageId = pageId;
                    continue;
                }

                //生成一个页面切片
                // 比如该用户请求的页面是1,2,4,5
                // 上次访问的页面id:lastPageId = 1
                // 这次请求的页面是:2
                // 那么生成的页面切片就是:1_2
                String pageSplit = lastPageId + "_" + pageId;

                //对这个页面切片判断一下,是否在使用者指定的页面中
                for (int i = 1; i < targetPages.length; i++) {
                    // 比如说:用户指定的页面流式1,2,4,5
                    // 遍历的时候,从索引1开始,就是从第二个页面开始
                    //那么第一个页面切片就是1_2
                    String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];
                    if (pageSplit.equals(targetPageSplit)) {
                        list.add(new Tuple2<>(pageSplit, 1));
                        break;
                    }
                }
                lastPageId = pageId;
            }


            return list;
        });
    }

    /**
     * 生成session粒度的用户访问行为数据
     *
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getsession2ActionRDD(JavaRDD<Row> actionRDD) {
        JavaPairRDD<String, Row> session2actionRDD =
                actionRDD.mapToPair((PairFunction<Row, String, Row>) row -> {
                    String sessionId = row.getString(2);
                    return new Tuple2<>(sessionId, row);
                });
        return session2actionRDD;
    }
}

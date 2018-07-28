package com.ljy.sessionanalyze.spark.session;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Optional;
import com.ljy.sessionanalyze.constant.Constants;
import com.ljy.sessionanalyze.dao.*;
import com.ljy.sessionanalyze.dao.factory.DAOFactory;
import com.ljy.sessionanalyze.domain.*;
import com.ljy.sessionanalyze.util.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import jodd.util.StringUtil;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

/**
 * 获取用户访问Session数据进行分析
 * 1. 获取使用者创建的任务信息
 * 任务信息中过滤的条件有:
 * 时间范围:起始日期,结束日期
 * 性别
 * 所在城市
 * 搜索关键字
 * 点击品类
 * 点击商品
 * 2. Spark作业是如何接收使用者穿件的任务信息
 * shell 脚本通知---SparkSubmit
 * 从MYsql的task表中根据taskID来获取任务信息
 * <p>
 * 3. Spark作业开始数据分析
 */
public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) {
        /**模板代码*/
        //创建配置信息类对象
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION);
        SparkUtils.setMaster(conf);

        //上下文对象,集群的入口类
        JavaSparkContext sc = new JavaSparkContext(conf);

        //sparkSQL的上下文
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

        //设置检查点
        sc.checkpointFile("hdfs://h1:9000/spark/session/" + DateUtils.getTodayDate());

        //生成模拟数据
        SparkUtils.mockData(sc, sqlContext);

        // 创建获取任务信息的实例
        final ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        //获取指定的任务
        final Long taskID =
                ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);

        final Task task = taskDAO.findById(taskID);

        if (task == null) {
            throw new RuntimeException("没有获取到taskid对应的数据");
        }

        //获取taskID对应的任务信息,也就是task_param字段对应的值
        // task_param的值就是使用者提供的查询条件
        final JSONObject taskParam = JSON.parseObject(task.getTaskParam());

        // 查询日期范围内的行为数据(点击,搜索,下单,支付)
        // 首先要从user_visit_action这张hive表中查询出按照指定日期范围得到的行为数据
        final JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);

        //生成Session粒度的基础数据
        //得到的数据的格式为<sessionID,actionRDD>
        JavaPairRDD<String, Row> sessionID2ActionRDD =
                getSessionID2ActionRDD(actionRDD);

        // 对于以后经常用到的基础数据,最好缓存起来,便于以后快速的获取
        sessionID2ActionRDD = sessionID2ActionRDD.cache();

        //对行为数据进行聚合
        //1.将行为数据按照SessionID进行分组
        //2.行为数据RDD(actionRDD)需要和用户信息进行join,
        //  这样救得到了Session粒度的明细数据
        //  明细数据包含Session对应的用户基本信息
        // 生成的数据格式为:<SessioniD,(Seesionid,searchKeyWorlds,clickCatrgoryIds,age,professional,sex，start_time)>
        JavaPairRDD<String, String> session2AggInfoRDD =
                aggregateBySession(sqlContext, sessionID2ActionRDD);


        //实现Accumulator累加器对数据字段值得累加
        Accumulator<String> sessionAggStatAccumulator
                = sc.accumulator("", new SessionAggStatAcc());


        // 以Session粒度的数据进行聚合,需要按照使用者指定的筛选条件进行过滤
        JavaPairRDD<String, String> filtedSessionId2AggrInfoRDD =
                filtedSeesionAndAggStat(session2AggInfoRDD, taskParam, sessionAggStatAccumulator);


        //把按照使用者的条件过滤后的数据进行缓存
        filtedSessionId2AggrInfoRDD = filtedSessionId2AggrInfoRDD.persist(StorageLevel.MEMORY_AND_DISK());

        //生成一个通用的RDD,通过筛选条件过滤出来的Session(filtedSessionId2AggrInfoRDD)来得到访问明细
        // 这里得到的是过滤的<SessionID,actionRow>
        JavaPairRDD<String, Row> sessionId2detailRDD =
                getSessionId2detailRDD(filtedSessionId2AggrInfoRDD, sessionID2ActionRDD);

        // 缓存通过筛选条件过滤出来的Session得到的明细数据
        sessionId2detailRDD = sessionId2detailRDD.persist(StorageLevel.MEMORY_AND_DISK());

        //上一个聚合的需求统计的结果是进行存储,没有调用调用一个action类型的算子
        // 所以必须在触发一个action算子后才能从累加器中获取到结果数据
        System.err.println(sessionId2detailRDD.count());

        //计算出各个范围的Session占比并存入数据库
        calculateAndPersistAggStat(sessionAggStatAccumulator.value(), task.getTaskid());


        // 按照时间比例随机抽取Session
        //1.计算出每个小时的Session数量
        //2.计算出每个小时Session数据量与一天Session数据量的占比
        //3.按照比例进行随机抽取
        randomExtractSession(sc, task.getTaskid(), filtedSessionId2AggrInfoRDD, sessionId2detailRDD);


        // top10 热门品类
        // 1.首先获取到通过筛选条件的Session访问过的所有品类
        // 2.计算出Session访问过的所有品类的点击,下单,支付次数,用到了join
        // 3.开始自定义排序
        // 4.将品类的点击,下单,支付次数封装到自定义排key中
        // 5.使用sortByKey进行自定义排序(降序)
        // 6.获取排序后的前10个品类
        // 7.将top10 热门品类的每一个品类点击次数,下单次数,支付次数写到数据库中

        List<Tuple2<CategorySortKey, String>> top10CategoryList
                = getTop10CategoryList(task.getTaskid(), sessionId2detailRDD);


        //获取top10热门品类中的每个品类取top10活跃session
        //1. 获取到符合筛选条件的Session明细数据
        //2. 按照Session粒度进行聚合,获取到Session对应的每个品类的点击次数
        //3. 按照品类id,分组取top10,并且获取到top10活跃Session
        //4. 把结果数据存到数据库
        getTop10Session(sc, task.getTaskid(), top10CategoryList, sessionId2detailRDD);


        sc.close();

    }

    /**
     * 获取top10活跃Session
     *
     * @param sc
     * @param taskid
     * @param top10CategoryList
     * @param sessionId2detailRDD
     */
    private static JavaPairRDD<String, String> getTop10Session(JavaSparkContext sc, long taskid, List<Tuple2<CategorySortKey, String>> top10CategoryList, JavaPairRDD<String, Row> sessionId2detailRDD) {

        //第一步,将top10热门品类的id转化为RDD
        List<Tuple2<Long, Long>> top10CategoryIdList = new ArrayList<>();

        for (Tuple2<CategorySortKey, String> category : top10CategoryList) {
            long categoryId = Long.parseLong(StringUtils.getFieldFromConcatString(category._2, "\\|", Constants.FIELD_CATEGORY_ID));
            top10CategoryIdList.add(new Tuple2<>(categoryId, categoryId));
        }

        // 将top10CategoryIdList转化为RDD
        final JavaPairRDD<Long, Long> top10CategoryRDD = sc.parallelizePairs(top10CategoryIdList);

        /**
         *  第二步,计算top10品类中被各个Session点击的次数
         */
        // 把明细数据以SessionId进行分组
        // rftewrtwertew     123456,123456    <123456,"rftewrtwertew,12">
        // rftewrtwertew1     123456,123456   <123456,"rftewrtwertew1,123">

        // <rftewrtwertew1,"123445,12345,12345">
        // <rftewrtwertew2,"123445,12345,12345">
        final JavaPairRDD<String, Iterable<Row>> sessionId2DetailsRDD = sessionId2detailRDD.groupByKey();

        //把品类id对应的Session和count数生成<categoryid,"sessionid,count">
        //<123445,"rftewrtwertew1,1">  <12345,"rftewrtwertew1,2">
        //<123445,"rftewrtwertew2,1">  <12345,"rftewrtwertew2,2">
        JavaPairRDD<Long, String> categoryId2SessionCountRDD =
                sessionId2DetailsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {

                    @Override
                    public Iterable<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> tup) throws Exception {
                        String sessionId = tup._1;//rftewrtwertew1
                        final Iterator<Row> it = tup._2.iterator();
                        //用于存储品类对应的点击次数<key=categoryid,value=次数>
                        Map<Long, Long> categoryCountMap = new HashMap<>();

                        //计算出当前Session中对应的每个品类的点击次数
                        while (it.hasNext()) {
                            final Row row = it.next();
                            if (row.get(6) != null) {
                                long categoryId = row.getLong(6);
                                Long count = categoryCountMap.get(categoryId);
                                if (count == null) count = 0L;
                                count++;
                                categoryCountMap.put(categoryId, count);
                            }
                        }
                        // 返回结果到一个list,格式<categoryId,"sessionid,count">
                        List<Tuple2<Long, String>> list = new ArrayList<>();
                        for (Map.Entry<Long, Long> entry : categoryCountMap.entrySet()) {
                            long categoryId = entry.getKey();
                            long count = entry.getValue();
                            String value = sessionId + "," + count;
                            list.add(new Tuple2<>(categoryId, value));

                            //<123445,"rftewrtwertew1,1">  <12345,"rftewrtwertew1,2">
                           //<123445,"rftewrtwertew2,1">  <12345,"rftewrtwertew2,2">
                        }
                        return list;
                    }
                });

        // 获取到top10热门品类被各个Session点击的次数
        // <categoryId,"Sessionid,count">
        // <categoryId,"Sessionid1,count">
        JavaPairRDD<Long, String> top10CategorySessionCountRDD =
                top10CategoryRDD.join(categoryId2SessionCountRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, String>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, String>> tup) throws Exception {
                        return new Tuple2<>(tup._1, tup._2._2);
                    }
                });


        /**
         * 第三步:分组取topN算法,实现获取每个品类的top10活跃用户
         */

        //以categoryId进行分组
        final JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD = top10CategorySessionCountRDD.groupByKey();
        //<categoryId,"Sessionid,36|"Sessionid1,45">

        JavaPairRDD<String, String> top10SessionRDD =
                top10CategorySessionCountsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>() {
                    @Override
                    public Iterable<Tuple2<String, String>> call(Tuple2<Long, Iterable<String>> tup) throws Exception {
                        long categoryId = tup._1;
                        final Iterator<String> it = tup._2.iterator();
                        //用来存储topN的排序数组
                        String[] top10Sessions = new String[10];
                        while (it.hasNext()) {
                            final String sessionCount = it.next();
                            long count = Long.parseLong(sessionCount.split(",")[1]);
                            //遍历排序数组,TopN算法
                            for (int i = 0; i < top10Sessions.length; i++) {
                                //判断,如果当前的下标为i的数据为空,那么直接当前将SessionCount赋值给i位数据
                                if (top10Sessions[i] == null) {
                                    top10Sessions[i] = sessionCount;
                                    break;
                                } else {
                                    long _count = Long.valueOf(top10Sessions[i].split(",")[1]);

                                    //判断如果SessionCount比I位的SessionCount(_count)大,
                                    if (count > _count) {
                                        //从排序数据最后以为开始,到i位所有的数据往后挪一位
                                        for (int j = 9; j > i; j--) {
                                            top10Sessions[j] = top10Sessions[j - 1];
                                        }
                                        // 将SessionCount赋值给i位
                                        top10Sessions[i] = sessionCount;
                                        break;
                                    }
                                }
                            }
                        }

                        List<Tuple2<String, String>> list = new ArrayList<>();
                        for (String sessionCount : top10Sessions) {
                            if (sessionCount != null) {
                                String sessionId = sessionCount.split(",")[0];
                                long count = Long.parseLong(sessionCount.split(",")[1]);

                                final Top10Session session = new Top10Session();
                                session.setCategoryid(categoryId);
                                session.setTaskid(taskid);
                                session.setSessionid(sessionId);
                                session.setClickCount(count);

                                final ITop10SessionDAO dao = DAOFactory.getTop10SessionDAO();
                                dao.insert(session);
                                list.add(new Tuple2<>(sessionId, sessionId));
                            }
                        }
                        return list;
                    }
                });

        final JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD = top10SessionRDD.join(sessionId2detailRDD);

        sessionDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> tup) throws Exception {
                Row row = tup._2._2;

                SessionDetail sessionDetail = new SessionDetail();

                sessionDetail.setTaskid(taskid);
                sessionDetail.setUserid(row.getLong(1));
                sessionDetail.setSessionid(row.getString(2));
                sessionDetail.setPageid(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.getLong(6));
                sessionDetail.setClickProductId(row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));

                final ISessionDetailDAO dao = DAOFactory.getSessionDetailDAO();
                dao.insert(sessionDetail);
            }
        });

        return top10SessionRDD;
    }

    /**
     * 计算top10 热门品类
     *
     * @param taskid
     * @param sessionId2detailRDD
     * @return
     */
    private static List<Tuple2<CategorySortKey, String>> getTop10CategoryList(long taskid, JavaPairRDD<String, Row> sessionId2detailRDD) {

        /**
         * 1.首先获取到通过筛选条件的Session访问过的所有品类
         */

        // 获取Session访问过的所有品类id(访问过指的是点击过,下单过,支付过)

        JavaPairRDD<Long, Long> categoryIdRDD = sessionId2detailRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tup) throws Exception {
                Row row = tup._2;
                // 用来存储点击品类,下单,支付信息的
                List<Tuple2<Long, Long>> list = new ArrayList<>();

                //添加点击品类信息
                Long clickCategoryId = row.getLong(6);
                if (clickCategoryId != null) {
                    list.add(new Tuple2<>(clickCategoryId, clickCategoryId));
                }

                //添加下单品类信息
                String orderCategoryIds = row.getString(8);
                addIdstoList(list, orderCategoryIds);

                // 添加支付品类信息
                String payCategoryIds = row.getString(10);
                addIdstoList(list, payCategoryIds);


                return list;
            }
        });

        /**
         * Session访问过的所有品类中,可能有重复的categoryId,需要去重
         * 如果不去重,在排序过程中,会对categoryId重复排序,最后会产生重复的数据
         */
        categoryIdRDD = categoryIdRDD.distinct();

        /**
         * 计算各品类的点击,下单,支付次数
         */
        // 计算各品类的点击次数
        JavaPairRDD<Long, Long> clickCategoryId2CountRdd = getClickCategoryId2CountRDD(sessionId2detailRDD);


        //计算各品类的下单次数
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionId2detailRDD);


        // 计算各品类的支付次数
        JavaPairRDD<Long, Long> payOrderCategoryId2CountRDD = getPayOrderCategoryId2CountRDD(sessionId2detailRDD);

        /**
         * 第三步:join各个品类的点击,下单,支付次数
         * categoryIdRDD 包含了所有符合条件的并且过滤掉重复品类的Session
         * 在第二步中分别计算了点击下单支付的次数,可能不是包含所有的品类
         * 比如:有的品类只是点击,但是没有下单,类似这种情况有很多
         * 所以在这里,如果要join,就不能用join,需要用leftoutjoin
         */

        JavaPairRDD<Long, String> categoryId2CountRDD = joinCategoeyAndDetail(
                categoryIdRDD,
                clickCategoryId2CountRdd,
                orderCategoryId2CountRDD,
                payOrderCategoryId2CountRDD
        );


        /**
         * 第四步:自定义排序品类
         */

        /**
         * 第五步:将数据映射为:<CategorySortKey,countInfo>格式的RDD,在进行二次排序
         */
        JavaPairRDD<CategorySortKey, String> sortKeyCountRDD = categoryId2CountRDD.mapToPair(new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {
            @Override
            public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> tup) throws Exception {
                String countInfo = tup._2;
                long cilckCount = Long.parseLong(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
                long corderCount = Long.parseLong(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
                long payCount = Long.parseLong(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT));
                //创建自定义排序
                CategorySortKey key = new CategorySortKey(cilckCount, corderCount, payCount);
                return new Tuple2<>(key, countInfo);
            }
        });

        final JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = sortKeyCountRDD.sortByKey(false);//false降序


        // 第六步,用take(10)取出热门品类,写入到数据库
        final List<Tuple2<CategorySortKey, String>> list = sortedCategoryCountRDD.take(10);
        list.forEach(tup -> {
            String countInfo = tup._2;
            long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID));
            final long payCount = tup._1.getPayCount();
            final long orderCount = tup._1.getOrderCount();
            final long clickCount = tup._1.getClickCount();
            Top10Category category = new Top10Category();
            category.setCategoryid(categoryId);
            category.setClickCount(clickCount);
            category.setOrderCount(orderCount);
            category.setPayCount(payCount);
            final ITop10CategoryDAO dao = DAOFactory.getTop10CategoryDAO();
            dao.insert(category);
        });
        return list;
    }

    /**
     * 连接品类RDD与数据RDD
     *
     * @param categoryIdRDD               Session访问过的所有品类id
     * @param clickCategoryId2CountRdd    各个品类的点击次数
     * @param orderCategoryId2CountRDD    各个品类的下单次数
     * @param payOrderCategoryId2CountRDD 各个品类的支付次数
     * @return
     */
    private static JavaPairRDD<Long, String> joinCategoeyAndDetail(JavaPairRDD<Long, Long> categoryIdRDD,
                                                                   JavaPairRDD<Long, Long> clickCategoryId2CountRdd,
                                                                   JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
                                                                   JavaPairRDD<Long, Long> payOrderCategoryId2CountRDD) {
        //注意,如果用leftOuterJoin可能出现右边RDD中join过来的值为空的情况
        // 所有的tupe中第二个值用Option<Long>类型,代表可能有值,也可能没有值

        final JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tempJoinRDD = categoryIdRDD.leftOuterJoin(clickCategoryId2CountRdd);
        //把数据生成格式调整为(categoryId,"categoryId=品类|cliclCount=点击次数");
        JavaPairRDD<Long, String> tempMapRDD = tempJoinRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tup) throws Exception {
                long categoryId = tup._1;
                final Optional<Long> optional = tup._2._2;
                long clickCount = 0;
                if (optional.isPresent()) {
                    clickCount = optional.get();
                }

                String value = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|" + Constants.FIELD_CLICK_COUNT + "=" + clickCount;
                return new Tuple2<>(categoryId, value);
            }
        });

        //再次与下单次数进行leftJoinOuterJoin
        tempMapRDD = tempMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tup) throws Exception {
                long categoryID = tup._1;
                String value = tup._2._1;
                final Optional<Long> optional = tup._2._2;
                long orderCount = 0l;
                if (optional.isPresent()) {
                    orderCount = optional.get();
                }
                value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;
                return new Tuple2<>(categoryID, value);
            }
        });


        //再次与下支付数进行leftJoinOuterJoin
        tempMapRDD = tempMapRDD.leftOuterJoin(payOrderCategoryId2CountRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tup) throws Exception {
                long categoryID = tup._1;
                String value = tup._2._1;
                final Optional<Long> optional = tup._2._2;
                long orderCount = 0l;
                if (optional.isPresent()) {
                    orderCount = optional.get();
                }
                value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + orderCount;
                return new Tuple2<>(categoryID, value);
            }
        });
        return tempMapRDD;
    }

    /**
     * 计算各品类的支付次数
     *
     * @param sessionId2detailRDD
     * @return
     */
    private static JavaPairRDD<Long, Long> getPayOrderCategoryId2CountRDD(JavaPairRDD<String, Row> sessionId2detailRDD) {
        final JavaPairRDD<String, Row> filtered = sessionId2detailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> tup) throws Exception {
                return tup._2.get(10) != null;
            }
        });

        //把过滤后的数据,生成一个个的元组,便于以后聚合
        JavaPairRDD<Long, Long> payorderCategoeyIdRDD = filtered.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tup) throws Exception {
                String payOrderCategoryIds = tup._2.getString(10);
                final String[] splited = payOrderCategoryIds.split(",");
                //用于存储切分后的数据<orderCategoryId,1>
                List<Tuple2<Long, Long>> list = new ArrayList<>();
                for (String id : splited) {
                    list.add(new Tuple2<>(Long.parseLong(id), 1L));
                }
                return list;
            }
        });

        return payorderCategoeyIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong + aLong2;
            }
        });
    }

    private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(JavaPairRDD<String, Row> sessionId2detailRDD) {
        // 过滤下单字段为空的数据
        final JavaPairRDD<String, Row> filtered = sessionId2detailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> tup) throws Exception {
                return tup._2.get(8) != null;
            }
        });

        //把过滤后的数据,生成一个个的元组,便于以后聚合
        JavaPairRDD<Long, Long> orderCategoeyIdRDD = filtered.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tup) throws Exception {
                String orderCategoryIds = tup._2.getString(8);
                final String[] splited = orderCategoryIds.split(",");
                //用于存储切分后的数据<orderCategoryId,1>
                List<Tuple2<Long, Long>> list = new ArrayList<>();
                for (String id : splited) {
                    list.add(new Tuple2<>(Long.parseLong(id), 1L));
                }
                return list;
            }
        });

        return orderCategoeyIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong + aLong2;
            }
        });
    }

    /**
     * 计算各品类的点击次数
     *
     * @param sessionId2detailRDD
     * @return
     */
    private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(JavaPairRDD<String, Row> sessionId2detailRDD) {
        // 把明细数据中的点击品类字段空字段过滤掉
        JavaPairRDD<String, Row> clickActionRDD = sessionId2detailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> tup) throws Exception {
                final Row row = tup._2;
                return row.get(6) != null;
            }
        });

        //将个点击品类后面跟一个1,生成一个元组(clickCategoryId,1),为做聚合做准备
        JavaPairRDD<Long, Long> clickCategoryIdRDD =
                clickActionRDD.mapToPair(new PairFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Tuple2<Long, Long> call(Tuple2<String, Row> tup) throws Exception {
                        long clickCategoryId = tup._2.getLong(6);
                        return new Tuple2<>(clickCategoryId, 1L);
                    }
                });

        // 计算各个品类的点击次数
        final JavaPairRDD<Long, Long> clickCategoryCount = clickCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong + aLong2;
            }
        });
        return clickCategoryCount;
    }

    private static void addIdstoList(List<Tuple2<Long, Long>> list, String Ids) {
        if (Ids != null) {
            final String[] orderCategoryIdsSplited = Ids.split(",");
            for (String orderCategoryId : orderCategoryIdsSplited) {
                final long longordercategoryid = Long.parseLong(orderCategoryId);
                list.add(new Tuple2<>(longordercategoryid, longordercategoryid));
            }
        }
    }

    /**
     * 按照时间比例随机抽取Session
     *
     * @param sc
     * @param taskid
     * @param filtedSessionId2AggrInfoRDD
     * @param sessionId2detailRDD
     */
    private static void randomExtractSession(JavaSparkContext sc,
                                             final long taskid,
                                             JavaPairRDD<String, String> filtedSessionId2AggrInfoRDD,
                                             JavaPairRDD<String, Row> sessionId2detailRDD) {

        // 计算出每小时的Session数量,filtedSessionId2AggrInfoRDD包含Session的start_time
        // 首先需要把数据格式调整为<date_hour,data>
        JavaPairRDD<String, String> time2SessionIdRDD = filtedSessionId2AggrInfoRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> tup) throws Exception {
                // 获取聚合数据
                String aggrInfo = tup._2;

                // 从聚合数据里面抽取startTime
                String startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME);

                // 获取startTime中的日期和时间
                String dateHour = DateUtils.getDateHour(startTime);

                return new Tuple2<>(dateHour, aggrInfo);
            }
        });

        // 要得到每天每小时的Session数量,然后计算出每天每小时的Session抽取索引,遍历每天每小时的Session
        //首先抽取出Session聚合数据,写入数据库:session_random_extract
        //time2SessionIdRDD的value值是每天某个小时的Session聚合数据

        //计算出每天每小时的Session数量
        final Map<String, Object> countMap = time2SessionIdRDD.countByKey();

        /**
         * 第二步,使用时间比例随机抽取算法,计算出每天每小时抽取的Session索引
         */

        //将countMap <date_hour,data>数据格式转化为<yyyy-MM-dd,<HH,count>>并放到Map里面
        // 存储一天中，每个小时段的Session数量
        HashMap<String, Map<String, Long>> dateHourCountMap =
                new HashMap<>();
        //把数据循环的放到dateHourCountMap里面
        for (Map.Entry<String, Object> entry : countMap.entrySet()) {
            // 抽取出日期和小时
            String dateHour = entry.getKey();
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];

            //取出每个小时的count数
            long count = Long.parseLong(String.valueOf(entry.getValue()));

            // 用来存储<hour,count>
            //TODO 这里是不是有问题呢？想了想，还是没有问题的。
            // 获取一天中某个小时的的Session数量
            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            //如果当前天没有存储到map中，new一个新的放进去
            if (hourCountMap == null) {
                hourCountMap = new HashMap<>();
                dateHourCountMap.put(date, hourCountMap);
            }
            // 小时不同，所以count不会被覆盖
            // 由于是引用，所以在这里存储也会也会修改dateHourCountMap中hourCountMap的keyvalue值
            hourCountMap.put(hour, count);
        }

        // 实现按时间比例抽取算法
        // 比如要抽取100个Session,先按照天数进行平分
        final int extranctNumber = 100 / dateHourCountMap.size();

        // Map<date,Map<hour,List(5,4,35,...)
        // 用random获取一天中每个小时段应该抽取的Session 下标
        Map<String, Map<String, List<Integer>>>
                dateHourExtranctMap = new HashMap<>();

        Random random = new Random();

        final Set<Map.Entry<String, Map<String, Long>>> set = dateHourCountMap.entrySet();
        for (Map.Entry<String, Map<String, Long>> dateHourCountEntry : set) {
            //获取日期
            final String date = dateHourCountEntry.getKey();
            // 获取到日期对应的小时和count数
            final Map<String, Long> hourCountMap = dateHourCountEntry.getValue();
            // 计算出当前这一天的Session总数
            long sessionCount = 0L;
            for (long hourCount : hourCountMap.values()) {
                sessionCount += hourCount;
            }

            // 把一天的Session数量put到dateHourExtranctMap
            Map<String, List<Integer>> hourExtractMap = dateHourExtranctMap.get(date);
            if (hourExtractMap == null) {
                hourExtractMap = new HashMap<>();
                dateHourExtranctMap.put(date, hourExtractMap);
            }
            // 遍历每个小时获取每个小时的Session数量
            for (Map.Entry<String, Long> hourCountEntry :
                    hourCountMap.entrySet()) {
                //小时
                final String hour = hourCountEntry.getKey();
                //小时对应的count数
                long count = hourCountEntry.getValue();

                //计算每个小时Session数量占当前Session数量的比例,乘以抽取的数量
                //最后计算出当前小数需要抽取的Session数量
                long hourExtractNumber = (int) ((count * 1.0 / sessionCount) * extranctNumber);

                //当前需要收取的Session数量有可能大于每小时的Session数量
                // 让当前小时需要抽取的Session数量直接等于每小时抽取的Session数量
                if (hourExtractNumber > count) {
                    hourExtractNumber = count;
                }

                // 获取当前小时存放随机的list,如果没有就创建一个
                List<Integer> extractIndexList = hourExtractMap.get(hour);
                if (extractIndexList == null) {
                    extractIndexList = new ArrayList<>();
                    hourExtractMap.put(hour, extractIndexList);
                }

                //生成上面计算出来的随机数,用whlie来判断生成的随机数不能是重复的
                for (int i = 0; i < hourExtractNumber; i++) {
                    int extraxtIndex = random.nextInt(((int) count));
                    while (extractIndexList.contains(extraxtIndex)) {
                        //重复的话随机生成随机数
                        extraxtIndex = random.nextInt((int) count);
                    }

                    extractIndexList.add(extraxtIndex);
                }

            }
        }

        // dateHourExtranctMap 封装到fastUtilDateHourExtractMap
        //fastUtilDateHourExtractMap 可以封装map,list,set 相比较普通的map,list,set占用的内存更小,
        // 所以传输的时候速度更快,占用的网络IO更少
        Map<String, Map<String, IntList>> fastUtilDateHourExtractMap = new HashMap<>();
        for (Map.Entry<String, Map<String, List<Integer>>> dateHourExtractEntry : dateHourExtranctMap.entrySet()) {
            String date = dateHourExtractEntry.getKey();
            Map<String, List<Integer>> hourExtractMap = dateHourExtractEntry.getValue();

            //存储<hour,extract>数据
            Map<String, IntList> fastUtilExtractMap = new HashMap<>();
            for (Map.Entry<String, List<Integer>> hourExtractEntry : hourExtractMap.entrySet()) {
                // hour
                String hour = hourExtractEntry.getKey();
                // extract
                final List<Integer> extractList = hourExtractEntry.getValue();
                IntList fastUtilExtractList = new IntArrayList();
                fastUtilExtractList.addAll(extractList);
                fastUtilExtractMap.put(hour, fastUtilExtractList);
            }

            fastUtilDateHourExtractMap.put(date, fastUtilExtractMap);

        }
        /**
         * 在集群执行任务的时候,有可能多个Extract远程的获取上面的map值
         * 这样会产生大量的网络IO,此时最好用广播变量把该值广播到每一个参与计算的Executor中，这样Executor中的每个task都可以共享
         */

        final Broadcast<Map<String, Map<String, IntList>>> dateHourExtractMapBroadcast = sc.broadcast(fastUtilDateHourExtractMap);

        /**
         * 第三步,遍历每天每小时的Session,根据随机索引进行抽取
         */

        // 获取到聚合数据,数据结构为:<dateHour,aggInfo>
        JavaPairRDD<String, Iterable<String>> time2SessionRDD = time2SessionIdRDD.groupByKey();

        // 用flatMap遍历所有的time2SessionRDD
        // 然后遍历每天每小时的Session
        // 如果发现某个Session正好在指定的这天这个小时随机抽取索引上,
        // 将该Session写入到session_random_extract中
        // 接下来将抽取出来的Session返回,生成一个新的javaRDD<String>
        // 用抽取数来的SessionID取join访问明细,并写入到数据库表session_detail中
        JavaPairRDD<String, String> extractSessionIdsRdd =
                time2SessionRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
                    @Override
                    public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tup) throws Exception {
                        List<Tuple2<String, String>> extractSessionIds = new ArrayList<>();
                        String dateHour = tup._1;
                        String date = dateHour.split("_")[0];
                        String hour = dateHour.split("_")[1];

                        final Iterator<String> it = tup._2.iterator();
                        //获取广播过来的值
                        final Map<String, Map<String, IntList>> dataHourExtractMap = dateHourExtractMapBroadcast.value();
                        //获取抽取索引list
                        final IntList extractIndexList = dataHourExtractMap.get(date).get(hour);
                        final ISessionRandomExtractDAO dao = DAOFactory.getSessionRandomExtractDAO();
                        int index = 0;
                        while (it.hasNext()) {
                            String sessionAggInfo = it.next();
                            if (extractIndexList.contains(index)) {
                                String sessionId = StringUtils.getFieldFromConcatString(sessionAggInfo, "\\|", Constants.FIELD_SESSION_ID);

                                //将数据存入数据库
                                final SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                                sessionRandomExtract.setTaskid(taskid);
                                sessionRandomExtract.setSessionid(sessionId);
                                sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(sessionAggInfo, "\\|", Constants.FIELD_START_TIME));
                                sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(sessionAggInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));
                                sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(sessionAggInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
                                dao.insert(sessionRandomExtract);

                                //将SessionID加入list
                                extractSessionIds.add(new Tuple2<>(sessionId, sessionId));
                            }
                            index++;
                        }

                        return extractSessionIds;
                    }
                });

        /**
         * 第四步:获取抽取出来的Session对应的明细数据写入数据库
         */
        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD =
                extractSessionIdsRdd.join(sessionId2detailRDD);

        extractSessionDetailRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Tuple2<String, Row>>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Tuple2<String, Row>>> it) throws Exception {
                //用来存储明细数据
                List<SessionDetail> sessionDetails = new ArrayList<>();
                while (it.hasNext()) {
                    final Tuple2<String, Tuple2<String, Row>> tuple = it.next();
                    final Row row = tuple._2._2;
                    final SessionDetail detail = new SessionDetail();

                    detail.setTaskid(taskid);
                    detail.setUserid(row.getLong(1));
                    detail.setSessionid(tuple._1);
                    detail.setPageid(row.getLong(3));
                    detail.setActionTime(row.getString(4));
                    detail.setSearchKeyword(row.getString(5));
                    detail.setClickCategoryId(row.getLong(6));
                    detail.setClickProductId(row.getLong(7));
                    detail.setOrderCategoryIds(row.getString(8));
                    detail.setOrderProductIds(row.getString(9));
                    detail.setPayCategoryIds(row.getString(10));
                    detail.setPayProductIds(row.getString(11));

                    sessionDetails.add(detail);
                }

                final ISessionDetailDAO dao = DAOFactory.getSessionDetailDAO();
                dao.insertBatch(sessionDetails);
            }
        });
    }

    private static void calculateAndPersistAggStat(String value, long taskid) {
        long sessionCount = Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));
        long visitLen_1s_3s =
                Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visitLen_4s_6s =
                Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visitLen_7s_9s =
                Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visitLen_10s_30s =
                Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visitLen_30s_60s =
                Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visitLen_1m_3m =
                Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visitLen_3m_10m =
                Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visitLen_10m_30m =
                Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visitLen_30m =
                Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m));


        long stepLen_1_3 =
                Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3));
        long stepLen_4_6 =
                Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6));
        long stepLen_7_9 =
                Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9));
        long stepLen_10_30 =
                Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30));
        long stepLen_30_60 =
                Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60));
        long stepLen_60 =
                Long.parseLong(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60));


        // 计算各个访问时长和步长的占比
        double visit_len_1s_3s = NumberUtils.formatDouble(visitLen_1s_3s * 1.0 / sessionCount, 2);
        double visit_len_4s_6s = NumberUtils.formatDouble(visitLen_4s_6s * 1.0 / sessionCount, 2);
        double visit_len_7s_9s = NumberUtils.formatDouble(visitLen_7s_9s * 1.0 / sessionCount, 2);
        double visit_len_10s_30s = NumberUtils.formatDouble(visitLen_10s_30s * 1.0 / sessionCount, 2);
        double visit_len_30s_60s = NumberUtils.formatDouble(visitLen_30s_60s * 1.0 / sessionCount, 2);
        double visit_len_1m_3m = NumberUtils.formatDouble(visitLen_1m_3m * 1.0 / sessionCount, 2);
        double visit_len_3m_10m = NumberUtils.formatDouble(visitLen_3m_10m * 1.0 / sessionCount, 2);
        double visit_len_10m_30m = NumberUtils.formatDouble(visitLen_10m_30m * 1.0 / sessionCount, 2);
        double visit_len_30m = NumberUtils.formatDouble(visitLen_30m * 1.0 / sessionCount, 2);

        double steplen_1_3 = NumberUtils.formatDouble(stepLen_1_3 * 1.0 / sessionCount, 2);
        double steplen_4_6 = NumberUtils.formatDouble(stepLen_4_6 * 1.0 / sessionCount, 2);
        double steplen_7_9 = NumberUtils.formatDouble(stepLen_7_9 * 1.0 / sessionCount, 2);
        double steplen_10_30 = NumberUtils.formatDouble(stepLen_10_30 * 1.0 / sessionCount, 2);
        double steplen_30_60 = NumberUtils.formatDouble(stepLen_30_60 * 1.0 / sessionCount, 2);
        double steplen_60 = NumberUtils.formatDouble(stepLen_60 * 1.0 / sessionCount, 2);

        //将运行结果存入数据库
        SessionAggrStat stat = new SessionAggrStat();
        stat.setTaskid(taskid);

        stat.setSession_count(sessionCount);

        stat.setVisit_length_1s_3s_ratio(visit_len_1s_3s);
        stat.setVisit_length_4s_6s_ratio(visit_len_4s_6s);
        stat.setVisit_length_7s_9s_ratio(visit_len_7s_9s);
        stat.setVisit_length_10s_30s_ratio(visit_len_10s_30s);
        stat.setVisit_length_30s_60s_ratio(visit_len_30s_60s);
        stat.setVisit_length_1m_3m_ratio(visit_len_1m_3m);
        stat.setVisit_length_3m_10m_ratio(visit_len_3m_10m);
        stat.setVisit_length_10m_30m_ratio(visit_len_10m_30m);
        stat.setVisit_length_30m_ratio(visit_len_30m);

        stat.setStep_length_1_3_ratio(steplen_1_3);
        stat.setStep_length_4_6_ratio(steplen_4_6);
        stat.setStep_length_7_9_ratio(steplen_7_9);
        stat.setStep_length_10_30_ratio(steplen_10_30);
        stat.setStep_length_30_60_ratio(steplen_30_60);
        stat.setStep_length_60_ratio(steplen_60);

        final ISessionAggrStatDAO dao = DAOFactory.getSessionAggrStatDAO();
        dao.insert(stat);
    }

    /**
     * 获取通过筛选条件的Session的访问明细
     *
     * @param filtedSessionId2AggrInfoRDD 过滤的包含Session数据和用户信息数据
     * @param sessionID2ActionRDD  Session数据
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionId2detailRDD(JavaPairRDD<String, String> filtedSessionId2AggrInfoRDD, JavaPairRDD<String, Row> sessionID2ActionRDD) {
        // 过滤后的数据和访问明细数据进行join
        JavaPairRDD<String, Row> sessionID2DetailRDD = filtedSessionId2AggrInfoRDD.join(sessionID2ActionRDD).mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tup) throws Exception {
                //返回Session和对应的明细数据
                return new Tuple2<>(tup._1, tup._2._2);
            }
        });
        return sessionID2DetailRDD;
    }

    /**
     * 按照使用者提供的条件进行聚合
     *
     * @param session2AggInfoRDD        基础数据
     * @param taskParam                 使用者的条件
     * @param sessionAggStatAccumulator 累加器
     * @return
     */
    private static JavaPairRDD<String, String> filtedSeesionAndAggStat(final JavaPairRDD<String,
            String> session2AggInfoRDD, JSONObject taskParam, final Accumulator<String> sessionAggStatAccumulator) {

        //把所有的使用者参数都取出来并拼接
        final String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        final String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);

        String profassionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keyworkds = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categories = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        //拼接
        StringBuffer sb = new StringBuffer();
        sb.append(startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                .append(endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                .append(profassionals != null ? Constants.PARAM_PROFESSIONALS + "=" + profassionals + "|" : "")
                .append(cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                .append(sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                .append(keyworkds != null ? Constants.PARAM_KEYWORDS + "=" + keyworkds + "|" : "")
                .append(categories != null ? Constants.PARAM_CATEGORY_IDS + "=" + categories + "|" : "");

        String _parameter = sb.toString();
        //把_parameter的最后一个"|"截取

        if (_parameter.endsWith("|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;
        // 根据筛选条件进行过滤
        JavaPairRDD<String, String> filtedSeesionAggInfoRDD = session2AggInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> tup) throws Exception {
                String aggInfo = tup._2;
                /**
                 * 依次按照筛选条件进行过滤
                 */

                //按照年龄
                if (!ValidUtils.between(aggInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                    return false;
                }

                //按照职业
                if (!ValidUtils.in(aggInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) {
                    return false;
                }

                //按照城市
                if (!ValidUtils.in(aggInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
                    return false;
                }

                //按照性别
                if (!ValidUtils.in(aggInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) {
                    return false;
                }

                //按照关键字
                if (!ValidUtils.in(aggInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
                    return false;
                }

                //按照点击品类
                if (!ValidUtils.in(aggInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) {
                    return false;
                }

                /**
                 * 执行到这里,说明该Session通过了用户指定的筛选条件
                 * 接下来要对Session的访问时长和访问步长进行统计
                 */

                // 根据Session对应的时长和步长的时间范围进行累加
                sessionAggStatAccumulator.add(Constants.SESSION_COUNT);

                long visitedLen = Long.valueOf(StringUtils.getFieldFromConcatString(aggInfo,
                        "\\|", Constants.FIELD_VISIT_LENGTH));
                long stepLen = Long.valueOf(StringUtils.getFieldFromConcatString(aggInfo,
                        "\\|", Constants.FIELD_STEP_LENGTH));

                // 计算访问时长范围
                calculateVisitLen(visitedLen);

                //计算访问步长范围
                callculateStepLen(stepLen);
                return true;
            }

            /**
             * 计算访问步长
             * @param stepLen
             */
            private void callculateStepLen(long stepLen) {
                if (stepLen >= 1 && stepLen <= 3) {
                    sessionAggStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                } else if (stepLen >= 4 && stepLen <= 6) {
                    sessionAggStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                } else if (stepLen >= 7 && stepLen <= 9) {
                    sessionAggStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                } else if (stepLen >= 10 && stepLen < 30) {
                    sessionAggStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                } else if (stepLen >= 30 && stepLen < 60) {
                    sessionAggStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                } else if (stepLen >= 60) {
                    sessionAggStatAccumulator.add(Constants.STEP_PERIOD_60);
                }
            }

            /**
             * 计算访问时长
             * @param visitedLen
             */
            private void calculateVisitLen(long visitedLen) {
                if (visitedLen >= 1 && visitedLen <= 3) {
                    sessionAggStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                } else if (visitedLen >= 4 && visitedLen <= 6) {
                    sessionAggStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                } else if (visitedLen >= 7 && visitedLen <= 9) {
                    sessionAggStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                } else if (visitedLen >= 10 && visitedLen < 30) {
                    sessionAggStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                } else if (visitedLen >= 30 && visitedLen < 60) {
                    sessionAggStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                } else if (visitedLen >= 60 && visitedLen < 180) {
                    sessionAggStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                } else if (visitedLen >= 180 && visitedLen < 600) {
                    sessionAggStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                } else if (visitedLen >= 600 && visitedLen < 1800) {
                    sessionAggStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                } else if (visitedLen >= 1800) {
                    sessionAggStatAccumulator.add(Constants.TIME_PERIOD_30m);
                }

            }
        });

        return filtedSeesionAggInfoRDD;
    }

    /**
     * 对行为数据按照session粒度进行聚合
     *
     * @param sqlContext
     * @param sessionID2ActionRDD
     * @return
     */
    private static JavaPairRDD<String, String> aggregateBySession(
            SQLContext sqlContext,
            JavaPairRDD<String, Row> sessionID2ActionRDD) {
        //对行为数据进行分组,开发时能不发生shuffle就不发生shuffle
        final JavaPairRDD<String, Iterable<Row>> sessionId2ActionParirRDD = sessionID2ActionRDD.groupByKey();

        //对每个Seesion分组进行聚合,
        // 将Session中所有的搜索关键字和点击品类都聚合起来
        //生成的最后格式:<userid,partAggInfo(seesionid,searchKeyWorlds,clickCategoeyIds)>
        JavaPairRDD<Long, String> userid2PartAggInfoRDD =
                sessionId2ActionParirRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tup) throws Exception {
                        String sessionID = tup._1;
                        final Iterator<Row> it = tup._2.iterator();

                        //用来存储搜索关键字和点击品类的
                        StringBuilder searchWorldsBuff = new StringBuilder();
                        StringBuilder clickCategoryIdsBuff = new StringBuilder();

                        // 用户存储userid
                        Long userid = null;

                        // 用来存储起始时间和结束时间
                        Date startTime = null;
                        Date endTime = null;

                        //用来存储Seesion的访问步长
                        int stepLen = 0;

                        // 遍历Session中的所有访问行为
                        while (it.hasNext()) {
                            // 提取每个访问行为的搜索关键字和点击品类
                            Row row = it.next();
                            if (userid == null) {
                                userid = row.getLong(1);
                            }

                            //注意.该行为数据是属于搜索行为,searchKeyWorlds是有值的
                            //如果该行为数据是点击品类行为,clickCategoryIds是有值
                            // 但是,任何的行为不可能两个字段都有值

                            String searchKeyWorld = row.getString(5);
                            String clickCategoryId = String.valueOf(row.getLong(6));

                            //追加搜索关键字
                            if (!StringUtil.isEmpty(searchKeyWorld)) {
                                if (!searchWorldsBuff.toString().contains(searchKeyWorld)) {
                                    searchWorldsBuff.append(searchKeyWorld + ",");
                                }
                            }

                            //追加点击品类
                            if (!StringUtil.isEmpty(clickCategoryId)) {
                                if (!clickCategoryIdsBuff.toString().contains(clickCategoryId))
                                    clickCategoryIdsBuff.append(clickCategoryId + ",");
                            }

                            // 计算Session的开始时间和结束时间
                            Date actionTime = DateUtils.parseTime(row.getString(4));

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

                            // 计算访问步长
                            stepLen++;
                        }

                        // 截取搜索关键字和点击品类的最后的","
                        String searchKeyWorlds = StringUtils.trimComma(searchWorldsBuff.toString());
                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuff.toString());

                        //计算访问时长,单位秒
                        long visitTime = (endTime.getTime() - startTime.getTime()) / 1000;

                        //聚合数据,数据以字符串拼接的方式:key=value|key=value
                        StringBuffer sb = new StringBuffer();
                        sb.append(Constants.FIELD_SESSION_ID + "=" + sessionID).append("|")
                                .append(Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeyWorlds).append("|")
                                .append(Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds).append("|")
                                .append(Constants.FIELD_VISIT_LENGTH + "=" + visitTime).append("|")
                                .append(Constants.FIELD_STEP_LENGTH + "=" + stepLen).append("|")
                                .append(Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)).append("|");


                        return new Tuple2<>(userid, sb.toString());
                    }
                });

        //查询所有的用户数据,映射成<userid,Row>
        String sql = "select * from user_info";
        final JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();

        JavaPairRDD<Long, Row> userId2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getLong(0), row);
            }
        });

        //将Session粒度的聚合数据(userid2PartAggInfoRDD)和用户信息进行join
        //最后生成的格式为<userid,<Sessioninfo,userinfo>>
        final JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRdd = userid2PartAggInfoRDD.join(userId2InfoRDD);

        //对join后的数据进行拼接,并返回<sessionid,fullAggInfo>
        JavaPairRDD<String, String> sessionId2FullAggInfoRdd =
                userid2FullInfoRdd.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tup) throws Exception {
                        //获取Session数据
                        String partAggInfo = tup._2._1;
                        //获取影虎信息
                        Row userInfoRow = tup._2._2;
                        //获取SessionID
                        String sessionID = StringUtils
                                .getFieldFromConcatString(partAggInfo,
                                        "\\|", Constants.FIELD_SESSION_ID);
                        //获取用户信息的年龄
                        int age = userInfoRow.getInt(3);
                        //获取用户信息的职业
                        String professional = userInfoRow.getString(4);
                        //获取用户信息的所在城市
                        String city = userInfoRow.getString(5);
                        //获取用户信息的性别
                        String sex = userInfoRow.getString(6);

                        StringBuffer sb = new StringBuffer();

                        sb.append(partAggInfo)
                                .append(Constants.FIELD_AGE + "=" + age).append("|")
                                .append(Constants.FIELD_PROFESSIONAL + "=" + professional).append("|")
                                .append(Constants.FIELD_CITY + "=" + city).append("|")
                                .append(Constants.FIELD_SEX + "=" + sex).append("|");
                        return new Tuple2<>(sessionID, sb.toString());
                    }
                });
        return sessionId2FullAggInfoRdd;
    }

    /**
     * 获取SeesionID对应的行为数据
     *
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionID2ActionRDD(JavaRDD<Row> actionRDD) {

        return actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
            @Override
            public Iterable<Tuple2<String, Row>> call(Iterator<Row> it) throws Exception {
                // 用来封装Session粒度的基础数据
                List<Tuple2<String, Row>> list = new ArrayList<>();
                while (it.hasNext()) {
                    final Row row = it.next();
                    final String sessionID = row.getString(2);
                    list.add(new Tuple2<>(sessionID, row));
                }
                return list;
            }
        });
    }
}

package com.ljy.sessionanalyze.spark.ad;

import com.google.common.base.Optional;
import com.ljy.sessionanalyze.conf.ConfigurationManager;
import com.ljy.sessionanalyze.constant.Constants;
import com.ljy.sessionanalyze.dao.IAdBlacklistDAO;
import com.ljy.sessionanalyze.dao.factory.DAOFactory;
import com.ljy.sessionanalyze.domain.*;
import com.ljy.sessionanalyze.util.DateUtils;
import com.ljy.sessionanalyze.util.SparkUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * 广告点击流量实时统计
 * 实现过程:
 * 1.实时的计算各个batch中的每天各个用户对各广告的点击量
 * 2.实时的将每天各个用户对各广告点击次数写入数据库,采用实时的更新方式
 * 3.使用filter过滤出每天某个对某个广告点击超过100次的用户的黑名单,更新到数据库
 * 4.使用Transform原语操作,对每个batch RDD 进行处理,实现动态加载黑名单生成RDD,
 * 然后进行join操作,过滤掉batch RDD 黑名单用户的广告点击行为
 * 5.使用updateBykey操作实时的计算每天各省个城市各广告的点击量并更新到数据库
 * 6.使用Transform结合SparkSQL统计每天各省份top3热门广告(开窗函数)
 * 7.使用窗口操作,对最近一小时的滑动窗口内的数据,计算出各广告每分钟的点击量,实时更新到数据库
 */
public class AdClickRrealTimeStatSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_AD);
        SparkUtils.setMaster(conf);

        final JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.checkpoint("hdfs://h1:9000/spark/sessionAnalyze/AdClickRrealTimeStatSpark");

        // 构建请求kafka的参数
        Map<String, String> kafkaParam = new HashMap<>();
        kafkaParam.put("metadata.broker.list",
                ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));

        // 构建topic
        String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);

        String[] kafkaTopicSplited = kafkaTopics.split(",");

        // 存储topic
        Set<String> topics = new HashSet<>(Arrays.asList(kafkaTopicSplited));

        //创建流的方式读取kafka数据
        //基于kafka direct
        // 原始数据
        final JavaPairInputDStream<String, String> adRealTimeLogDSTream = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParam,
                topics);
        // 根据动态黑名单运行数据过滤,得到的是过滤掉了在黑名单数据库中已存在的那部分用户后的白名单,这这份白名单中有可能有产生了黑名单(count>=100)
        JavaPairDStream<String, String> filtedAdRealTimeLongDSStream =
                filterdByBliackList(adRealTimeLogDSTream);


        // 动态生成黑名单
        // 1. 计算出每个batch中的每天每个用户对每个广告的点击量,并入数据库
        // 2. 依据上面的结果,对每个batch中按照date,userId,adId聚合的数据都要遍历一遍
        //     查询对应的累加的点击次数,如果超过了100次,就认为是黑名单用户
        //     然后对黑名单用户进行去重并存储到MySQL中
        // 将新产生的黑名单更新到数据库中
        dynamicGernerateBlackList(filtedAdRealTimeLongDSStream);

        // 业务一:计算每天各省各城市各广告的点击量实时统计
        // 生成的数据格式为<yyyyMMdd_province_city_adId,clickCount>
        JavaPairDStream<String, Long> adRealTimeStatDStream =
                calculateRealTimeStat(filtedAdRealTimeLongDSStream);

        //业务二:实时统计每天每个省top3热门广告
        calculateProvinceTop3Ad(adRealTimeStatDStream);

        // 业务三:实时统计每天每个广告在最近一小时的点击趋势
        calculateClickCountByWindow(filtedAdRealTimeLongDSStream);

        jssc.start();
        jssc.awaitTermination();
    }

    /**
     * 实时统计每天每个广告在最近一小时的点击趋势
     *
     * @param filtedAdRealTimeLongDSStream
     */
    private static void calculateClickCountByWindow(JavaPairDStream<String, String> filtedAdRealTimeLongDSStream) {
        // 将数据映射为<yyyyMMddhhmm_adId,1L>
        JavaPairDStream<String, Long> pairDStream = filtedAdRealTimeLongDSStream.mapToPair(tup -> {
            // 原始的数据格式为<"offset","timestamp province,city userid adId">
            String[] splited = tup._2.split("\\s");

            String timeMinute = DateUtils.formatTimeMinute(new Date(Long.parseLong(splited[0])));

            long adId = Long.parseLong(splited[4]);

            return new Tuple2<>(timeMinute + "_" + adId, 1L);
        });

        // 每次数来一个信息的batch,都要获取最近一小时的所有batch
        // 然后根据key进行聚合,统计出一小时内各分钟个广告的点击量
        JavaPairDStream<String,Long>aggRDD = pairDStream.reduceByKeyAndWindow((Function2<Long, Long, Long>) (v1, v2) -> v1+v2,Durations.minutes(60),Durations.seconds(10));

        //到这里,aggRDD就是每次都可以拿到最近一小时内各分钟各广告的点击量
        //结果数据存储MySQL
        aggRDD.foreachRDD((Function<JavaPairRDD<String, Long>, Void>) rdd -> {
            rdd.foreachPartition(it->{
                List<AdClickTrend> adClickTrends = new ArrayList<>();
                while (it.hasNext()) {
                    final Tuple2<String, Long> tup = it.next();
                    final String[] splited = tup._1.split("_");
                    String dateMinute = splited[0];
                    final String dagte = DateUtils.formatDate(DateUtils.parseDateKey(dateMinute.substring(0, 8)));
                    String hour = dagte.substring(8,10);
                    String min = dateMinute.substring(10);
                    long adId = Long.parseLong(splited[1]);
                    long clickCount = tup._2;
                    AdClickTrend adClickTrend = new AdClickTrend();
                    adClickTrend.setAdid(adId);
                    adClickTrend.setClickCount(clickCount);
                    adClickTrend.setDate(dagte);
                    adClickTrend.setMinute(min);
                    adClickTrend.setHour(hour);
                    adClickTrends.add(adClickTrend);
                }

                DAOFactory.getAdClickTrendDAO().updateBatch(adClickTrends);
            });
            return null;
        });
    }

    /**
     * 实时统计每天每个省top3热门广告
     *
     * @param adRealTimeStatDStream
     */
    private static void calculateProvinceTop3Ad(JavaPairDStream<String, Long> adRealTimeStatDStream) {
        // adRealTimeStatDStream(格式为yyyyMMdd_province_city_adId,clickCount>)数据
        //
        JavaDStream<Row> rowJavaDStream = adRealTimeStatDStream.transform(new Function<JavaPairRDD<String, Long>, JavaRDD<Row>>() {
            @Override
            public JavaRDD<Row> call(JavaPairRDD<String, Long> rdd) throws Exception {
                JavaPairRDD<String, Long> mappedRDD = rdd.mapToPair(new PairFunction<Tuple2<String, Long>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, Long> tup) throws Exception {
                        final String[] keySplited = tup._1.split("_");
                        String date = keySplited[0];
                        String province = keySplited[1];
                        long addId = Long.parseLong(keySplited[3]);

                        long clickCount = tup._2;

                        String key = date + "_" + province + "_" + addId;
                        return new Tuple2<>(key, clickCount);
                    }
                });

                //将mappedRDD的value以省份进行聚合
                final JavaPairRDD<String, Long> dailyAdClickCountByProvince = mappedRDD.reduceByKey((v1, v2) -> v1 + v2);

                //将聚合后的结果转化为DataFrame
                //生成一张临时表
                //通过开窗函数获取到各省top3热门广告
                JavaRDD<Row> rowRDD = dailyAdClickCountByProvince.map(tup -> {
                    String[] keysSplited = tup._1.split("_");
                    String date = keysSplited[0];
                    String province = keysSplited[1];
                    long adId = Long.parseLong(keysSplited[2]);
                    //点击量
                    long count = tup._2;

                    return RowFactory.create(date, province, adId, count);
                });

                StructType schema = DataTypes.createStructType(
                        Arrays.asList(
                                DataTypes.createStructField("date", DataTypes.StringType, true),
                                DataTypes.createStructField("province", DataTypes.StringType, true),
                                DataTypes.createStructField("ad_id", DataTypes.LongType, true),
                                DataTypes.createStructField("click_count", DataTypes.LongType, true)
                        )
                );
                // 创建sqlContext对象,此时还不能创建SQLContext对象,因为该版本SparkSQL不支持开窗函数
                HiveContext sqlContext = new HiveContext(rdd.context());

                //映射
                DataFrame daliyAdClickCountByProvinceDF = sqlContext.createDataFrame(rowRDD, schema);
                daliyAdClickCountByProvinceDF.registerTempTable("temp_daily_ad_click_count_prov");

                //使用开窗函数统计处出结果
                String sql =
                        "select " +
                                "date," +
                                "province," +
                                "ad_id," +
                                "click_count " +
                                "from (" +
                                "select date,province,ad_id,click_count,ROW_NUMBER() OVER(PARTITION BY province ORDER BY click_count DESC) rank " +
                                " from temp_daily_ad_click_count_prov) t " +
                                "where rank<=3";
                final DataFrame provinceTop3DF = sqlContext.sql(sql);
                return provinceTop3DF.toJavaRDD();
            }
        });
        // rowJavaDStream:每个batch 都是刷新出来的各省最热门的top3广告
        // 将结果存储到MySQL

        rowJavaDStream.foreachRDD(rdd -> {
            rdd.foreachPartition(it -> {
                List<AdProvinceTop3> list = new ArrayList<>();
                while (it.hasNext()) {
                    final Row row = it.next();
                    String date = row.getString(0);
                    String province = row.getString(1);
                    long adId = row.getLong(2);
                    long clickCount = row.getLong(3);
                    AdProvinceTop3 adProvinceTop3 = new AdProvinceTop3();
                    adProvinceTop3.setDate(date);
                    adProvinceTop3.setAdid(adId);
                    adProvinceTop3.setClickCount(clickCount);
                    adProvinceTop3.setProvince(province);
                    list.add(adProvinceTop3);
                }

                DAOFactory.getAdProvinceTop3DAO().updateBatch(list);
            });
            return null;
        });
    }

    /**
     * 计算每天各省各城市各广告的点击量实时统计
     *
     * @param filtedAdRealTimeLongDSStream
     * @return
     */
    private static JavaPairDStream<String, Long> calculateRealTimeStat(JavaPairDStream<String, String> filtedAdRealTimeLongDSStream) {
        /**
         * 计算该业务,会实时的把结果更新到数据库中
         * J2EE平台就会把结果实时的以各种效果展示出来
         * J2EE平台会每隔几分钟从数据库中获取一次数据
         */

        // 对原始数据进行Map,把结果数据映射为<date_province_city_adId,1L>
        JavaPairDStream<String, Long> mappedDStream = filtedAdRealTimeLongDSStream.mapToPair(tup -> {
            // 把原始数据切分并获取个字段
            String[] logSplited = tup._2.split("\\s");
            String timestamp = logSplited[0];
            Date date = new Date(Long.valueOf(timestamp));

            String dateKey = DateUtils.formatDateKey(date);

            String province = logSplited[1];
            String city = logSplited[2];
            String adId = logSplited[4];
            String key = dateKey + "_" + province + "_" + city + "_" + adId;
            return new Tuple2<>(key, 1L);
        });

        // 集合,按批次累加
        // 在这个DStream中相当于每天各省各城市各广告的点击次数
        JavaPairDStream<String, Long> aggDStream = mappedDStream.updateStateByKey((Function2<List<Long>, Optional<Long>, Optional<Long>>) (values, optionnal) -> {
            //optionnal存储的是历史批次结果
            // 首先optional判断,之前的这个key值是否有对应的值
            long clickCount = 0L;
            //如果之前存在值,就以之前的状态值为起点进行累加
            if (optionnal.isPresent()) {
                clickCount = optionnal.get();
            }

            //values代表当前批次中 每个key对应的所有值
            // 比如当前批次batch RDD中点击量为4,values(1,1,1,1)
            for (Long value : values) {
                clickCount += value;
            }


            return Optional.of(clickCount);
        });

        // 将结果直接化
        aggDStream.foreachRDD((Function<JavaPairRDD<String, Long>, Void>) rdd -> {
            rdd.foreachPartition((VoidFunction<Iterator<Tuple2<String, Long>>>) it -> {
                List<AdStat> adStats = new ArrayList<>();
                while (it.hasNext()) {
                    final Tuple2<String, Long> tup = it.next();
                    final String[] keySplited = tup._1.split("_");
                    String date = keySplited[0];
                    String province = keySplited[1];
                    String city = keySplited[2];
                    long adId = Long.parseLong(keySplited[3]);

                    long clickCount = tup._2;

                    AdStat adStat = new AdStat();
                    adStat.setAdid(adId);
                    adStat.setCity(city);
                    adStat.setDate(date);
                    adStat.setProvince(province);
                    adStat.setClickCount(clickCount);
                    adStats.add(adStat);
                }

                DAOFactory.getAdStatDAO().updateBatch(adStats);

            });
            return null;
        });
        return aggDStream;
    }

    /**
     * 动态生成黑名单
     *
     * @param filtedAdRealTimeLongDSStream
     */
    private static void dynamicGernerateBlackList(JavaPairDStream<String, String> filtedAdRealTimeLongDSStream) {
        // 通过原始实时日志处理,将日志格式处理成:<yyyyMMdd_userId_adId,1L>
        JavaPairDStream<String, Long> dailyUserAdClickDStream = filtedAdRealTimeLongDSStream.mapToPair((PairFunction<Tuple2<String, String>, String, Long>) tup -> {
            // 从tup中获取每一条原始实时日志
            String log = tup._2;
            final String[] logSplited = log.split("\\s");
            final String timestemp = logSplited[0];
            Date date = new Date(Long.parseLong(timestemp));

            final String dateKey = DateUtils.formatDate(date);
            long userId = Long.valueOf(logSplited[3]);
            long adId = Long.valueOf(logSplited[4]);

            String key = dateKey + "_" + userId + "_" + adId;
            return new Tuple2<>(key, 1L);
        });

        // 针对处理后的日志格式进行聚合操作,这样就可以得到每个batch中每天每个用户对每个广告的点击量
        final JavaPairDStream<String, Long> dilyUserAdClickCountDStream = dailyUserAdClickDStream.reduceByKey((v1, v2) -> v1 + v2);

        // 到这里,获取到<yyyyMMdd_userId_addId,count>

        // 把每天各个用户的点击量存储到数据库中
        dilyUserAdClickCountDStream.foreachRDD((Function<JavaPairRDD<String, Long>, Void>) rdd -> {
            rdd.foreachPartition((VoidFunction<Iterator<Tuple2<String, Long>>>) it -> {
                // 对每个分区的数据获取一次数据库连接
                // 每次都是从连接池中获取,而不是每次都新建
                List<AdUserClickCount> adUserClickCounts = new ArrayList<>();
                while (it.hasNext()) {
                    final Tuple2<String, Long> next = it.next();
                    final String[] keysSplited = next._1.split("_");
                    // 获取String类型的日志
                    String date = DateUtils.formatDate(DateUtils.parseDateKey(keysSplited[0]));
                    long userId = Long.valueOf(keysSplited[1]);
                    long adId = Long.valueOf(keysSplited[2]);

                    long clickCount = next._2;

                    AdUserClickCount adUserClickCount = new AdUserClickCount();
                    adUserClickCount.setAdid(adId);
                    adUserClickCount.setClickCount(clickCount);
                    adUserClickCount.setUserid(userId);
                    adUserClickCount.setDate(date);

                    adUserClickCounts.add(adUserClickCount);
                }
                DAOFactory.getAdUserClickCountDAO().updateBatch(adUserClickCounts);
            });
            return null;
        });

        // 到这里,在数据中,已经有了累计的每天各个用户对各个广告的点击量
        // 接下来遍历每个batch中所有的记录
        // 对每天记录都去查询一下这一天每个用户对每个广告的累计点击量
        // 判断,如果某个用户某天对某个广告的点击量大于等于100次,
        // 就断定这个用户是一个机器人,把该用户更新到黑名单用户表中
        JavaPairDStream<String, Long> blackListDStream = dilyUserAdClickCountDStream.filter((Function<Tuple2<String, Long>, Boolean>) tup -> {
            // 切分key
            final String[] keySplited = tup._1.split("_");
            String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
            long userId = Long.valueOf(keySplited[1]);
            long adId = Long.valueOf(keySplited[2]);

            //获取点击量
            final int count = DAOFactory.getAdUserClickCountDAO().findClickCountByMultiKey(date, userId, adId);
            //判断是否大于等于100
            if (count >= 100) {
                //机器人操作
                return true;
            }
            //非机器人操作
            return false;
        });


        // blackListDStream,过滤出来的已经在每天某个广告的点击量
        // 超过100次的用户
        // 遍历这个DStream的每个RDD,然后将黑名单用户更新到数据库
        // 注意:blackListDStream 中可能有重复的userId,需要去重

        // 首先获取到一个RDD
        JavaDStream<Long> blackListUserIdDStream =
                blackListDStream.map((Function<Tuple2<String, Long>, Long>) tup -> {
                    String[] keysSplited = tup._1.split("_");
                    return Long.valueOf(keysSplited[1]);
                });

        // 根据UserId进行去重
        final JavaDStream<Long> distinctBlackIdDStream = blackListUserIdDStream.transform((Function<JavaRDD<Long>, JavaRDD<Long>>) JavaRDD::distinct);

        // 将黑名单用户进行持久化
        distinctBlackIdDStream.foreachRDD((Function<JavaRDD<Long>, Void>) longJavaRDD -> {
            longJavaRDD.foreachPartition((VoidFunction<Iterator<Long>>) it -> {
                List<AdBlacklist> adBlacklists = new ArrayList<>();
                while (it.hasNext()) {
                    final Long userId = it.next();
                    AdBlacklist adBlacklist = new AdBlacklist();
                    adBlacklist.setUserid(userId);
                    adBlacklists.add(adBlacklist);
                }

                DAOFactory.getAdBlacklistDAO().insertBatch(adBlacklists);
            });
            return null;
        });

    }

    /**
     * 实现过滤黑名单机制
     *
     * @param adRealTimeLogDSTream
     * @return
     */
    private static JavaPairDStream<String, String> filterdByBliackList(JavaPairInputDStream<String, String> adRealTimeLogDSTream) {
        // 接收到原始的用户点击行为日志后,根据数据库黑名单进行实时过滤
        // 使用Transform将DStream中的每个batch RDD进行处理,转化为任意其他的RDD
        // 返回的格式为:<userId,tup>
        JavaPairDStream<String, String> filtedAdRealTimeLogDStream =
                adRealTimeLogDSTream.transformToPair((Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>) rdd -> {
                    //首先从数据库中查询黑名单,并转化成RDD
                    final IAdBlacklistDAO dao = DAOFactory.getAdBlacklistDAO();
                    final List<AdBlacklist> adBlacklists = dao.findAll();

                    // 封装黑名单用户,格式为<userId,true>
                    List<Tuple2<Long, Boolean>> tuples = new ArrayList<>();
                    adBlacklists.forEach(item -> tuples.add(new Tuple2<>(item.getUserid(), true)));

                    JavaSparkContext sc = new JavaSparkContext(rdd.context());

                    final JavaPairRDD<Long, Boolean> blackListRDD = sc.parallelizePairs(tuples);
                    // 将原始数据RDD映射为<userId,Tuple2<kafka-key,kafka-value>>
                    JavaPairRDD<Long, Tuple2<String, String>> mappedRDD = rdd.mapToPair((PairFunction<Tuple2<String, String>, Long, Tuple2<String, String>>) tup -> {
                        // 获取用户的点击行为
                        String log = tup._2;
                        // 原始数据的格式为<offset,(timestamp province city userId adId)>
                        final String[] logSplited = log.split("\\s");
                        long userId = Long.parseLong(logSplited[3]);

                        return new Tuple2<>(userId, tup);
                    });

                    // 将原始日志数据与黑名单RDD进行join,此时需要用leftOuterJoin
                    // 如果原始日志userId没有在对应的黑名单,一定join不到的
                    final JavaPairRDD<Long, Tuple2<Tuple2<String, String>, com.google.common.base.Optional<Boolean>>> joinedRDD = mappedRDD.leftOuterJoin(blackListRDD);

                    // 过滤黑名单
                    JavaPairRDD<Long, Tuple2<Tuple2<String, String>, com.google.common.base.Optional<Boolean>>> filtedRDD = joinedRDD.filter((Function<Tuple2<Long, Tuple2<Tuple2<String, String>, com.google.common.base.Optional<Boolean>>>, Boolean>) tup -> {
                        //获取join过来的黑名单的userId对应的bool值
                        final com.google.common.base.Optional<Boolean> optional = tup._2._2;
                        // 如果这个值存在,说明原始数据中的userId join到了某个黑名单用户
                        if (optional.isPresent() && optional.get()) {
                            return false;
                        }
                        return true;
                    });

                    // 返回根据黑名单过滤后的数据
                    return filtedRDD.mapToPair(tup -> tup._2._1);
                });

        return filtedAdRealTimeLogDStream;
    }
}

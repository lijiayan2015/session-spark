package com.ljy.sessionanalyze.spark.product;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ljy.sessionanalyze.conf.ConfigurationManager;
import com.ljy.sessionanalyze.constant.Constants;
import com.ljy.sessionanalyze.dao.factory.DAOFactory;
import com.ljy.sessionanalyze.domain.AreaTop3Product;
import com.ljy.sessionanalyze.domain.Task;
import com.ljy.sessionanalyze.spark.page.ConcatLongStringUDF;
import com.ljy.sessionanalyze.spark.page.GetJsonObjectUDF;
import com.ljy.sessionanalyze.spark.page.GroupConcatDistinctUDAF;
import com.ljy.sessionanalyze.util.ParamUtils;
import com.ljy.sessionanalyze.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 按区域统计top3热门商品
 */
public class AreaTop3ProductSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName(Constants.SPARK_APP_NAME_PRODUCT);
        SparkUtils.setMaster(conf);
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

        //注册自定义函数
        sqlContext.udf().register("group_concat_distinct", new GroupConcatDistinctUDAF());
        sqlContext.udf().register("concat_long_string", new ConcatLongStringUDF(), DataTypes.StringType);
        sqlContext.udf().register("get_json_object", new GetJsonObjectUDF(), DataTypes.StringType);

        //获取数据
        SparkUtils.mockData(sc, sqlContext);

        //获取taskId
        long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PRODUCT);

        final Task task = DAOFactory.getTaskDAO().findById(taskId);

        if (task == null) throw new RuntimeException("task任务信息获取失败");

        JSONObject taskParam = JSON.parseObject(task.getTaskParam());

        //获取使用者指定的开始时间和结束时间
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        //查询用户指定日期范围内的点击行为数据<city_id,点击行为>
        // 技术点:hive数据源的使用
        JavaPairRDD<Long, Row> cityId2ClickActionRDD =
                getCityId2ClickActionRDD(sqlContext, startDate, endDate);

        // 从MySQL表(city_info)中查询城市信息,返回的格式为<cityID,cityInfo>
        // 技术点2,异构数据MySQL的使用
        JavaPairRDD<Long, Row> cityId2CityInfoRDD = getCityId2CityInfoRDD(sqlContext);

        // 生成点击商品基础信息临时表
        // 技术点3:将RDD转化为DataFrame,并注册临时表
        generateTtempClickProductBasicTable(sqlContext, cityId2ClickActionRDD, cityId2CityInfoRDD);

        //生成各区域商品点击次数
        // area product_id clickCount city_infos
        generateTempAreaPrudctCountTable(sqlContext);

        // 生成包含完整商品信息的各区域各商品点击次数临时表
        // 技术点4:内置if函数的使用
        generateTempAreaFullProductClickCountTable(sqlContext);

        // 使用开窗函数获取各个区域点击次数top3热门商品
        // 技术点5:开窗函数
        JavaRDD<Row> areaTop3ProductRDD = getAreaTop3ProductRDD(sqlContext);

        // 就这个业务需求而言,最终的结果是很少的
        // 一共就几个区域,每个区域只去top3的商品,最终的数据也就几十个
        // 所以可以直接将数据collect到driver端,在用批量插入的方式,一次性插入插入到数据库表
        List<Row> rows = areaTop3ProductRDD.collect();

        // save
        persistAreaProduct(taskId, rows);
        sc.stop();
    }

    /**
     * 将结果数据存储到MySQL
     *
     * @param taskId
     * @param rows
     */
    private static void persistAreaProduct(long taskId, List<Row> rows) {
        List<AreaTop3Product> list = new ArrayList<>();
        rows.forEach(row -> {
            AreaTop3Product product = new AreaTop3Product();
            product.setTaskid(taskId);
            product.setArea(row.getString(0));
            product.setAreaLevel(row.getString(1));
            product.setProductid(row.getLong(2));
            product.setClickCount(row.getLong(3));
            product.setCityInfos(row.getString(4));
            product.setProductName(row.getString(5));
            product.setProductStatus(row.getString(6));
            list.add(product);
        });
        DAOFactory.getAreaTop3ProductDAO().insertBatch(list);
    }

    /**
     * 获取区域top3热门商品
     *
     * @param sqlContext
     * @return
     */
    private static JavaRDD<Row> getAreaTop3ProductRDD(SQLContext sqlContext) {
        /**
         * 使用开窗函数进行子查询
         * 按照area进行分组,给每个分组内的数据按照点击次数进行降序排序,并打一个行标
         * 然后在外层查询中,过滤出各个组内行标排名前3的行数
         */

        /**
         * 按照区域进行分级:
         * 华北,华东,华南,华中,西北,西南,东北
         * A级:华北,华东
         * B级:华南,华中
         * C级:西北,西南
         * D级:东北
         */

        /*String sql = "select " +
                "area," +
                "case " +
                "when area='华北' or area='华东' then 'A 级' " +
                "when area='华南' or area='华中' then 'B 级' " +
                "when area='西北' or area='西南' then 'C 级' " +
                "else 'D 级' " +
                "end area_level," +
                "product_id," +
                "click_count," +
                "city_infos," +
                "product_name," +
                "product_status " +
                "from(" +
                "select area," +
                "product_id," +
                "click_count," +
                "city_infos," +
                "product_name," +
                "product_status," +
                "ROW_NUMBER() OVER(PARTITION BY area ORDER BY click_count DESC) rank " +
                "from temp_area_fullprod_click_count" +
                ") t" + " where rank <= 3";*/
        String sql =
                "select " +
                        "area," +
                        "case " +
                        "when area='华北' or area='华东' then 'A级' " +
                        "when area='华南' or area='华中' then 'B级' " +
                        "when area='西北' or area='西南' then 'C级' " +
                        "else 'D级' " +
                        "end area_level," +
                        "product_id," +
                        "click_count," +
                        "city_infos," +
                        "product_name," +
                        "product_status " +
                        "from(" +
                        "select " +
                        "area," +
                        "product_id," +
                        "click_count," +
                        "city_infos," +
                        "product_name," +
                        "product_status," +
                        "ROW_NUMBER() OVER (PARTITION BY area ORDER BY click_count DESC) rank " +
                        "from temp_area_fullprod_click_count " +
                        ") t " +
                        "where rank <= 3";

        DataFrame df = sqlContext.sql(sql);

        return df.toJavaRDD();
    }

    /**
     * 生成包含完整商品信息的各区域各商品点击次数临时表
     *
     * @param sqlContext
     */
    private static void generateTempAreaFullProductClickCountTable(SQLContext sqlContext) {
        /**
         * 将之前得到的各区域商品点击次数表(temp_area_product_click_count)的product_id字段去
         * 关联商品信息表(product_info)的product_id
         * 其中,product_status需要特殊处理:0,1分别代表了自营和第三方商品,放在了一个json里面
         * GetJsonObjectUDF()函数是从json串中获取指定字段的值
         * if()函数判断,如果product_status为0,就是自营商品,如果为1,就是第三方商品
         * 此时该表的字段由area,product_id,click_count,city_info,product_name,product_status
         */
        String sql = "select " +
                "tapcc.area," +
                "tapcc.product_id," +
                "tapcc.click_count," +
                "tapcc.city_infos," +
                "pi.product_name," +
                "if(get_json_object(pi.extend_info,'product_status')='0'," +
                "'Self','Third Party') product_status " +
                "from temp_area_product_click_count tapcc " +
                "join product_info pi " +
                "on tapcc.product_id = pi.product_id";
        final DataFrame df = sqlContext.sql(sql);
        df.registerTempTable("temp_area_fullprod_click_count");
    }


    /**
     * 生成各区域商品点击次数
     *
     * @param sqlContext
     */
    private static void generateTempAreaPrudctCountTable(SQLContext sqlContext) {

        // 按照area和ProductID两个字段进行分组
        // 计算出各区域商品的点击次数
        // 可以获取到每个area下的每个productID的城市信息,并拼接为字符串
        String sql = "select area,product_id,count(*) click_count,group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos from temp_click_product_basic group by area,product_id";

        final DataFrame df = sqlContext.sql(sql);
        // area product_id clickCount city_infos
        df.registerTempTable("temp_area_product_click_count");
    }

    /**
     * 生成点击商品基础信息临时表
     *
     * @param sqlContext
     * @param cityId2ClickActionRDD
     * @param cityId2CityInfoRDD
     */
    private static void generateTtempClickProductBasicTable(
            SQLContext sqlContext,
            JavaPairRDD<Long, Row> cityId2ClickActionRDD,
            JavaPairRDD<Long, Row> cityId2CityInfoRDD) {
        //将点击行为和城市信息进行关联,join
        final JavaPairRDD<Long, Tuple2<Row, Row>> joinedRDD =
                cityId2ClickActionRDD.join(cityId2CityInfoRDD);
        // 将上面join的结果数据转化成一个JavaRDD(Row),
        // 是因为转化成Row后才能将RDD转化为DataFrame
        JavaRDD<Row> mappedRDD = joinedRDD.map(tup -> {
            long cityId = tup._1;
            Row clickAction = tup._2._1;
            Row cityInfo = tup._2._2;

            long productId = clickAction.getLong(1);
            String cityName = cityInfo.getString(1);
            String area = cityInfo.getString(2);
            return RowFactory.create(cityId, cityName, area, productId);
        });
        // 构建Schema信息
        List<StructField> list = new ArrayList<>();
        list.add(DataTypes.createStructField("city_id", DataTypes.LongType, true));
        list.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
        list.add(DataTypes.createStructField("area", DataTypes.StringType, true));
        list.add(DataTypes.createStructField("product_id", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(list);
        // 生成dataFrame
        final DataFrame df = sqlContext.createDataFrame(mappedRDD, schema);
        //生成临时表字段:city_id  city_name area product_id
        df.registerTempTable("temp_click_product_basic");
    }

    private static JavaPairRDD<Long, Row> getCityId2CityInfoRDD(SQLContext sqlContext) {
        String url;
        String user;
        String passwd;
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            passwd = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
        } else {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
            passwd = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
        }
        // 用来存储请求MySQL的连接配置信息
        Map<String, String> options = new HashMap<>();
        options.put("url", url);
        options.put("dbtable", "city_info");
        options.put("user", user);
        options.put("password", passwd);

        //获取MySQL中city_info表中的数据
        final DataFrame cityInfoDF = sqlContext.read().format("jdbc").options(options).load();

        // 返回RDD
        JavaRDD<Row> cityInfoRDD = cityInfoDF.toJavaRDD();
        JavaPairRDD<Long, Row> cityId2CityInfoRDD = cityInfoRDD.mapToPair(row -> {
            long cityId = Long.valueOf(row.getInt(0));
            return new Tuple2<>(cityId, row);
        });

        return cityId2CityInfoRDD;
    }

    /**
     * 查询指定日期范围内的点击行为数据
     *
     * @param sqlContext sqlContext
     * @param startDate  开始日期
     * @param endDate    结束日期
     * @return <city_id,actionRow>
     */
    private static JavaPairRDD<Long, Row> getCityId2ClickActionRDD(SQLContext sqlContext, String startDate, String endDate) {

        // 从User_visit_action技术标中查询用户的行为数据
        // 第一个限定:click_product_id限定为不为空的访问行为,这个字段的值就代表点击行为
        // 第二个限定:在使用者指定的日期范围内的数据
        String sql = "select " +
                "city_id," +
                "click_product_id product_id " +
                "from user_visit_action " +
                "where click_product_id is not null " +
                "and session_date>='" + startDate + "' and session_date<='" + endDate + "'";

        final DataFrame clickActionDF = sqlContext.sql(sql);

        //把生成的dataFrame转化我RDD
        final JavaRDD<Row> clickActionRDD = clickActionDF.toJavaRDD();
        return clickActionRDD.mapToPair(row -> {
            final long city_id = row.getLong(0);
            return new Tuple2<>(city_id, row);
        });
    }

}

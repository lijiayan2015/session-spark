package com.ljy.mianshi.tohbase

import java.util.Calendar

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object ToHbase {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("ToHbase")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    val seq = Seq(
      "site1,user1,2016-11-20 02:12:22",
      "site1,user2,2016-11-28 04:41:23",
      "site1,user3,2016-11-20 11:46:32",
      "site1,user1,2016-11-20 11:02:11",
      "site2,user1,2016-11-20 15:25:22",
      "site3,user5,2016-11-29 08:32:54",
      "site3,user7,2016-11-22 08:08:26",
      "site4,user7,2016-11-20 10:35:37",
      "site4,user7,2016-11-24 11:54:48"

    )
    val sourceRDD: RDD[String] = sc.parallelize(seq)

    val pvRDD: RDD[PVAndUV] = sourceRDD.map(item => {
      val splited = item.split(",")
      val site = splited(0)
      val user = splited(1)
      val date = getDateOrHour(splited(2), DateType.DATE)
      val hour = getDateOrHour(splited(2), DateType.HOUR)
      PVAndUV(site, user, date, hour)
    })
    import sQLContext.implicits._
    val df: DataFrame = pvRDD.toDF()
//

    df.registerTempTable("pv")
    sQLContext.sql("select site,date,hour,count(user) as pv,count(distinct user) as uv from pv group by site,date,hour").show()
    sc.stop()

  }

  def getDateOrHour(dateString: String, datetype: String): String = {
    val calendar = Calendar.getInstance()
    val fdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val fdf1 = FastDateFormat.getInstance("yyyy-MM-dd")
    val date = fdf.parse(dateString)
    calendar.setTime(date)
    if (datetype == DateType.DATE) return fdf1.format(date)
    if (datetype == DateType.HOUR) calendar.get(Calendar.HOUR_OF_DAY) + ""
    else null
  }

  case class PVAndUV(site: String, user: String, date: String, hour: String)

}

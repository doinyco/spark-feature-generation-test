import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.collection.mutable

/**
 * getItem
 */
object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[16]").getOrCreate()

    val events = getEvents(spark, fromFile = true)
    val sessions = getSessions(spark, fromFile = true)
    val dayRanges = List(7, 28, 61, 92, 182, 365)
    val featureNames: List[String] = List(
      "PRE_%_sessions",
      "PRE_%_PAGE_COUNT",
      "PRE_%_VIEW_COUNT",
      "PRE_%_ROW_COUNT"
    )

//    val result: DataFrame = calculateFeatures1(dayRanges, featureNames, events, sessions) // 36
//    val result: DataFrame = calculateFeatures1_select(dayRanges, featureNames, events, sessions) // 34

//        val result: DataFrame = calculateFeatures2(dayRanges, featureNames, events, sessions) // 34
//    val result: DataFrame = calculateFeatures2_2(dayRanges, featureNames, events, sessions)
//      val result: DataFrame = calculateFeatures2_3(dayRanges, featureNames, events, sessions)
    val result: DataFrame = calculateFeatures3(dayRanges, featureNames, events, sessions) // 9
//    val result: DataFrame = calculateFeatures4(dayRanges, featureNames, events, sessions) // 9

    writeResults(result, "results.parquet")
//    writeResults(result, null)
  }

  private def writeResults(results: DataFrame, filename: String): Unit = {
    if (filename != null) {
      results.write.parquet("results.parquet")
    } else {
      results.explain()
      results.printSchema()
      results.show(truncate = false)
    }
  }

  private def getEvents(spark: SparkSession, fromFile: Boolean): DataFrame = {
    if (fromFile) {
      spark.read.json("events.json")
    }

    import spark.implicits._

    Seq(
      (8, 10),
      (9, 20),
      (10, 30),
      (11, 40),
    ).toDF("cid", "eventdate")
  }

  private def getSessions(spark: SparkSession, fromFile: Boolean): DataFrame = {
    if (fromFile) {
      spark.read.json("sessions.json")
    }

    import spark.implicits._

    Seq(
      (8, 10, Seq(1, 2, 3), 5, 6),
      (8, 20, Seq(2, 3, 4, 2), 15, 16),
      (8, 30, Seq(1, 2, 3, 4, 5, 6), 10, 10),
      (9, 10, Seq(1, 2), 5, 5),
      (10, 11, Seq(1), 1, 1),
      (10, 12, Seq(2), 1, 1),
      (10, 13, Seq(3), 1, 1),
      (10, 14, Seq(4), 1, 1),
      (10, 15, Seq(5), 1, 1),
      (1, 10, Seq(1), 1, 1),
    ).toDF("cid", "sessiondate", "sessionids", "pagecount", "viewcount")
  }

  private def calculateFeatures1(dayRanges: List[Int], featureNames: List[String], events: DataFrame, sessions: DataFrame): DataFrame = {
    val lambdas = Map(
      "PRE_%_sessions" -> ((acc: Column, x: Column) => array_union(acc("PRE_%_sessions"), x("sessionids"))),
      "PRE_%_PAGE_COUNT" -> ((acc: Column, x: Column) => acc("PRE_%_PAGE_COUNT") + x("pagecount")),
      "PRE_%_VIEW_COUNT" -> ((acc: Column, x: Column) => acc("PRE_%_VIEW_COUNT") + x("viewcount")),
      "PRE_%_ROW_COUNT" -> ((acc: Column, x: Column) => acc("PRE_%_ROW_COUNT") + 1.0)
    )

    val groupedsessions = sessions
      .groupBy(col("cid"))
      .agg(collect_list(struct(
        col("sessiondate"),
        col("sessionids"),
        col("pagecount"),
        col("viewcount"))).as("combined"))

    events
      .join(groupedsessions, Seq("cid"), "left_outer")
      .withColumns(dayRanges.map(r => r.toString -> aggregate(
        filter(
          when(col("combined").isNull, array()).otherwise(col("combined")),
          x => x("sessiondate") - col("eventdate") <= r),
        struct(featureNames.map(f => (if (f == "PRE_%_sessions") typedLit(List[Int]()) else lit(0.0)).as(f)): _*),
        (acc: Column, x: Column) => struct(featureNames.map(f => lambdas(f)(acc, x).as(f)): _*),
        acc => struct(featureNames.map(f => (if (f == "PRE_%_sessions") size(acc(f)).cast(DoubleType) else acc(f)).as(f.replace("%", r.toString))): _*))
      ).toMap)
      .select(col("cid") +: col("eventdate") +: dayRanges.map(r => col(r.toString + ".*")): _*)
  }

  private def calculateFeatures1_select(dayRanges: List[Int], featureNames: List[String], events: DataFrame, sessions: DataFrame): DataFrame = {
    val lambdas = Map(
      "PRE_%_sessions" -> ((acc: Column, x: Column) => array_union(acc("PRE_%_sessions"), x("sessionids"))),
      "PRE_%_PAGE_COUNT" -> ((acc: Column, x: Column) => acc("PRE_%_PAGE_COUNT") + x("pagecount")),
      "PRE_%_VIEW_COUNT" -> ((acc: Column, x: Column) => acc("PRE_%_VIEW_COUNT") + x("viewcount")),
      "PRE_%_ROW_COUNT" -> ((acc: Column, x: Column) => acc("PRE_%_ROW_COUNT") + 1.0)
    )

    val groupedsessions = sessions
      .groupBy(col("cid"))
      .agg(collect_list(struct(
        col("sessiondate"),
        col("sessionids"),
        col("pagecount"),
        col("viewcount"))).as("combined"))

    events
      .join(groupedsessions, Seq("cid"), "left_outer")
      .select(
        col("cid") +:
          col("eventdate") +:
          dayRanges.map(r => aggregate(
            filter(
              when(col("combined").isNull, array()).otherwise(col("combined")),
              x => x("sessiondate") - col("eventdate") <= r
            ),
            struct(featureNames.map(f => (if (f == "PRE_%_sessions") typedLit(List[Int]()) else lit(0.0)).as(f)): _*),
            (acc: Column, x: Column) => struct(featureNames.map(f => lambdas(f)(acc, x).as(f)): _*),
            acc => struct(
              featureNames.map(f => (
                if (f.equals("PRE_%_sessions"))
                  size(acc(f)).cast(DoubleType)
                else
                  acc(f)
                ).as(f.replace("%", r.toString))): _*
            )
          ).as(r.toString)): _*
      )
      .select(col("cid") +: col("eventdate") +: dayRanges.map(r => col(r.toString + ".*")): _*)
  }

  private def calculateFeatures2(dayRanges: List[Int], featureNames: List[String], events: DataFrame, sessions: DataFrame): DataFrame = {
    val initial = struct(dayRanges.map(r => struct(featureNames.map(f => {
      if (f == "PRE_%_sessions") {
        typedLit(List[Int]()).as(f)
      } else {
        lit(0.0).as(f)
      }
    }): _*).as(r.toString)): _*)

    val lambdas = Map(
      "PRE_%_sessions" -> ((acc: Column, x: Column) => array_union(acc("PRE_%_sessions"), x("sessionids"))),
      "PRE_%_PAGE_COUNT" -> ((acc: Column, x: Column) => acc("PRE_%_PAGE_COUNT") + x("pagecount")),
      "PRE_%_VIEW_COUNT" -> ((acc: Column, x: Column) => acc("PRE_%_VIEW_COUNT") + x("viewcount")),
      "PRE_%_ROW_COUNT" -> ((acc: Column, x: Column) => acc("PRE_%_ROW_COUNT") + 1.0),
    )

    val groupedsessions = sessions
      .withColumnRenamed("cid", "cid2")
      .groupBy("cid2")
      .agg(collect_list(struct(
        col("sessiondate"),
        col("sessionids"),
        col("pagecount"),
        col("viewcount"))).as("combined")
      )

    events.join(groupedsessions, events("cid") === groupedsessions("cid2"), "left_outer")
      .withColumn("results", aggregate(
        when(col("combined").isNotNull, col("combined")).otherwise(array()),
        initial,
        (acc, x) => struct(
          dayRanges.map(r => when(
            x("sessiondate") - col("eventdate") <= r,
            struct(featureNames.map(f => lambdas(f)(acc(r.toString), x).as(f)): _*).as(r.toString)
          ).otherwise(acc(r.toString))): _*
        )))
      .select(
        col("cid")
        +: col("eventdate")
        +: dayRanges.flatMap(x => featureNames.map(y => {
        if (y == "PRE_%_sessions") {
          size(col("results")(x.toString)(y)).cast(DoubleType).as(y.replace("%", x.toString))
        } else {
          col("results")(x.toString)(y).as(y.replace("%", x.toString))
        }
      })): _*)
  }

  private def calculateFeatures2_2(dayRanges: List[Int], featureNames: List[String], events: DataFrame, sessions: DataFrame): DataFrame = {
    val initial = struct(
      dayRanges.map(r => struct(
        featureNames.map(f => (if (f == "PRE_%_sessions") typedLit(List[Int]()) else lit(0)).as(f)): _*).as(r.toString)
      ): _*
    )

    val lambdas = Map(
      "PRE_%_sessions" -> ((acc: Column, x: Column, f: String) => array_union(acc(f), x("sessionids"))),
      "PRE_%_PAGE_COUNT" -> ((acc: Column, x: Column, f: String) => acc(f) + x("pagecount")),
      "PRE_%_VIEW_COUNT" -> ((acc: Column, x: Column, f: String) => acc(f) + x("viewcount"))
    )

    events.join(sessions
      .withColumnRenamed("cid", "cid2")
      .groupBy("cid2")
      .agg(collect_list(struct(
        col("sessiondate"),
        col("sessionids"),
        col("pagecount"),
        col("viewcount"))).as("combined")
      ), "left_outer")
      .where(col("cid") === col("cid2"))
      .withColumn("results", aggregate(
        when(col("combined").isNotNull, col("combined")).otherwise(array()),
        initial,
        (acc, x) => struct(
          dayRanges.map(r => when(
            x("sessiondate") - col("eventdate") <= r,
            struct(featureNames.map(f => lambdas(f)(acc(r.toString), x, f).as(f)): _*).as(r.toString)
          ).otherwise(acc(r.toString))): _*
        )))
      .select(
        col("cid")
          +: col("eventdate")
          +: dayRanges.flatMap(x => featureNames.map(y => {
          if (y == "PRE_%_sessions") {
            size(col("results")(x.toString)(y)).cast(DoubleType).as(y.replace("%", x.toString))
          } else {
            col("results")(x.toString)(y).as(y.replace("%", x.toString))
          }
        })): _*)
  }

  private def calculateFeatures2_3(dayRanges: List[Int], featureNames: List[String], events: DataFrame, sessions: DataFrame): DataFrame = {
    val inits: mutable.ListBuffer[Column] = mutable.ListBuffer()

    for (f <- featureNames) {
        inits.addOne((if (f == "PRE_%_sessions") typedLit(List[Int]()) else lit(0.0)).as(f))
    }

    val initials: mutable.ListBuffer[Column] = mutable.ListBuffer()

    for (r <- dayRanges) {
      initials.addOne(struct(inits.toList: _*).as(r.toString))
    }

    val initial: Column = struct(initials.toList: _*)

    val lambdas = Map(
      "PRE_%_sessions" -> ((acc: Column, x: Column) => array_union(acc("PRE_%_sessions"), x("sessionids"))),
      "PRE_%_PAGE_COUNT" -> ((acc: Column, x: Column) => acc("PRE_%_PAGE_COUNT") + x("pagecount")),
      "PRE_%_VIEW_COUNT" -> ((acc: Column, x: Column) => acc("PRE_%_VIEW_COUNT") + x("viewcount")),
      "PRE_%_ROW_COUNT" -> ((acc: Column, x: Column) => acc("PRE_%_ROW_COUNT") + 1.0),
    )

    val groupedsessions = sessions
      .groupBy(col("cid"))
      .agg(collect_list(struct(
        col("sessiondate"),
        col("sessionids"),
        col("pagecount"),
        col("viewcount"))).as("combined")
      )

    val joined: DataFrame = events.join(groupedsessions, Seq("cid"), "left_outer")

    val aggFn = (acc: Column, x: Column) => {
      val rs: mutable.ListBuffer[Column] = mutable.ListBuffer()
      for (r <- dayRanges) {
        val rssP: mutable.ListBuffer[Column] = mutable.ListBuffer()
        for (f <- featureNames) {
          rssP.addOne(lambdas(f)(acc(r.toString), x).as(f))
        }

        val comp: Column = when(
          x("sessiondate") - col("eventdate") <= r,
          struct(rssP.toList: _*).as(r.toString)
        ).otherwise(acc(r.toString))
        rs.addOne(comp)
      }
      struct(rs.toList: _*)
    }

    val splitFn = (acc: Column) => {
      val rs: mutable.ListBuffer[Column] = mutable.ListBuffer()
      for (x <- dayRanges) {
        for (y <- featureNames) {
          val rcol = (if (y == "PRE_%_sessions")
            size(acc(x.toString)(y)).cast(DoubleType)
          else
            acc(x.toString)(y)).as(y.replace("%", x.toString))
          rs.addOne(rcol)
        }
      }
      rs.toList
    }

    val results: Column = aggregate(
      when(col("combined").isNotNull, col("combined")).otherwise(array()),
      initial,
      aggFn
    )

    val resultsDf:DataFrame = joined.withColumn("results", results)

    resultsDf.select(col("cid") +: col("eventdate") +: splitFn(col("results")): _*)
  }

  private def calculateFeatures3(dayRanges: List[Int], featureNames: List[String], events: DataFrame, sessions: DataFrame): DataFrame = {
    val combinedsessions: DataFrame = sessions
      .withColumnRenamed("cid", "cid2")
      .withColumn("combined", struct(
        col("sessiondate"),
        col("sessionids"),
        col("pagecount"),
        col("viewcount")),
      ).select("cid2", "combined")

    val groupedcombinedsessions = combinedsessions.groupBy("cid2").agg(collect_list("combined").as("combined"))

    val joineddf = events.join(groupedcombinedsessions, events("cid") === groupedcombinedsessions("cid2"), "left_outer")

    val names = dayRanges.flatMap(r => featureNames.map(name => name.replace("%", r.toString)))

    val sparkUdf: UserDefinedFunction = udf(udf3)

    val result: DataFrame = joineddf.withColumn("result", sparkUdf(
      col("eventdate"),
      col("combined"),
      typedLit(dayRanges),
      typedLit(featureNames)))

    result.select(col("cid") +: col("eventdate") +: names.map(k => col("result")(k).as(k)): _*)
  }

  private val udf3 = (eventDate: Int, sessions: Seq[Row], dayRanges: List[Int], featureNames: List[String]) => {
    val results: mutable.Map[String, Double] = mutable.Map()
    val sets: mutable.Map[Int, mutable.Set[Int]] = mutable.Map()

    for (dayRange <- dayRanges) {
      sets(dayRange) = mutable.Set[Int]()
      for (featureName <- featureNames) {
        results(featureName.replace("%", dayRange.toString)) = 0.0
      }
    }

    if (sessions != null) {
      for (session <- sessions) {
        for (dayRange <- dayRanges) {
          if (session.getInt(0) - eventDate <= dayRange) {
            for (featureName <- featureNames) {
              val fullFeatureName: String = featureName.replace("%", dayRange.toString)
              if (featureName == "PRE_%_sessions") {
                sets(dayRange).addAll(session.getSeq[Int](1))
                results(fullFeatureName) = sets(dayRange).size
              } else if (featureName == "PRE_%_PAGE_COUNT") {
                results(fullFeatureName) += session.getInt(2)
              } else if (featureName == "PRE_%_VIEW_COUNT") {
                results(fullFeatureName) += session.getInt(3)
              } else if (featureName == "PRE_%_ROW_COUNT") {
                results(fullFeatureName) += 1
              }
            }
          }
        }
      }
    }

    results
  }

  private def calculateFeatures4(dayRanges: List[Int], featureNames: List[String], events: DataFrame, sessions: DataFrame): DataFrame = {
    val sessions2 = sessions.withColumnRenamed("cid", "cid2")
    val df = events.join(sessions2, events("cid") === sessions2("cid2"), "left_outer")
    df.show(truncate = false)
    df.filter(col("sessiondate") - col("eventdate") <= 5).agg(struct(
      count_distinct(col("sessionids")),
      sum("pagecount"),
      sum("viewcount"),
    )).show(truncate = false)
    val df2 = df.groupBy(col("cid"), col("sessiondate") - col("eventdate") <= 5, col("sessiondate") - col("eventdate") <= 15).agg(collect_list(col("cid2")))
    //df2.show(truncate = false)
    df2
  }
}

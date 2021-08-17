import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType

import java.sql.Date
object FirstTaxiTask extends App{
  val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("SparkByExamples.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  case class Taxi(
                   VendorID: Int,
                   tpep_pickup_datetime: Date,
                   tpep_dropoff_datetime: Date,
                   passenger_count: Int,
                   trip_distance: Double,
                   RatecodeID: Int,
                   store_and_fwd_flag: String,
                   PULocationID: Double,
                   DOLocationID: Double,
                   payment_type: Int,
                   fare_amount: Double,
                   extra: Double,
                   mta_tax: Double,
                   tip_amount: Double,
                   tolls_amount: Double,
                   improvement_surcharge: Double,
                   total_amount: Double,
                   congestion_surcharge: Double
                 )

  val schema = Encoders.product[Taxi].schema
  val ds = spark.read
    .schema(schema)
    .option("header", "true")
    .option("charset", "UTF8")
    .option("delimiter", ",")
    .csv("src/main/resources/yellow_tripdata_2020-01.csv")
    .as[Taxi].cache()
  ds.show()
  //ds.printSchema()
  val count_date = ds.groupBy("tpep_pickup_datetime").
    agg(count("*").as("trip_count"))
    .orderBy("tpep_pickup_datetime")

  val count0 = ds.filter(ds("passenger_count") === 0)
    .groupBy(ds("tpep_pickup_datetime"))
    .agg(count(ds("passenger_count"))
      .as("p0"),
      min(ds("total_amount")).as("min_price_0p"),
      max(ds("total_amount")).as("max_price_0p"),
    )

  val count1 = ds.filter(ds("passenger_count") === 1)
    .groupBy(ds("tpep_pickup_datetime"))
    .agg(count(ds("passenger_count"))
      .as("p1"),
      min(ds("total_amount")).as("min_price_1p"),
      max(ds("total_amount")).as("max_price_1p"),
    )

  val count2 = ds.filter(ds("passenger_count") === 2)
    .groupBy(ds("tpep_pickup_datetime"))
    .agg(count(ds("passenger_count"))
      .as("p2"),
      min(ds("total_amount")).as("min_price_2p"),
      max(ds("total_amount")).as("max_price_2p"),
    )

  val count3 = ds.filter(ds("passenger_count") === 3)
    .groupBy(ds("tpep_pickup_datetime"))
    .agg(count(ds("passenger_count"))
      .as("p3"),
      min(ds("total_amount")).as("min_price_3p"),
      max(ds("total_amount")).as("max_price_3p"),
    )

  val count4 = ds.filter(ds("passenger_count") >= 4)
    .groupBy(ds("tpep_pickup_datetime"))
    .agg(count(ds("passenger_count"))
      .as("p4"),
      min(ds("total_amount")).as("min_price_4p"),
      max(ds("total_amount")).as("max_price_4p"),
    )

  val fin = count_date
    .join(count0, Seq("tpep_pickup_datetime"), "inner")
    .join(count1, Seq("tpep_pickup_datetime"), "inner")
    .join(count2, Seq("tpep_pickup_datetime"), "inner")
    .join(count3, Seq("tpep_pickup_datetime"), "inner")
    .join(count4, Seq("tpep_pickup_datetime"), "inner").as("Table").orderBy("tpep_pickup_datetime").
    select(
      $"Table.tpep_pickup_datetime",
      ($"Table.p0" / $"Table.trip_count" * 100.00).as("percentage_zero"),
      $"Table.min_price_0p",
      $"Table.max_price_0p",
      ($"Table.p1" / $"Table.trip_count" * 100.00).as("percentage_1p"),
      $"Table.min_price_1p",
      $"Table.max_price_1p",
      ($"Table.p2" / $"Table.trip_count" * 100.00).as("percentage_2p"),
      $"Table.min_price_2p",
      $"Table.max_price_2p",
      ($"Table.p3" / $"Table.trip_count" * 100.00).as("percentage_3p"),
      $"Table.min_price_3p",
      $"Table.max_price_3p",
      ($"Table.p4" / $"Table.trip_count" * 100.00).as("percentage_4p_plus"),
      $"Table.min_price_4p",
      $"Table.max_price_4p",
    )
  fin.show()
  fin.repartition(2).write.mode(SaveMode.Overwrite).parquet("/tmp/parquet/taxi")

  /////////////
  //  val fin0 = count_date
  //    .join(count0, count0("tpep_pickup_datetime") === count_date("tpep_pickup_datetime"), "inner")
  //   // .orderBy(count1("tpep_pickup_datetime"))
  //    .select(count_date("tpep_pickup_datetime").as("data"),
  //      (count0("p0") / count_date("trip_count") * 100.00).as("P0"))
  //  val fin1 = count_date
  //    .join(count1, count1("tpep_pickup_datetime") === count_date("tpep_pickup_datetime"), "inner")
  //    .select(count_date("tpep_pickup_datetime").as("data1"),
  //      (count1("p1") / count_date("trip_count") * 100.00).as("P1"))
  //  val fin2 = count_date
  //    .join(count2, count2("tpep_pickup_datetime") === count_date("tpep_pickup_datetime"), "inner")
  //    .select(count_date("tpep_pickup_datetime").as("data2"),
  //      (count2("p2") / count_date("trip_count") * 100.00).as("P2"))
  //  val fin3 = count_date
  //    .join(count3, count3("tpep_pickup_datetime") === count_date("tpep_pickup_datetime"), "inner")
  //    .select(count_date("tpep_pickup_datetime").as("data3"),
  //      (count3("p3") / count_date("trip_count") * 100.00).as("P3"))
  //  val fin4 = count_date
  //    .join(count4, count4("tpep_pickup_datetime") === count_date("tpep_pickup_datetime"), "inner")
  //    .select(count_date("tpep_pickup_datetime").as("data4"),
  //      (count4("p4") / count_date("trip_count") * 100.00).as("P4"))
  //  val fin = fin0.join(fin1, fin0("data") === fin1("data1")).as("T1")
  //    .join(fin2, $"T1.data" === fin2("data2")).as("T2")
  //    .join(fin3, $"T2.data" === fin3("data3")).as("T3")
  //    .join(fin4, $"T3.data" === fin4("data4")).orderBy("data").as("Res")
  //    .select($"Res.data".as("date"),
  //      $"Res.P0".as("percentage_zero"),
  //      $"Res.P1".as("percentage_1p"),
  //      $"Res.P2".as("percentage_2p"),
  //      $"Res.P3".as("percentage_3p"),
  //      $"Res.P4".as("percentage_4p_plus"))
  //    //.select($"Res.data".as("date"), "P0", "P1", "P2" , "P3" , "P4")
  //  fin.show()
  ///////////////////
}

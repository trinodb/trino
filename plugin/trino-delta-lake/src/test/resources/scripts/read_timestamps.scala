import scala.reflect.runtime.universe.TypeTag

import java.sql.Timestamp

import org.apache.spark.sql.Encoder

case class Types(col_0: String,
                 col_1: Timestamp,
                 col_2: Timestamp,
                 col_3: Timestamp,
                 col_4: Timestamp,
                 col_5: Timestamp,
                 col_6: Timestamp,
                 col_7: Timestamp,
                 col_8: Timestamp,
                 col_9: Timestamp,
                 col_10: Timestamp,
                 col_11: Timestamp,
                 col_12: Timestamp,
                 col_13: Timestamp,
                 col_14: Timestamp,
                 col_15: Timestamp,
                 col_16: Timestamp,
                 col_17: Timestamp,
                 col_18: Timestamp,
                 col_19: Timestamp,
                 col_20: Timestamp,
                 col_21: Timestamp,
                 col_22: Timestamp,
                 col_23: Timestamp,
                 col_24: Timestamp,
                 col_25: Timestamp,
                 col_26: Timestamp,
                 col_27: Timestamp,
                 col_28: Timestamp,
                 col_29: Timestamp,
                 col_30: Timestamp,
                 col_31: Timestamp,
                 col_32: Timestamp,
                 col_33: Timestamp,
                 col_34: Timestamp,
                 col_35: Timestamp,
                 col_36: Timestamp)

import spark.implicits._

val location = "/delta/export/read_timestamps"

def saveEntity[T: Encoder: TypeTag](entity: T, mergeSchema: Boolean = false): Unit = {
  Seq(entity).toDS()
    .write
    .partitionBy("col_0",
      "col_1",
      "col_2",
      "col_3",
      "col_4",
      "col_5",
      "col_6",
      "col_7",
      "col_8",
      "col_9",
      "col_10",
      "col_11",
      "col_12",
      "col_13",
      "col_14",
      "col_15",
      "col_16",
      "col_17",
      "col_18")
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(location)
}

def saveTimestamps(timezone: String): Unit = {
  spark.conf.set("spark.sql.session.timeZone", timezone)
  saveEntity(
    Types(
      // stored as partition values
      timezone,
      Timestamp.valueOf("1900-01-01 00:00:00.000"),
      Timestamp.valueOf("1952-04-03 01:02:03.456789"),
      Timestamp.valueOf("1970-01-01 00:00:00.000"),
      Timestamp.valueOf("1970-02-03 04:05:06.789"),
      Timestamp.valueOf("2017-07-01 00:00:00.000"),
      Timestamp.valueOf("1970-01-01 00:05:00.123456789"),
      Timestamp.valueOf("1969-12-31 23:05:00.123456789"),
      Timestamp.valueOf("1970-01-01 01:05:00.123456789"),
      Timestamp.valueOf("1996-10-27 01:05:00.987"),
      Timestamp.valueOf("1996-10-27 00:05:00.987"),
      Timestamp.valueOf("1996-10-27 02:05:00.987"),
      Timestamp.valueOf("1983-04-01 00:05:00.3456789"),
      Timestamp.valueOf("1983-03-31 23:05:00.3456789"),
      Timestamp.valueOf("1983-04-01 01:05:00.3456789"),
      Timestamp.valueOf("1983-09-30 23:59:00.654321"),
      Timestamp.valueOf("1983-09-30 22:59:00.654321"),
      Timestamp.valueOf("1983-10-01 00:59:00.654321"),
      Timestamp.valueOf("9999-12-31 23:59:59.999999999"),
      // stored in Parquet
      Timestamp.valueOf("1900-01-01 00:00:00.000"),
      Timestamp.valueOf("1952-04-03 01:02:03.456789"),
      Timestamp.valueOf("1970-01-01 00:00:00.000"),
      Timestamp.valueOf("1970-02-03 04:05:06.789"),
      Timestamp.valueOf("2017-07-01 00:00:00.000"),
      Timestamp.valueOf("1970-01-01 00:05:00.123456789"),
      Timestamp.valueOf("1969-12-31 23:05:00.123456789"),
      Timestamp.valueOf("1970-01-01 01:05:00.123456789"),
      Timestamp.valueOf("1996-10-27 01:05:00.987"),
      Timestamp.valueOf("1996-10-27 00:05:00.987"),
      Timestamp.valueOf("1996-10-27 02:05:00.987"),
      Timestamp.valueOf("1983-04-01 00:05:00.3456789"),
      Timestamp.valueOf("1983-03-31 23:05:00.3456789"),
      Timestamp.valueOf("1983-04-01 01:05:00.3456789"),
      Timestamp.valueOf("1983-09-30 23:59:00.654321"),
      Timestamp.valueOf("1983-09-30 22:59:00.654321"),
      Timestamp.valueOf("1983-10-01 00:59:00.654321"),
      Timestamp.valueOf("9999-12-31 23:59:59.999999999")
    )
  )
}

saveTimestamps("UTC")
saveTimestamps("Europe/Vilnius")
saveTimestamps("America/Bahia_Banderas")
saveTimestamps("Europe/Warsaw")

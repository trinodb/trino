import scala.reflect.runtime.universe.TypeTag

import java.math.BigDecimal
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.Encoder

case class Address (street: String, city: String, state: String, zip: String)
case class Phone (number: String, label: String)
case class Types(p_string: String,
                 p_byte: Byte,
                 p_short: Short,
                 p_int: Int,
                 p_long: Long,
                 p_decimal: BigDecimal,
                 p_boolean: Boolean,
                 p_float: Float,
                 p_double: Double,
                 p_date: Date,
                 p_timestamp: Timestamp,
                 t_string: String,
                 t_byte: Byte,
                 t_short: Short,
                 t_int: Int,
                 t_long: Long,
                 t_decimal: BigDecimal,
                 t_boolean: Boolean,
                 t_float: Float,
                 t_double: Double,
                 t_date: Date,
                 t_timestamp: Timestamp,
                 t_phones: Array[Phone],
                 t_address: Address)

import spark.implicits._

val location = "/delta/export/partitions"
def saveEntity[T: Encoder: TypeTag](entity: T, mergeSchema: Boolean = false): Unit = {
    Seq(entity).toDS()
            .write
            .partitionBy("p_string", "p_byte", "p_short", "p_int", "p_long", "p_decimal", "p_boolean", "p_float", "p_double", "p_date", "p_timestamp")
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(location)
}

for (i <- 1 to 30) {
    saveEntity(
        Types(
            "Alice",
            123,
            12345,
            123456789,
            1234567890123456789L,
            new BigDecimal("12345678901234567890.123456789012345678"), // decimal(38,18) with full precision and scale
            true,
            Math.PI.toFloat,
            Math.PI,
            Date.valueOf("2014-01-01"),
            Timestamp.valueOf("2014-01-01 23:00:01.123456789"),
            "Bob",
            -77,
            23456,
            i,
            2345678901234567890L,
            new BigDecimal("23456789012345678901.234567890123456789"), // decimal(38,18) with full precision and scale
            false,
            Math.E.toFloat,
            Math.E,
            Date.valueOf("2020-01-01"),
            Timestamp.valueOf("2020-01-01 23:00:01.123456789"),
            Array(Phone("123-555-0000", "Cell"), Phone("123-444-0001", "Home")),
            Address("100 Main St", "Anytown", "NY", "12345")
        )
    )
}

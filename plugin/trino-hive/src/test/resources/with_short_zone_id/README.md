The ORC file is generated using Apache Spark 2.3.0 libraries.

```java
import org.apache.spark.sql.SparkSession;

import java.util.TimeZone;

import static java.lang.String.format;

public class Main
{
    public static void main(String[] args)
    {
        // Make sure to set default timezone using short timezone id
        TimeZone.setDefault(TimeZone.getTimeZone("EST"));

        String tableName = "with_short_zone_id";
        String warehouseDirectory = "/Users/vikashkumar/spark/hive_warehouse";

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("shortZoneId")
                .config("spark.sql.warehouse.dir", warehouseDirectory)
                .enableHiveSupport()
                .getOrCreate();

        spark.sql("DROP TABLE IF EXISTS " + tableName);
        spark.sql(format("CREATE TABLE %s (id INT, firstName STRING, lastName STRING) STORED AS ORC LOCATION '%s/%1$s/data'", tableName, warehouseDirectory));
        spark.sql(format("INSERT INTO %s VALUES (1, 'John', 'Doe')", tableName));

        spark.sql("SELECT * FROM " + tableName)
                .show(false);

        spark.stop();
    }
}
```
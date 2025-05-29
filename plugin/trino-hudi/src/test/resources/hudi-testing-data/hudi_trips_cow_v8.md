## Create script

Structure of table:
- COW table in table version 8 with MDT and column stats enabled
- Using Hudi 1.0.2 release
- Non-partitioned table
- One large parquet file for testing projection and reader

```scala
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.table.HoodieTableConfig._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.keygen.constant.KeyGeneratorOptions._
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.QuickstartUtils._
import spark.implicits._

val tableName = "hudi_trips_cow_v8"
val basePath = "file:///tmp/hudi_trips_cow_v8"

val dataGen = new DataGenerator
val inserts = convertToStringList(dataGen.generateInserts(40000))
val df = spark.read.json(spark.sparkContext.parallelize(inserts, 1))
df.write.format("hudi").
  options(getQuickstartWriteConfigs).
  option(RECORDKEY_FIELD_OPT_KEY, "uuid").
  option(PARTITIONPATH_FIELD_OPT_KEY, "").
  option(TABLE_NAME, tableName).
  mode(Overwrite).
  save(basePath)
```

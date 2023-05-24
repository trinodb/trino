/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive.parquet;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.BaseTestParquetWithBloomFilters;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.format.CompressionCodec.SNAPPY;
import static org.apache.parquet.hadoop.ParquetOutputFormat.BLOOM_FILTER_ENABLED;
import static org.apache.parquet.hadoop.ParquetOutputFormat.WRITER_VERSION;

public class TestHiveParquetWithBloomFilters
        extends BaseTestParquetWithBloomFilters
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = HiveQueryRunner.builder().build();
        dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data");
        return queryRunner;
    }

    @Override
    protected CatalogSchemaTableName createParquetTableWithBloomFilter(String columnName, List<Integer> testValues)
    {
        // create the managed table
        String tableName = "parquet_with_bloom_filters_" + randomNameSuffix();
        CatalogSchemaTableName catalogSchemaTableName = new CatalogSchemaTableName("hive", new SchemaTableName("tpch", tableName));
        assertUpdate(format("CREATE TABLE %s (%s INT) WITH (format = 'PARQUET')", catalogSchemaTableName, columnName));

        // directly write data to the managed table
        Path tableLocation = Path.of("%s/tpch/%s".formatted(dataDirectory, tableName));
        Path fileLocation = tableLocation.resolve("bloomFilterFile.parquet");
        writeParquetFileWithBloomFilter(fileLocation.toFile(), columnName, testValues);

        return catalogSchemaTableName;
    }

    public static void writeParquetFileWithBloomFilter(File tempFile, String columnName, List<Integer> testValues)
    {
        List<ObjectInspector> objectInspectors = singletonList(javaIntObjectInspector);
        List<String> columnNames = ImmutableList.of(columnName);

        JobConf jobConf = new JobConf(newEmptyConfiguration());
        jobConf.setEnum(WRITER_VERSION, PARQUET_1_0);
        jobConf.setBoolean(BLOOM_FILTER_ENABLED, true);

        try {
            ParquetTester.writeParquetColumn(
                    jobConf,
                    tempFile,
                    SNAPPY,
                    ParquetTester.createTableProperties(columnNames, objectInspectors),
                    getStandardStructObjectInspector(columnNames, objectInspectors),
                    new Iterator<?>[] {testValues.iterator()},
                    Optional.empty(),
                    false,
                    DateTimeZone.getDefault());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

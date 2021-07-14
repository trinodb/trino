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
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static com.google.common.io.Resources.getResource;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.hive.HiveType.HIVE_TIMESTAMP;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestParquetPageSourceFactory
{
    private ParquetPageSourceFactory parquetPageSourceFactory;

    @BeforeClass
    public void setUp()
    {
        parquetPageSourceFactory = new ParquetPageSourceFactory(
                HDFS_ENVIRONMENT,
                new FileFormatDataSourceStats(),
                new ParquetReaderConfig(),
                new HiveConfig());
    }

    @AfterClass(alwaysRun = true)
    public void cleanUp()
    {
        parquetPageSourceFactory = null;
    }

    @Test
    public void testGetNestedMixedRepetitionColumnType()
    {
        RowType rowType = rowType(
                RowType.field(
                        "optional_level2",
                        rowType(RowType.field(
                                "required_level3",
                                IntegerType.INTEGER))));
        HiveColumnHandle columnHandle = new HiveColumnHandle(
                "optional_level1",
                0,
                HiveType.valueOf("struct<optional_level2:struct<required_level3:int>>"),
                rowType,
                Optional.of(
                        new HiveColumnProjectionInfo(
                                ImmutableList.of(1, 1),
                                ImmutableList.of("optional_level2", "required_level3"),
                                toHiveType(IntegerType.INTEGER),
                                IntegerType.INTEGER)),
                REGULAR,
                Optional.empty());
        MessageType fileSchema = new MessageType(
                "hive_schema",
                new GroupType(OPTIONAL, "optional_level1",
                        new GroupType(OPTIONAL, "optional_level2",
                                new PrimitiveType(REQUIRED, INT32, "required_level3"))));
        assertEquals(
                ParquetPageSourceFactory.getColumnType(columnHandle, fileSchema, true).get(),
                fileSchema.getType("optional_level1"));
    }

    @Test
    public void testCreatePageSourceNotEmptyWithParquetSerDeAndHudiRealtimeInputFormat()
            throws URISyntaxException
    {
        Optional<ReaderPageSource> optionalPageSource = getReaderPageSource("org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat");
        assertTrue(optionalPageSource.isPresent());
    }

    @Test
    public void testCreatePageSourceNotEmptyWithParquetSerDeAndParquetInputFormat()
            throws URISyntaxException
    {
        Optional<ReaderPageSource> optionalPageSource = getReaderPageSource(HiveStorageFormat.PARQUET.getInputFormat());
        assertTrue(optionalPageSource.isPresent());
    }

    private Optional<ReaderPageSource> getReaderPageSource(String inputFormat)
            throws URISyntaxException
    {
        Properties schema = new Properties();
        schema.setProperty(SERIALIZATION_LIB, HiveStorageFormat.PARQUET.getSerDe());
        schema.setProperty(FILE_INPUT_FORMAT, inputFormat);
        File file = new File(getResource("issue-5483.parquet").toURI());
        Type columnType = createTimestampType(10);
        return parquetPageSourceFactory.createPageSource(
                new Configuration(false),
                SESSION,
                new Path(file.toURI()),
                0L,
                file.length(),
                file.length(),
                schema,
                List.of(createBaseColumn("created", 0, HIVE_TIMESTAMP, columnType, REGULAR, Optional.empty())),
                TupleDomain.all(),
                Optional.empty(),
                OptionalInt.empty(),
                false,
                AcidTransaction.NO_ACID_TRANSACTION);
    }
}

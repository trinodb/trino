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

import com.google.common.io.Resources;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.benchmark.StandardFileFormats;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.HiveType.HIVE_TIMESTAMP;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.testing.MaterializedResult.materializeSourceDataStream;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTimestampMicros
{
    @Test(dataProvider = "testTimestampMicrosDataProvider")
    public void testTimestampMicros(HiveTimestampPrecision timestampPrecision, LocalDateTime expected)
            throws Exception
    {
        ConnectorSession session = getHiveSession(new HiveConfig()
                .setTimestampPrecision(timestampPrecision));

        File parquetFile = new File(Resources.getResource("issue-5483.parquet").toURI());
        Type columnType = createTimestampType(timestampPrecision.getPrecision());

        try (ConnectorPageSource pageSource = createPageSource(session, parquetFile, "created", HIVE_TIMESTAMP, columnType)) {
            MaterializedResult result = materializeSourceDataStream(session, pageSource, List.of(columnType)).toTestTypes();
            assertThat(result.getMaterializedRows())
                    .containsOnly(new MaterializedRow(List.of(expected)));
        }
    }

    @Test(dataProvider = "testTimestampMicrosDataProvider")
    public void testTimestampMicrosAsTimestampWithTimeZone(HiveTimestampPrecision timestampPrecision, LocalDateTime expected)
            throws Exception
    {
        ConnectorSession session = getHiveSession(new HiveConfig()
                .setTimestampPrecision(timestampPrecision));

        File parquetFile = new File(Resources.getResource("issue-5483.parquet").toURI());
        Type columnType = createTimestampWithTimeZoneType(timestampPrecision.getPrecision());

        try (ConnectorPageSource pageSource = createPageSource(session, parquetFile, "created", HIVE_TIMESTAMP, columnType)) {
            MaterializedResult result = materializeSourceDataStream(session, pageSource, List.of(columnType)).toTestTypes();
            assertThat(result.getMaterializedRows())
                    .containsOnly(new MaterializedRow(List.of(expected.atZone(ZoneId.of("UTC")))));
        }
    }

    @DataProvider
    public static Object[][] testTimestampMicrosDataProvider()
    {
        return new Object[][] {
                {HiveTimestampPrecision.MILLISECONDS, LocalDateTime.parse("2020-10-12T16:26:02.907")},
                {HiveTimestampPrecision.MICROSECONDS, LocalDateTime.parse("2020-10-12T16:26:02.906668")},
                {HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("2020-10-12T16:26:02.906668")},
        };
    }

    private ConnectorPageSource createPageSource(ConnectorSession session, File parquetFile, String columnName, HiveType columnHiveType, Type columnType)
    {
        // TODO after https://github.com/trinodb/trino/pull/5283, replace the method with
        //  return FileFormat.PRESTO_PARQUET.createFileFormatReader(session, HDFS_ENVIRONMENT, parquetFile, columnNames, columnTypes);

        HivePageSourceFactory pageSourceFactory = StandardFileFormats.TRINO_PARQUET.getHivePageSourceFactory(HDFS_ENVIRONMENT).orElseThrow();

        Properties schema = new Properties();
        schema.setProperty(SERIALIZATION_LIB, HiveStorageFormat.PARQUET.getSerde());

        ReaderPageSource pageSourceWithProjections = pageSourceFactory.createPageSource(
                new Configuration(false),
                session,
                new Path(parquetFile.toURI()),
                0,
                parquetFile.length(),
                parquetFile.length(),
                schema,
                List.of(createBaseColumn(columnName, 0, columnHiveType, columnType, REGULAR, Optional.empty())),
                TupleDomain.all(),
                Optional.empty(),
                OptionalInt.empty(),
                false,
                AcidTransaction.NO_ACID_TRANSACTION)
                .orElseThrow();

        pageSourceWithProjections.getReaderColumns()
                .ifPresent(projections -> { throw new IllegalStateException("Unexpected projections: " + projections); });

        return pageSourceWithProjections.get();
    }
}

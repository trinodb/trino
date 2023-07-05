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
import io.trino.filesystem.Location;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.benchmark.StandardFileFormats;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.MaterializedResult.materializeSourceDataStream;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOnlyNulls
{
    @Test
    public void testOnlyNulls()
            throws Exception
    {
        // The file contains only nulls and a dictionary page with 0 entries.
        File parquetFile = new File(Resources.getResource("issue-10873.parquet").toURI());
        String columnName = "x";
        Type columnType = INTEGER;

        HiveColumnHandle column = createBaseColumn(columnName, 0, HiveType.toHiveType(columnType), columnType, REGULAR, Optional.empty());

        // match not null
        try (ConnectorPageSource pageSource = createPageSource(parquetFile, column, TupleDomain.withColumnDomains(Map.of(column, Domain.notNull(columnType))))) {
            MaterializedResult result = materializeSourceDataStream(getHiveSession(new HiveConfig()), pageSource, List.of(columnType)).toTestTypes();
            assertThat(result.getMaterializedRows()).isEmpty();
        }

        // match null
        try (ConnectorPageSource pageSource = createPageSource(parquetFile, column, TupleDomain.withColumnDomains(Map.of(column, Domain.onlyNull(columnType))))) {
            MaterializedResult result = materializeSourceDataStream(getHiveSession(new HiveConfig()), pageSource, List.of(columnType)).toTestTypes();

            assertThat(result.getMaterializedRows())
                    .isEqualTo(List.of(
                            new MaterializedRow(singletonList(null)),
                            new MaterializedRow(singletonList(null)),
                            new MaterializedRow(singletonList(null)),
                            new MaterializedRow(singletonList(null))));
        }
    }

    private static ConnectorPageSource createPageSource(File parquetFile, HiveColumnHandle column, TupleDomain<HiveColumnHandle> domain)
    {
        HivePageSourceFactory pageSourceFactory = StandardFileFormats.TRINO_PARQUET.getHivePageSourceFactory(HDFS_ENVIRONMENT).orElseThrow();

        Properties schema = new Properties();
        schema.setProperty(SERIALIZATION_LIB, HiveStorageFormat.PARQUET.getSerde());

        return pageSourceFactory.createPageSource(
                        getHiveSession(new HiveConfig()),
                        Location.of(parquetFile.getPath()),
                        0,
                        parquetFile.length(),
                        parquetFile.length(),
                        schema,
                        List.of(column),
                        domain,
                        Optional.empty(),
                        OptionalInt.empty(),
                        false,
                        AcidTransaction.NO_ACID_TRANSACTION)
                .orElseThrow()
                .get();
    }
}

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
import com.google.common.io.Resources;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveType;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.parquet.ParquetUtil.createPageSource;
import static io.trino.spi.predicate.Domain.notNull;
import static io.trino.spi.predicate.Domain.onlyNull;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.MaterializedResult.materializeSourceDataStream;
import static java.util.Collections.singletonList;
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
        try (ConnectorPageSource pageSource = createPageSource(SESSION, parquetFile, ImmutableList.of(column), TupleDomain.withColumnDomains(Map.of(column, notNull(columnType))))) {
            MaterializedResult result = materializeSourceDataStream(getHiveSession(new HiveConfig()), pageSource, List.of(columnType)).toTestTypes();
            assertThat(result.getMaterializedRows()).isEmpty();
        }

        // match null
        try (ConnectorPageSource pageSource = createPageSource(SESSION, parquetFile, ImmutableList.of(column), TupleDomain.withColumnDomains(Map.of(column, onlyNull(columnType))))) {
            MaterializedResult result = materializeSourceDataStream(getHiveSession(new HiveConfig()), pageSource, List.of(columnType)).toTestTypes();

            assertThat(result.getMaterializedRows())
                    .isEqualTo(List.of(
                            new MaterializedRow(singletonList(null)),
                            new MaterializedRow(singletonList(null)),
                            new MaterializedRow(singletonList(null)),
                            new MaterializedRow(singletonList(null))));
        }
    }
}

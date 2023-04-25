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
package io.trino.testing;

import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.spi.connector.CatalogSchemaTableName;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseTestParquetWithBloomFilters
        extends AbstractTestQueryFramework
{
    protected Path dataDirectory;
    private static final String COLUMN_NAME = "dataColumn";
    // containing extreme values, so the row group cannot be eliminated by the column chunk's min/max statistics
    private static final List<Integer> TEST_VALUES = Arrays.asList(Integer.MIN_VALUE, Integer.MAX_VALUE, 1, 3, 7, 10, 15);
    private static final int MISSING_VALUE = 0;

    @Test
    public void verifyBloomFilterEnabled()
    {
        assertThat(query(format("SHOW SESSION LIKE '%s.parquet_use_bloom_filter'", getSession().getCatalog().orElseThrow())))
                .skippingTypesCheck()
                .matches(result -> result.getRowCount() == 1)
                .matches(result -> {
                    String value = (String) result.getMaterializedRows().get(0).getField(1);
                    return value.equals("true");
                });
    }

    @Test
    public void testBloomFilterRowGroupPruning()
    {
        CatalogSchemaTableName tableName = createParquetTableWithBloomFilter(COLUMN_NAME, TEST_VALUES);

        // assert table is populated with data
        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName,
                queryStats -> {},
                results -> assertThat(results.getOnlyColumnAsSet()).isEqualTo(ImmutableSet.copyOf(TEST_VALUES)));

        // When reading bloom filter is enabled, row groups are pruned when searching for a missing value
        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE " + COLUMN_NAME + " = " + MISSING_VALUE,
                queryStats -> {
                    assertThat(queryStats.getPhysicalInputPositions()).isEqualTo(0);
                    assertThat(queryStats.getProcessedInputPositions()).isEqualTo(0);
                },
                results -> assertThat(results.getRowCount()).isEqualTo(0));

        // When reading bloom filter is enabled, row groups are not pruned when searching for a value present in the file
        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE " + COLUMN_NAME + " = " + TEST_VALUES.get(0),
                queryStats -> {
                    assertThat(queryStats.getPhysicalInputPositions()).isGreaterThan(0);
                    assertThat(queryStats.getProcessedInputPositions()).isEqualTo(queryStats.getPhysicalInputPositions());
                },
                results -> assertThat(results.getRowCount()).isEqualTo(1));

        // When reading bloom filter is disabled, row groups are not pruned when searching for a missing value
        assertQueryStats(
                bloomFiltersDisabled(getSession()),
                "SELECT * FROM " + tableName + " WHERE " + COLUMN_NAME + " = " + MISSING_VALUE,
                queryStats -> {
                    assertThat(queryStats.getPhysicalInputPositions()).isGreaterThan(0);
                    assertThat(queryStats.getProcessedInputPositions()).isEqualTo(queryStats.getPhysicalInputPositions());
                },
                results -> assertThat(results.getRowCount()).isEqualTo(0));

        assertUpdate("DROP TABLE " + tableName);
    }

    private static Session bloomFiltersDisabled(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "parquet_use_bloom_filter", "false")
                .build();
    }

    protected abstract CatalogSchemaTableName createParquetTableWithBloomFilter(String columnName, List<Integer> testValues);
}

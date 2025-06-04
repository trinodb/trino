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

package io.trino.plugin.tpcds;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.tpcds.Table;
import io.trino.tpcds.column.CallCenterColumn;
import io.trino.tpcds.column.WebSiteColumn;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTpcdsMetadataStatistics
{
    private static final EstimateAssertion estimateAssertion = new EstimateAssertion(0.01);
    private static final ConnectorSession session = null;
    private final TpcdsMetadata metadata = new TpcdsMetadata();

    @Test
    public void testNoTableStatsForNotSupportedSchema()
    {
        Stream.of("sf0.001", "sf0.1", "sf10")
                .forEach(schemaName -> Table.getBaseTables()
                        .forEach(table -> {
                            SchemaTableName schemaTableName = new SchemaTableName(schemaName, table.getName());
                            ConnectorTableHandle tableHandle = metadata.getTableHandle(session, schemaTableName, Optional.empty(), Optional.empty());
                            TableStatistics tableStatistics = metadata.getTableStatistics(session, tableHandle);
                            assertThat(tableStatistics.getRowCount().isUnknown()).isTrue();
                            assertThat(tableStatistics.getColumnStatistics()).isEmpty();
                        }));
    }

    @Test
    public void testTableStatsExistenceSupportedSchema()
    {
        Stream.of("sf0.01", "tiny", "sf1", "sf1.000")
                .forEach(schemaName -> Table.getBaseTables()
                        .forEach(table -> {
                            SchemaTableName schemaTableName = new SchemaTableName(schemaName, table.getName());
                            ConnectorTableHandle tableHandle = metadata.getTableHandle(session, schemaTableName, Optional.empty(), Optional.empty());
                            TableStatistics tableStatistics = metadata.getTableStatistics(session, tableHandle);
                            assertThat(tableStatistics.getRowCount().isUnknown()).isFalse();
                            for (ColumnHandle column : metadata.getColumnHandles(session, tableHandle).values()) {
                                assertThat(tableStatistics.getColumnStatistics()).containsKey(column);
                                assertThat(tableStatistics.getColumnStatistics().get(column)).isNotNull();
                            }
                        }));
    }

    @Test
    public void testTableStatsDetails()
    {
        SchemaTableName schemaTableName = new SchemaTableName("sf1", Table.CALL_CENTER.getName());
        ConnectorTableHandle tableHandle = metadata.getTableHandle(session, schemaTableName, Optional.empty(), Optional.empty());
        TableStatistics tableStatistics = metadata.getTableStatistics(session, tableHandle);

        estimateAssertion.assertClose(tableStatistics.getRowCount(), Estimate.of(6), "Row count does not match");

        // all columns have stats
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        for (ColumnHandle column : columnHandles.values()) {
            assertThat(tableStatistics.getColumnStatistics()).containsKey(column);
            assertThat(tableStatistics.getColumnStatistics().get(column)).isNotNull();
        }

        // identifier
        assertColumnStatistics(
                tableStatistics.getColumnStatistics().get(columnHandles.get(CallCenterColumn.CC_CALL_CENTER_SK.getName())),
                ColumnStatistics.builder()
                        .setNullsFraction(Estimate.of(0))
                        .setDistinctValuesCount(Estimate.of(6))
                        .setRange(new DoubleRange(1, 6))
                        .build());

        // varchar
        assertColumnStatistics(
                tableStatistics.getColumnStatistics().get(columnHandles.get(CallCenterColumn.CC_CALL_CENTER_ID.getName())),
                ColumnStatistics.builder()
                        .setNullsFraction(Estimate.of(0))
                        .setDistinctValuesCount(Estimate.of(3))
                        .setDataSize(Estimate.of(48.0))
                        .build());

        // char
        assertColumnStatistics(
                tableStatistics.getColumnStatistics().get(columnHandles.get(CallCenterColumn.CC_ZIP.getName())),
                ColumnStatistics.builder()
                        .setNullsFraction(Estimate.of(0))
                        .setDistinctValuesCount(Estimate.of(1))
                        .setDataSize(Estimate.of(5.0))
                        .build());

        // decimal
        assertColumnStatistics(
                tableStatistics.getColumnStatistics().get(columnHandles.get(CallCenterColumn.CC_GMT_OFFSET.getName())),
                ColumnStatistics.builder()
                        .setNullsFraction(Estimate.of(0))
                        .setDistinctValuesCount(Estimate.of(1))
                        .setRange(new DoubleRange(-5, -5))
                        .build());

        // date
        assertColumnStatistics(
                tableStatistics.getColumnStatistics().get(columnHandles.get(CallCenterColumn.CC_REC_START_DATE.getName())),
                ColumnStatistics.builder()
                        .setNullsFraction(Estimate.of(0))
                        .setDistinctValuesCount(Estimate.of(4))
                        .setRange(new DoubleRange(10227L, 11688L))
                        .build());

        // only null values
        assertColumnStatistics(
                tableStatistics.getColumnStatistics().get(columnHandles.get(CallCenterColumn.CC_CLOSED_DATE_SK.getName())),
                ColumnStatistics.builder()
                        .setNullsFraction(Estimate.of(1))
                        .setDistinctValuesCount(Estimate.of(0))
                        .build());
    }

    @Test
    public void testNullFraction()
    {
        SchemaTableName schemaTableName = new SchemaTableName("sf1", Table.WEB_SITE.getName());
        ConnectorTableHandle tableHandle = metadata.getTableHandle(session, schemaTableName, Optional.empty(), Optional.empty());
        TableStatistics tableStatistics = metadata.getTableStatistics(session, tableHandle);

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);

        // some null values
        assertColumnStatistics(
                tableStatistics.getColumnStatistics().get(columnHandles.get(WebSiteColumn.WEB_REC_END_DATE.getName())),
                ColumnStatistics.builder()
                        .setNullsFraction(Estimate.of(0.5))
                        .setDistinctValuesCount(Estimate.of(3))
                        .setRange(new DoubleRange(10819L, 11549L))
                        .build());
    }

    private void assertColumnStatistics(ColumnStatistics actual, ColumnStatistics expected)
    {
        estimateAssertion.assertClose(actual.getNullsFraction(), expected.getNullsFraction(), "Nulls fraction");
        estimateAssertion.assertClose(actual.getDataSize(), expected.getDataSize(), "Data size");
        estimateAssertion.assertClose(actual.getDistinctValuesCount(), expected.getDistinctValuesCount(), "Distinct values count");
        assertThat(actual.getRange()).isEqualTo(expected.getRange());
    }
}

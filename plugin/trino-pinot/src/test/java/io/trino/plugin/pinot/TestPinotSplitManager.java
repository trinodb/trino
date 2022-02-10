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
package io.trino.plugin.pinot;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.pinot.query.DynamicTable;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.plugin.pinot.PinotSplit.SplitType.BROKER;
import static io.trino.plugin.pinot.PinotSplit.SplitType.SEGMENT;
import static io.trino.plugin.pinot.query.DynamicTableBuilder.buildFromPql;
import static io.trino.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static io.trino.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPinotSplitManager
        extends TestPinotQueryBase
{
    // Test table and related info
    private final PinotSplitManager pinotSplitManager = new PinotSplitManager(new MockPinotClient(pinotConfig));

    @Test
    public void testSplitsBroker()
    {
        SchemaTableName schemaTableName = new SchemaTableName("default", format("SELECT %s, %s FROM %s LIMIT %d", "AirlineID", "OriginStateName", "airlineStats", 100));
        DynamicTable dynamicTable = buildFromPql(pinotMetadata, schemaTableName, mockClusterInfoFetcher);

        PinotTableHandle pinotTableHandle = new PinotTableHandle("default", dynamicTable.getTableName(), TupleDomain.all(), OptionalLong.empty(), Optional.of(dynamicTable));
        List<PinotSplit> splits = getSplitsHelper(pinotTableHandle, 1, false);
        assertSplits(splits, 1, BROKER);
    }

    @Test(expectedExceptions = PinotSplitManager.QueryNotAdequatelyPushedDownException.class)
    public void testBrokerNonShortQuery()
    {
        PinotTableHandle pinotTableHandle = new PinotTableHandle(realtimeOnlyTable.getSchemaName(), realtimeOnlyTable.getTableName());
        List<PinotSplit> splits = getSplitsHelper(pinotTableHandle, 1, true);
        assertSplits(splits, 1, BROKER);
    }

    @Test
    public void testRealtimeSegmentSplitsManySegmentPerServer()
    {
        testSegmentSplitsHelperNoFilter(realtimeOnlyTable, Integer.MAX_VALUE, 2);
    }

    private void testSegmentSplitsHelperNoFilter(PinotTableHandle table, int segmentsPerSplit, int expectedNumSplits)
    {
        PinotTableHandle pinotTableHandle = new PinotTableHandle(table.getSchemaName(), table.getTableName());
        List<PinotSplit> splits = getSplitsHelper(pinotTableHandle, segmentsPerSplit, false);
        assertSplits(splits, expectedNumSplits, SEGMENT);
        splits.forEach(this::assertSegmentSplitWellFormed);
    }

    private void testSegmentSplitsHelperWithFilter(PinotTableHandle table, int segmentsPerSplit, int expectedNumSplits)
    {
        PinotTableHandle pinotTableHandle = new PinotTableHandle(table.getSchemaName(), table.getTableName());
        List<PinotSplit> splits = getSplitsHelper(pinotTableHandle, segmentsPerSplit, false);
        assertSplits(splits, expectedNumSplits, SEGMENT);
        splits.forEach(this::assertSegmentSplitWellFormed);
    }

    @Test
    public void testHybridSegmentSplitsOneSegmentPerServer()
    {
        testSegmentSplitsHelperNoFilter(hybridTable, 1, 8);
        testSegmentSplitsHelperWithFilter(hybridTable, 1, 8);
    }

    private void assertSplits(List<PinotSplit> splits, int numSplitsExpected, PinotSplit.SplitType splitType)
    {
        assertEquals(splits.size(), numSplitsExpected);
        splits.forEach(s -> assertEquals(s.getSplitType(), splitType));
    }

    private void assertSegmentSplitWellFormed(PinotSplit split)
    {
        assertEquals(split.getSplitType(), SEGMENT);
        assertTrue(split.getSegmentHost().isPresent());
        assertFalse(split.getSegments().isEmpty());
    }

    public static ConnectorSession createSessionWithNumSplits(int numSegmentsPerSplit, boolean forbidSegmentQueries, PinotConfig pinotConfig)
    {
        return TestingConnectorSession.builder()
                .setTimeZoneKey(UTC_KEY)
                .setStart(Instant.now())
                .setPropertyMetadata(new PinotSessionProperties(pinotConfig).getSessionProperties())
                .setPropertyValues(ImmutableMap.<String, Object>builder()
                        .put(PinotSessionProperties.SEGMENTS_PER_SPLIT, numSegmentsPerSplit)
                        .put(PinotSessionProperties.FORBID_SEGMENT_QUERIES, forbidSegmentQueries)
                        .buildOrThrow())
                .build();
    }

    private List<PinotSplit> getSplitsHelper(PinotTableHandle pinotTable, int numSegmentsPerSplit, boolean forbidSegmentQueries)
    {
        ConnectorSession session = createSessionWithNumSplits(numSegmentsPerSplit, forbidSegmentQueries, pinotConfig);
        ConnectorSplitSource splitSource = pinotSplitManager.getSplits(null, session, pinotTable, UNGROUPED_SCHEDULING, DynamicFilter.EMPTY);
        List<PinotSplit> splits = new ArrayList<>();
        while (!splitSource.isFinished()) {
            splits.addAll(getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, 1000)).getSplits().stream().map(s -> (PinotSplit) s).collect(toList()));
        }

        return splits;
    }
}

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
package io.trino.plugin.atop;

import com.google.inject.Inject;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static java.util.Objects.requireNonNull;

public class AtopSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;
    private final ZoneId timeZone;
    private final int maxHistoryDays;

    @Inject
    public AtopSplitManager(NodeManager nodeManager, AtopConnectorConfig config)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        timeZone = config.getTimeZoneId();
        maxHistoryDays = config.getMaxHistoryDays();
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        AtopTableHandle tableHandle = (AtopTableHandle) table;

        List<ConnectorSplit> splits = new ArrayList<>();
        ZonedDateTime end = ZonedDateTime.now(timeZone);
        for (Node node : nodeManager.getWorkerNodes()) {
            ZonedDateTime start = end.minusDays(maxHistoryDays - 1).withHour(0).withMinute(0).withSecond(0).withNano(0);
            while (start.isBefore(end)) {
                ZonedDateTime splitEnd = start.withHour(23).withMinute(59).withSecond(59).withNano(0);
                Domain splitDomain = Domain.create(
                        ValueSet.ofRanges(Range.range(
                                TIMESTAMP_TZ_MILLIS,
                                packDateTimeWithZone(start.toInstant().toEpochMilli(), UTC_KEY),
                                true,
                                packDateTimeWithZone(splitEnd.toInstant().toEpochMilli(), UTC_KEY),
                                true)),
                        false);
                if (tableHandle.getStartTimeConstraint().overlaps(splitDomain) && tableHandle.getEndTimeConstraint().overlaps(splitDomain)) {
                    splits.add(new AtopSplit(node.getHostAndPort(), start.toEpochSecond(), start.getZone().getId()));
                }
                start = start.plusDays(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
            }
        }

        return new FixedSplitSource(splits);
    }
}

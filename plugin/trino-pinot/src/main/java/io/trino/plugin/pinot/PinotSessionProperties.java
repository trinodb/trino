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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;

public class PinotSessionProperties
{
    private static final String CONNECTION_TIMEOUT = "connection_timeout";
    private static final String PREFER_BROKER_QUERIES = "prefer_broker_queries";
    private static final String RETRY_COUNT = "retry_count";
    private static final String NON_AGGREGATE_LIMIT_FOR_BROKER_QUERIES = "non_aggregate_limit_for_broker_queries";
    private static final String AGGREGATION_PUSHDOWN_ENABLED = "aggregation_pushdown_enabled";
    private static final String COUNT_DISTINCT_PUSHDOWN_ENABLED = "count_distinct_pushdown_enabled";

    @VisibleForTesting
    public static final String FORBID_SEGMENT_QUERIES = "forbid_segment_queries";

    @VisibleForTesting
    public static final String SEGMENTS_PER_SPLIT = "segments_per_split";

    private final List<PropertyMetadata<?>> sessionProperties;

    public static int getSegmentsPerSplit(ConnectorSession session)
    {
        int segmentsPerSplit = session.getProperty(SEGMENTS_PER_SPLIT, Integer.class);
        return segmentsPerSplit <= 0 ? Integer.MAX_VALUE : segmentsPerSplit;
    }

    public static boolean isPreferBrokerQueries(ConnectorSession session)
    {
        return session.getProperty(PREFER_BROKER_QUERIES, Boolean.class);
    }

    public static boolean isForbidSegmentQueries(ConnectorSession session)
    {
        return session.getProperty(FORBID_SEGMENT_QUERIES, Boolean.class);
    }

    public static Duration getConnectionTimeout(ConnectorSession session)
    {
        return session.getProperty(CONNECTION_TIMEOUT, Duration.class);
    }

    public static int getPinotRetryCount(ConnectorSession session)
    {
        return session.getProperty(RETRY_COUNT, Integer.class);
    }

    public static int getNonAggregateLimitForBrokerQueries(ConnectorSession session)
    {
        return session.getProperty(NON_AGGREGATE_LIMIT_FOR_BROKER_QUERIES, Integer.class);
    }

    public static boolean isAggregationPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(AGGREGATION_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isCountDistinctPushdownEnabled(ConnectorSession session)
    {
        // This should never fail as this method would never be called unless aggregation pushdown is enabled
        verify(isAggregationPushdownEnabled(session), "%s must be enabled when %s is enabled", AGGREGATION_PUSHDOWN_ENABLED, COUNT_DISTINCT_PUSHDOWN_ENABLED);
        return session.getProperty(COUNT_DISTINCT_PUSHDOWN_ENABLED, Boolean.class);
    }

    @Inject
    public PinotSessionProperties(PinotConfig pinotConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        PREFER_BROKER_QUERIES,
                        "Prefer queries to broker even when parallel scan is enabled for aggregation queries",
                        pinotConfig.isPreferBrokerQueries(),
                        false),
                booleanProperty(
                        FORBID_SEGMENT_QUERIES,
                        "Forbid segment queries",
                        pinotConfig.isForbidSegmentQueries(),
                        false),
                integerProperty(
                        RETRY_COUNT,
                        "Retry count for retriable pinot data fetch calls",
                        pinotConfig.getFetchRetryCount(),
                        false),
                integerProperty(
                        NON_AGGREGATE_LIMIT_FOR_BROKER_QUERIES,
                        "Max limit for non aggregate queries to the pinot broker",
                        pinotConfig.getNonAggregateLimitForBrokerQueries(),
                        false),
                durationProperty(
                        CONNECTION_TIMEOUT,
                        "Connection Timeout to talk to Pinot servers",
                        pinotConfig.getConnectionTimeout(),
                        false),
                integerProperty(
                        SEGMENTS_PER_SPLIT,
                        "Number of segments of the same host per split",
                        pinotConfig.getSegmentsPerSplit(),
                        value -> checkArgument(value > 0, "Number of segments per split must be more than zero"),
                        false),
                booleanProperty(
                        AGGREGATION_PUSHDOWN_ENABLED,
                        "Enable aggregation pushdown",
                        pinotConfig.isAggregationPushdownEnabled(),
                        false),
                booleanProperty(
                        COUNT_DISTINCT_PUSHDOWN_ENABLED,
                        "Enable count distinct pushdown",
                        pinotConfig.isCountDistinctPushdownEnabled(),
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}

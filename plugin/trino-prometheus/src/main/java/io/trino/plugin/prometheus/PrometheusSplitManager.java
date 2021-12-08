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
package io.trino.plugin.prometheus;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;

import javax.inject.Inject;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.plugin.prometheus.PrometheusClient.TIMESTAMP_COLUMN_TYPE;
import static io.trino.plugin.prometheus.PrometheusErrorCode.PROMETHEUS_UNKNOWN_ERROR;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static java.time.Instant.ofEpochMilli;
import static java.util.Objects.requireNonNull;

public class PrometheusSplitManager
        implements ConnectorSplitManager
{
    static final long OFFSET_MILLIS = 1L;
    private final PrometheusClient prometheusClient;
    private final PrometheusClock prometheusClock;

    private final URI prometheusURI;
    private final Duration maxQueryRangeDuration;
    private final Duration queryChunkSizeDuration;

    @Inject
    public PrometheusSplitManager(PrometheusClient prometheusClient, PrometheusClock prometheusClock, PrometheusConnectorConfig config)
    {
        this.prometheusClient = requireNonNull(prometheusClient, "prometheusClient is null");
        this.prometheusClock = requireNonNull(prometheusClock, "prometheusClock is null");

        requireNonNull(config, "config is null");
        this.prometheusURI = config.getPrometheusURI();
        this.maxQueryRangeDuration = config.getMaxQueryRangeDuration();
        this.queryChunkSizeDuration = config.getQueryChunkSizeDuration();
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            SplitSchedulingStrategy splitSchedulingStrategy,
            DynamicFilter dynamicFilter)
    {
        PrometheusTableHandle tableHandle = (PrometheusTableHandle) connectorTableHandle;
        PrometheusTable table = prometheusClient.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());

        // this can happen if table is removed during a query
        if (table == null) {
            throw new TableNotFoundException(tableHandle.toSchemaTableName());
        }
        List<ConnectorSplit> splits = generateTimesForSplits(prometheusClock.now(), maxQueryRangeDuration, queryChunkSizeDuration, tableHandle)
                .stream()
                .map(time -> {
                    try {
                        return new PrometheusSplit(buildQuery(
                                prometheusURI,
                                time,
                                table.getName(),
                                queryChunkSizeDuration).toString());
                    }
                    catch (URISyntaxException e) {
                        throw new TrinoException(PROMETHEUS_UNKNOWN_ERROR, "split URI invalid: " + e.getMessage());
                    }
                }).collect(Collectors.toList());
        return new FixedSplitSource(splits);
    }

    // URIBuilder handles URI encode
    private static URI buildQuery(URI baseURI, String time, String metricName, Duration queryChunkSizeDuration)
            throws URISyntaxException
    {
        List<NameValuePair> nameValuePairs = new ArrayList<>(2);
        nameValuePairs.add(new BasicNameValuePair("query", metricName + "[" + queryChunkSizeDuration.roundTo(queryChunkSizeDuration.getUnit()) +
                Duration.timeUnitToString(queryChunkSizeDuration.getUnit()) + "]"));
        nameValuePairs.add(new BasicNameValuePair("time", time));
        return new URIBuilder(baseURI.toString())
                .setPath("api/v1/query")
                .setParameters(nameValuePairs)
                .build();
    }

    /**
     * Utility method to get the end times in decimal seconds that divide up the query into chunks
     * The times will be used in queries to Prometheus like: `http://localhost:9090/api/v1/query?query=up[21d]&time=1568229904.000"`
     * ** NOTE: Prometheus instant query wants the duration and end time specified.
     * We use now() for the defaultUpperBound when none is specified, for instance, from predicate push down
     *
     * @return list of end times as decimal epoch seconds, like ["1568053244.143", "1568926595.321"]
     */
    protected static List<String> generateTimesForSplits(Instant defaultUpperBound, Duration maxQueryRangeDurationRequested, Duration queryChunkSizeDurationRequested,
            PrometheusTableHandle tableHandle)
    {
        Optional<PrometheusPredicateTimeInfo> predicateRange = tableHandle.getPredicate()
                .flatMap(PrometheusSplitManager::determinePredicateTimes);

        EffectiveLimits effectiveLimits = new EffectiveLimits(defaultUpperBound, maxQueryRangeDurationRequested, predicateRange);
        Instant upperBound = effectiveLimits.getUpperBound();
        java.time.Duration maxQueryRangeDuration = effectiveLimits.getMaxQueryRangeDuration();

        java.time.Duration queryChunkSizeDuration = java.time.Duration.ofMillis(queryChunkSizeDurationRequested.toMillis());
        if (maxQueryRangeDuration.isNegative()) {
            throw new IllegalArgumentException("prometheus.max.query.range.duration may not be negative");
        }
        if (queryChunkSizeDuration.isNegative()) {
            throw new IllegalArgumentException("prometheus.query.chunk.size.duration may not be negative");
        }
        if (queryChunkSizeDuration.isZero()) {
            throw new IllegalArgumentException("prometheus.query.chunk.size.duration may not be zero");
        }
        BigDecimal maxQueryRangeDecimal = BigDecimal.valueOf(maxQueryRangeDuration.getSeconds()).add(BigDecimal.valueOf(maxQueryRangeDuration.getNano(), 9));
        BigDecimal queryChunkSizeDecimal = BigDecimal.valueOf(queryChunkSizeDuration.getSeconds()).add(BigDecimal.valueOf(queryChunkSizeDuration.getNano(), 9));

        int numChunks = maxQueryRangeDecimal.divide(queryChunkSizeDecimal, 0, RoundingMode.UP).intValue();

        return Lists.reverse(IntStream.range(0, numChunks)
                .mapToObj(n -> {
                    long endTime = upperBound.toEpochMilli() -
                            n * queryChunkSizeDuration.toMillis() - n * OFFSET_MILLIS;
                    return endTime;
                })
                .map(PrometheusSplitManager::decimalSecondString)
                .collect(Collectors.toList()));
    }

    protected static Optional<PrometheusPredicateTimeInfo> determinePredicateTimes(TupleDomain<ColumnHandle> predicate)
    {
        Optional<Map<ColumnHandle, Domain>> maybeColumnHandleDomainMap = predicate.getDomains();
        Optional<Set<ColumnHandle>> maybeKeySet = maybeColumnHandleDomainMap.map(Map::keySet);
        Optional<Set<ColumnHandle>> maybeOnlyPromColHandles = maybeKeySet.map(keySet -> keySet.stream()
                .filter(PrometheusColumnHandle.class::isInstance)
                .collect(Collectors.toSet()));
        Optional<Set<PrometheusColumnHandle>> maybeOnlyTimeStampColumnHandles = maybeOnlyPromColHandles.map(handles -> handles.stream()
                .map(PrometheusColumnHandle.class::cast)
                .filter(handle -> handle.getColumnType().equals(TIMESTAMP_COLUMN_TYPE))
                .filter(handle -> handle.getColumnName().equals("timestamp"))
                .collect(Collectors.toSet()));

        // below we have a set of ColumnHandle that are all PrometheusColumnHandle AND of TimestampType wrapped in Optional: maybeOnlyTimeStampColumnHandles
        // the ColumnHandles in maybeOnlyTimeStampColumnHandles are keys to the map maybeColumnHandleDomainMap
        // and the values in that map are Domains which hold the timestamp predicate range info
        Map<ColumnHandle, Domain> columnHandleDomainMap = maybeColumnHandleDomainMap.orElse(ImmutableMap.of());
        Optional<Set<Domain>> maybeTimeDomains = maybeOnlyTimeStampColumnHandles.map(columnHandles -> columnHandles.stream()
                .map(columnHandleDomainMap::get)
                .collect(Collectors.toSet()));
        return processTimeDomains(maybeTimeDomains);
    }

    private static Optional<PrometheusPredicateTimeInfo> processTimeDomains(Optional<Set<Domain>> maybeTimeDomains)
    {
        return maybeTimeDomains.map(timeDomains -> {
            PrometheusPredicateTimeInfo.Builder timeInfoBuilder = PrometheusPredicateTimeInfo.builder();
            timeDomains.forEach(domain -> {
                Range span = domain.getValues().getRanges().getSpan();
                if (!span.isLowUnbounded()) {
                    long packedValue = (long) span.getLowBoundedValue();
                    Instant instant = ofEpochMilli(unpackMillisUtc(packedValue));
                    timeInfoBuilder.setPredicateLowerTimeBound(Optional.of(instant));
                }
                if (!span.isHighUnbounded()) {
                    long packedValue = (long) span.getHighBoundedValue();
                    Instant instant = ofEpochMilli(unpackMillisUtc(packedValue));
                    timeInfoBuilder.setPredicateUpperTimeBound(Optional.of(instant));
                }
            });
            return timeInfoBuilder.build();
        });
    }

    static String decimalSecondString(long millis)
    {
        return new BigDecimal(Long.toString(millis)).divide(new BigDecimal(1000L)).toPlainString();
    }

    private static class EffectiveLimits
    {
        private final Instant upperBound;
        private final java.time.Duration maxQueryRangeDuration;

        /**
         * If no upper bound is specified by the predicate, we use the time now() as the defaultUpperBound
         * if predicate LOWER bound is set AND predicate UPPER bound is set:
         * max duration          = upper bound - lower bound
         * effective upper bound = predicate upper bound
         * if predicate LOWER bound is NOT set AND predicate UPPER bound is set:
         * max duration          = config max duration
         * effective upper bound = predicate upper bound
         * if predicate LOWER bound is set AND predicate UPPER bound is NOT set:
         * max duration          = defaultUpperBound - lower bound
         * effective upper bound = defaultUpperBound
         * if predicate LOWER bound is NOT set AND predicate UPPER bound is NOT set:
         * max duration          = config max duration
         * effective upper bound = defaultUpperBound
         *
         * @param defaultUpperBound If no upper bound is specified by the predicate, we use the time now() as the defaultUpperBound
         * @param maxQueryRangeDurationRequested Likely from config properties
         * @param maybePredicateRange Optional of pushed down predicate values for high and low timestamp values
         */
        public EffectiveLimits(Instant defaultUpperBound, Duration maxQueryRangeDurationRequested, Optional<PrometheusPredicateTimeInfo> maybePredicateRange)
        {
            if (maybePredicateRange.isPresent()) {
                if (maybePredicateRange.get().getPredicateUpperTimeBound().isPresent()) {
                    // predicate upper bound set
                    upperBound = maybePredicateRange.get().getPredicateUpperTimeBound().get();
                }
                else {
                    // predicate upper bound NOT set
                    upperBound = defaultUpperBound;
                }
                // here we're just working out the max duration using the above upperBound for upper bound
                if (maybePredicateRange.get().getPredicateLowerTimeBound().isPresent()) {
                    // predicate lower bound set
                    maxQueryRangeDuration = java.time.Duration.between(maybePredicateRange.get().getPredicateLowerTimeBound().get(), upperBound);
                }
                else {
                    // predicate lower bound NOT set
                    maxQueryRangeDuration = java.time.Duration.ofMillis(maxQueryRangeDurationRequested.toMillis());
                }
            }
            else {
                // no predicate set, so no predicate value for upper bound, use defaultUpperBound (possibly now()) for upper bound and config for max durations
                upperBound = defaultUpperBound;
                maxQueryRangeDuration = java.time.Duration.ofMillis(maxQueryRangeDurationRequested.toMillis());
            }
        }

        public Instant getUpperBound()
        {
            return upperBound;
        }

        public java.time.Duration getMaxQueryRangeDuration()
        {
            return maxQueryRangeDuration;
        }
    }
}

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
package io.prestosql.plugin.prometheus;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.units.Duration;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Marker;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TimestampType;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;

import javax.inject.Inject;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.prestosql.plugin.prometheus.PrometheusErrorCode.PROMETHEUS_UNKNOWN_ERROR;
import static java.util.Objects.requireNonNull;

public class PrometheusSplitManager
        implements ConnectorSplitManager
{
    static final long OFFSET_MILLIS = 1L;
    private final PrometheusClient prometheusClient;
    private final PrometheusConnectorConfig config;

    @Inject
    public PrometheusSplitManager(PrometheusClient prometheusClient, PrometheusConnectorConfig config)
    {
        this.prometheusClient = requireNonNull(prometheusClient, "client is null");
        this.config = requireNonNull(config, "config is null");
        requireNonNull(config.getQueryChunkSizeDuration(), "prometheus.query.chunk.size.duration is null");
        requireNonNull(config.getMaxQueryRangeDuration(), "prometheus.max.query.range.duration is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            SplitSchedulingStrategy splitSchedulingStrategy)
    {
        PrometheusTableHandle tableHandle = (PrometheusTableHandle) connectorTableHandle;
        PrometheusTable table = prometheusClient.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());

        // this can happen if table is removed during a query
        if (table == null) {
            throw new TableNotFoundException(tableHandle.toSchemaTableName());
        }
        List<ConnectorSplit> splits = generateTimesForSplits(PrometheusTimeMachine.now(), config.getMaxQueryRangeDuration(), config.getQueryChunkSizeDuration(), tableHandle)
                .stream()
                .map(time -> {
                    try {
                        return new PrometheusSplit(buildQuery(config.getPrometheusURI(),
                                time,
                                table.getName(),
                                config.getQueryChunkSizeDuration()));
                    }
                    catch (URISyntaxException e) {
                        throw new PrestoException(PROMETHEUS_UNKNOWN_ERROR, "split URI invalid: " + e.getMessage());
                    }
                }).collect(Collectors.toList());
        return new FixedSplitSource(splits);
    }

    // URIBuilder handles URI encode
    private URI buildQuery(URI baseURI, String time, String metricName, Duration queryChunkSizeDuration)
            throws URISyntaxException
    {
        List<NameValuePair> nameValuePairs = new ArrayList<>(2);
        nameValuePairs.add(new BasicNameValuePair("query", metricName + "[" + String.valueOf(queryChunkSizeDuration.roundTo(queryChunkSizeDuration.getUnit())) +
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
     * We use now() for the defaultUpperBound when none is specified, for instance, from predicate pushdown
     *
     * @param defaultUpperBound a LocalDateTime likely from PrometheusTimeMachine class for testability
     * @param maxQueryRangeDurationStr a String like `21d`
     * @param queryChunkSizeDurationStr a String like `1d`
     * @return list of end times as decimal epoch seconds, like ["1568053244.143", "1568926595.321"]
     */
    protected static List<String> generateTimesForSplits(LocalDateTime defaultUpperBound, Duration maxQueryRangeDurationRequested, Duration queryChunkSizeDurationRequested,
            PrometheusTableHandle tableHandle)
    {
        Optional<PrometheusPredicateTimeInfo> predicateRange = tableHandle.getPredicate()
                .flatMap(predicate -> determinePredicateTimes(predicate));

        EffectiveLimits effectiveLimits = new EffectiveLimits(defaultUpperBound, maxQueryRangeDurationRequested, predicateRange);
        ZonedDateTime upperBound = effectiveLimits.getUpperBound();
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

        return Lists.reverse(IntStream.range(0, numChunks).mapToObj(n -> {
            long endTime = upperBound.toInstant().toEpochMilli() -
                    n * queryChunkSizeDuration.toMillis() - n * OFFSET_MILLIS;
            return endTime;
        }).map(endTimeMilli -> decimalSecondString(endTimeMilli)).collect(Collectors.toList()));
    }

    protected static Optional<PrometheusPredicateTimeInfo> determinePredicateTimes(TupleDomain<ColumnHandle> predicate)
    {
        Optional<Map<ColumnHandle, Domain>> maybeColumnHandleDomainMap = predicate.getDomains();
        Optional<Set<ColumnHandle>> maybeKeySet = maybeColumnHandleDomainMap.map(columnHandleDomainMap -> columnHandleDomainMap.keySet());
        Optional<Set<ColumnHandle>> maybeOnlyPromColHandles = maybeKeySet.map(keySet -> keySet.stream()
                .filter(columnHandle -> columnHandle instanceof PrometheusColumnHandle).collect(Collectors.toSet()));
        Optional<Set<ColumnHandle>> maybeOnlyTimeStampColumnHandles = maybeOnlyPromColHandles.map(columnHandles -> columnHandles.stream()
                .filter(colHandle -> ((PrometheusColumnHandle) colHandle).getColumnType() instanceof TimestampType &&
                        ((PrometheusColumnHandle) colHandle).getColumnName().equals("timestamp")).collect(Collectors.toSet()));

        // below we have a set of ColumnHandle that are all PrometheusColumnHandle AND of TimestampType wrapped in Optional: maybeOnlyTimeStampColumnHandles
        // the ColumnHandles in maybeOnlyTimeStampColumnHandles are keys to the map maybeColumnHandleDomainMap
        // and the values in that map are Domains which hold the timestamp predicate range info
        Map<ColumnHandle, Domain> columnHandleDomainMap = maybeColumnHandleDomainMap.orElse(ImmutableMap.of());
        Optional<Set<Domain>> maybeTimeDomains = maybeOnlyTimeStampColumnHandles
                .map(columnHandles -> columnHandles.stream().map(colHandle -> columnHandleDomainMap.get(colHandle)).collect(Collectors.toSet()));
        return processTimeDomains(maybeTimeDomains);
    }

    private static Optional<PrometheusPredicateTimeInfo> processTimeDomains(Optional<Set<Domain>> maybeTimeDomains)
    {
        return maybeTimeDomains.map(timeDomains -> {
            PrometheusPredicateTimeInfo.Builder timeInfoBuilder = PrometheusPredicateTimeInfo.builder();
            timeDomains.stream()
                    .forEach(domain -> {
                        if (!domain.getValues().getRanges().getSpan().includes(Marker.lowerUnbounded(TimestampType.TIMESTAMP))) {
                            timeInfoBuilder.predicateLowerTimeBound = Optional.of(ZonedDateTime.ofInstant(Instant.ofEpochMilli(
                                    (long) domain.getValues().getRanges().getSpan().getLow().getValue()),
                                    ZoneId.systemDefault()));
                        }
                        if (!domain.getValues().getRanges().getSpan().includes(Marker.upperUnbounded(TimestampType.TIMESTAMP))) {
                            timeInfoBuilder.predicateUpperTimeBound = Optional.of(ZonedDateTime.ofInstant(Instant.ofEpochMilli(
                                    (long) domain.getValues().getRanges().getSpan().getHigh().getValue()),
                                    ZoneId.systemDefault()));
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
        private ZonedDateTime upperBound;
        private java.time.Duration maxQueryRangeDuration;

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
        public EffectiveLimits(LocalDateTime defaultUpperBound, Duration maxQueryRangeDurationRequested, Optional<PrometheusPredicateTimeInfo> maybePredicateRange)
        {
            ZonedDateTime defaultUpperBoundZoned = ZonedDateTime.of(defaultUpperBound, ZoneId.systemDefault());

            if (maybePredicateRange.isPresent()) {
                if (maybePredicateRange.get().predicateUpperTimeBound.isPresent()) {
                    // predicate upper bound set
                    upperBound = maybePredicateRange.get().predicateUpperTimeBound.get();
                }
                else {
                    // predicate upper bound NOT set
                    upperBound = defaultUpperBoundZoned;
                }
                // here we're just working out the max duration using the above upperBound for upper bound
                if (maybePredicateRange.get().predicateLowerTimeBound.isPresent()) {
                    // predicate lower bound set
                    maxQueryRangeDuration = java.time.Duration.between(maybePredicateRange.get().predicateLowerTimeBound.get(), upperBound);
                }
                else {
                    // predicate lower bound NOT set
                    maxQueryRangeDuration = java.time.Duration.ofMillis(maxQueryRangeDurationRequested.toMillis());
                }
            }
            else {
                // no predicate set, so no predicate value for upper bound, use defaultUpperBound (possibly now()) for upper bound and config for max durations
                upperBound = defaultUpperBoundZoned;
                maxQueryRangeDuration = java.time.Duration.ofMillis(maxQueryRangeDurationRequested.toMillis());
            }
        }

        public ZonedDateTime getUpperBound()
        {
            return upperBound;
        }

        public java.time.Duration getMaxQueryRangeDuration()
        {
            return maxQueryRangeDuration;
        }
    }
}

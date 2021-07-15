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
package io.trino.plugin.cassandra;

import com.datastax.driver.core.VersionNumber;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.cassandra.util.CassandraCqlUtils;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class CassandraClusteringPredicatesExtractor
{
    private final ClusteringPushDownResult clusteringPushDownResult;
    private final TupleDomain<ColumnHandle> predicates;

    public CassandraClusteringPredicatesExtractor(List<CassandraColumnHandle> clusteringColumns, TupleDomain<ColumnHandle> predicates, VersionNumber cassandraVersion)
    {
        this.predicates = requireNonNull(predicates, "predicates is null");
        this.clusteringPushDownResult = getClusteringKeysSet(clusteringColumns, predicates, requireNonNull(cassandraVersion, "cassandraVersion is null"));
    }

    public String getClusteringKeyPredicates()
    {
        return clusteringPushDownResult.getDomainQuery();
    }

    public TupleDomain<ColumnHandle> getUnenforcedConstraints()
    {
        return predicates.filter(((columnHandle, domain) -> !clusteringPushDownResult.hasBeenFullyPushed(columnHandle)));
    }

    private static ClusteringPushDownResult getClusteringKeysSet(List<CassandraColumnHandle> clusteringColumns, TupleDomain<ColumnHandle> predicates, VersionNumber cassandraVersion)
    {
        ImmutableSet.Builder<ColumnHandle> fullyPushedColumnPredicates = ImmutableSet.builder();
        ImmutableList.Builder<String> clusteringColumnSql = ImmutableList.builder();
        int allProcessedClusteringColumns = 0;
        for (CassandraColumnHandle columnHandle : clusteringColumns) {
            Domain domain = predicates.getDomains().get().get(columnHandle);
            if (domain == null) {
                break;
            }
            if (domain.isNullAllowed()) {
                break;
            }

            int currentlyProcessedClusteringColumn = allProcessedClusteringColumns;
            String predicateString = domain.getValues().getValuesProcessor().transform(
                    ranges -> {
                        if (ranges.getRangeCount() == 1) {
                            fullyPushedColumnPredicates.add(columnHandle);
                            return translateRangeIntoCql(columnHandle, getOnlyElement(ranges.getOrderedRanges()));
                        }
                        if (ranges.getOrderedRanges().stream().allMatch(Range::isSingleValue)) {
                            if (isInExpressionNotAllowed(clusteringColumns, cassandraVersion, currentlyProcessedClusteringColumn)) {
                                return translateRangeIntoCql(columnHandle, ranges.getSpan());
                            }

                            String inValues = ranges.getOrderedRanges().stream()
                                    .map(range -> toCqlLiteral(columnHandle, range.getSingleValue()))
                                    .collect(joining(","));
                            fullyPushedColumnPredicates.add(columnHandle);
                            return CassandraCqlUtils.validColumnName(columnHandle.getName()) + " IN (" + inValues + ")";
                        }
                        return translateRangeIntoCql(columnHandle, ranges.getSpan());
                    }, discreteValues -> {
                        if (discreteValues.isInclusive()) {
                            if (discreteValues.getValuesCount() == 0) {
                                return null;
                            }
                            if (discreteValues.getValuesCount() == 1) {
                                fullyPushedColumnPredicates.add(columnHandle);
                                return format("%s = %s",
                                        CassandraCqlUtils.validColumnName(columnHandle.getName()),
                                        toCqlLiteral(columnHandle, getOnlyElement(discreteValues.getValues())));
                            }
                            if (isInExpressionNotAllowed(clusteringColumns, cassandraVersion, currentlyProcessedClusteringColumn)) {
                                return null;
                            }

                            String inValues = discreteValues.getValues().stream()
                                    .map(columnHandle.getCassandraType()::toCqlLiteral)
                                    .collect(joining(","));
                            fullyPushedColumnPredicates.add(columnHandle);
                            return CassandraCqlUtils.validColumnName(columnHandle.getName()) + " IN (" + inValues + " )";
                        }
                        return null;
                    }, allOrNone -> null);

            if (predicateString == null) {
                break;
            }
            clusteringColumnSql.add(predicateString);
            // Check for last clustering column should only be restricted by range condition
            if (predicateString.contains(">") || predicateString.contains("<")) {
                break;
            }
            allProcessedClusteringColumns++;
        }
        List<String> clusteringColumnPredicates = clusteringColumnSql.build();

        return new ClusteringPushDownResult(fullyPushedColumnPredicates.build(), Joiner.on(" AND ").join(clusteringColumnPredicates));
    }

    /**
     * IN restriction allowed only on last clustering column for Cassandra version <= 2.2.0
     */
    private static boolean isInExpressionNotAllowed(List<CassandraColumnHandle> clusteringColumns, VersionNumber cassandraVersion, int currentlyProcessedClusteringColumn)
    {
        return cassandraVersion.compareTo(VersionNumber.parse("2.2.0")) < 0 && currentlyProcessedClusteringColumn != (clusteringColumns.size() - 1);
    }

    private static String toCqlLiteral(CassandraColumnHandle columnHandle, Object value)
    {
        return columnHandle.getCassandraType().toCqlLiteral(value);
    }

    private static String translateRangeIntoCql(CassandraColumnHandle columnHandle, Range range)
    {
        if (columnHandle.getCassandraType().getKind() == CassandraType.Kind.TUPLE) {
            // Building CQL literals for TUPLE type is not supported
            return null;
        }

        if (range.isAll()) {
            return null;
        }
        if (range.isSingleValue()) {
            return format("%s = %s",
                    CassandraCqlUtils.validColumnName(columnHandle.getName()),
                    toCqlLiteral(columnHandle, range.getSingleValue()));
        }

        String lowerBoundPredicate = null;
        String upperBoundPredicate = null;
        if (!range.isLowUnbounded()) {
            String lowBound = toCqlLiteral(columnHandle, range.getLowBoundedValue());
            lowerBoundPredicate = format(
                    "%s %s %s",
                    CassandraCqlUtils.validColumnName(columnHandle.getName()),
                    range.isLowInclusive() ? ">=" : ">",
                    lowBound);
        }
        if (!range.isHighUnbounded()) {
            String highBound = toCqlLiteral(columnHandle, range.getHighBoundedValue());
            upperBoundPredicate = format(
                    "%s %s %s",
                    CassandraCqlUtils.validColumnName(columnHandle.getName()),
                    range.isHighInclusive() ? "<=" : "<",
                    highBound);
        }
        if (lowerBoundPredicate != null && upperBoundPredicate != null) {
            return format("%s AND %s ", lowerBoundPredicate, upperBoundPredicate);
        }
        if (lowerBoundPredicate != null) {
            return lowerBoundPredicate;
        }
        return upperBoundPredicate;
    }

    private static class ClusteringPushDownResult
    {
        private final Set<ColumnHandle> fullyPushedColumnPredicates;
        private final String domainQuery;

        public ClusteringPushDownResult(Set<ColumnHandle> fullyPushedColumnPredicates, String domainQuery)
        {
            this.fullyPushedColumnPredicates = ImmutableSet.copyOf(requireNonNull(fullyPushedColumnPredicates, "fullyPushedColumnPredicates is null"));
            this.domainQuery = requireNonNull(domainQuery);
        }

        public boolean hasBeenFullyPushed(ColumnHandle column)
        {
            return fullyPushedColumnPredicates.contains(column);
        }

        public String getDomainQuery()
        {
            return domainQuery;
        }
    }
}

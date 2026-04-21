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
    private final CassandraTypeManager cassandraTypeManager;
    private final ClusteringPushDownResult clusteringPushDownResult;
    private final TupleDomain<ColumnHandle> predicates;

    public CassandraClusteringPredicatesExtractor(CassandraTypeManager cassandraTypeManager, List<CassandraColumnHandle> clusteringColumns, TupleDomain<ColumnHandle> predicates)
    {
        this.cassandraTypeManager = requireNonNull(cassandraTypeManager, "cassandraTypeManager is null");
        this.predicates = requireNonNull(predicates, "predicates is null");
        this.clusteringPushDownResult = getClusteringKeysSet(clusteringColumns, predicates);
    }

    public String getClusteringKeyPredicates()
    {
        return clusteringPushDownResult.domainQuery();
    }

    public TupleDomain<ColumnHandle> getUnenforcedConstraints()
    {
        return predicates.filter((columnHandle, domain) -> !clusteringPushDownResult.hasBeenFullyPushed(columnHandle));
    }

    private ClusteringPushDownResult getClusteringKeysSet(List<CassandraColumnHandle> clusteringColumns, TupleDomain<ColumnHandle> predicates)
    {
        ImmutableSet.Builder<ColumnHandle> fullyPushedColumnPredicates = ImmutableSet.builder();
        ImmutableList.Builder<String> clusteringColumnSql = ImmutableList.builder();
        for (CassandraColumnHandle columnHandle : clusteringColumns) {
            Domain domain = predicates.getDomains().get().get(columnHandle);
            if (domain == null) {
                break;
            }
            if (domain.isNullAllowed()) {
                break;
            }

            String predicateString = domain.getValues().getValuesProcessor().transform(
                    ranges -> {
                        if (ranges.getRangeCount() == 1) {
                            fullyPushedColumnPredicates.add(columnHandle);
                            return translateRangeIntoCql(columnHandle, getOnlyElement(ranges.getOrderedRanges()));
                        }
                        if (ranges.getOrderedRanges().stream().allMatch(Range::isSingleValue)) {
                            String inValues = ranges.getOrderedRanges().stream()
                                    .map(range -> toCqlLiteral(columnHandle, range.getSingleValue()))
                                    .collect(joining(","));
                            fullyPushedColumnPredicates.add(columnHandle);
                            return CassandraCqlUtils.validColumnName(columnHandle.name()) + " IN (" + inValues + ")";
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
                                        CassandraCqlUtils.validColumnName(columnHandle.name()),
                                        toCqlLiteral(columnHandle, getOnlyElement(discreteValues.getValues())));
                            }

                            String inValues = discreteValues.getValues().stream()
                                    .map(value -> cassandraTypeManager.toCqlLiteral(columnHandle.cassandraType(), value))
                                    .collect(joining(","));
                            fullyPushedColumnPredicates.add(columnHandle);
                            return CassandraCqlUtils.validColumnName(columnHandle.name()) + " IN (" + inValues + " )";
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
        }
        List<String> clusteringColumnPredicates = clusteringColumnSql.build();

        return new ClusteringPushDownResult(fullyPushedColumnPredicates.build(), Joiner.on(" AND ").join(clusteringColumnPredicates));
    }

    private String toCqlLiteral(CassandraColumnHandle columnHandle, Object value)
    {
        return cassandraTypeManager.toCqlLiteral(columnHandle.cassandraType(), value);
    }

    private String translateRangeIntoCql(CassandraColumnHandle columnHandle, Range range)
    {
        if (columnHandle.cassandraType().kind() == CassandraType.Kind.TUPLE || columnHandle.cassandraType().kind() == CassandraType.Kind.UDT) {
            // Building CQL literals for TUPLE and UDT type is not supported
            return null;
        }

        if (range.isAll()) {
            return null;
        }
        if (range.isSingleValue()) {
            return format("%s = %s",
                    CassandraCqlUtils.validColumnName(columnHandle.name()),
                    toCqlLiteral(columnHandle, range.getSingleValue()));
        }

        String lowerBoundPredicate = null;
        String upperBoundPredicate = null;
        if (!range.isLowUnbounded()) {
            String lowBound = toCqlLiteral(columnHandle, range.getLowBoundedValue());
            lowerBoundPredicate = format(
                    "%s %s %s",
                    CassandraCqlUtils.validColumnName(columnHandle.name()),
                    range.isLowInclusive() ? ">=" : ">",
                    lowBound);
        }
        if (!range.isHighUnbounded()) {
            String highBound = toCqlLiteral(columnHandle, range.getHighBoundedValue());
            upperBoundPredicate = format(
                    "%s %s %s",
                    CassandraCqlUtils.validColumnName(columnHandle.name()),
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

    private record ClusteringPushDownResult(Set<ColumnHandle> fullyPushedColumnPredicates, String domainQuery)
    {
        private ClusteringPushDownResult
        {
            fullyPushedColumnPredicates = ImmutableSet.copyOf(requireNonNull(fullyPushedColumnPredicates, "fullyPushedColumnPredicates is null"));
            requireNonNull(domainQuery);
        }

        public boolean hasBeenFullyPushed(ColumnHandle column)
        {
            return fullyPushedColumnPredicates.contains(column);
        }
    }
}

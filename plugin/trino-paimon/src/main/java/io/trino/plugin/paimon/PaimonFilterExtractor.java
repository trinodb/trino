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
package io.trino.plugin.paimon;

import io.airlift.slice.Slice;
import io.trino.plugin.paimon.catalog.PaimonTrinoCatalog;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static org.apache.paimon.fileindex.FileIndexCommon.toMapKey;

/**
 * Extract filter from trino.
 */
public class PaimonFilterExtractor
{
    private PaimonFilterExtractor() {}

    public static final String TRINO_MAP_ELEMENT_AT_FUNCTION_NAME = "element_at";

    /**
     * Extract filter from trino , include ExpressionFilter.
     */
    public static Optional<TrinoFilter> extract(
            ConnectorSession session,
            PaimonTrinoCatalog catalog,
            PaimonTableHandle paimonTableHandle,
            Constraint constraint)
    {
        TupleDomain<PaimonColumnHandle> oldFilter = paimonTableHandle.getPredicate();
        TupleDomain<PaimonColumnHandle> newFilter =
                constraint
                        .getSummary()
                        .transformKeys(PaimonColumnHandle.class::cast)
                        .intersect(oldFilter);

        if (oldFilter.equals(newFilter)) {
            return Optional.empty();
        }

        Map<PaimonColumnHandle, Domain> trinoColumnHandleForExpressionFilter =
                extractTrinoColumnHandleForExpressionFilter(constraint);

        LinkedHashMap<PaimonColumnHandle, Domain> acceptedDomains = new LinkedHashMap<>();
        LinkedHashMap<PaimonColumnHandle, Domain> unsupportedDomains = new LinkedHashMap<>();
        new PaimonFilterConverter(paimonTableHandle.table(session, catalog).rowType())
                .convert(newFilter, acceptedDomains, unsupportedDomains);

        List<String> partitionKeys = paimonTableHandle.table(session, catalog).partitionKeys();
        LinkedHashMap<PaimonColumnHandle, Domain> unenforcedDomains = new LinkedHashMap<>();
        acceptedDomains.forEach(
                (columnHandle, domain) -> {
                    if (!partitionKeys.contains(columnHandle.getColumnName())) {
                        unenforcedDomains.put(columnHandle, domain);
                    }
                });

        acceptedDomains.putAll(trinoColumnHandleForExpressionFilter);

        @SuppressWarnings({"unchecked", "rawtypes"})
        TupleDomain<ColumnHandle> remain =
                (TupleDomain)
                        TupleDomain.withColumnDomains(unsupportedDomains)
                                .intersect(TupleDomain.withColumnDomains(unenforcedDomains));

        return Optional.of(new TrinoFilter(TupleDomain.withColumnDomains(acceptedDomains), remain));
    }

    /**
     * Extract Expression filter from trino Constraint. Extract Trino Expression filter ( e.g.
     * element_at(jsonmap, 'a') = '1' ) to TrinoColumnHandle.
     */
    public static Map<PaimonColumnHandle, Domain> extractTrinoColumnHandleForExpressionFilter(
            Constraint constraint)
    {
        Map<PaimonColumnHandle, Domain> expressionPredicates = Collections.emptyMap();

        if (constraint.getExpression() instanceof Call expression) {
            Map<String, ColumnHandle> assignments = constraint.getAssignments();

            if (expression.getFunctionName().equals(EQUAL_OPERATOR_FUNCTION_NAME)) {
                expressionPredicates = handleExpressionEqualOrIn(assignments, expression, false);
            }
            else if (expression.getFunctionName().equals(IN_PREDICATE_FUNCTION_NAME)) {
                expressionPredicates = handleExpressionEqualOrIn(assignments, expression, true);
            }
            else if (expression.getFunctionName().equals(AND_FUNCTION_NAME)) {
                expressionPredicates = handleAndArguments(assignments, expression);
            }
            // TODO: Support "or" clause
        }
        return expressionPredicates;
    }

    /**
     * Expression filter support the case of "AND" and "IN".
     */
    private static Map<PaimonColumnHandle, Domain> handleAndArguments(
            Map<String, ColumnHandle> assignments, Call expression)
    {
        Map<PaimonColumnHandle, Domain> expressionPredicates = new HashMap<>();

        expression.getArguments().stream()
                .map(argument -> (Call) argument)
                .forEach(
                        argument -> {
                            if (argument.getFunctionName().equals(EQUAL_OPERATOR_FUNCTION_NAME)) {
                                expressionPredicates.putAll(
                                        handleExpressionEqualOrIn(assignments, argument, false));
                            }
                            else if (argument.getFunctionName()
                                    .equals(IN_PREDICATE_FUNCTION_NAME)) {
                                expressionPredicates.putAll(
                                        handleExpressionEqualOrIn(assignments, argument, true));
                            }
                        });

        return expressionPredicates;
    }

    private static Map<PaimonColumnHandle, Domain> handleExpressionEqualOrIn(
            Map<String, ColumnHandle> assignments, Call expression, boolean inClause)
    {
        Call elementAtExpression = (Call) expression.getArguments().get(0);

        String functionName = elementAtExpression.getFunctionName().getName();

        switch (functionName) {
            case TRINO_MAP_ELEMENT_AT_FUNCTION_NAME: {
                Variable columnExpression =
                        (Variable) elementAtExpression.getArguments().get(0);
                Constant columnKey = (Constant) elementAtExpression.getArguments().get(1);

                Constant elementAtValue = (Constant) expression.getArguments().get(1);
                List<Range> values;
                Type elementType;
                if (inClause) {
                    elementType = ((ArrayType) elementAtValue.getType()).getElementType();
                    values =
                            elementAtValue.getChildren().stream()
                                    .filter(a -> ((Constant) a).getValue() != null)
                                    .map(
                                            arguemnt ->
                                                    Range.equal(
                                                            arguemnt.getType(),
                                                            ((Constant) arguemnt).getValue()))
                                    .collect(Collectors.toList());
                }
                else {
                    elementType = elementAtValue.getType();
                    values =
                            elementAtValue.getValue() == null
                                    ? Collections.emptyList()
                                    : ImmutableList.of(
                                    Range.equal(
                                            elementAtValue.getType(),
                                            elementAtValue.getValue()));
                }
                if (columnKey.getValue() == null) {
                    throw new RuntimeException("Expression pares failed: " + expression);
                }

                return handleElementAtArguments(
                        assignments,
                        columnExpression.getName(),
                        ((Slice) columnKey.getValue()).toStringUtf8(),
                        elementType,
                        values);
            }
            default: {
                return Collections.emptyMap();
            }
        }
    }

    /**
     * Using paimon, trino only supports element_at function to extract values from map type.
     */
    private static Map<PaimonColumnHandle, Domain> handleElementAtArguments(
            Map<String, ColumnHandle> assignments,
            String columnName,
            String nestedName,
            Type elementType,
            List<Range> ranges)
    {
        Map<PaimonColumnHandle, Domain> expressionPredicates = Maps.newHashMap();
        PaimonColumnHandle paimonColumnHandle = (PaimonColumnHandle) assignments.get(columnName);
        Type trinoType = paimonColumnHandle.getTrinoType();
        if (trinoType instanceof MapType) {
            expressionPredicates.put(
                    PaimonColumnHandle.of(
                            toMapKey(columnName, nestedName),
                            PaimonTypeUtils.toPaimonType(trinoType),
                            paimonColumnHandle.getColumnId()),
                    Domain.create(SortedRangeSet.copyOf(elementType, ranges), false));
        }
        return expressionPredicates;
    }

    /**
     * TrinoFilter for paimon trinoMetadata applyFilter.
     */
    public static class TrinoFilter
    {
        private final TupleDomain<PaimonColumnHandle> filter;
        private final TupleDomain<ColumnHandle> remainFilter;

        public TrinoFilter(
                TupleDomain<PaimonColumnHandle> filter, TupleDomain<ColumnHandle> remainFilter)
        {
            this.filter = filter;
            this.remainFilter = remainFilter;
        }

        public TupleDomain<PaimonColumnHandle> getFilter()
        {
            return filter;
        }

        public TupleDomain<ColumnHandle> getRemainFilter()
        {
            return remainFilter;
        }
    }
}

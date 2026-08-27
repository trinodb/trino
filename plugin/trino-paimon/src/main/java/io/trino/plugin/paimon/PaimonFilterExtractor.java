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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static java.util.Objects.requireNonNull;
import static org.apache.paimon.fileindex.FileIndexCommon.toMapKey;

public class PaimonFilterExtractor
{
    public static final String TRINO_MAP_ELEMENT_AT_FUNCTION_NAME = "element_at";

    private PaimonFilterExtractor() {}

    /**
     * Extract filter from trino, include ExpressionFilter.
     *
     * @param catalog
     *         the Trino catalog
     * @param paimonTableHandle
     *         the Trino table handle
     * @param constraint
     *         the constraint to extract filters from
     * @return an Optional containing the extracted TrinoFilter, or empty if no new
     *         filters
     */
    public static Optional<TrinoFilter> extract(
            Catalog catalog,
            PaimonTableHandle paimonTableHandle,
            ConnectorSession session,
            Constraint constraint)
    {
        requireNonNull(catalog, "catalog is null");
        requireNonNull(paimonTableHandle, "paimonTableHandle is null");
        requireNonNull(session, "session is null");
        requireNonNull(constraint, "constraint is null");
        Table table = PaimonTableHandle.schemaAwareReadTable(
                paimonTableHandle.tableWithDynamicOptions(catalog, session),
                !paimonTableHandle.usesHistoricalReadSchema(session));
        return extract(
                paimonTableHandle,
                constraint,
                PaimonTableHandle.effectiveReadRowType(table),
                table.partitionKeys(),
                !table.rowType().equals(PaimonTableHandle.effectiveReadRowType(table)));
    }

    static Optional<TrinoFilter> extract(
            PaimonTableHandle paimonTableHandle,
            Constraint constraint,
            RowType rowType,
            List<String> partitionKeys)
    {
        return extract(paimonTableHandle, constraint, rowType, partitionKeys, false);
    }

    static Optional<TrinoFilter> extract(
            PaimonTableHandle paimonTableHandle,
            Constraint constraint,
            RowType rowType,
            List<String> partitionKeys,
            boolean virtualRowTrackingColumns)
    {
        requireNonNull(paimonTableHandle, "paimonTableHandle is null");
        requireNonNull(constraint, "constraint is null");
        requireNonNull(rowType, "rowType is null");
        requireNonNull(partitionKeys, "partitionKeys is null");
        TupleDomain<PaimonColumnHandle> oldFilter = paimonTableHandle.getFilter();
        TupleDomain<PaimonColumnHandle> summaryFilter = constraint.getSummary().transformKeys(PaimonFilterExtractor::getSummaryColumn)
                .intersect(oldFilter);

        Map<PaimonColumnHandle, Domain> trinoColumnHandleForExpressionFilter = extractTrinoColumnHandleForExpressionFilter(
                constraint);

        LinkedHashMap<PaimonColumnHandle, Domain> acceptedDomains = new LinkedHashMap<>();
        LinkedHashMap<PaimonColumnHandle, Domain> unsupportedDomains = new LinkedHashMap<>();
        TupleDomain<PaimonColumnHandle> acceptedFilter;
        if (summaryFilter.isNone()) {
            acceptedFilter = TupleDomain.none();
        }
        else {
            new PaimonFilterConverter(rowType).convert(summaryFilter, acceptedDomains, unsupportedDomains);
            if (virtualRowTrackingColumns) {
                moveVirtualEngineOnlyDomainsToUnsupported(acceptedDomains, unsupportedDomains);
            }
            acceptedFilter = TupleDomain.withColumnDomains(acceptedDomains);
        }

        LinkedHashMap<PaimonColumnHandle, Domain> unenforcedDomains = new LinkedHashMap<>();
        Set<String> partitionKeyNames = FieldNameUtils.toLowerCase(partitionKeys).stream()
                .collect(Collectors.toUnmodifiableSet());
        acceptedDomains.forEach((columnHandle, domain) -> {
            if (!partitionKeyNames.contains(FieldNameUtils.toLowerCase(columnHandle.getColumnName()))) {
                unenforcedDomains.put(columnHandle, domain);
            }
        });
        List<PaimonColumnHandle> pushedFilterColumns = new ArrayList<>(acceptedDomains.keySet());
        pushedFilterColumns.addAll(trinoColumnHandleForExpressionFilter.keySet());
        boolean partitionOnlyPushedFilter = pushedFilterColumns.stream()
                .map(PaimonColumnHandle::getColumnName)
                .map(FieldNameUtils::toLowerCase)
                .allMatch(partitionKeyNames::contains);

        TupleDomain<PaimonColumnHandle> expressionFilter = TupleDomain.withColumnDomains(
                trinoColumnHandleForExpressionFilter);
        TupleDomain<PaimonColumnHandle> newFilter = oldFilter.intersect(acceptedFilter).intersect(expressionFilter);

        if (oldFilter.equals(newFilter)) {
            return Optional.empty();
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        TupleDomain<ColumnHandle> remain = (TupleDomain) TupleDomain.withColumnDomains(unsupportedDomains)
                .intersect(TupleDomain.withColumnDomains(unenforcedDomains));

        ConnectorExpression remainingExpression = trinoColumnHandleForExpressionFilter.isEmpty() && remain.isAll()
                ? Constant.TRUE
                : constraint.getExpression();

        return Optional
                .of(new TrinoFilter(newFilter, remain, remainingExpression, partitionOnlyPushedFilter));
    }

    private static void moveVirtualEngineOnlyDomainsToUnsupported(
            LinkedHashMap<PaimonColumnHandle, Domain> acceptedDomains,
            LinkedHashMap<PaimonColumnHandle, Domain> unsupportedDomains)
    {
        acceptedDomains.entrySet().removeIf(entry -> {
            if (!PaimonColumnHandle.isHiddenColumnName(entry.getKey().getColumnName())) {
                return false;
            }
            if (isPaimonRowIdColumn(entry.getKey())) {
                return false;
            }
            unsupportedDomains.put(entry.getKey(), entry.getValue());
            return true;
        });
    }

    private static boolean isPaimonRowIdColumn(PaimonColumnHandle columnHandle)
    {
        requireNonNull(columnHandle, "columnHandle is null");
        return PaimonColumnHandle.PAIMON_ROW_ID_NAME.equalsIgnoreCase(columnHandle.getColumnName());
    }

    private static PaimonColumnHandle getSummaryColumn(ColumnHandle column)
    {
        if (!(requireNonNull(column, "constraint summary contains null column") instanceof PaimonColumnHandle paimonColumnHandle)) {
            throw new IllegalStateException("Paimon filter extraction requires PaimonColumnHandle, got: "
                    + column.getClass().getName());
        }
        return paimonColumnHandle;
    }

    /**
     * Extract Expression filter from trino Constraint. Extract Trino Expression
     * filter ( e.g. element_at(jsonmap, 'a') = '1' ) to PaimonColumnHandle.
     *
     * @param constraint
     *         the constraint to extract expression filters from
     * @return a map of PaimonColumnHandle to Domain representing the extracted
     *         expression filters
     */
    public static Map<PaimonColumnHandle, Domain> extractTrinoColumnHandleForExpressionFilter(Constraint constraint)
    {
        requireNonNull(constraint, "constraint is null");
        return extractExpressionPredicates(constraint.getAssignments(), constraint.getExpression());
    }

    private static Map<PaimonColumnHandle, Domain> extractExpressionPredicates(
            Map<String, ColumnHandle> assignments,
            ConnectorExpression expression)
    {
        if (!(expression instanceof Call call)) {
            return Collections.emptyMap();
        }
        if (call.getFunctionName().equals(EQUAL_OPERATOR_FUNCTION_NAME)) {
            return handleExpressionEqualOrIn(assignments, call, false);
        }
        if (call.getFunctionName().equals(IN_PREDICATE_FUNCTION_NAME)) {
            return handleExpressionEqualOrIn(assignments, call, true);
        }
        if (call.getFunctionName().equals(AND_FUNCTION_NAME)) {
            return handleAndArguments(assignments, call);
        }
        if (call.getFunctionName().equals(OR_FUNCTION_NAME)) {
            return handleOrArguments(assignments, call);
        }
        return Collections.emptyMap();
    }

    /**
     * Expression filter support the case of "AND" and "IN".
     */
    private static Map<PaimonColumnHandle, Domain> handleAndArguments(
            Map<String, ColumnHandle> assignments,
            Call expression)
    {
        Map<PaimonColumnHandle, Domain> expressionPredicates = new LinkedHashMap<>();

        expression.getArguments().forEach(argument ->
                mergeConjuncts(expressionPredicates, extractExpressionPredicates(assignments, argument)));

        return expressionPredicates;
    }

    private static void mergeConjuncts(
            Map<PaimonColumnHandle, Domain> target,
            Map<PaimonColumnHandle, Domain> conjuncts)
    {
        conjuncts.forEach((column, domain) -> target.merge(column, domain, Domain::intersect));
    }

    /**
     * Expression filter support for "OR" clause. Handles OR expressions by
     * combining domains for the same column.
     */
    private static Map<PaimonColumnHandle, Domain> handleOrArguments(
            Map<String, ColumnHandle> assignments,
            Call expression)
    {
        Map<PaimonColumnHandle, Domain> combinedPredicates = new LinkedHashMap<>();
        Set<PaimonColumnHandle> extractedColumns = Set.of();

        // Collect all predicates from OR arguments
        for (ConnectorExpression argument : expression.getArguments()) {
            Map<PaimonColumnHandle, Domain> argumentPredicates = extractExpressionPredicates(assignments, argument);
            if (argumentPredicates.isEmpty()) {
                return Collections.emptyMap();
            }
            if (extractedColumns.isEmpty()) {
                extractedColumns = Set.copyOf(argumentPredicates.keySet());
            }
            else if (!extractedColumns.equals(Set.copyOf(argumentPredicates.keySet()))) {
                return Collections.emptyMap();
            }

            // Merge domains for the same columns
            for (Map.Entry<PaimonColumnHandle, Domain> entry : argumentPredicates.entrySet()) {
                PaimonColumnHandle column = entry.getKey();
                Domain domain = entry.getValue();

                combinedPredicates.merge(column, domain, Domain::union);
            }
        }

        return combinedPredicates;
    }

    private static Map<PaimonColumnHandle, Domain> handleExpressionEqualOrIn(
            Map<String, ColumnHandle> assignments,
            Call expression,
            boolean inClause)
    {
        if (expression.getArguments().size() != 2) {
            return Collections.emptyMap();
        }
        ConnectorExpression left = expression.getArguments().get(0);
        ConnectorExpression right = expression.getArguments().get(1);
        ConnectorExpression elementAtArgument = left;
        ConnectorExpression comparisonValue = right;
        if (!inClause && right instanceof Call && !(left instanceof Call)) {
            elementAtArgument = right;
            comparisonValue = left;
        }
        if (!(elementAtArgument instanceof Call elementAtExpression)) {
            return Collections.emptyMap();
        }

        String functionName = elementAtExpression.getFunctionName().getName();

        return switch (functionName) {
            case TRINO_MAP_ELEMENT_AT_FUNCTION_NAME -> {
                if (elementAtExpression.getArguments().size() != 2) {
                    yield Collections.emptyMap();
                }
                if (!(elementAtExpression.getArguments().get(0) instanceof Variable columnExpression)) {
                    yield Collections.emptyMap();
                }
                if (!(elementAtExpression.getArguments().get(1) instanceof Constant columnKey)) {
                    yield Collections.emptyMap();
                }

                List<Range> values;
                Type elementType;
                if (inClause) {
                    if (!(comparisonValue instanceof Call arrayExpression)
                            || !arrayExpression.getFunctionName().equals(ARRAY_CONSTRUCTOR_FUNCTION_NAME)
                            || !(arrayExpression.getType() instanceof ArrayType arrayType)) {
                        yield Collections.emptyMap();
                    }
                    elementType = arrayType.getElementType();
                    List<ConnectorExpression> arrayArguments = arrayExpression.getArguments();
                    values = new ArrayList<>(arrayArguments.size());
                    for (ConnectorExpression argument : arrayArguments) {
                        if (!(argument instanceof Constant constant)
                                || constant.getValue() == null
                                || !constant.getType().equals(elementType)) {
                            yield Collections.emptyMap();
                        }
                        values.add(Range.equal(elementType, constant.getValue()));
                    }
                }
                else {
                    if (!(comparisonValue instanceof Constant elementAtValue)) {
                        yield Collections.emptyMap();
                    }
                    elementType = elementAtValue.getType();
                    values = elementAtValue.getValue() == null
                            ? Collections.emptyList()
                            : ImmutableList.of(Range.equal(elementAtValue.getType(), elementAtValue.getValue()));
                }
                if (values.isEmpty()) {
                    yield Collections.emptyMap();
                }
                if (!(columnKey.getValue() instanceof Slice nestedName)) {
                    yield Collections.emptyMap();
                }

                yield handleElementAtArguments(
                        assignments,
                        columnExpression.getName(),
                        nestedName.toStringUtf8(),
                        elementType,
                        values);
            }
            default -> Collections.emptyMap();
        };
    }

    /**
     * Using paimon, trino only supports element_at function to extract values from
     * map type.
     */
    private static Map<PaimonColumnHandle, Domain> handleElementAtArguments(
            Map<String, ColumnHandle> assignments,
            String columnName,
            String nestedName,
            Type elementType,
            List<Range> ranges)
    {
        Map<PaimonColumnHandle, Domain> expressionPredicates = new LinkedHashMap<>();
        if (!(assignments.get(columnName) instanceof PaimonColumnHandle paimonColumnHandle)) {
            return expressionPredicates;
        }
        Type trinoType = paimonColumnHandle.getTrinoType();
        if (paimonColumnHandle.logicalType().getTypeRoot() == DataTypeRoot.MAP
                && trinoType instanceof MapType mapType
                && elementType.equals(mapType.getValueType())) {
            expressionPredicates.put(
                    PaimonColumnHandle.of(toMapKey(paimonColumnHandle.getColumnName(), nestedName),
                            paimonColumnHandle.logicalType(),
                            paimonColumnHandle.getTrinoType()),
                    Domain.create(SortedRangeSet.copyOf(elementType, ranges), false));
        }
        return expressionPredicates;
    }

    /**
     * TrinoFilter for paimon trinoMetadata applyFilter.
     */
    public record TrinoFilter(
            TupleDomain<PaimonColumnHandle> filter,
            TupleDomain<ColumnHandle> remainFilter,
            ConnectorExpression remainingExpression,
            boolean partitionOnlyPushedFilter) {}
}

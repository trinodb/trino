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
package io.trino.plugin.iceberg.gpu;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.TableType;
import io.trino.plugin.iceberg.gpu.engine.DeviceTable;
import io.trino.plugin.iceberg.gpu.engine.GpuEngine;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.plugin.iceberg.gpu.engine.DeviceTable.EQ;
import static io.trino.plugin.iceberg.gpu.engine.DeviceTable.GE;
import static io.trino.plugin.iceberg.gpu.engine.DeviceTable.GT;
import static io.trino.plugin.iceberg.gpu.engine.DeviceTable.LE;
import static io.trino.plugin.iceberg.gpu.engine.DeviceTable.LT;
import static io.trino.spi.expression.StandardFunctions.ADD_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.MULTIPLY_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NEGATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.SUBTRACT_FUNCTION_NAME;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.util.Objects.requireNonNull;

/**
 * The Iceberg connector's metadata with three operations taken over, when TornadoVM is available.
 *
 * <ul>
 *   <li>{@code applyFilter}: single-range predicates over null-free numeric and date columns are
 *       enforced by this connector, on the device under a pushed-down aggregation and on the scan's
 *       pages otherwise, so that an aggregation over a filtered table can be pushed down at all.
 *   <li>{@code applyProjection}: products of affine factors of null-free DOUBLE columns become
 *       columns this connector computes.
 *   <li>{@code applyAggregation}: {@code sum}, {@code avg} and {@code count}, grouped by up to four
 *       columns, run on the device when the snapshot's data files can be read there.
 * </ul>
 *
 * Every other call goes to the Iceberg connector with this connector's handles unwrapped.
 */
final class IcebergGpuMetadata
        implements InvocationHandler
{
    private static final Logger log = Logger.get(IcebergGpuMetadata.class);

    static final int FACTORS = 4;
    private static final int MAX_GROUP_KEYS = 4;

    /**
     * Pushdowns that would change what the scan returns: declined on a handle this connector extended.
     */
    private static final Set<String> DECLINED = ImmutableSet.of("applyLimit", "applyTopN", "applySample", "applyTableScanRedirect", "applyJoin", "applyTableFunction");

    /**
     * Snapshot and string column to whether all its values fit a device group key; decided once.
     */
    private static final Map<String, Boolean> STRING_KEYS_FIT = new ConcurrentHashMap<>();

    private final ConnectorMetadata delegate;
    private final ConnectorSplitManager splitManager;
    private final ConnectorTransactionHandle transaction;
    private final boolean deviceAvailable;
    private final Map<String, Optional<DataFiles.Listing>> listings = new HashMap<>();

    private IcebergGpuMetadata(ConnectorMetadata delegate, ConnectorSplitManager splitManager, ConnectorTransactionHandle transaction, boolean deviceAvailable)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.transaction = requireNonNull(transaction, "transaction is null");
        this.deviceAvailable = deviceAvailable;
    }

    static ConnectorMetadata wrap(ConnectorMetadata delegate, ConnectorSplitManager splitManager, ConnectorTransactionHandle transaction, boolean deviceAvailable)
    {
        return (ConnectorMetadata) Proxy.newProxyInstance(
                IcebergGpuMetadata.class.getClassLoader(),
                new Class<?>[] {ConnectorMetadata.class},
                new IcebergGpuMetadata(delegate, splitManager, transaction, deviceAvailable));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable
    {
        if (method.getDeclaringClass() == Object.class) {
            return switch (method.getName()) {
                case "equals" -> proxy == args[0];
                case "hashCode" -> System.identityHashCode(proxy);
                default -> "IcebergGpuMetadata(" + delegate + ")";
            };
        }
        String name = method.getName();
        int arity = args == null ? 0 : args.length;
        if (deviceAvailable && name.equals("applyFilter") && arity == 3) {
            return applyFilter((ConnectorSession) args[0], (ConnectorTableHandle) args[1], (Constraint) args[2]);
        }
        if (deviceAvailable && name.equals("applyProjection") && arity == 4) {
            return applyProjection((ConnectorSession) args[0], (ConnectorTableHandle) args[1], (List<ConnectorExpression>) args[2], (Map<String, ColumnHandle>) args[3]);
        }
        if (deviceAvailable && name.equals("applyAggregation") && arity == 5) {
            return applyAggregation(
                    (ConnectorSession) args[0],
                    (ConnectorTableHandle) args[1],
                    (List<AggregateFunction>) args[2],
                    (Map<String, ColumnHandle>) args[3],
                    (List<List<ColumnHandle>>) args[4]);
        }
        if (DECLINED.contains(name) && hasGpuHandle(args)) {
            return Optional.empty();
        }
        if (name.equals("getColumnMetadata") && arity == 3 && args[2] instanceof GpuColumnHandle column) {
            return new ColumnMetadata(column.name(), column.type());
        }
        if (name.equals("getTableProperties") && arity == 2 && args[1] instanceof GpuTableHandle gpu && gpu.aggregation().isPresent()) {
            return new ConnectorTableProperties();
        }
        Object[] unwrapped = null;
        if (args != null) {
            unwrapped = args.clone();
            for (int i = 0; i < arity; i++) {
                if (unwrapped[i] instanceof GpuTableHandle gpu) {
                    unwrapped[i] = gpu.base();
                }
            }
        }
        try {
            return method.invoke(delegate, unwrapped);
        }
        catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private static boolean hasGpuHandle(Object[] args)
    {
        if (args == null) {
            return false;
        }
        for (Object arg : args) {
            if (arg instanceof GpuTableHandle) {
                return true;
            }
        }
        return false;
    }

    /**
     * The snapshot's data files, when the handle reads a whole data table the device can read.
     */
    private Optional<DataFiles.Listing> deviceFiles(ConnectorSession session, ConnectorTableHandle base)
    {
        if (!(base instanceof IcebergTableHandle iceberg) || iceberg.getTableType() != TableType.DATA || iceberg.getSnapshotId().isEmpty()) {
            return Optional.empty();
        }
        String key = iceberg.getSchemaTableName() + "@" + iceberg.getSnapshotId().orElseThrow();
        return listings.computeIfAbsent(key, _ -> {
            IcebergTableHandle whole = iceberg;
            if (!iceberg.getEnforcedPredicate().isAll()) {
                // the files of a pruned scan are not the table's: list the snapshot without pruning
                return Optional.empty();
            }
            return DataFiles.list(splitManager, transaction, session, whole);
        });
    }

    /**
     * The names of the columns the table's statistics count no nulls in.
     */
    private Set<String> nullFreeColumns(ConnectorSession session, ConnectorTableHandle base)
    {
        TableStatistics statistics = delegate.getTableStatistics(session, base);
        ImmutableSet.Builder<String> names = ImmutableSet.builder();
        for (Map.Entry<ColumnHandle, ColumnStatistics> entry : statistics.getColumnStatistics().entrySet()) {
            if (entry.getKey() instanceof IcebergColumnHandle column
                    && !entry.getValue().getNullsFraction().isUnknown()
                    && entry.getValue().getNullsFraction().getValue() == 0) {
                names.add(column.getName());
            }
        }
        return names.build();
    }

    private Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        GpuTableHandle gpu = handle instanceof GpuTableHandle extended ? extended : null;
        if (gpu != null && gpu.aggregation().isPresent()) {
            return Optional.empty();
        }
        ConnectorTableHandle base = GpuTableHandle.unwrap(handle);
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> iceberg = delegate.applyFilter(session, base, constraint);
        ConnectorTableHandle newBase = iceberg.map(ConstraintApplicationResult::getHandle).orElse(base);

        List<GpuPredicate> predicates = new ArrayList<>();
        Map<String, ColumnHandle> sources = new LinkedHashMap<>();
        if (gpu != null) {
            predicates.addAll(gpu.predicates());
            sources.putAll(gpu.sources());
        }
        Map<ColumnHandle, Domain> remaining = new HashMap<>();
        boolean claimed = false;
        if (base instanceof IcebergTableHandle && constraint.getSummary().getDomains().isPresent()) {
            Set<String> nullFree = nullFreeColumns(session, base);
            for (Map.Entry<ColumnHandle, Domain> entry : constraint.getSummary().getDomains().get().entrySet()) {
                Optional<List<GpuPredicate>> translated = Optional.empty();
                if (entry.getKey() instanceof IcebergColumnHandle column && nullFree.contains(column.getName())) {
                    translated = predicates(column, entry.getValue());
                }
                if (translated.isEmpty()) {
                    remaining.put(entry.getKey(), entry.getValue());
                    continue;
                }
                predicates.addAll(translated.get());
                sources.put(((IcebergColumnHandle) entry.getKey()).getName(), entry.getKey());
                claimed = true;
            }
        }
        if (!claimed) {
            if (iceberg.isEmpty()) {
                return Optional.empty();
            }
            // Iceberg's own result; this connector's earlier predicates stay with the handle.
            ConstraintApplicationResult<ConnectorTableHandle> result = iceberg.get();
            ConnectorTableHandle newHandle = result.getHandle();
            if (gpu != null) {
                newHandle = new GpuTableHandle(result.getHandle(), gpu.predicates(), gpu.sources(), Optional.empty());
            }
            return Optional.of(new ConstraintApplicationResult<>(
                    newHandle,
                    result.getRemainingFilter(),
                    result.getRemainingExpression().orElse(constraint.getExpression()),
                    result.isPrecalculateStatistics()));
        }
        TupleDomain<ColumnHandle> remainingFilter = TupleDomain.withColumnDomains(remaining);
        if (constraint.getSummary().isNone()) {
            remainingFilter = TupleDomain.none();
        }
        return Optional.of(new ConstraintApplicationResult<>(
                new GpuTableHandle(newBase, predicates, sources, Optional.empty()),
                remainingFilter,
                constraint.getExpression(),
                false));
    }

    /**
     * A domain as predicates: one range over a numeric or date column.
     */
    private static Optional<List<GpuPredicate>> predicates(IcebergColumnHandle column, Domain domain)
    {
        Type type = column.getType();
        if (!column.isBaseColumn() || !(type instanceof DoubleType || type instanceof BigintType || type instanceof IntegerType || type instanceof DateType)) {
            return Optional.empty();
        }
        if (domain.isNone() || domain.getValues().isAll() || domain.isNullAllowed()) {
            return Optional.empty();
        }
        List<Range> ranges = domain.getValues().getRanges().getOrderedRanges();
        if (ranges.size() != 1) {
            return Optional.empty();
        }
        Range range = ranges.getFirst();
        String name = column.getName();
        if (range.isSingleValue()) {
            return Optional.of(ImmutableList.of(new GpuPredicate(name, EQ, number(range.getSingleValue()))));
        }
        ImmutableList.Builder<GpuPredicate> predicates = ImmutableList.builder();
        if (!range.isLowUnbounded()) {
            predicates.add(new GpuPredicate(name, range.isLowInclusive() ? GE : GT, number(range.getLowBoundedValue())));
        }
        if (!range.isHighUnbounded()) {
            predicates.add(new GpuPredicate(name, range.isHighInclusive() ? LE : LT, number(range.getHighBoundedValue())));
        }
        return Optional.of(predicates.build());
    }

    private static double number(Object value)
    {
        if (value instanceof Double doubleValue) {
            return doubleValue;
        }
        return ((Number) value).doubleValue();
    }

    private Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        GpuTableHandle gpu = handle instanceof GpuTableHandle extended ? extended : null;
        if (gpu != null && gpu.aggregation().isPresent()) {
            return Optional.empty();
        }
        boolean ownColumns = assignments.values().stream().anyMatch(GpuColumnHandle.class::isInstance);
        ConnectorTableHandle base = GpuTableHandle.unwrap(handle);
        Set<String> nullFree = ImmutableSet.of();
        if (base instanceof IcebergTableHandle && projections.stream().anyMatch(Call.class::isInstance)) {
            nullFree = nullFreeColumns(session, base);
        }
        List<ConnectorExpression> newProjections = new ArrayList<>();
        Map<String, Assignment> newAssignments = new LinkedHashMap<>();
        Map<String, ColumnHandle> sources = new LinkedHashMap<>();
        if (gpu != null) {
            sources.putAll(gpu.sources());
        }
        boolean pushed = false;
        int index = 0;
        for (ConnectorExpression projection : projections) {
            if (projection instanceof Variable variable) {
                newAssignments.putIfAbsent(variable.getName(), new Assignment(variable.getName(), assignments.get(variable.getName()), variable.getType()));
                newProjections.add(variable);
                continue;
            }
            Optional<List<Factor>> factors = factors(projection, assignments, nullFree);
            if (factors.isEmpty() || factors.get().size() > FACTORS || !(projection.getType() instanceof DoubleType)) {
                if (ownColumns) {
                    return Optional.empty();
                }
                return icebergProjection(session, handle, gpu, projections, assignments);
            }
            ImmutableList.Builder<String> columns = ImmutableList.builder();
            ImmutableList.Builder<Double> a = ImmutableList.builder();
            ImmutableList.Builder<Double> b = ImmutableList.builder();
            for (Factor factor : factors.get()) {
                columns.add(factor.column().map(IcebergColumnHandle::getName).orElse(""));
                a.add(factor.a());
                b.add(factor.b());
                factor.column().ifPresent(column -> sources.put(column.getName(), column));
            }
            String name = "expression$" + index++ + "$" + Integer.toHexString(projection.toString().hashCode());
            GpuColumnHandle expression = GpuColumnHandle.expressionColumn(name, new GpuExpression(columns.build(), a.build(), b.build()));
            newAssignments.put(name, new Assignment(name, expression, DOUBLE));
            newProjections.add(new Variable(name, DOUBLE));
            pushed = true;
        }
        if (!pushed) {
            if (ownColumns) {
                return Optional.empty();
            }
            return icebergProjection(session, handle, gpu, projections, assignments);
        }
        List<GpuPredicate> predicates = ImmutableList.of();
        if (gpu != null) {
            predicates = gpu.predicates();
        }
        GpuTableHandle result = new GpuTableHandle(base, predicates, sources, Optional.empty());
        return Optional.of(new ProjectionApplicationResult<>(result, newProjections, ImmutableList.copyOf(newAssignments.values()), false));
    }

    /**
     * The Iceberg connector's projection pushdown, keeping this connector's predicates.
     */
    private Optional<ProjectionApplicationResult<ConnectorTableHandle>> icebergProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            GpuTableHandle gpu,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        Optional<ProjectionApplicationResult<ConnectorTableHandle>> result = delegate.applyProjection(session, GpuTableHandle.unwrap(handle), projections, assignments);
        if (result.isEmpty() || gpu == null) {
            return result;
        }
        ProjectionApplicationResult<ConnectorTableHandle> projection = result.get();
        return Optional.of(new ProjectionApplicationResult<>(
                new GpuTableHandle(projection.getHandle(), gpu.predicates(), gpu.sources(), Optional.empty()),
                projection.getProjections(),
                projection.getAssignments(),
                projection.isPrecalculateStatistics()));
    }

    /**
     * One factor {@code a * column + b}; without a column, the constant {@code a + b}.
     */
    private record Factor(Optional<IcebergColumnHandle> column, double a, double b) {}

    /**
     * The expression as a product of affine factors over null-free DOUBLE base columns.
     */
    private static Optional<List<Factor>> factors(ConnectorExpression expression, Map<String, ColumnHandle> assignments, Set<String> nullFree)
    {
        if (expression instanceof Constant constant && constant.getValue() instanceof Number number && constant.getType() instanceof DoubleType) {
            return Optional.of(ImmutableList.of(new Factor(Optional.empty(), 0, number.doubleValue())));
        }
        if (expression instanceof Variable variable) {
            if (assignments.get(variable.getName()) instanceof IcebergColumnHandle column
                    && column.isBaseColumn()
                    && column.getType() instanceof DoubleType
                    && nullFree.contains(column.getName())) {
                return Optional.of(ImmutableList.of(new Factor(Optional.of(column), 1, 0)));
            }
            return Optional.empty();
        }
        if (!(expression instanceof Call call)) {
            return Optional.empty();
        }
        FunctionName function = call.getFunctionName();
        List<ConnectorExpression> arguments = call.getArguments();
        if (function.equals(MULTIPLY_FUNCTION_NAME)) {
            Optional<List<Factor>> left = factors(arguments.get(0), assignments, nullFree);
            Optional<List<Factor>> right = factors(arguments.get(1), assignments, nullFree);
            if (left.isEmpty() || right.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(ImmutableList.<Factor>builder().addAll(left.get()).addAll(right.get()).build());
        }
        if (function.equals(ADD_FUNCTION_NAME) || function.equals(SUBTRACT_FUNCTION_NAME)) {
            Optional<Factor> left = affine(arguments.get(0), assignments, nullFree);
            Optional<Factor> right = affine(arguments.get(1), assignments, nullFree);
            if (left.isEmpty() || right.isEmpty() || (left.get().column().isPresent() && right.get().column().isPresent())) {
                return Optional.empty();
            }
            double sign = function.equals(ADD_FUNCTION_NAME) ? 1 : -1;
            Optional<IcebergColumnHandle> column = left.get().column().or(() -> right.get().column());
            return Optional.of(ImmutableList.of(new Factor(column, left.get().a() + sign * right.get().a(), left.get().b() + sign * right.get().b())));
        }
        if (function.equals(NEGATE_FUNCTION_NAME)) {
            return affine(arguments.get(0), assignments, nullFree)
                    .map(inner -> ImmutableList.of(new Factor(inner.column(), -inner.a(), -inner.b())));
        }
        if (function.equals(CAST_FUNCTION_NAME) && arguments.get(0).getType() instanceof DoubleType) {
            return factors(arguments.get(0), assignments, nullFree);
        }
        if (function.equals(CAST_FUNCTION_NAME)
                && arguments.get(0) instanceof Constant constant
                && constant.getValue() instanceof Long value
                && expression.getType() instanceof DoubleType) {
            return Optional.of(ImmutableList.of(new Factor(Optional.empty(), 0, value.doubleValue())));
        }
        return Optional.empty();
    }

    /**
     * The expression as one affine factor ({@code a*x+b} or a constant).
     */
    private static Optional<Factor> affine(ConnectorExpression expression, Map<String, ColumnHandle> assignments, Set<String> nullFree)
    {
        Optional<List<Factor>> factors = factors(expression, assignments, nullFree);
        if (factors.isEmpty() || factors.get().size() != 1) {
            return Optional.empty();
        }
        Factor factor = factors.get().getFirst();
        if (factor.column().isEmpty()) {
            return Optional.of(new Factor(Optional.empty(), 0, factor.a() + factor.b()));
        }
        return Optional.of(factor);
    }

    private Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        GpuTableHandle gpu = handle instanceof GpuTableHandle extended ? extended : null;
        if ((gpu != null && gpu.aggregation().isPresent()) || groupingSets.size() != 1 || groupingSets.getFirst().size() > MAX_GROUP_KEYS) {
            return Optional.empty();
        }
        ConnectorTableHandle base = GpuTableHandle.unwrap(handle);
        if (!(base instanceof IcebergTableHandle icebergTable) || icebergTable.getLimit().isPresent()) {
            return Optional.empty();
        }
        List<GpuPredicate> predicates = ImmutableList.of();
        Map<String, ColumnHandle> sources = ImmutableMap.of();
        if (gpu != null) {
            predicates = gpu.predicates();
            sources = gpu.sources();
        }
        // count(*) alone over the whole table: Iceberg answers it from the manifests.
        if (groupingSets.getFirst().isEmpty()
                && predicates.isEmpty()
                && aggregates.stream().allMatch(function -> function.getFunctionName().equals("count") && function.getArguments().isEmpty())) {
            return Optional.empty();
        }
        // The device reads every data file of the snapshot: it cannot honour Iceberg's pruning.
        Optional<DataFiles.Listing> listing = deviceFiles(session, base);
        if (listing.isEmpty()) {
            return Optional.empty();
        }
        Set<String> nullFree = nullFreeColumns(session, base);
        Map<String, GpuColumnHandle> columns = new LinkedHashMap<>();
        for (Map.Entry<String, ColumnHandle> source : sources.entrySet()) {
            Optional<GpuColumnHandle> column = deviceColumn((IcebergColumnHandle) source.getValue(), listing.get());
            if (column.isEmpty()) {
                return Optional.empty();
            }
            columns.put(source.getKey(), column.get());
        }

        ImmutableList.Builder<GpuColumnHandle> groupBy = ImmutableList.builder();
        for (ColumnHandle groupKey : groupingSets.getFirst()) {
            if (!(groupKey instanceof IcebergColumnHandle iceberg) || !nullFree.contains(iceberg.getName())) {
                return Optional.empty();
            }
            Optional<GpuColumnHandle> column = deviceColumn(iceberg, listing.get());
            if (column.isEmpty() || column.get().kind().orElseThrow() == DeviceTable.Kind.DOUBLE) {
                return Optional.empty();
            }
            if (column.get().kind().orElseThrow() == DeviceTable.Kind.PACKED_STRING && !stringKeyFits(listing.get(), column.get())) {
                return Optional.empty();
            }
            groupBy.add(column.get());
            columns.put(column.get().name(), column.get());
        }

        ImmutableList.Builder<GpuColumnHandle> outputs = ImmutableList.builder();
        ImmutableList.Builder<ConnectorExpression> projections = ImmutableList.builder();
        ImmutableList.Builder<Assignment> newAssignments = ImmutableList.builder();
        for (int i = 0; i < aggregates.size(); i++) {
            AggregateFunction function = aggregates.get(i);
            if (function.isDistinct() || function.getFilter().isPresent() || !function.getSortItems().isEmpty()) {
                return Optional.empty();
            }
            Optional<GpuColumnHandle> argument = Optional.empty();
            if (!function.getArguments().isEmpty()) {
                if (function.getArguments().size() != 1 || !(function.getArguments().getFirst() instanceof Variable variable)) {
                    return Optional.empty();
                }
                ColumnHandle column = assignments.get(variable.getName());
                if (column instanceof GpuColumnHandle expression) {
                    argument = Optional.of(expression);
                }
                else if (column instanceof IcebergColumnHandle iceberg && nullFree.contains(iceberg.getName())) {
                    argument = deviceColumn(iceberg, listing.get());
                    argument.ifPresent(tableColumn -> columns.put(tableColumn.name(), tableColumn));
                }
                if (argument.isEmpty() || (!(argument.get().type() instanceof DoubleType) && !function.getFunctionName().equals("count"))) {
                    return Optional.empty();
                }
            }
            Type output = function.getOutputType();
            boolean supported = switch (function.getFunctionName()) {
                case "sum", "avg" -> argument.isPresent() && output instanceof DoubleType;
                case "count" -> output instanceof BigintType;
                default -> false;
            };
            if (!supported) {
                return Optional.empty();
            }
            String name = "aggregate$" + i;
            GpuColumnHandle aggregate = GpuColumnHandle.aggregateColumn(name, output, new GpuAggregate(function.getFunctionName(), argument));
            outputs.add(aggregate);
            projections.add(new Variable(name, output));
            newAssignments.add(new Assignment(name, aggregate, output));
        }
        // The group keys stay the scan's Iceberg columns: Trino maps grouping symbols to the scan's
        // assignments, and the page source finds the keys among the aggregation's by name.
        GpuTableHandle aggregated = new GpuTableHandle(base, predicates, sources, Optional.of(new GpuAggregation(groupBy.build(), outputs.build(), columns)));
        return Optional.of(new AggregationApplicationResult<>(aggregated, projections.build(), newAssignments.build(), ImmutableMap.of(), false));
    }

    /**
     * A table column as the device reads it, when its type is one the device holds.
     */
    private static Optional<GpuColumnHandle> deviceColumn(IcebergColumnHandle column, DataFiles.Listing listing)
    {
        if (!column.isBaseColumn()) {
            return Optional.empty();
        }
        Integer fileIndex = listing.columnIndices().get(column.getId());
        Optional<DeviceTable.Kind> kind = kind(column.getType());
        if (fileIndex == null || kind.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(GpuColumnHandle.tableColumn(column.getName(), column.getType(), fileIndex, kind.get()));
    }

    private static Optional<DeviceTable.Kind> kind(Type type)
    {
        if (type instanceof IntegerType || type instanceof DateType) {
            return Optional.of(DeviceTable.Kind.INT);
        }
        if (type instanceof BigintType) {
            return Optional.of(DeviceTable.Kind.LONG);
        }
        if (type instanceof DoubleType) {
            return Optional.of(DeviceTable.Kind.DOUBLE);
        }
        if (type instanceof VarcharType) {
            return Optional.of(DeviceTable.Kind.PACKED_STRING);
        }
        return Optional.empty();
    }

    /**
     * Whether a string column can be a device group key: every value in 8 bytes. A failed check is a no.
     */
    private static boolean stringKeyFits(DataFiles.Listing listing, GpuColumnHandle column)
    {
        return STRING_KEYS_FIT.computeIfAbsent(listing.key() + "#" + column.name(), _ -> {
            try {
                return GpuEngine.instance().fits(
                        IcebergGpuPageSourceProvider.engineTable(listing.key(), listing.files(), listing.rows()),
                        new DeviceTable.Column(column.fileIndex(), DeviceTable.Kind.PACKED_STRING));
            }
            catch (RuntimeException e) {
                log.warn(e, "Group key %s stays with Iceberg: the device could not check its values", column.name());
                return false;
            }
        });
    }
}

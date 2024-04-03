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
package io.trino.sql.planner.iterative.rule;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.errorprone.annotations.FormatMethod;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.geospatial.KdbTree;
import io.trino.geospatial.KdbTreeUtils;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.split.SplitSource;
import io.trino.split.SplitSource.SplitBatch;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.BuiltinFunctionCallBuilder;
import io.trino.sql.planner.ResolvedFunctionCallBuilder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.Rule.Context;
import io.trino.sql.planner.iterative.Rule.Result;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.UnnestNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.SystemSessionProperties.getSpatialPartitioningTableName;
import static io.trino.SystemSessionProperties.isSpatialJoinEnabled;
import static io.trino.matching.Capture.newCapture;
import static io.trino.spi.StandardErrorCode.INVALID_SPATIAL_PARTITIONING;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.util.SpatialJoinUtils.extractSupportedSpatialComparisons;
import static io.trino.util.SpatialJoinUtils.extractSupportedSpatialFunctions;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Applies to broadcast spatial joins, inner and left, expressed via ST_Contains,
 * ST_Intersects and ST_Distance functions.
 * <p>
 * For example:
 * <ul>
 * <li>{@code SELECT ... FROM a, b WHERE ST_Contains(b.geometry, a.geometry)}</li>
 * <li>{@code SELECT ... FROM a, b WHERE ST_Intersects(b.geometry, a.geometry)}</li>
 * <li>{@code SELECT ... FROM a, b WHERE ST_Distance(b.geometry, a.geometry) <= 300}</li>
 * <li>{@code SELECT ... FROM a, b WHERE 15.5 > ST_Distance(b.geometry, a.geometry)}</li>
 * </ul>
 * <p>
 * Joins expressed via ST_Contains and ST_Intersects functions must match all of
 * the following criteria:
 * <p>
 * - arguments of the spatial function are non-scalar expressions;
 * - one of the arguments uses symbols from left side of the join, the other from right.
 * <p>
 * Joins expressed via ST_Distance function must use less than or less than or equals operator
 * to compare ST_Distance value with a radius and must match all of the following criteria:
 * <p>
 * - arguments of the spatial function are non-scalar expressions;
 * - one of the arguments uses symbols from left side of the join, the other from right;
 * - radius is either scalar expression or uses symbols only from the right (build) side of the join.
 * <p>
 * For inner join, replaces cross join node and a qualifying filter on top with a single
 * spatial join node.
 * <p>
 * For both inner and left joins, pushes non-trivial expressions of the spatial function
 * arguments and radius into projections on top of join child nodes.
 * <p>
 * Examples:
 * <pre>
 * Point-in-polygon inner join
 *      {@code ST_Contains(ST_GeometryFromText(a.wkt), ST_Point(b.longitude, b.latitude))}
 * becomes a spatial join
 *      {@code ST_Contains(st_geometryfromtext, st_point)}
 * with {@code st_geometryfromtext -> 'ST_GeometryFromText(a.wkt)'} and
 * {@code st_point -> 'ST_Point(b.longitude, b.latitude)'} projections on top of child nodes.
 *
 * Distance query
 *      {@code ST_Distance(ST_Point(a.lon, a.lat), ST_Point(b.lon, b.lat)) <= 10 / (111.321 * cos(radians(b.lat)))}
 * becomes a spatial join
 *      {@code ST_Distance(st_point_a, st_point_b) <= radius}
 * with {@code st_point_a -> 'ST_Point(a.lon, a.lat)', st_point_b -> 'ST_Point(b.lon, b.lat)'}
 * and {@code radius -> '10 / (111.321 * cos(radians(b.lat)))'} projections on top of child nodes.
 * </pre>
 */
public class ExtractSpatialJoins
{
    private static final TypeSignature GEOMETRY_TYPE_SIGNATURE = new TypeSignature("Geometry");
    private static final TypeSignature SPHERICAL_GEOGRAPHY_TYPE_SIGNATURE = new TypeSignature("SphericalGeography");
    private static final String KDB_TREE_TYPENAME = "KdbTree";

    private final PlannerContext plannerContext;
    private final SplitManager splitManager;
    private final PageSourceManager pageSourceManager;

    public ExtractSpatialJoins(PlannerContext plannerContext, SplitManager splitManager, PageSourceManager pageSourceManager)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceManager = requireNonNull(pageSourceManager, "pageSourceManager is null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                new ExtractSpatialInnerJoin(plannerContext, splitManager, pageSourceManager),
                new ExtractSpatialLeftJoin(plannerContext, splitManager, pageSourceManager));
    }

    @VisibleForTesting
    public static final class ExtractSpatialInnerJoin
            implements Rule<FilterNode>
    {
        private static final Capture<JoinNode> JOIN = newCapture();
        private static final Pattern<FilterNode> PATTERN = filter()
                .with(source().matching(join().capturedAs(JOIN).matching(JoinNode::isCrossJoin)));

        private final PlannerContext plannerContext;
        private final SplitManager splitManager;
        private final PageSourceManager pageSourceManager;

        public ExtractSpatialInnerJoin(PlannerContext plannerContext, SplitManager splitManager, PageSourceManager pageSourceManager)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.splitManager = requireNonNull(splitManager, "splitManager is null");
            this.pageSourceManager = requireNonNull(pageSourceManager, "pageSourceManager is null");
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return isSpatialJoinEnabled(session);
        }

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(FilterNode node, Captures captures, Context context)
        {
            JoinNode joinNode = captures.get(JOIN);
            Expression filter = node.getPredicate();
            List<Call> spatialFunctions = extractSupportedSpatialFunctions(filter);
            for (Call spatialFunction : spatialFunctions) {
                Result result = tryCreateSpatialJoin(context, joinNode, filter, node.getId(), node.getOutputSymbols(), spatialFunction, Optional.empty(), plannerContext, splitManager, pageSourceManager);
                if (!result.isEmpty()) {
                    return result;
                }
            }

            List<Comparison> spatialComparisons = extractSupportedSpatialComparisons(filter);
            for (Comparison spatialComparison : spatialComparisons) {
                Result result = tryCreateSpatialJoin(context, joinNode, filter, node.getId(), node.getOutputSymbols(), spatialComparison, plannerContext, splitManager, pageSourceManager);
                if (!result.isEmpty()) {
                    return result;
                }
            }

            return Result.empty();
        }
    }

    @VisibleForTesting
    public static final class ExtractSpatialLeftJoin
            implements Rule<JoinNode>
    {
        private static final Pattern<JoinNode> PATTERN = join().matching(node -> node.getCriteria().isEmpty() && node.getFilter().isPresent() && node.getType() == LEFT);

        private final PlannerContext plannerContext;
        private final SplitManager splitManager;
        private final PageSourceManager pageSourceManager;

        public ExtractSpatialLeftJoin(PlannerContext plannerContext, SplitManager splitManager, PageSourceManager pageSourceManager)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.splitManager = requireNonNull(splitManager, "splitManager is null");
            this.pageSourceManager = requireNonNull(pageSourceManager, "pageSourceManager is null");
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return isSpatialJoinEnabled(session);
        }

        @Override
        public Pattern<JoinNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(JoinNode joinNode, Captures captures, Context context)
        {
            Expression filter = joinNode.getFilter().get();
            List<Call> spatialFunctions = extractSupportedSpatialFunctions(filter);
            for (Call spatialFunction : spatialFunctions) {
                Result result = tryCreateSpatialJoin(context, joinNode, filter, joinNode.getId(), joinNode.getOutputSymbols(), spatialFunction, Optional.empty(), plannerContext, splitManager, pageSourceManager);
                if (!result.isEmpty()) {
                    return result;
                }
            }

            List<Comparison> spatialComparisons = extractSupportedSpatialComparisons(filter);
            for (Comparison spatialComparison : spatialComparisons) {
                Result result = tryCreateSpatialJoin(context, joinNode, filter, joinNode.getId(), joinNode.getOutputSymbols(), spatialComparison, plannerContext, splitManager, pageSourceManager);
                if (!result.isEmpty()) {
                    return result;
                }
            }

            return Result.empty();
        }
    }

    private static Result tryCreateSpatialJoin(
            Context context,
            JoinNode joinNode,
            Expression filter,
            PlanNodeId nodeId,
            List<Symbol> outputSymbols,
            Comparison spatialComparison,
            PlannerContext plannerContext,
            SplitManager splitManager,
            PageSourceManager pageSourceManager)
    {
        PlanNode leftNode = joinNode.getLeft();
        PlanNode rightNode = joinNode.getRight();

        List<Symbol> leftSymbols = leftNode.getOutputSymbols();
        List<Symbol> rightSymbols = rightNode.getOutputSymbols();

        Expression radius;
        Optional<Symbol> newRadiusSymbol;
        Comparison newComparison;
        if (spatialComparison.operator() == LESS_THAN || spatialComparison.operator() == LESS_THAN_OR_EQUAL) {
            // ST_Distance(a, b) <= r
            radius = spatialComparison.right();
            Set<Symbol> radiusSymbols = extractUnique(radius);
            if (radiusSymbols.isEmpty() || (rightSymbols.containsAll(radiusSymbols) && containsNone(leftSymbols, radiusSymbols))) {
                newRadiusSymbol = newRadiusSymbol(context, radius);
                newComparison = new Comparison(spatialComparison.operator(), spatialComparison.left(), toExpression(newRadiusSymbol, radius));
            }
            else {
                return Result.empty();
            }
        }
        else {
            // r >= ST_Distance(a, b)
            radius = spatialComparison.left();
            Set<Symbol> radiusSymbols = extractUnique(radius);
            if (radiusSymbols.isEmpty() || (rightSymbols.containsAll(radiusSymbols) && containsNone(leftSymbols, radiusSymbols))) {
                newRadiusSymbol = newRadiusSymbol(context, radius);
                newComparison = new Comparison(spatialComparison.operator().flip(), spatialComparison.right(), toExpression(newRadiusSymbol, radius));
            }
            else {
                return Result.empty();
            }
        }

        Expression newFilter = replaceExpression(filter, ImmutableMap.of(spatialComparison, newComparison));
        PlanNode newRightNode = newRadiusSymbol.map(symbol -> addProjection(context, rightNode, symbol, radius)).orElse(rightNode);

        JoinNode newJoinNode = new JoinNode(
                joinNode.getId(),
                joinNode.getType(),
                leftNode,
                newRightNode,
                joinNode.getCriteria(),
                joinNode.getLeftOutputSymbols(),
                joinNode.getRightOutputSymbols(),
                joinNode.isMaySkipOutputDuplicates(),
                Optional.of(newFilter),
                joinNode.getLeftHashSymbol(),
                joinNode.getRightHashSymbol(),
                joinNode.getDistributionType(),
                joinNode.isSpillable(),
                joinNode.getDynamicFilters(),
                joinNode.getReorderJoinStatsAndCost());

        return tryCreateSpatialJoin(context, newJoinNode, newFilter, nodeId, outputSymbols, (Call) newComparison.left(), Optional.of(newComparison.right()), plannerContext, splitManager, pageSourceManager);
    }

    private static Result tryCreateSpatialJoin(
            Context context,
            JoinNode joinNode,
            Expression filter,
            PlanNodeId nodeId,
            List<Symbol> outputSymbols,
            Call spatialFunction,
            Optional<Expression> radius,
            PlannerContext plannerContext,
            SplitManager splitManager,
            PageSourceManager pageSourceManager)
    {
        // TODO Add support for distributed left spatial joins
        Optional<String> spatialPartitioningTableName = joinNode.getType() == INNER ? getSpatialPartitioningTableName(context.getSession()) : Optional.empty();
        Optional<KdbTree> kdbTree = spatialPartitioningTableName.map(tableName -> loadKdbTree(tableName, context.getSession(), plannerContext.getMetadata(), splitManager, pageSourceManager));

        List<Expression> arguments = spatialFunction.arguments();
        verify(arguments.size() == 2);

        Expression firstArgument = arguments.get(0);
        Expression secondArgument = arguments.get(1);

        Type sphericalGeographyType = plannerContext.getTypeManager().getType(SPHERICAL_GEOGRAPHY_TYPE_SIGNATURE);
        if (firstArgument.type().equals(sphericalGeographyType)
                || secondArgument.type().equals(sphericalGeographyType)) {
            return Result.empty();
        }

        Set<Symbol> firstSymbols = extractUnique(firstArgument);
        Set<Symbol> secondSymbols = extractUnique(secondArgument);

        if (firstSymbols.isEmpty() || secondSymbols.isEmpty()) {
            return Result.empty();
        }

        Optional<Symbol> newFirstSymbol = newGeometrySymbol(context, firstArgument);
        Optional<Symbol> newSecondSymbol = newGeometrySymbol(context, secondArgument);

        PlanNode leftNode = joinNode.getLeft();
        PlanNode rightNode = joinNode.getRight();

        PlanNode newLeftNode;
        PlanNode newRightNode;

        // Check if the order of arguments of the spatial function matches the order of join sides
        int alignment = checkAlignment(joinNode, firstSymbols, secondSymbols);
        if (alignment > 0) {
            newLeftNode = newFirstSymbol.map(symbol -> addProjection(context, leftNode, symbol, firstArgument)).orElse(leftNode);
            newRightNode = newSecondSymbol.map(symbol -> addProjection(context, rightNode, symbol, secondArgument)).orElse(rightNode);
        }
        else if (alignment < 0) {
            newLeftNode = newSecondSymbol.map(symbol -> addProjection(context, leftNode, symbol, secondArgument)).orElse(leftNode);
            newRightNode = newFirstSymbol.map(symbol -> addProjection(context, rightNode, symbol, firstArgument)).orElse(rightNode);
        }
        else {
            return Result.empty();
        }

        Expression newFirstArgument = toExpression(newFirstSymbol, firstArgument);
        Expression newSecondArgument = toExpression(newSecondSymbol, secondArgument);

        Optional<Symbol> leftPartitionSymbol = Optional.empty();
        Optional<Symbol> rightPartitionSymbol = Optional.empty();
        if (kdbTree.isPresent()) {
            leftPartitionSymbol = Optional.of(context.getSymbolAllocator().newSymbol("pid", INTEGER));
            rightPartitionSymbol = Optional.of(context.getSymbolAllocator().newSymbol("pid", INTEGER));

            if (alignment > 0) {
                newLeftNode = addPartitioningNodes(plannerContext, context, newLeftNode, leftPartitionSymbol.get(), kdbTree.get(), newFirstArgument, Optional.empty());
                newRightNode = addPartitioningNodes(plannerContext, context, newRightNode, rightPartitionSymbol.get(), kdbTree.get(), newSecondArgument, radius);
            }
            else {
                newLeftNode = addPartitioningNodes(plannerContext, context, newLeftNode, leftPartitionSymbol.get(), kdbTree.get(), newSecondArgument, Optional.empty());
                newRightNode = addPartitioningNodes(plannerContext, context, newRightNode, rightPartitionSymbol.get(), kdbTree.get(), newFirstArgument, radius);
            }
        }

        ResolvedFunction resolvedFunction = spatialFunction.function();
        Expression newSpatialFunction = ResolvedFunctionCallBuilder.builder(resolvedFunction)
                .addArgument(newFirstArgument)
                .addArgument(newSecondArgument)
                .build();
        Expression newFilter = replaceExpression(filter, ImmutableMap.of(spatialFunction, newSpatialFunction));

        return Result.ofPlanNode(new SpatialJoinNode(
                nodeId,
                SpatialJoinNode.Type.fromJoinNodeType(joinNode.getType()),
                newLeftNode,
                newRightNode,
                outputSymbols,
                newFilter,
                leftPartitionSymbol,
                rightPartitionSymbol,
                kdbTree.map(KdbTreeUtils::toJson)));
    }

    private static KdbTree loadKdbTree(String tableName, Session session, Metadata metadata, SplitManager splitManager, PageSourceManager pageSourceManager)
    {
        QualifiedObjectName name = toQualifiedObjectName(tableName, session.getCatalog().get(), session.getSchema().get());
        TableHandle tableHandle = metadata.getTableHandle(session, name)
                .orElseThrow(() -> new TrinoException(INVALID_SPATIAL_PARTITIONING, format("Table not found: %s", name)));
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        List<ColumnHandle> visibleColumnHandles = columnHandles.values().stream()
                .filter(handle -> !metadata.getColumnMetadata(session, tableHandle, handle).isHidden())
                .collect(toImmutableList());
        checkSpatialPartitioningTable(visibleColumnHandles.size() == 1, "Expected single column for table %s, but found %s columns", name, columnHandles.size());

        ColumnHandle kdbTreeColumn = Iterables.getOnlyElement(visibleColumnHandles);

        Optional<KdbTree> kdbTree = Optional.empty();
        try (SplitSource splitSource = splitManager.getSplits(session, session.getQuerySpan(), tableHandle, DynamicFilter.EMPTY, alwaysTrue())) {
            while (!Thread.currentThread().isInterrupted()) {
                SplitBatch splitBatch = getFutureValue(splitSource.getNextBatch(1000));
                List<Split> splits = splitBatch.getSplits();

                for (Split split : splits) {
                    try (ConnectorPageSource pageSource = pageSourceManager.createPageSource(session, split, tableHandle, ImmutableList.of(kdbTreeColumn), DynamicFilter.EMPTY)) {
                        do {
                            getFutureValue(pageSource.isBlocked());
                            Page page = pageSource.getNextPage();
                            if (page != null && page.getPositionCount() > 0) {
                                checkSpatialPartitioningTable(kdbTree.isEmpty(), "Expected exactly one row for table %s, but found more", name);
                                checkSpatialPartitioningTable(page.getPositionCount() == 1, "Expected exactly one row for table %s, but found %s rows", name, page.getPositionCount());
                                String kdbTreeJson = VARCHAR.getSlice(page.getBlock(0), 0).toStringUtf8();
                                try {
                                    kdbTree = Optional.of(KdbTreeUtils.fromJson(kdbTreeJson));
                                }
                                catch (IllegalArgumentException e) {
                                    checkSpatialPartitioningTable(false, "Invalid JSON string for KDB tree: %s", e.getMessage());
                                }
                            }
                        }
                        while (!pageSource.isFinished());
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }

                if (splitBatch.isLastBatch()) {
                    break;
                }
            }
        }

        checkSpatialPartitioningTable(kdbTree.isPresent(), "Expected exactly one row for table %s, but got none", name);
        return kdbTree.get();
    }

    @FormatMethod
    private static void checkSpatialPartitioningTable(boolean condition, String message, Object... arguments)
    {
        if (!condition) {
            throw new TrinoException(INVALID_SPATIAL_PARTITIONING, format(message, arguments));
        }
    }

    private static QualifiedObjectName toQualifiedObjectName(String name, String catalog, String schema)
    {
        ImmutableList<String> ids = ImmutableList.copyOf(Splitter.on('.').split(name));
        if (ids.size() == 3) {
            return new QualifiedObjectName(ids.get(0), ids.get(1), ids.get(2));
        }

        if (ids.size() == 2) {
            return new QualifiedObjectName(catalog, ids.get(0), ids.get(1));
        }

        if (ids.size() == 1) {
            return new QualifiedObjectName(catalog, schema, ids.get(0));
        }

        throw new TrinoException(INVALID_SPATIAL_PARTITIONING, format("Invalid name: %s", name));
    }

    private static int checkAlignment(JoinNode joinNode, Set<Symbol> maybeLeftSymbols, Set<Symbol> maybeRightSymbols)
    {
        List<Symbol> leftSymbols = joinNode.getLeft().getOutputSymbols();
        List<Symbol> rightSymbols = joinNode.getRight().getOutputSymbols();

        if (leftSymbols.containsAll(maybeLeftSymbols)
                && containsNone(leftSymbols, maybeRightSymbols)
                && rightSymbols.containsAll(maybeRightSymbols)
                && containsNone(rightSymbols, maybeLeftSymbols)) {
            return 1;
        }

        if (leftSymbols.containsAll(maybeRightSymbols)
                && containsNone(leftSymbols, maybeLeftSymbols)
                && rightSymbols.containsAll(maybeLeftSymbols)
                && containsNone(rightSymbols, maybeRightSymbols)) {
            return -1;
        }

        return 0;
    }

    private static Expression toExpression(Optional<Symbol> optionalSymbol, Expression defaultExpression)
    {
        return optionalSymbol.map(symbol -> (Expression) symbol.toSymbolReference()).orElse(defaultExpression);
    }

    private static Optional<Symbol> newGeometrySymbol(Context context, Expression expression)
    {
        if (expression instanceof Reference) {
            return Optional.empty();
        }

        return Optional.of(context.getSymbolAllocator().newSymbol(expression));
    }

    private static Optional<Symbol> newRadiusSymbol(Context context, Expression expression)
    {
        if (expression instanceof Reference) {
            return Optional.empty();
        }

        return Optional.of(context.getSymbolAllocator().newSymbol(expression));
    }

    private static PlanNode addProjection(Context context, PlanNode node, Symbol symbol, Expression expression)
    {
        Assignments.Builder projections = Assignments.builder();
        for (Symbol outputSymbol : node.getOutputSymbols()) {
            projections.putIdentity(outputSymbol);
        }

        projections.put(symbol, expression);
        return new ProjectNode(context.getIdAllocator().getNextId(), node, projections.build());
    }

    private static PlanNode addPartitioningNodes(PlannerContext plannerContext, Context context, PlanNode node, Symbol partitionSymbol, KdbTree kdbTree, Expression geometry, Optional<Expression> radius)
    {
        Assignments.Builder projections = Assignments.builder();
        for (Symbol outputSymbol : node.getOutputSymbols()) {
            projections.putIdentity(outputSymbol);
        }

        TypeSignature typeSignature = new TypeSignature(KDB_TREE_TYPENAME);
        BuiltinFunctionCallBuilder spatialPartitionsCall = BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                .setName("spatial_partitions")
                .addArgument(typeSignature, new Cast(new Constant(VARCHAR, Slices.utf8Slice(KdbTreeUtils.toJson(kdbTree))), plannerContext.getTypeManager().getType(typeSignature)))
                .addArgument(GEOMETRY_TYPE_SIGNATURE, geometry);
        radius.ifPresent(value -> spatialPartitionsCall.addArgument(DOUBLE, value));
        Call partitioningFunction = spatialPartitionsCall.build();

        Symbol partitionsSymbol = context.getSymbolAllocator().newSymbol(partitioningFunction);
        projections.put(partitionsSymbol, partitioningFunction);

        return new UnnestNode(
                context.getIdAllocator().getNextId(),
                new ProjectNode(context.getIdAllocator().getNextId(), node, projections.build()),
                node.getOutputSymbols(),
                ImmutableList.of(new UnnestNode.Mapping(partitionsSymbol, ImmutableList.of(partitionSymbol))),
                Optional.empty(),
                INNER);
    }

    private static boolean containsNone(Collection<Symbol> values, Collection<Symbol> testValues)
    {
        return values.stream().noneMatch(ImmutableSet.copyOf(testValues)::contains);
    }
}

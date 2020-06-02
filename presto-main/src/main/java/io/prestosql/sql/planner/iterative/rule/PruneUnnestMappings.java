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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.FunctionCallBuilder;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolAllocator;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.UnnestNode;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.LambdaArgumentDeclaration;
import io.prestosql.sql.tree.LambdaExpression;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.Row;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.type.FunctionType;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.QueryUtil.identifier;
import static io.prestosql.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.prestosql.sql.planner.iterative.rule.DereferencePushdown.extractDereferences;
import static io.prestosql.sql.planner.iterative.rule.DereferencePushdown.getBase;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.unnest;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;

/**
 * Transforms:
 * <pre>
 *  Project(D := f1(A), E := f2(B_VARCHAR), G := f3(B_ROW.X))
 *      Unnest(replicate = [A], unnest = (B_ARRAY -> [B_BIGINT, B_VARCHAR, B_ROW]))
 *          Source(
 *              A bigint,
 *              B_ARRAY array(row(_bigint bigint, _varchar varchar, _row row(x bigint, y bigint))))
 *  </pre>
 * to:
 * <pre>
 *  Project(D := f1(A), E := f2(B_VARCHAR), G := f3(B_ROW_X))
 *      Unnest(replicate = [A], unnest = (transformed -> [B_VARCHAR, B_ROW_X]))
 *          Project(A, transformed := transform(e -> row(e._varchar, e._row.x)))
 *              Source(A, B_ARAAY)
 * </pre>
 * <p>
 */
public class PruneUnnestMappings
        implements Rule<ProjectNode>
{
    private static final Capture<UnnestNode> CHILD = newCapture();
    private final TypeAnalyzer typeAnalyzer;
    private final Metadata metadata;

    public PruneUnnestMappings(TypeAnalyzer typeAnalyzer, Metadata metadata)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(unnest().capturedAs(CHILD)));
    }

    @Override
    public Result apply(ProjectNode projectNode, Captures captures, Context context)
    {
        UnnestNode unnestNode = captures.get(CHILD);

        // Extract expressions in project node's assignments and unnest node's filter
        ImmutableList.Builder<Expression> expressionsBuilder = ImmutableList.<Expression>builder()
                .addAll(projectNode.getAssignments().getExpressions());
        unnestNode.getFilter().ifPresent(expressionsBuilder::add);
        List<Expression> expressions = expressionsBuilder.build();

        // Collect referenced symbols and dereference projections
        Set<Symbol> symbolsReferenced = SymbolsExtractor.extractUnique(expressions);
        Set<DereferenceExpression> dereferences = extractDereferences(expressions, false);

        // Transform mappings with lambda representations
        List<Optional<Transformation>> transformations = unnestNode.getMappings().stream()
                .map(mapping -> {
                    // Do not prune mappings for which input is being referred
                    Symbol input = mapping.getInput();
                    if (unnestNode.getReplicateSymbols().contains(input) || symbolsReferenced.contains(input)) {
                        return Optional.<Transformation>empty();
                    }

                    Type inputType = context.getSymbolAllocator().getTypes().get(input);
                    if (inputType instanceof MapType) {
                        // TODO: Transform map values
                        return Optional.<Transformation>empty();
                    }

                    return transformArray(mapping, symbolsReferenced, dereferences, context.getSession(), context.getSymbolAllocator());
                })
                .collect(toImmutableList());

        if (transformations.stream().allMatch(Optional::isEmpty)) {
            return Result.empty();
        }

        ImmutableList.Builder<UnnestNode.Mapping> newMappingsBuilder = ImmutableList.builder();
        Assignments.Builder projectionsBuilder = Assignments.builder();

        // Prepare new mappings
        for (int i = 0; i < unnestNode.getMappings().size(); i++) {
            UnnestNode.Mapping original = unnestNode.getMappings().get(i);

            if (transformations.get(i).isEmpty()) {
                newMappingsBuilder.add(original);
                projectionsBuilder.putIdentity(original.getInput());
                continue;
            }

            Transformation transformation = transformations.get(i).get();
            UnnestNode.Mapping newMapping = transformation.getMapping();
            newMappingsBuilder.add(newMapping);
            projectionsBuilder.put(newMapping.getInput(), transformation.getProjection());
        }

        Map<Expression, SymbolReference> expressionReplacements = transformations.stream()
                .filter(Optional::isPresent)
                .flatMap(transformation -> transformation.get().getExpressionReplacements().entrySet().stream())
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        ProjectNode newProjectNode = new ProjectNode(
                context.getIdAllocator().getNextId(),
                new UnnestNode(
                        context.getIdAllocator().getNextId(),
                        new ProjectNode(
                                context.getIdAllocator().getNextId(),
                                unnestNode.getSource(),
                                projectionsBuilder
                                        .putIdentities(unnestNode.getReplicateSymbols())
                                        .build()),
                        unnestNode.getReplicateSymbols(),
                        newMappingsBuilder.build(),
                        unnestNode.getOrdinalitySymbol(),
                        unnestNode.getJoinType(),
                        unnestNode.getFilter()
                                .map(filter -> replaceExpression(filter, expressionReplacements))),
                projectNode.getAssignments()
                        .rewrite(expression -> replaceExpression(expression, expressionReplacements)));

        return Result.ofPlanNode(newProjectNode);
    }

    private Optional<Transformation> transformArray(
            UnnestNode.Mapping mapping,
            Set<Symbol> referredSymbols,
            Set<DereferenceExpression> dereferences,
            Session session,
            SymbolAllocator symbolAllocator)
    {
        Type inputType = symbolAllocator.getTypes().get(mapping.getInput());
        checkArgument(inputType instanceof ArrayType, "Unexpected input type");
        ArrayType arrayType = (ArrayType) inputType;

        // Exclude primitive arrays
        if (!(arrayType.getElementType() instanceof RowType)) {
            return Optional.empty();
        }

        List<Symbol> outputSymbols = mapping.getOutputs();

        // Prepare symbols to remove or prune
        Set<Symbol> removeSymbols = outputSymbols.stream()
                .filter(symbol -> !referredSymbols.contains(symbol))
                .collect(toImmutableSet());
        Map<Symbol, List<DereferenceExpression>> pruneSymbols = dereferences.stream()
                .filter(expression -> outputSymbols.contains(getBase(expression)))
                .collect(groupingBy(expression -> getBase(expression)));

        if (outputSymbols.stream().allMatch(symbol -> !removeSymbols.contains(symbol) && !pruneSymbols.containsKey(symbol))) {
            // Nothing to remove or prune
            return Optional.empty();
        }

        // Create new symbols for dereferences to pushdown
        Assignments dereferenceAssignments = Assignments.of(
                pruneSymbols.values().stream()
                    .flatMap(Collection::stream)
                    .collect(toImmutableSet()),
                session,
                symbolAllocator,
                typeAnalyzer);
        Map<Expression, SymbolReference> dereferenceReplacements = dereferenceAssignments.getMap().entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getValue(), entry -> entry.getKey().toSymbolReference()));

        // Prepare projections for transforming the array through a lambda expression
        ImmutableList.Builder<Symbol> newOutputSymbolsBuilder = ImmutableList.builder();
        ImmutableList.Builder<Expression> newRowFieldsBuilder = ImmutableList.builder();

        for (Symbol outputSymbol : outputSymbols) {
            // Remove symbol if not referenced
            if (removeSymbols.contains(outputSymbol)) {
                continue;
            }

            // Prune Symbol if dereferences are available
            List<DereferenceExpression> symbolDereferences = pruneSymbols.getOrDefault(outputSymbol, ImmutableList.of());
            if (!symbolDereferences.isEmpty()) {
                symbolDereferences.forEach(projection -> {
                    newOutputSymbolsBuilder.add(Symbol.from(dereferenceReplacements.get(projection)));
                    newRowFieldsBuilder.add(projection);
                });
                continue;
            }

            // Full Symbol is being referenced
            newOutputSymbolsBuilder.add(outputSymbol);
            newRowFieldsBuilder.add(outputSymbol.toSymbolReference());
        }

        List<Symbol> newOutputSymbols = newOutputSymbolsBuilder.build();
        List<Expression> newRowFields = newRowFieldsBuilder.build();

        // Retain at least one output if everything is removed
        // TODO: replace with a constant
        if (newOutputSymbols.isEmpty()) {
            newOutputSymbols = ImmutableList.of(outputSymbols.get(0));
            newRowFields = ImmutableList.of(outputSymbols.get(0).toSymbolReference());
        }

        // Construct the new element type
        List<Type> newOutputTypes = newOutputSymbols.stream()
                .map(symbol -> symbolAllocator.getTypes().get(symbol))
                .collect(toImmutableList());

        Type prunedElementType;
        if (newOutputTypes.size() == 1) {
            // Do not construct a row for a single element
            prunedElementType = newOutputTypes.get(0);
        }
        else {
            prunedElementType = RowType.from(
                    IntStream.range(0, newOutputSymbols.size()).boxed()
                            .map(fieldIndex -> RowType.field("f" + fieldIndex, newOutputTypes.get(fieldIndex)))
                            .collect(toImmutableList()));
        }

        // Create new lambda expression for pruned element
        RowType elementRowType = (RowType) (arrayType.getElementType());
        Symbol lambdaSymbol = symbolAllocator.newSymbol("transformarray$element", arrayType.getElementType());
        LambdaExpression lambdaExpression = createLambdaExpression(lambdaSymbol, elementRowType.getFields(), newRowFields, outputSymbols);

        // Construct a transformed array using transform function
        Expression transformedArray = new FunctionCallBuilder(metadata)
                .setName(QualifiedName.of("transform"))
                .addArgument(new ArrayType(elementRowType), mapping.getInput().toSymbolReference())
                .addArgument(new FunctionType(singletonList(elementRowType), prunedElementType), lambdaExpression)
                .build();

        Symbol newInputSymbol = symbolAllocator.newSymbol(transformedArray, new ArrayType(prunedElementType));

        return Optional.of(new Transformation(
                new UnnestNode.Mapping(newInputSymbol, newOutputSymbols),
                transformedArray,
                dereferenceReplacements));
    }

    private LambdaExpression createLambdaExpression(
            Symbol lambdaSymbol,
            List<RowType.Field> originalFields,
            List<Expression> newFields,
            List<Symbol> outputSymbols)
    {
        Map<Expression, Expression> lambdaMapping = IntStream.range(0, outputSymbols.size()).boxed()
                .collect(toImmutableMap(
                        fieldIndex -> outputSymbols.get(fieldIndex).toSymbolReference(),
                        fieldIndex -> {
                            RowType.Field field = originalFields.get(fieldIndex);
                            return new DereferenceExpression(lambdaSymbol.toSymbolReference(), identifier(field.getName().get()));
                        }));

        List<Expression> rowFieldsForLambda = newFields.stream()
                .map(projection -> replaceExpression(projection, lambdaMapping))
                .collect(toImmutableList());

        Expression elementTransformation;
        if (newFields.size() == 1) {
            elementTransformation = rowFieldsForLambda.get(0);
        }
        else {
            elementTransformation = new Row(rowFieldsForLambda);
        }

        return new LambdaExpression(
                ImmutableList.of(new LambdaArgumentDeclaration(identifier(lambdaSymbol.getName()))),
                elementTransformation);
    }

    private static class Transformation
    {
        private final UnnestNode.Mapping mapping;
        private final Expression projection;
        private final Map<Expression, SymbolReference> expressionReplacements;

        Transformation(
                UnnestNode.Mapping mapping,
                Expression projection,
                Map<Expression, SymbolReference> expressionReplacements)
        {
            this.mapping = mapping;
            this.projection = projection;
            this.expressionReplacements = expressionReplacements;
        }

        UnnestNode.Mapping getMapping()
        {
            return mapping;
        }

        Expression getProjection()
        {
            return projection;
        }

        Map<Expression, SymbolReference> getExpressionReplacements()
        {
            return expressionReplacements;
        }
    }
}

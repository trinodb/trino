package io.trino.sql.dialect.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.Attributes.JoinType;
import io.trino.sql.dialect.trino.operation.CorrelatedJoin;
import io.trino.sql.dialect.trino.operation.FieldSelection;
import io.trino.sql.dialect.trino.operation.Filter;
import io.trino.sql.dialect.trino.operation.Output;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
import io.trino.sql.dialect.trino.operation.Values;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.SourceNode;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ValuesNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.Context.argumentMapping;
import static io.trino.sql.dialect.trino.Context.composedMapping;
import static io.trino.sql.dialect.trino.Dialect.irType;
import static io.trino.sql.dialect.trino.Dialect.trinoType;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.OperationAndMapping;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION_ROW;
import static io.trino.sql.dialect.trino.operation.Values.valuesWithoutFields;
import static java.util.Objects.requireNonNull;

/**
 * A rewriter transforming a tree of PlanNodes into a MLIR program based on `PlanVisitor`.
 * For scalar expressions, uses another rewriter `ScalarProgramBuilder` based on `IrVisitor`.
 */
public class RelationalProgramBuilder
        extends PlanVisitor<OperationAndMapping, Context>
{
    private final ProgramBuilder.ValueNameAllocator nameAllocator;
    private final ImmutableMap.Builder<Value, SourceNode> valueMap;

    public RelationalProgramBuilder(ProgramBuilder.ValueNameAllocator nameAllocator, ImmutableMap.Builder<Value, SourceNode> valueMap)
    {
        this.nameAllocator = requireNonNull(nameAllocator, "nameAllocator is null");
        this.valueMap = requireNonNull(valueMap, "valueMap is null");
    }

    @Override
    protected OperationAndMapping visitPlan(PlanNode node, Context context)
    {
        throw new UnsupportedOperationException("The new IR does not support " + node.getClass().getSimpleName() + " yet");
    }

    @Override
    public OperationAndMapping visitCorrelatedJoin(CorrelatedJoinNode node, Context context)
    {
        OperationAndMapping input = node.getInput().accept(this, context);
        String resultName = nameAllocator.newName();

        Type inputRowType = relationRowType(trinoType(input.operation().result().type()));

        // model correlation as field selection (lambda)
        Block.Parameter correlationParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(inputRowType));
        Block correlation = fieldSelectorBlock("^correlationSelector", correlationParameter, input.mapping(), node.getCorrelation());
        valueMap.put(correlationParameter, correlation);

        // model subquery as a lambda
        Block.Parameter subqueryParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(inputRowType));
        Block.Builder subqueryBuilder = new Block.Builder(Optional.of("^subquery"), ImmutableList.of(subqueryParameter));
        node.getSubquery().accept(
                this,
                new Context(subqueryBuilder, composedMapping(context, argumentMapping(subqueryParameter, input.mapping()))));
        addReturnOperation(subqueryBuilder);
        Map<Symbol, String> subqueryMapping = deriveOutputMapping(relationRowType(trinoType(subqueryBuilder.recentOperation().result().type())), node.getSubquery().getOutputSymbols());
        Map<AttributeKey, Object> subqueryAttributes = subqueryBuilder.recentOperation().attributes();
        Block subquery = subqueryBuilder.build();
        valueMap.put(subqueryParameter, subquery);

        // model filter as a lambda
        Block.Parameter firstFilterParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(inputRowType));
        Block.Parameter secondFilterParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(relationRowType(trinoType(subquery.getReturnedType()))));
        Block.Builder filterBuilder = new Block.Builder(Optional.of("^filter"), ImmutableList.of(firstFilterParameter, secondFilterParameter));
        node.getFilter().accept(
                new ScalarProgramBuilder(nameAllocator, valueMap),
                new Context(filterBuilder, composedMapping(context, ImmutableList.of(
                        argumentMapping(firstFilterParameter, input.mapping()),
                        argumentMapping(secondFilterParameter, subqueryMapping)))));
        addReturnOperation(filterBuilder);
        Block filter = filterBuilder.build();
        valueMap.put(firstFilterParameter, filter);
        valueMap.put(secondFilterParameter, filter);

        CorrelatedJoin correlatedJoin = new CorrelatedJoin(
                resultName,
                input.operation().result(),
                correlation,
                subquery,
                filter,
                JoinType.of(node.getType()),
                input.operation().attributes(),
                subqueryAttributes
        );
        valueMap.put(correlatedJoin.result(), correlatedJoin);

        Map<Symbol, String> outputMapping = deriveOutputMapping(relationRowType(trinoType(correlatedJoin.result().type())), node.getOutputSymbols());
        context.block().addOperation(correlatedJoin);
        return new OperationAndMapping(correlatedJoin, outputMapping);
    }

    @Override
    public OperationAndMapping visitFilter(FilterNode node, Context context)
    {
        OperationAndMapping input = node.getSource().accept(this, context);
        String resultName = nameAllocator.newName();

        // model filter predicate as a lambda (Block)
        Block.Parameter predicateParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(relationRowType(trinoType(input.operation().result().type()))));
        Block.Builder predicateBuilder = new Block.Builder(Optional.of("^predicate"), ImmutableList.of(predicateParameter));

        node.getPredicate().accept(
                new ScalarProgramBuilder(nameAllocator, valueMap),
                new Context(predicateBuilder, composedMapping(context, argumentMapping(predicateParameter, input.mapping()))));

        addReturnOperation(predicateBuilder);

        Block predicate = predicateBuilder.build();
        valueMap.put(predicateParameter, predicate);

        Filter filter = new Filter(resultName, input.operation().result(), predicate, input.operation().attributes());
        valueMap.put(filter.result(), filter);
        Map<Symbol, String> outputMapping = deriveOutputMapping(relationRowType(trinoType(filter.result().type())), node.getOutputSymbols());
        context.block().addOperation(filter);
        return new OperationAndMapping(filter, outputMapping);
    }

    @Override
    public OperationAndMapping visitOutput(OutputNode node, Context context)
    {
        OperationAndMapping input = node.getSource().accept(this, context);
        String resultName = nameAllocator.newName();

        // model output fields selection as a lambda (Block)
        Block.Parameter fieldSelectorParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(relationRowType(trinoType(input.operation().result().type()))));
        Block fieldSelectorBlock = fieldSelectorBlock("^outputFieldSelector", fieldSelectorParameter, input.mapping(), node.getOutputSymbols());
        valueMap.put(fieldSelectorParameter, fieldSelectorBlock);

        Output output = new Output(resultName, input.operation().result(), fieldSelectorBlock, node.getColumnNames());
        valueMap.put(output.result(), output);
        context.block().addOperation(output);
        return new OperationAndMapping(output, ImmutableMap.of()); // unlike OutputNode, the Output operation returns Void, not a relation
    }

    @Override
    public OperationAndMapping visitValues(ValuesNode node, Context context)
    {
        String resultName = nameAllocator.newName();

        Values values;
        if (node.getOutputSymbols().isEmpty()) {
            values = valuesWithoutFields(resultName, node.getRowCount());
        }
        else {
            // model each component row of Values as a no-argument Block
            // TODO we could use sequential names for blocks: "row_1", "row_2"...
            List<Block> rows = node.getRows().orElseThrow().stream()
                    .map(rowExpression -> {
                        Block.Builder rowBlock = new Block.Builder(Optional.of("^row"), ImmutableList.of());
                        rowExpression.accept(
                                new ScalarProgramBuilder(nameAllocator, valueMap),
                                new Context(rowBlock, context.symbolMapping()));
                        addReturnOperation(rowBlock);
                        return rowBlock.build();
                    })
                    .collect(toImmutableList());
            RowType rowType = RowType.anonymous(node.getOutputSymbols().stream()
                    .map(Symbol::type)
                    .collect(toImmutableList()));
            values = new Values(resultName, rowType, rows);
        }
        valueMap.put(values.result(), values);
        Map<Symbol, String> outputMapping = deriveOutputMapping(relationRowType(trinoType(values.result().type())), node.getOutputSymbols());
        context.block().addOperation(values);
        return new OperationAndMapping(values, outputMapping);
    }

    /**
     * A type of relation is represented as MultisetType of row type being either RowType or EmptyRowType. This method extracts the element type.
     */
    public static Type relationRowType(Type relationType)
    {
        if (!IS_RELATION.test(relationType)) {
            throw new TrinoException(IR_ERROR, "not a relation type. expected multiset of row");
        }

        return ((MultisetType) relationType).getElementType();
    }

    /**
     * Map each output symbol of the PlanNode to a corresponding field name in the Operations output row type.
     */
    private static Map<Symbol, String> deriveOutputMapping(Type relationRowType, List<Symbol> outputSymbols)
    {
        if (!IS_RELATION_ROW.test(relationRowType)) {
            throw new TrinoException(IR_ERROR, "not a relation row type. expected RowType or EmptyRowType");
        }

        if (relationRowType.equals(EMPTY_ROW)) {
            if (!outputSymbols.isEmpty()) {
                throw new TrinoException(IR_ERROR, "relation row type mismatch: output symbols present for EmptyRowType");
            }
            return ImmutableMap.of();
        }

        RowType rowType = (RowType) relationRowType;
        if (rowType.getFields().size() != outputSymbols.size()) {
            throw new TrinoException(IR_ERROR, "relation RowType does not match output symbols");
        }

        // Using a HashMap because it can handle duplicates.
        // If a PlanNode outputs some symbol twice, we will use the first occurrence for mapping.
        // As a result, the downstream references to the symbol will be mapped to the same output field,
        // and the other field will be unused and eligible for pruning.
        Map<Symbol, String> mapping = HashMap.newHashMap(outputSymbols.size());
        for (int i = 0; i < outputSymbols.size(); i++) {
            Symbol symbol = outputSymbols.get(i);
            String fieldName = rowType.getFields().get(i).getName().orElseThrow();
            mapping.putIfAbsent(symbol, fieldName);
        }
        return mapping;
    }

    private Block fieldSelectorBlock(String blockName, Block.Parameter inputRow, Map<Symbol, String> inputSymbolMapping, List<Symbol> selectedSymbolsList)
    {
        return fieldSelectorBlock(
                blockName,
                ImmutableList.of(inputRow),
                ImmutableList.of(inputSymbolMapping),
                ImmutableList.of(selectedSymbolsList));
    }

    /**
     * A helper method to express input field selection as a lambda.
     * Useful for operations which pass selected input fields on output, for example join, unnest.
     * Also useful for selecting input columns necessary for the operation's logic, for example ordering columns, partitioning columns.
     *
     * @param blockName name for the result Block
     * @param inputRows arguments to the lambda representing input rows from which we want to select fields
     * @param inputSymbolMappings mapping symbol --> row field name for each input row
     * @param selectedSymbolsLists list of symbols to select from each input row
     * @return a row containing selected fields from all input rows
     */
    private Block fieldSelectorBlock(String blockName, List<Block.Parameter> inputRows, List<Map<Symbol, String>> inputSymbolMappings, List<List<Symbol>> selectedSymbolsLists)
    {
        if (inputRows.size() != inputSymbolMappings.size()) {
            throw new TrinoException(IR_ERROR, "inputs and input symbol mappings do not match");
        }
        if (inputRows.size() != selectedSymbolsLists.size()) {
            throw new TrinoException(IR_ERROR, "inputs and symbol lists do not match");
        }

        ImmutableList.Builder<Operation> selections = ImmutableList.builder();

        Block.Builder selectorBlock = new Block.Builder(Optional.of(blockName), inputRows);
        for (int i = 0; i < inputRows.size(); i++) {
            Block.Parameter parameter = inputRows.get(i);
            Map<Symbol, String> symbolMapping = inputSymbolMappings.get(i);
            List<Symbol> symbols = selectedSymbolsLists.get(i);
            for (Symbol symbol : symbols) {
                String value = nameAllocator.newName();
                FieldSelection fieldSelection = new FieldSelection(value, parameter, symbolMapping.get(symbol), ImmutableMap.of()); // TODO pass appropriate row-specific input attributes through lambda arguments
                valueMap.put(fieldSelection.result(), fieldSelection);
                selectorBlock.addOperation(fieldSelection);
                selections.add(fieldSelection);
            }
        }
        // build a row of selected items
        String rowValue = nameAllocator.newName();
        Row rowConstructor = new Row(
                rowValue,
                selections.build().stream().map(Operation::result).collect(toImmutableList()),
                selections.build().stream().map(Operation::attributes).collect(toImmutableList()));
        valueMap.put(rowConstructor.result(), rowConstructor);
        selectorBlock.addOperation(rowConstructor);

        addReturnOperation(selectorBlock);

        return selectorBlock.build();
    }

    /**
     * Return the value of the recent operation in the builder
     */
    private void addReturnOperation(Block.Builder builder)
    {
        String returnValue = nameAllocator.newName();
        Operation recentOperation = builder.recentOperation();
        Return returnOperation = new Return(returnValue, recentOperation.result(), recentOperation.attributes());
        valueMap.put(returnOperation.result(), returnOperation);
        builder.addOperation(returnOperation);
    }

    /**
     * Assigns unique lowercase names, compliant with IS_RELATION_ROW type constraint: f_1, f_2, ...
     * Indexing from 1 for similarity with SQL indexing.
     */
    public static RowType assignRelationRowTypeFieldNames(RowType relationRowType)
    {
        ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
        for (int i = 0; i < relationRowType.getTypeParameters().size(); i++) {
            fields.add(new RowType.Field(
                    Optional.of(String.format("f_%s", i + 1)),
                    relationRowType.getTypeParameters().get(i)));
        }
        return RowType.from(fields.build());
    }

    /**
     * A result of transforming a PlanNode into an Operation.
     * Maps each output symbol of the PlanNode to a corresponding field name in the Operations output RowType.
     */
    public record OperationAndMapping(Operation operation, Map<Symbol, String> mapping)
    {
        public OperationAndMapping(Operation operation, Map<Symbol, String> mapping)
        {
            this.operation = requireNonNull(operation, "operation is null");
            this.mapping = ImmutableMap.copyOf(requireNonNull(mapping, "mapping is null"));
        }
    }
}

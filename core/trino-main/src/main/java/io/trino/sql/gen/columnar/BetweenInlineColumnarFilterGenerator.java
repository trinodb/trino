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
package io.trino.sql.gen.columnar;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.ForLoop;
import io.airlift.bytecode.control.IfStatement;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.project.InputChannels;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.sql.gen.CallSiteBinder;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.add;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.lessThan;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.sql.gen.columnar.CallColumnarFilterGenerator.generateInvocation;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.createClassInstance;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.declareBlockVariables;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.generateBlockMayHaveNull;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.generateBlockPositionNotNull;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.generateGetInputChannels;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.updateOutputPositions;
import static io.trino.util.CompilerUtils.makeClassName;
import static java.util.Objects.requireNonNull;

public class BetweenInlineColumnarFilterGenerator
{
    private final Reference valueReference;
    private final Map<Symbol, Integer> layout;
    private final ResolvedFunction lessThanOrEqual;
    private final List<Expression> leftArguments;
    private final List<Expression> rightArguments;
    private final FunctionManager functionManager;

    public BetweenInlineColumnarFilterGenerator(Between between, Map<Symbol, Integer> layout, Metadata metadata, FunctionManager functionManager)
    {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.layout = requireNonNull(layout, "layout is null");

        // Between requires evaluate once semantic for the value being tested
        // Until we can pre-project it into a temporary variable, we apply columnar evaluation only on InputReference
        checkArgument(between.value() instanceof Reference, "valueExpression is not a Reference");
        this.valueReference = (Reference) between.value();
        this.lessThanOrEqual = metadata.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(between.value().type(), between.max().type()));
        this.leftArguments = ImmutableList.of(between.min(), valueReference);
        this.rightArguments = ImmutableList.of(valueReference, between.max());
    }

    public Class<? extends ColumnarFilter> generateColumnarFilter()
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(ColumnarFilter.class.getSimpleName() + "_between", Optional.empty()),
                type(Object.class),
                type(ColumnarFilter.class));
        CallSiteBinder callSiteBinder = new CallSiteBinder();

        FieldDefinition inputChannelsField = generateGetInputChannels(classDefinition);
        generateConstructor(classDefinition, inputChannelsField);

        generateFilterRangeMethod(callSiteBinder, classDefinition);
        generateFilterListMethod(callSiteBinder, classDefinition);

        return createClassInstance(callSiteBinder, classDefinition);
    }

    private static void generateConstructor(ClassDefinition classDefinition, FieldDefinition inputChannelsField)
    {
        Parameter inputChannelsParam = arg("inputChannels", InputChannels.class);
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC), inputChannelsParam);

        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        body.append(thisVariable.setField(inputChannelsField, inputChannelsParam));
        body.ret();
    }

    private void generateFilterRangeMethod(CallSiteBinder binder, ClassDefinition classDefinition)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter outputPositions = arg("outputPositions", int[].class);
        Parameter offset = arg("offset", int.class);
        Parameter size = arg("size", int.class);
        Parameter page = arg("page", SourcePage.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filterPositionsRange",
                type(int.class),
                ImmutableList.of(session, outputPositions, offset, size, page));
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        declareBlockVariables(ImmutableList.of(valueReference), layout, page, scope, body);

        Variable outputPositionsCount = scope.declareVariable("outputPositionsCount", body, constantInt(0));
        Variable position = scope.declareVariable(int.class, "position");
        Variable result = scope.declareVariable(boolean.class, "result");

        IfStatement ifStatement = new IfStatement()
                .condition(generateBlockMayHaveNull(ImmutableList.of(valueReference), layout, scope));
        body.append(ifStatement);

        /* if (block_0.mayHaveNull()) {
         *     for (position = offset; position < offset + size; position++) {
         *         if (!block_0.isNull(position)) {
         *             boolean result = less_than_or_equal(constant, block_0, position);
         *             if (result) {
         *                 result = less_than_or_equal(block_0, position, constant);
         *             }
         *             outputPositions[outputPositionsCount] = position;
         *             outputPositionsCount += result ? 1 : 0;
         *         }
         *     }
         * }
         */
        ifStatement.ifTrue(new ForLoop("nullable range based loop")
                .initialize(position.set(offset))
                .condition(lessThan(position, add(offset, size)))
                .update(position.increment())
                .body(new IfStatement()
                        .condition(generateBlockPositionNotNull(ImmutableList.of(valueReference), layout, scope, position))
                        .ifTrue(computeAndAssignResult(binder, scope, result, position, outputPositions, outputPositionsCount))));

        /* for (position = offset; position < offset + size; position++) {
         *     boolean result = less_than_or_equal(constant, block_0, position);
         *     if (result) {
         *         result = less_than_or_equal(block_0, position, constant);
         *     }
         *     outputPositions[outputPositionsCount] = position;
         *     outputPositionsCount += result ? 1 : 0;
         * }
         */
        ifStatement.ifFalse(new ForLoop("non-nullable range based loop")
                .initialize(position.set(offset))
                .condition(lessThan(position, add(offset, size)))
                .update(position.increment())
                .body(computeAndAssignResult(binder, scope, result, position, outputPositions, outputPositionsCount)));

        body.append(outputPositionsCount.ret());
    }

    private void generateFilterListMethod(CallSiteBinder binder, ClassDefinition classDefinition)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter outputPositions = arg("outputPositions", int[].class);
        Parameter activePositions = arg("activePositions", int[].class);
        Parameter offset = arg("offset", int.class);
        Parameter size = arg("size", int.class);
        Parameter page = arg("page", SourcePage.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filterPositionsList",
                type(int.class),
                ImmutableList.of(session, outputPositions, activePositions, offset, size, page));
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        declareBlockVariables(ImmutableList.of(valueReference), layout, page, scope, body);

        Variable outputPositionsCount = scope.declareVariable("outputPositionsCount", body, constantInt(0));
        Variable index = scope.declareVariable(int.class, "index");
        Variable position = scope.declareVariable(int.class, "position");
        Variable result = scope.declareVariable(boolean.class, "result");

        IfStatement ifStatement = new IfStatement()
                .condition(generateBlockMayHaveNull(ImmutableList.of(valueReference), layout, scope));
        body.append(ifStatement);

        /* if (block_0.mayHaveNull()) {
         *     for (int index = offset; index < offset + size; index++) {
         *         int position = activePositions[index];
         *         if (!block_0.isNull(position)) {
         *             boolean result = less_than_or_equal(constant, block_0, position);
         *             if (result) {
         *                 result = less_than_or_equal(block_0, position, constant);
         *             }
         *             outputPositions[outputPositionsCount] = position;
         *             outputPositionsCount += result ? 1 : 0;
         *         }
         *     }
         * }
         */
        ifStatement.ifTrue(new ForLoop("nullable positions loop")
                .initialize(index.set(offset))
                .condition(lessThan(index, add(offset, size)))
                .update(index.increment())
                .body(new BytecodeBlock()
                        .append(position.set(activePositions.getElement(index)))
                        .append(new IfStatement()
                                .condition(generateBlockPositionNotNull(ImmutableList.of(valueReference), layout, scope, position))
                                .ifTrue(computeAndAssignResult(binder, scope, result, position, outputPositions, outputPositionsCount)))));

        /* for (int index = offset; index < offset + size; index++) {
         *     int position = activePositions[index];
         *     boolean result = less_than_or_equal(constant, block_0, position);
         *     if (result) {
         *         result = less_than_or_equal(block_0, position, constant);
         *     }
         *     outputPositions[outputPositionsCount] = position;
         *     outputPositionsCount += result ? 1 : 0;
         * }
         */
        ifStatement.ifFalse(new ForLoop("non-nullable positions loop")
                .initialize(index.set(offset))
                .condition(lessThan(index, add(offset, size)))
                .update(index.increment())
                .body(new BytecodeBlock()
                        .append(position.set(activePositions.getElement(index)))
                        .append(computeAndAssignResult(binder, scope, result, position, outputPositions, outputPositionsCount))));

        body.append(outputPositionsCount.ret());
    }

    private BytecodeBlock computeAndAssignResult(CallSiteBinder binder, Scope scope, Variable result, Variable position, Parameter outputPositions, Variable outputPositionsCount)
    {
        return new BytecodeBlock()
                .append(generateInvocation(functionManager, binder, lessThanOrEqual, leftArguments, layout, scope, position)
                        .putVariable(result))
                .append(new IfStatement()
                        .condition(result)
                        .ifTrue(generateInvocation(functionManager, binder, lessThanOrEqual, rightArguments, layout, scope, position)
                                .putVariable(result)))
                .append(updateOutputPositions(result, position, outputPositions, outputPositionsCount));
    }
}

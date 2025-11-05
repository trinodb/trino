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
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.ForLoop;
import io.airlift.bytecode.control.IfStatement;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.sql.gen.CallSiteBinder;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.SpecialForm;

import java.util.Optional;
import java.util.function.Supplier;

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
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.SpecialForm.Form.BETWEEN;
import static io.trino.util.CompilerUtils.makeClassName;
import static java.util.Objects.requireNonNull;

public class BetweenInlineColumnarFilterGenerator
{
    private final InputReferenceExpression valueExpression;
    private final CallExpression leftExpression;
    private final CallExpression rightExpression;
    private final FunctionManager functionManager;

    public BetweenInlineColumnarFilterGenerator(SpecialForm specialForm, FunctionManager functionManager)
    {
        checkArgument(specialForm.form() == BETWEEN, "specialForm should be BETWEEN");
        checkArgument(specialForm.arguments().size() == 3, "BETWEEN should have 3 arguments %s", specialForm.arguments());
        checkArgument(specialForm.functionDependencies().size() == 1, "BETWEEN should have 1 functional dependency %s", specialForm.functionDependencies());
        this.functionManager = requireNonNull(functionManager, "functionManager is null");

        // Between requires evaluate once semantic for the value being tested
        // Until we can pre-project it into a temporary variable, we apply columnar evaluation only on InputReference
        checkArgument(specialForm.arguments().getFirst() instanceof InputReferenceExpression, "valueExpression is not an InputReference");
        this.valueExpression = (InputReferenceExpression) specialForm.arguments().get(0);
        ResolvedFunction lessThanOrEqual = specialForm.getOperatorDependency(LESS_THAN_OR_EQUAL);
        this.leftExpression = call(lessThanOrEqual, specialForm.arguments().get(1), valueExpression);
        this.rightExpression = call(lessThanOrEqual, valueExpression, specialForm.arguments().get(2));
    }

    public Supplier<ColumnarFilter> generateColumnarFilter()
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(ColumnarFilter.class.getSimpleName() + "_between", Optional.empty()),
                type(Object.class),
                type(ColumnarFilter.class));
        CallSiteBinder callSiteBinder = new CallSiteBinder();

        classDefinition.declareDefaultConstructor(a(PUBLIC));

        generateGetInputChannels(callSiteBinder, classDefinition, valueExpression);

        generateFilterRangeMethod(callSiteBinder, classDefinition);
        generateFilterListMethod(callSiteBinder, classDefinition);

        return createClassInstance(callSiteBinder, classDefinition);
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

        declareBlockVariables(ImmutableList.of(valueExpression), page, scope, body);

        Variable outputPositionsCount = scope.declareVariable("outputPositionsCount", body, constantInt(0));
        Variable position = scope.declareVariable(int.class, "position");
        Variable result = scope.declareVariable(boolean.class, "result");

        IfStatement ifStatement = new IfStatement()
                .condition(generateBlockMayHaveNull(ImmutableList.of(valueExpression), scope));
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
                        .condition(generateBlockPositionNotNull(ImmutableList.of(valueExpression), scope, position))
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

        declareBlockVariables(ImmutableList.of(valueExpression), page, scope, body);

        Variable outputPositionsCount = scope.declareVariable("outputPositionsCount", body, constantInt(0));
        Variable index = scope.declareVariable(int.class, "index");
        Variable position = scope.declareVariable(int.class, "position");
        Variable result = scope.declareVariable(boolean.class, "result");

        IfStatement ifStatement = new IfStatement()
                .condition(generateBlockMayHaveNull(ImmutableList.of(valueExpression), scope));
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
                                .condition(generateBlockPositionNotNull(ImmutableList.of(valueExpression), scope, position))
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
                .append(generateInvocation(functionManager, binder, leftExpression, scope, position)
                        .putVariable(result))
                .append(new IfStatement()
                        .condition(result)
                        .ifTrue(generateInvocation(functionManager, binder, rightExpression, scope, position)
                                .putVariable(result)))
                .append(updateOutputPositions(result, position, outputPositions, outputPositionsCount));
    }
}

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
package io.trino.plugin.pinot.query.expression;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.block.Block;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.pinot.query.expression.PinotExpressionRules.columnReference;
import static io.trino.plugin.pinot.query.expression.PinotExpressionRules.isSupportedLiteralType;
import static io.trino.plugin.pinot.query.expression.PinotExpressionRules.jsonMatch;
import static io.trino.plugin.pinot.query.expression.PinotExpressionRules.jsonPath;
import static io.trino.plugin.pinot.query.expression.PinotExpressionRules.literal;
import static io.trino.plugin.pinot.query.expression.PinotExpressionRules.pathOf;
import static io.trino.plugin.pinot.query.expression.RewriteJsonExtractScalarIsNull.JSON_EXTRACT_SCALAR;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;

/**
 * Implements {@code contains(ARRAY[...], json_extract_scalar(column, path))}
 * and {@code contains(ARRAY[...], CAST(json_extract_scalar(column, path) AS integer type))}.
 */
public class RewriteJsonContains
        implements ConnectorExpressionRule<Call, String>
{
    private static final FunctionName CONTAINS = new FunctionName("contains");

    private static final Capture<Constant> VALUES = newCapture();
    private static final Capture<Call> SEARCHED = newCapture();

    private static final Pattern<Call> PATTERN = call()
            .with(functionName().equalTo(CONTAINS))
            .with(argumentCount().equalTo(2))
            .with(argument(0).matching(constant().capturedAs(VALUES)))
            .with(argument(1).matching(call().capturedAs(SEARCHED)));

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<String> rewrite(Call call, Captures captures, RewriteContext<String> context)
    {
        Constant values = captures.get(VALUES);
        if (!(values.getType() instanceof ArrayType arrayType) || values.getValue() == null || !isSupportedLiteralType(arrayType.getElementType())) {
            return Optional.empty();
        }

        Call searched = captures.get(SEARCHED);
        boolean cast = searched.getFunctionName().equals(CAST_FUNCTION_NAME);
        if (cast) {
            if (arrayType.getElementType() instanceof VarcharType) {
                return Optional.empty();
            }
            if (searched.getArguments().size() != 1 || !(searched.getArguments().getFirst() instanceof Call inner) || !searched.getType().equals(arrayType.getElementType())) {
                return Optional.empty();
            }
            searched = inner;
        }
        else if (!(arrayType.getElementType() instanceof VarcharType)) {
            return Optional.empty();
        }

        if (!searched.getFunctionName().equals(JSON_EXTRACT_SCALAR) || searched.getArguments().size() != 2) {
            return Optional.empty();
        }
        ConnectorExpression columnArgument = searched.getArguments().get(0);
        ConnectorExpression pathArgument = searched.getArguments().get(1);
        if (!(columnArgument instanceof Variable variable) || !(pathArgument instanceof Constant pathConstant)) {
            return Optional.empty();
        }
        Optional<String> column = columnReference(context, variable);
        Optional<String> path = pathOf(pathConstant);
        if (column.isEmpty() || path.isEmpty()) {
            return Optional.empty();
        }

        Optional<String> literals = literals(arrayType.getElementType(), (Block) values.getValue());
        if (literals.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(jsonMatch(column.get(), jsonPath(path.get()) + " IN (" + literals.get() + ")"));
    }

    private static Optional<String> literals(Type elementType, Block block)
    {
        if (block.getPositionCount() == 0) {
            return Optional.empty();
        }
        ImmutableList.Builder<String> literals = ImmutableList.builder();
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                return Optional.empty();
            }
            Object value = elementType instanceof VarcharType varcharType ? varcharType.getSlice(block, position) : elementType.getLong(block, position);
            literals.add(literal(elementType, value));
        }
        return Optional.of(String.join(", ", literals.build()));
    }
}

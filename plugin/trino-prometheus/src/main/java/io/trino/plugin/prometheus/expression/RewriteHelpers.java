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
package io.trino.plugin.prometheus.expression;

import io.airlift.slice.Slice;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.function.Predicate;

import static com.google.common.base.Verify.verify;

public final class RewriteHelpers
{
    private RewriteHelpers() {}

    public static final Predicate<Call> LABEL_MAP_CHECK_CORRECT_TYPE = connectorExpression -> {
        List<ConnectorExpression> arguments = connectorExpression.getArguments();

        if (arguments.isEmpty() || !(arguments.getFirst() instanceof Variable)) {
            return false;
        }

        String variableName = ((Variable) arguments.getFirst()).getName();
        if (!variableName.equals("labels")) {
            return false;
        }
        return isCharOrStringType(connectorExpression.getType());
    };

    public static String extractLabelName(Captures captures, Capture<Call> captureKey)
    {
        // Extract the label name from the call expression, which is actually the key of the labels map.
        List<ConnectorExpression> arguments = captures.get(captureKey).getArguments();

        if (arguments.isEmpty() || !(arguments.getLast() instanceof Constant constant)) {
            throw new IllegalArgumentException("Expected the last argument to be a constant representing the label name");
        }

        Object labelNameObject = constant.getValue();
        if (!(labelNameObject instanceof Slice slice)) {
            throw new IllegalArgumentException("Expected label name to be a Slice, but got: " + (labelNameObject == null ? "null" : labelNameObject.getClass().getSimpleName()));
        }
        return slice.toStringUtf8();
    }

    public static boolean isCharOrStringType(Type type)
    {
        return type instanceof CharType || type instanceof VarcharType;
    }

    public static String extractVarchar(ConnectorExpression expression)
    {
        verify(isCharOrStringType(expression.getType()), "Unexpected type of argument: '%s' (expected VARCHAR)", expression.getType());

        if (!(expression instanceof Constant constant)) {
            throw new IllegalArgumentException("Expected a Constant expression, but got: " + expression.getClass().getSimpleName());
        }
        Object value = constant.getValue();
        if (!(value instanceof Slice slice)) {
            throw new IllegalArgumentException("Expected value to be a Slice, but got: " + (value == null ? "null" : value.getClass().getSimpleName()));
        }
        return slice.toStringUtf8();
    }
}

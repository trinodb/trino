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
package io.trino.plugin.mongodb.expression;

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.mongodb.MongoColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import org.bson.Document;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.variable;
import static io.trino.plugin.mongodb.TypeUtils.isPushdownSupportedArrayElementType;
import static io.trino.plugin.mongodb.TypeUtils.translateValue;
import static io.trino.spi.type.BooleanType.BOOLEAN;

public class ContainsRewriteRule
        implements ConnectorExpressionRule<Call, Document>
{
    private static final Capture<Variable> COLUMN = Capture.newCapture();
    private static final Capture<Constant> VALUE = Capture.newCapture();
    private static final FunctionName CONTAINS_FUNCTION_NAME = new FunctionName("contains");

    private static final Pattern<Call> PATTERN = call()
            .with(functionName().equalTo(CONTAINS_FUNCTION_NAME))
            .with(type().equalTo(BOOLEAN))
            .with(argumentCount().equalTo(2))
            .with(argument(0).matching(variable().capturedAs(COLUMN)))
            .with(argument(1).matching(constant().capturedAs(VALUE)));

    public ContainsRewriteRule()
    {
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<Document> rewrite(Call expression, Captures captures, RewriteContext<Document> context)
    {
        Variable arrayVariable = captures.get(COLUMN);
        Constant constValue = captures.get(VALUE);

        String columnName = arrayVariable.getName();

        if (!context.getAssignments().containsKey(columnName)) {
            return Optional.empty();
        }

        MongoColumnHandle columnHandle = (MongoColumnHandle) context.getAssignment(columnName);
        Type elementType = ((ArrayType) columnHandle.getType()).getElementType();
        if (!isPushdownSupportedArrayElementType(elementType)) {
            return Optional.empty();
        }

        Optional<Object> mongoValue = translateValueForJson(constValue.getValue(), elementType);

        return mongoValue.map(o -> new Document(columnHandle.getQualifiedName(), o));
    }

    private Optional<Object> translateValueForJson(Object trinoNativeValue, Type type)
    {
        return translateValue(trinoNativeValue, type).map(this::mapValueForJson);
    }

    private Object mapValueForJson(Object value)
    {
        /*
         * LocalDate and LocalDateTime in org.bson.Document causes an error when using Document.toJson().
         * So convert it to Date object.
         */
        if (value instanceof LocalDate localDate) {
            return Date.from(localDate.atStartOfDay().toInstant(ZoneOffset.UTC));
        }
        if (value instanceof LocalDateTime localDateTime) {
            return Date.from(localDateTime.toInstant(ZoneOffset.UTC));
        }

        return value;
    }
}

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

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.matching.Property;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.mongodb.MongoColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
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
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.mongodb.TypeUtils.isPushdownSupportedArrayElementType;
import static io.trino.plugin.mongodb.TypeUtils.translateValue;

public class ContainsRewriteRule
        implements ConnectorExpressionRule<Call, Document>
{
    private final Pattern<Call> expressionPattern;

    public ContainsRewriteRule()
    {
        Pattern<Call> p = Pattern.typeOf(Call.class);
        Property<Call, ?, FunctionName> functionName = Property.property("functionName", Call::getFunctionName);
        this.expressionPattern = p.with(functionName.equalTo(new FunctionName("contains")));
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return this.expressionPattern;
    }

    @Override
    public Optional<Document> rewrite(Call expression, Captures captures, RewriteContext<Document> context)
    {
        List<ConnectorExpression> arguments = expression.getArguments();

        if (arguments.size() != 2 || !(arguments.get(0) instanceof Variable) || !(arguments.get(1) instanceof Constant)) {
            return Optional.empty();
        }

        Variable arrVariable = (Variable) arguments.get(0);
        Constant constValue = (Constant) arguments.get(1);

        String columnName = arrVariable.getName();

        if (!context.getAssignments().containsKey(columnName)) {
            return Optional.empty();
        }

        MongoColumnHandle columnHandle = (MongoColumnHandle) context.getAssignment(columnName);
        Type elementType = ((ArrayType) columnHandle.getType()).getElementType();
        if (!isPushdownSupportedArrayElementType(elementType)) {
            return Optional.empty();
        }

        Optional<Object> mongoValue = translateValue(constValue.getValue(), elementType);

        return mongoValue.map(o -> new Document(columnHandle.getQualifiedName(), mapValueForJson(o)));
    }

    private Object mapValueForJson(Object value)
    {
        /*
         * LocalDate and LocalDateTime in org.bson.Document causes an error when using Document.toJson().
         * So convert it to Date object.
         */
        if (value instanceof LocalDate ld) {
            return Date.from(ld.atStartOfDay().toInstant(ZoneOffset.UTC));
        }
        if (value instanceof LocalDateTime ldt) {
            return Date.from(ldt.toInstant(ZoneOffset.UTC));
        }

        return value;
    }
}

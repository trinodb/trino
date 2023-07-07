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

import io.airlift.slice.Slice;
import io.trino.matching.Pattern;
import io.trino.spi.expression.Constant;
import io.trino.spi.type.Type;

import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class RewriteVarcharConstant
        extends RewriteConstant
{
    private static final Pattern<Constant> PATTERN = constant().with(type().equalTo(VARCHAR));

    @Override
    public Pattern<Constant> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<FilterExpression> handleNonNullValue(Type type, Object value)
    {
        Slice slice = (Slice) value;
        return Optional.of(new FilterExpression(slice.toStringUtf8(), FilterExpression.ExpressionType.LITERAL));
    }
}

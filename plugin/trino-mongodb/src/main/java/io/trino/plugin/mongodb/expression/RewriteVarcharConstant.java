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
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Constant;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.spi.type.Chars.padSpaces;

public class RewriteVarcharConstant
        extends RewriteConstant
        implements ConnectorExpressionRule<Constant, MongoExpression>
{
    private static final Pattern<Constant> PATTERN = constant().with(type().matching(type -> type instanceof VarcharType || type instanceof CharType));

    @Override
    public Pattern<Constant> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<MongoExpression> rewrite(Constant constant, Captures captures, RewriteContext<MongoExpression> context)
    {
        Slice slice = (Slice) constant.getValue();
        Type type = constant.getType();
        if (slice == null) {
            return handleNullValue();
        }
        if (type instanceof CharType charType) {
            slice = padSpaces(slice, charType);
        }
        return Optional.of(new MongoExpression(slice.toStringUtf8()));
    }
}

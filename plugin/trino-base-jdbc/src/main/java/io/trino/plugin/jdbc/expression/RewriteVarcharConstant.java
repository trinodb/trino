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
package io.trino.plugin.jdbc.expression;

import io.airlift.slice.Slice;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Constant;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Optional;
import java.util.function.Function;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RewriteVarcharConstant
        implements ConnectorExpressionRule<Constant, String>
{
    private static final Pattern<Constant> PATTERN = constant().with(type().matching(VarcharType.class::isInstance));
    private final Function<Type, Optional<String>> typeMapping;

    public RewriteVarcharConstant(Function<Type, Optional<String>> typeMapping)
    {
        this.typeMapping = requireNonNull(typeMapping, "typeMapping is null");
    }

    @Override
    public Pattern<Constant> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<String> rewrite(Constant constant, Captures captures, RewriteContext<String> context)
    {
        Slice slice = (Slice) constant.getValue();
        if (slice == null) {
            return typeMapping.apply(constant.getType()).map(typedCast -> format("CAST(NULL AS %s)", typedCast));
        }
        return Optional.of("'" + slice.toStringUtf8().replace("'", "''") + "'");
    }
}

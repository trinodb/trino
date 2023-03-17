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
package io.trino.plugin.sqlserver;

import com.google.common.base.CharMatcher;
import io.airlift.slice.Slice;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Constant;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;

public class RewriteUnicodeVarcharConstant
        implements ConnectorExpressionRule<Constant, String>
{
    private static final Pattern<Constant> PATTERN = constant().with(type().matching(VarcharType.class::isInstance));
    private static final CharMatcher UNICODE_CHARACTER_MATCHER = CharMatcher.ascii().negate().precomputed();

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
            return Optional.empty();
        }

        String sliceUtf8String = slice.toStringUtf8();
        boolean isUnicodeString = UNICODE_CHARACTER_MATCHER.matchesAnyOf(sliceUtf8String);

        if (isUnicodeString) {
            return Optional.of("N'" + sliceUtf8String.replace("'", "''") + "'");
        }

        return Optional.of("'" + sliceUtf8String.replace("'", "''") + "'");
    }
}

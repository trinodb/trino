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
package io.trino.jsonpath;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.metadata.Metadata;
import io.trino.operator.scalar.JoniRegexpFunctions;
import io.trino.spi.type.Type;
import io.trino.type.CharVarcharCoercion;
import io.trino.type.JoniRegexp;
import io.trino.type.Re2JRegexp;
import io.trino.type.RegulatorRegexp;

import java.util.function.Predicate;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;

public final class JsonPathRegex
{
    private JsonPathRegex() {}

    public static Type resolveRegexType(Metadata metadata, CharVarcharCoercion charVarcharCoercion)
    {
        // Use the same engine as the configured SQL functions, including its type parameters.
        return metadata.resolveBuiltinFunction(charVarcharCoercion, "regexp_like", ImmutableList.of(VARCHAR, VARCHAR))
                .signature().getArgumentTypes().get(1);
    }

    public static Predicate<Slice> compile(Type regexType, String pattern)
    {
        Object regex = regexType.getObject(writeNativeValue(VARCHAR, utf8Slice(pattern)), 0);
        return switch (regex) {
            case JoniRegexp joni -> source -> JoniRegexpFunctions.regexpLike(source, joni);
            case RegulatorRegexp regulator -> regulator.regex()::contains;
            case Re2JRegexp re2j -> re2j::matches;
            default -> throw new IllegalArgumentException("Unsupported regex type: " + regexType);
        };
    }
}

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
package io.trino.util;

import io.airlift.slice.Slice;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.analyzer.LiteralInterpreter;
import io.trino.sql.tree.Literal;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.SliceUtf8.rightTrim;
import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.spi.type.Chars.byteCountWithoutTrailingSpace;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Locale.ENGLISH;

public final class ColumnDefaultOptions
{
    private ColumnDefaultOptions() {}

    public static NullableValue evaluateLiteral(LiteralInterpreter literalInterpreter, Literal literal, Type type)
    {
        try {
            Object value = literalInterpreter.evaluate(literal, type);
            // NullableValue constructor checks the type compatibility between type and value
            return new NullableValue(type, noTruncationCast(value, type));
        }
        catch (RuntimeException e) {
            throw semanticException(INVALID_LITERAL, literal, e, "'%s' is not a valid %s literal", literal, type.getDisplayName().toUpperCase(ENGLISH));
        }
    }

    private static Object noTruncationCast(Object value, Type type)
    {
        if ((!(type instanceof VarcharType) && !(type instanceof CharType))) {
            return value;
        }
        int targetLength;
        if (type instanceof VarcharType varcharType) {
            if (varcharType.isUnbounded()) {
                return value;
            }
            targetLength = varcharType.getBoundedLength();
        }
        else {
            targetLength = ((CharType) type).getLength();
        }

        Slice slice = (Slice) value;
        long spaceTrimmedLength = spaceTrimmedLength(slice);
        checkArgument(targetLength >= spaceTrimmedLength, "Cannot truncate non-space characters when casting value '%s' to %s".formatted(slice.toStringUtf8(), type));
        return rightTrim(slice);
    }

    private static long spaceTrimmedLength(Slice slice)
    {
        return countCodePoints(slice, 0, byteCountWithoutTrailingSpace(slice, 0, slice.length()));
    }
}

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
package io.trino.type;

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.type.LikeFunctions.LIKE_PATTERN_FUNCTION_NAME;
import static io.trino.type.LikeFunctions.getEscapeCharacter;

public final class LikePatternFunctions
{
    private LikePatternFunctions() {}

    @ScalarFunction(value = LIKE_PATTERN_FUNCTION_NAME, hidden = true)
    @SqlType(LikePatternType.NAME)
    public static LikePattern likePattern(@SqlType("varchar") Slice pattern)
    {
        return LikePattern.compile(pattern.toStringUtf8(), Optional.empty(), false);
    }

    @ScalarFunction(value = LIKE_PATTERN_FUNCTION_NAME, hidden = true)
    @SqlType(LikePatternType.NAME)
    public static LikePattern likePattern(@SqlType("varchar") Slice pattern, @SqlType("varchar") Slice escape)
    {
        try {
            return LikePattern.compile(pattern.toStringUtf8(), getEscapeCharacter(Optional.of(escape)), false);
        }
        catch (RuntimeException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }
}

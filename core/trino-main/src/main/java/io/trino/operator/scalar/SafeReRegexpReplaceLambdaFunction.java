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
package io.trino.operator.scalar;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BufferedArrayValueBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.ArrayType;
import io.trino.sql.gen.lambda.UnaryFunctionInterface;
import io.trino.type.SafeReRegexp;
import io.trino.type.SafeReRegexpType;
import org.safere.Matcher;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VarcharType.VARCHAR;

@ScalarFunction("regexp_replace")
@Description("Replaces substrings matching a regular expression using a lambda function")
public final class SafeReRegexpReplaceLambdaFunction
{
    private final BufferedArrayValueBuilder arrayValueBuilder = BufferedArrayValueBuilder.createBuffered(new ArrayType(VARCHAR));

    @LiteralParameters("x")
    @SqlType("varchar")
    @SqlNullable
    public Slice regexpReplace(
            @SqlType("varchar") Slice source,
            @SqlType(SafeReRegexpType.NAME) SafeReRegexp pattern,
            @SqlType("function(array(varchar), varchar(x))") UnaryFunctionInterface replaceFunction)
    {
        String input = source.toStringUtf8();

        // If there is no match we can simply return the original source without doing copy.
        Matcher matcher = pattern.matcher(input);
        if (!matcher.find()) {
            return source;
        }

        StringBuilder output = new StringBuilder(input.length());

        int groupCount = matcher.groupCount();
        int appendPosition = 0;

        do {
            int start = matcher.start();
            int end = matcher.end();

            // Append the un-matched part
            if (appendPosition < start) {
                output.append(input, appendPosition, start);
            }
            appendPosition = end;

            // Append the capturing groups to the target block that will be passed to lambda
            Block target = arrayValueBuilder.build(groupCount, elementBuilder -> {
                for (int i = 1; i <= groupCount; i++) {
                    String matchedGroup = matcher.group(i);
                    if (matchedGroup != null) {
                        VARCHAR.writeSlice(elementBuilder, utf8Slice(matchedGroup));
                    }
                    else {
                        elementBuilder.appendNull();
                    }
                }
            });

            // Call the lambda function to replace the block, and append the result to output
            Slice replaced = (Slice) replaceFunction.apply(target);
            if (replaced == null) {
                // replacing a substring with null (unknown) makes the entire string null
                return null;
            }
            output.append(replaced.toStringUtf8());
        }
        while (matcher.find());

        // Append the rest of un-matched
        output.append(input, appendPosition, input.length());
        return utf8Slice(output.toString());
    }
}

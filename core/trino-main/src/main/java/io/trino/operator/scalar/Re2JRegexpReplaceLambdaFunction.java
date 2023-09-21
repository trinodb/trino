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

import com.google.re2j.Matcher;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.spi.block.Block;
import io.trino.spi.block.BufferedArrayValueBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.ArrayType;
import io.trino.sql.gen.lambda.UnaryFunctionInterface;
import io.trino.type.Re2JRegexp;
import io.trino.type.Re2JRegexpType;

import static io.trino.spi.type.VarcharType.VARCHAR;

@ScalarFunction("regexp_replace")
@Description("Replaces substrings matching a regular expression using a lambda function")
public final class Re2JRegexpReplaceLambdaFunction
{
    private final BufferedArrayValueBuilder arrayValueBuilder = BufferedArrayValueBuilder.createBuffered(new ArrayType(VARCHAR));

    @LiteralParameters("x")
    @SqlType("varchar")
    @SqlNullable
    public Slice regexpReplace(
            @SqlType("varchar") Slice source,
            @SqlType(Re2JRegexpType.NAME) Re2JRegexp pattern,
            @SqlType("function(array(varchar), varchar(x))") UnaryFunctionInterface replaceFunction)
    {
        // If there is no match we can simply return the original source without doing copy.
        Matcher matcher = pattern.matcher(source);
        if (!matcher.find()) {
            return source;
        }

        SliceOutput output = new DynamicSliceOutput(source.length());

        int groupCount = matcher.groupCount();
        int appendPosition = 0;

        do {
            int start = matcher.start();
            int end = matcher.end();

            // Append the un-matched part
            if (appendPosition < start) {
                output.writeBytes(source, appendPosition, start - appendPosition);
            }
            appendPosition = end;

            // Append the capturing groups to the target block that will be passed to lambda
            Block target = arrayValueBuilder.build(groupCount, elementBuilder -> {
                for (int i = 1; i <= groupCount; i++) {
                    Slice matchedGroupSlice = matcher.group(i);
                    if (matchedGroupSlice != null) {
                        VARCHAR.writeSlice(elementBuilder, matchedGroupSlice);
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
            output.appendBytes(replaced);
        }
        while (matcher.find());

        // Append the rest of un-matched
        output.writeBytes(source, appendPosition, source.length() - appendPosition);
        return output.slice();
    }
}

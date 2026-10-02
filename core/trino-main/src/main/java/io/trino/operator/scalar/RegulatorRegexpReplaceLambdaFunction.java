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
import io.trino.type.RegulatorRegexp;
import io.trino.type.RegulatorRegexpType;

import static io.trino.spi.type.VarcharType.VARCHAR;

@ScalarFunction("regexp_replace")
@Description("Replaces substrings matching a regular expression using a lambda function")
public final class RegulatorRegexpReplaceLambdaFunction
{
    private final BufferedArrayValueBuilder arrayValueBuilder = BufferedArrayValueBuilder.createBuffered(new ArrayType(VARCHAR));

    @LiteralParameters("x")
    @SqlType("varchar")
    @SqlNullable
    public Slice regexpReplace(
            @SqlType("varchar") Slice source,
            @SqlType(RegulatorRegexpType.NAME) RegulatorRegexp pattern,
            @SqlType("function(array(varchar), varchar(x))") UnaryFunctionInterface replaceFunction)
    {
        return pattern.regex().replace(source, groups -> {
            Block target = arrayValueBuilder.build(groups.size(), elementBuilder -> {
                for (Slice group : groups) {
                    if (group == null) {
                        elementBuilder.appendNull();
                    }
                    else {
                        VARCHAR.writeSlice(elementBuilder, group);
                    }
                }
            });
            return (Slice) replaceFunction.apply(target);
        });
    }
}

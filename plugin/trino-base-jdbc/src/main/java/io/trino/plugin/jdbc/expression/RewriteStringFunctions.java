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

import io.trino.spi.type.BooleanType;
import io.trino.spi.type.VarcharType;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.expression.Call.LIKE_PATTERN_FUNCTION_NAME;
import static java.lang.String.format;

public class RewriteStringFunctions
{
    private RewriteStringFunctions() {}

    public static RewriteFunctionCall like()
    {
        return RewriteFunctionCall
                .builder(LIKE_PATTERN_FUNCTION_NAME)
                .withResultType(BooleanType.class)
                .withRewrite(RewriteStringFunctions::formatQuery)
                .withArgumentTypes(VarcharType.class, VarcharType.class)
                .build();
    }

    public static RewriteFunctionCall likeWithEscape()
    {
        return RewriteFunctionCall
                .builder(LIKE_PATTERN_FUNCTION_NAME)
                .withResultType(BooleanType.class)
                .withRewrite(RewriteStringFunctions::formatQueryWithEscape)
                .withArgumentTypes(VarcharType.class, VarcharType.class, VarcharType.class)
                .build();
    }

    public static RewriteFunctionCall upper()
    {
        return RewriteFunctionCall
                .builder("upper")
                .withResultType(VarcharType.class)
                .withRewrite(arguments -> format("UPPER(%s)", arguments.get(0)))
                .withArgumentTypes(VarcharType.class)
                .build();
    }

    private static String formatQueryWithEscape(List<String> arguments)
    {
        checkState(arguments.size() == 3, "Like with escape expects 3 arguments");
        return format("%s LIKE %s ESCAPE %s", arguments.get(0), arguments.get(1), arguments.get(2));
    }

    private static String formatQuery(List<String> arguments)
    {
        checkState(arguments.size() == 2, "Like without escape expects 2 arguments");
        return format("%s LIKE %s", arguments.get(0), arguments.get(1));
    }
}

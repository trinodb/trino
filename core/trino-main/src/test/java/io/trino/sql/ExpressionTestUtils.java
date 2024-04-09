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
package io.trino.sql;

import io.trino.sql.ir.Expression;
import io.trino.sql.planner.assertions.ExpressionVerifier;
import io.trino.sql.planner.assertions.SymbolAliases;

public final class ExpressionTestUtils
{
    private ExpressionTestUtils() {}

    public static void assertExpressionEquals(Expression actual, Expression expected)
    {
        assertExpressionEquals(actual, expected, new SymbolAliases());
    }

    public static void assertExpressionEquals(Expression actual, Expression expected, SymbolAliases symbolAliases)
    {
        ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);
        if (!verifier.process(actual, expected)) {
            failNotEqual(actual, expected, null);
        }
    }

    private static void failNotEqual(Object actual, Object expected, String message)
    {
        String formatted = "";
        if (message != null) {
            formatted = message + " ";
        }
        throw new AssertionError(formatted + " expected [" + expected + "] but found [" + actual + "]");
    }
}

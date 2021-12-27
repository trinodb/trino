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
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.util.Failures.checkCondition;

public final class LuhnCheckFunction
{
    private LuhnCheckFunction() {}

    @Description("Checks that a string of digits is valid according to the Luhn algorithm")
    @ScalarFunction("luhn_check")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean luhnCheck(@SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        if (slice.length() == 0) {
            return false;
        }

        int nDigits = slice.length();
        int sum = 0;
        boolean secondNumber = false;

        for (int i = nDigits - 1; i >= 0; i--) {
            byte b = slice.getByte(i);
            checkCondition(b >= '0' && b <= '9', INVALID_FUNCTION_ARGUMENT, "Input contains non-digit character");
            int n = b - '0';

            // starting from the right double the value of every other number
            if (secondNumber) {
                n *= 2;
            }

            // if the value is greater than 9 subtract 9 from n
            if (n > 9) {
                n -= 9;
            }

            // add to sum
            sum += n;

            // update second number check
            secondNumber = !secondNumber;
        }
        //check returns true if mod 10 is zero
        return (sum % 10 == 0);
    }
}

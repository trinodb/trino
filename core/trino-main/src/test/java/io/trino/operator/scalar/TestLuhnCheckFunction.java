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

import org.testng.annotations.Test;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.BooleanType.BOOLEAN;

public class TestLuhnCheckFunction
        extends AbstractTestFunctions
{
    @Test
    public void testLuhnCheck()
    {
        assertFunction("luhn_check('4242424242424242')", BOOLEAN, true);
        assertFunction("luhn_check('1234567891234567')", BOOLEAN, false);
        assertFunction("luhn_check('')", BOOLEAN, false);
        assertFunction("luhn_check(NULL)", BOOLEAN, null);
        assertInvalidFunction("luhn_check('abcd424242424242')", INVALID_FUNCTION_ARGUMENT);
        assertFunction("luhn_check('123456789')", BOOLEAN, false);
        assertInvalidFunction("luhn_check('\u4EA0\u4EFF\u4EA112345')", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("luhn_check('4242\u4FE124242424242')", INVALID_FUNCTION_ARGUMENT);
    }
}

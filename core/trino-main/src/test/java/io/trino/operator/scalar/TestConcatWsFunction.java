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

import io.trino.spi.TrinoException;
import io.trino.spi.type.VarcharType;
import org.testng.annotations.Test;

public class TestConcatWsFunction
        extends AbstractTestFunctions
{
    private static final VarcharType RETURN_TYPE = VarcharType.createUnboundedVarcharType();

    @Test
    public void testSimple()
    {
        assertFunction("concat_ws('abc', 'def')", "def");
        assertFunction("concat_ws(',', 'def')", "def");
        assertFunction("concat_ws(',', 'def', 'pqr', 'mno')", "def,pqr,mno");
        assertFunction("concat_ws('abc', 'def', 'pqr')", "defabcpqr");
    }

    @Test
    public void testEmpty()
    {
        assertFunction("concat_ws('', 'def')", "def");
        assertFunction("concat_ws('', 'def', 'pqr')", "defpqr");
        assertFunction("concat_ws('', '', 'pqr')", "pqr");
        assertFunction("concat_ws('', 'def', '')", "def");
        assertFunction("concat_ws('', '', '')", "");
        assertFunction("concat_ws(',', 'def', '')", "def,");
        assertFunction("concat_ws(',', 'def', '', 'pqr')", "def,,pqr");
        assertFunction("concat_ws(',', '', 'pqr')", ",pqr");
    }

    @Test
    public void testNull()
    {
        assertFunction("concat_ws(NULL, 'def')", null);
        assertFunction("concat_ws(NULL, cast(NULL as VARCHAR))", null);
        assertFunction("concat_ws(NULL, 'def', 'pqr')", null);
        assertFunction("concat_ws(',', cast(NULL as VARCHAR))", "");
        assertFunction("concat_ws(',', NULL, 'pqr')", "pqr");
        assertFunction("concat_ws(',', 'def', NULL)", "def");
        assertFunction("concat_ws(',', 'def', NULL, 'pqr')", "def,pqr");
        assertFunction("concat_ws(',', 'def', NULL, NULL, 'mno', 'xyz', NULL, 'box')", "def,mno,xyz,box");
    }

    @Test
    public void testArray()
    {
        assertFunction("concat_ws(',', ARRAY[])", "");
        assertFunction("concat_ws(',', ARRAY['abc'])", "abc");
        assertFunction("concat_ws(',', ARRAY['abc', 'def', 'pqr', 'xyz'])", "abc,def,pqr,xyz");
        assertFunction("concat_ws(null, ARRAY['abc'])", null);
        assertFunction("concat_ws(',', cast(NULL as array(varchar)))", null);
        assertFunction("concat_ws(',', ARRAY['abc', null, null, 'xyz'])", "abc,xyz");
        assertFunction("concat_ws(',', ARRAY['abc', '', '', 'xyz','abcdefghi'])", "abc,,,xyz,abcdefghi");
    }

    @Test(expectedExceptions = TrinoException.class, expectedExceptionsMessageRegExp = ".*Unexpected parameters.*")
    public void testBadArray()
    {
        assertFunction("concat_ws(',', ARRAY[1, 15])", "");
    }

    @Test(expectedExceptions = TrinoException.class, expectedExceptionsMessageRegExp = ".*Unexpected parameters.*")
    public void testBadArguments()
    {
        assertFunction("concat_ws(',', 1, 15)", "");
    }

    @Test(expectedExceptions = TrinoException.class, expectedExceptionsMessageRegExp = "There must be two or more.*")
    public void testLowArguments()
    {
        assertFunction("concat_ws(',')", "");
    }

    private void assertFunction(String call, String expected)
    {
        assertFunction(call, RETURN_TYPE, expected);
    }
}

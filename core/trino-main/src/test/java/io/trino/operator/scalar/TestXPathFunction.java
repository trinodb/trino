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

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.VarcharType;
import org.testng.annotations.Test;

import java.util.Arrays;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class TestXPathFunction
        extends AbstractTestFunctions
{
    @Test
    public void testXpathString()
    {
        assertFunction("xpath_string('<a><b>bb</b><c>cc</c></a>','a/b')", VarcharType.createVarcharType(25), "bb");
        assertFunction("xpath_string('<a><b>bb</b><c>cc</c></a>','a')", VarcharType.createVarcharType(25), "bbcc");
        assertFunction("xpath_string('<a><b>bb</b><c>cc</c></a>','a/d')", VarcharType.createVarcharType(25), "");
        assertFunction("xpath_string('<a><b>b1</b><b>b2</b></a>','//b')", VarcharType.createVarcharType(25), "b1");
        assertFunction("xpath_string('<a><b>b1</b><b>b2</b></a>','a/b[2]')", VarcharType.createVarcharType(25), "b2");
        assertFunction("xpath_string('<a><b>b1</b><b id=\"b_2\">b2</b></a>','a/b[@id=\"b_2\"]')", VarcharType.createVarcharType(34), "b2");
        assertInvalidFunction("xpath_string('\"\"','detail/reason')", INVALID_FUNCTION_ARGUMENT, "Error parsing xml data '\"\"'");
    }

    @Test
    public void testXpath()
    {
        assertFunction("xpath('<a><b id=\"foo\">b1</b><b id=\"bar\">b2</b></a>','//@id')",
                new ArrayType(VARCHAR),
                Arrays.asList("foo", "bar"));
        assertFunction("xpath('<a><b id=\"1\"><c/></b><b id=\"2\"><c/></b></a>','/descendant::c/ancestor::b/@id')",
                new ArrayType(VARCHAR),
                Arrays.asList("1", "2"));
        assertFunction("xpath('<a><b>b1</b><b>b2</b></a>','a/*')",
                new ArrayType(VARCHAR),
                Arrays.asList());
        assertFunction("xpath('<a><b>b1</b><b>b2</b></a>','a/*/text()')",
                new ArrayType(VARCHAR),
                Arrays.asList("b1", "b2"));
        assertFunction("xpath('<a><b class=\"bb\">b1</b><b>b2</b><b>b3</b><c class=\"bb\">c1</c><c>c2</c></a>','a/*[@class=\"bb\"]/text()')",
                new ArrayType(VARCHAR),
                Arrays.asList("b1", "c1"));
    }

    @Test
    public void testXpathBoolean()
    {
        assertFunction("xpath_boolean('<a><b>b</b></a>','a/b')",
                BOOLEAN,
                true);
        assertFunction("xpath_boolean('<a><b>b</b></a>','a/c')",
                BOOLEAN,
                false);
        assertFunction("xpath_boolean('<a><b>b</b></a>','a/b = \"b\"')",
                BOOLEAN,
                true);
        assertFunction("xpath_boolean('<a><b>10</b></a>','a/b < 10')",
                BOOLEAN,
                false);
    }

    @Test
    public void testXpathDouble()
    {
        assertFunction("xpath_double('<a>b</a>','a = 10')",
                DoubleType.DOUBLE,
                0.0);
        assertFunction("xpath_double('<a>this is not a number</a>','a')",
                DoubleType.DOUBLE,
                Double.NaN);
        assertFunction("xpath_double('<a><b>2000000000</b><c>40000000000</c></a>','a/b * a/c')",
                DoubleType.DOUBLE,
                8.0E19);
    }
}

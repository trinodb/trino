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

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.ArrayType;
import org.testng.annotations.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Double.NaN;

public class TestXPathFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testXPath()
    {
        assertFunction("xpath('<a><b>b1</b><b>b2</b></a>', '//b')", new ArrayType(createVarcharType(25)), ImmutableList.of());
        assertFunction("xpath('<a><b>b1</b><b>b2</b></a>', '//b')", new ArrayType(createVarcharType(25)), ImmutableList.of());
        assertFunction("xpath('<a><b>b1</b><b>b2</b></a>','a/*/text()')", new ArrayType(createVarcharType(25)), ImmutableList.of("b1", "b2"));
        assertFunction("xpath('<a><b class=\"bb\">b1</b><b>b2</b><b>b3</b><c class=\"bb\">c1</c><c>c2</c></a>', 'a/*[@class=\"bb\"]/text()')",
                new ArrayType(createVarcharType(74)), ImmutableList.of("b1", "c1"));

        // test illegal xml string

        // test non-ascii characters
        assertFunction("xpath('<a><b>中文</b></a>', '/a/b/text()')", new ArrayType(createVarcharType(16)), ImmutableList.of("中文"));
        assertFunction("xpath('<a><b>\\xe4\\xb8\\xad\\xe6\\x96\\x87</b></a>', '/a/b/text()')", new ArrayType(createVarcharType(38)),
                ImmutableList.of("\\xe4\\xb8\\xad\\xe6\\x96\\x87"));
    }

    @Test
    public void testXPathWithNull()
    {
        assertFunction("xpath('<a><b>b1</b><b>b2</b></a>',null)", new ArrayType(createVarcharType(25)), null);
        assertFunction("xpath(null,null)", new ArrayType(createVarcharType(0)), null);
        assertFunction("xpath_boolean(null, 'a/c')", BOOLEAN, null);
        assertFunction("xpath_boolean(null, null)", BOOLEAN, null);
        assertFunction("xpath_long(null, 'a/c')", BIGINT, null);
        assertFunction("xpath_long(null, null)", BIGINT, null);
        assertFunction("xpath_double(null, '//a')", DOUBLE, null);
        assertFunction("xpath_double(null, null)", DOUBLE, null);
        assertFunction("xpath_string(null, 'a/b')", VARCHAR, null);
        assertFunction("xpath_string(null, null)", VARCHAR, null);
    }

    @Test
    public void testXpathWithIllegal()
    {
        assertFunction("xpath('/a', '/')", new ArrayType(createVarcharType(2)), ImmutableList.of());
        assertFunction("xpath('<a>hello<b>world</b>', '/')", new ArrayType(createVarcharType(20)), ImmutableList.of());
        assertFunction("xpath_boolean('/a', '/')", BOOLEAN, null);
        assertFunction("xpath_boolean('<a>hello<b>world</b>', '/')", BOOLEAN, null);
        assertFunction("xpath_long('/a', '/')", BIGINT, null);
        assertFunction("xpath_long('<a>hello<b>world</b>', '/')", BIGINT, null);
        assertFunction("xpath_double('/a', '/')", DOUBLE, null);
        assertFunction("xpath_double('<a>hello<b>world</b>', '/')", DOUBLE, null);
        assertFunction("xpath_string('/a', '/')", VARCHAR, null);
        assertFunction("xpath_string('<a>hello<b>world</b>', '/')", VARCHAR, null);
    }

    @Test
    public void testXPathBoolean()
    {
        assertFunction("xpath_boolean('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', 'a/text()')", BOOLEAN, false);
        assertFunction("xpath_boolean('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', 'a/b')", BOOLEAN, true);
        assertFunction("xpath_boolean('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', '/a')", BOOLEAN, true);
    }

    @Test
    public void testXPathLong()
    {
        assertFunction("xpath_long('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', 'count(//b)')", BIGINT, 3L);
        assertFunction("xpath_long('<a><b>12</b><b>23</b><b>34</b><c>c1</c><c>c2</c></a>', 'sum(//b/text())')", BIGINT, 69L);
        assertFunction("xpath_long('<a><b>12</b><b>23</b><b>-34</b><c>c1</c><c>c2</c></a>', 'sum(//b/text())')", BIGINT, 1L);
        assertFunction("xpath_long('<a><b>120000000001</b><b>230000000000000000001</b><b>34</b><c>c1</c><c>c2</c></a>', 'sum(//b/text())')",
                BIGINT, 9223372036854775807L);

        assertFunction("xpath_long('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', 'a/text()')", BIGINT, 0L);
        assertFunction("xpath_long('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', 'a/b')", BIGINT, 0L);
        assertFunction("xpath_long('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', '/a')", BIGINT, 0L);
    }

    @Test
    public void testXPathDouble()
    {
        assertFunction("xpath_double('<a>b</a>', 'a = 10')", DOUBLE, 0.0);
        assertFunction("xpath_double('<a>this is not a number</a>', 'a')", DOUBLE, NaN);
        assertFunction("xpath_double('<a><b>2000000000</b><c>40000000000</c></a>', 'a/b * a/c')", DOUBLE, 8.0E19);
        assertFunction("xpath_double('<a><b>2000000000.123456789</b><c>40000000000.98765432001</c></a>', 'a/b * a/c')", DOUBLE, 8.000000000691357E19);
        assertFunction("xpath_double('<a><b>20000</b><c>400000</c></a>', null)", DOUBLE, null);
    }

    @Test
    public void testXPathString()
    {
        assertFunction("xpath_string('<a><b>bb</b><c>cc</c></a>', 'a/b')", VARCHAR, "bb");
        assertFunction("xpath_string('<a><b>bb</b><c>cc</c></a>', 'a')", VARCHAR, "bbcc");
        assertFunction("xpath_string('<a><b>bb</b><c>cc</c></a>', 'a/d')", VARCHAR, "");
        assertFunction("xpath_string('<a><b>b1</b><b>b2</b></a>', '//b')", VARCHAR, "b1");
        assertFunction("xpath_string('<a><b>b1</b><b id=\"b_2\">b2</b></a>', 'a/b[@id=\"b_2\"]')", VARCHAR, "b2");
        assertFunction("xpath_string('<a><b>b1</b><b id=\"b_2\">b2</b></a>', null)", VARCHAR, null);
        assertFunction("xpath_string('<a><b>b1</b><b id=\"b_2\">b2</b></a>', '/a/b[@id=\"b_2\"]')", VARCHAR, "b2");
    }
}

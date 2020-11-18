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
package io.prestosql.type;

import io.prestosql.operator.scalar.AbstractTestFunctions;
import io.prestosql.spi.type.TSRangeType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.prestosql.spi.StandardErrorCode.CONSTRAINT_VIOLATION;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.TSRange.fromString;
import static io.prestosql.spi.type.TSRangeType.TSRANGE_MICROS;
import static io.prestosql.spi.type.TSRangeType.TSRANGE_MILLIS;
import static io.prestosql.spi.type.TSRangeType.TSRANGE_NANOS;
import static io.prestosql.spi.type.TSRangeType.TSRANGE_SECONDS;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public class TestTSRangeOperators
        extends AbstractTestFunctions
{
    // test tsrange with long-slice format
    @DataProvider(name = "tsrangeTypes")
    public Object[][] tsrangeTypes()
    {
        return new Object[][] {
                {TSRANGE_SECONDS}, {TSRANGE_MILLIS}, {TSRANGE_MICROS}, {TSRANGE_NANOS}
        };
    }

    @Test(dataProvider = "tsrangeTypes")
    public void testVarcharToTSRangeLongCast(TSRangeType type)
    {
        final int precision = type.getPrecision();
        assertFunction(format("CAST('[1,2)' AS TSRANGE(%s))", precision), type, fromString("[1,2)"));
        assertFunction(format("CAST('[2,3]' AS TSRANGE(%s))", precision), type, fromString("[2,3]"));
        assertFunction(format("CAST('(2,3]' AS TSRANGE(%s))", precision), type, fromString("(2,3]"));
        assertFunction(format("CAST('[2,2]' AS TSRANGE(%s))", precision), type, fromString("[2,2]"));
        assertFunction(format("CAST('(2,2]' AS TSRANGE(%s))", precision), type, fromString("empty"));
        assertFunction(format("CAST('[2,2)' AS TSRANGE(%s))", precision), type, fromString("empty"));
        assertFunction(format("CAST('(2,2)' AS TSRANGE(%s))", precision), type, fromString("empty"));
        assertFunction(format("CAST('(0,0)' AS TSRANGE(%s))", precision), type, fromString("empty"));
        assertFunction(format("CAST('(0,0)' AS TSRANGE(%s))", precision), type, fromString("empty"));
        assertInvalidCast(format("CAST('[3,2]' AS TSRANGE(%s))", precision), "Cannot cast value to tsrange: [3,2]");
        assertInvalidCast(format("CAST('[-1,3]' AS TSRANGE(%s))", precision), "Cannot cast value to tsrange: [-1,3]");
        assertInvalidCast(format("CAST('-$?a' AS TSRANGE(%s))", precision), "Cannot cast value to tsrange: -$?a");
        // timestamp format
        assertFunction(format("CAST('[2020-01-01 00:00:00,2020-01-01 05:00:00)' AS TSRANGE(%s))", precision), type,
                fromString("[2020-01-01 00:00:00,2020-01-01 05:00:00)"));
    }

    @Test
    public void testTSRangeToVarcharCast()
    {
        assertFunction("CAST(TSRANGE '[2,3)' AS VARCHAR)", VARCHAR, fromString("[2,3)").toString());
        assertFunction("CAST(TSRANGE '[2,3]' AS VARCHAR)", VARCHAR, fromString("[2,3]").toString());
        assertFunction("CAST(TSRANGE '(1,1]' AS VARCHAR)", VARCHAR, "empty");
        assertFunction("CAST(TSRANGE '[2,2)' AS VARCHAR)", VARCHAR, "empty");
        assertFunction("CAST(TSRANGE '(3,3)' AS VARCHAR)", VARCHAR, "empty");
        assertInvalidCast("CAST(TSRANGE '(3,2]' AS VARCHAR)", "Cannot cast value to tsrange: (3,2]");
    }

    @Test
    public void testEquals()
    {
        assertFunction("TSRANGE '[1,2)' = TSRANGE '[1,2)'", BOOLEAN, true);
        assertFunction("TSRANGE '[1,2)' = TSRANGE '[2,3)'", BOOLEAN, false);
        assertFunction("TSRANGE '[1,2)' = TSRANGE '[1,2]'", BOOLEAN, false);
    }

    // verify same behaviour for "+","*","-" as for postgres
    @Test
    public void testUnion()
    {
        assertFunction("TSRANGE '(1,3)' + TSRANGE '(2,4)'", TSRANGE_MILLIS, fromString("(1,4)"));
        // commutative (a,b)+(c,d) = (c,d)+(a,b)
        assertFunction("TSRANGE '(2,4)' + TSRANGE '(1,3)'", TSRANGE_MILLIS, fromString("(1,4)"));
        assertFunction("TSRANGE '(2,4)' + TSRANGE 'empty'", TSRANGE_MILLIS, fromString("(2,4)"));
        assertFunction("TSRANGE '(1,2)' + TSRANGE '[2,5]'", TSRANGE_MILLIS, fromString("(1,5]"));
        assertFunction("TSRANGE '(1,2]' + TSRANGE '(2,5]'", TSRANGE_MILLIS, fromString("(1,5]"));
        assertInvalidFunction("TSRANGE '(1,2)' + TSRANGE '(2,5)'",
                CONSTRAINT_VIOLATION, "Result of tsrange union would not be contiguous");
    }

    @Test
    public void testIntersection()
    {
        //identity
        assertFunction("TSRANGE '(1,3)' * TSRANGE '(1,3)'", TSRANGE_MILLIS, fromString("(1,3)"));
        assertFunction("TSRANGE '(1,3)' * TSRANGE '(2,4)'", TSRANGE_MILLIS, fromString("(2,3)"));
        // commutative (a,b)*(c,d) = (c,d)*(a,b)
        assertFunction("TSRANGE '(2,4)' * TSRANGE '(1,3)'", TSRANGE_MILLIS, fromString("(2,3)"));
        assertFunction("TSRANGE '(1,3]' * TSRANGE '[3,4)'", TSRANGE_MILLIS, fromString("[3,3]"));
        assertFunction("TSRANGE '(1,3)' * TSRANGE '[1,3]'", TSRANGE_MILLIS, fromString("(1,3)"));
        assertFunction("TSRANGE '(2,4)' * TSRANGE '(3,5]'", TSRANGE_MILLIS, fromString("(3,4)"));
        assertFunction("TSRANGE '(3,5)' * TSRANGE '(2,4)'", TSRANGE_MILLIS, fromString("(3,4)"));
        assertFunction("TSRANGE '(1,3)' * TSRANGE '[3,4)'", TSRANGE_MILLIS, fromString("empty"));
        assertFunction("TSRANGE '(1,3]' * TSRANGE '(3,4)'", TSRANGE_MILLIS, fromString("empty"));
        assertFunction("TSRANGE '(1,3]' * TSRANGE '(5,6)'", TSRANGE_MILLIS, fromString("empty"));
    }

    @Test
    public void testDifference()
    {
        assertFunction("TSRANGE '(5,15)' - TSRANGE '(10,20)'", TSRANGE_MILLIS, fromString("(5,10]"));
        assertFunction("TSRANGE '(5,15)' - TSRANGE '[10,20)'", TSRANGE_MILLIS, fromString("(5,10)"));
        assertFunction("TSRANGE '(5,15)' - TSRANGE '[0,10)'", TSRANGE_MILLIS, fromString("[10,15)"));
        assertFunction("TSRANGE '[5,20]' - TSRANGE '[5,20)'", TSRANGE_MILLIS, fromString("[20,20]"));
        assertInvalidFunction("TSRANGE '[5,20]' - TSRANGE '(5,20)'",
                CONSTRAINT_VIOLATION, "Result of tsrange union would not be contiguous");
        assertFunction("TSRANGE '(10,15)' - TSRANGE '(5,20)'", TSRANGE_MILLIS, fromString("empty"));
        assertFunction("TSRANGE '(10,15)' - TSRANGE 'empty'", TSRANGE_MILLIS, fromString("(10,15)"));
        assertFunction("TSRANGE '(10,15)' - TSRANGE '(10,15)'", TSRANGE_MILLIS, fromString("empty"));
        assertFunction("TSRANGE '(10,15)' - TSRANGE '(10,14)'", TSRANGE_MILLIS, fromString("[14,15)"));
    }
}

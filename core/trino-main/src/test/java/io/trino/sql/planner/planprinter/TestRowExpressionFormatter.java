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
package io.trino.sql.planner.planprinter;

import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.metadata.Metadata;
import io.trino.metadata.MetadataManager;
import io.trino.spi.block.LongArrayBlockBuilder;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;
import io.trino.sql.relational.VariableReferenceExpression;
import org.testng.annotations.Test;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.MODULUS;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.NEGATION;
import static io.trino.spi.function.OperatorType.SUBSCRIPT;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.constantNull;
import static io.trino.sql.relational.SpecialForm.Form.AND;
import static io.trino.sql.relational.SpecialForm.Form.IS_NULL;
import static io.trino.sql.relational.SpecialForm.Form.OR;
import static io.trino.type.ColorType.COLOR;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.lang.Float.floatToIntBits;
import static org.testng.Assert.assertEquals;

public class TestRowExpressionFormatter
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();

    private static final VariableReferenceExpression C_BIGINT = new VariableReferenceExpression("c_bigint", BIGINT);
    private static final RowExpressionFormatter FORMATTER = new RowExpressionFormatter();
    private static final VariableReferenceExpression C_BIGINT_ARRAY = new VariableReferenceExpression("c_bigint_array", new ArrayType(BIGINT));

    @Test
    public void testConstants()
    {
        // null
        RowExpression constantExpression = constantNull(UNKNOWN);
        assertEquals(format(constantExpression), "null");

        // boolean
        constantExpression = constant(true, BOOLEAN);
        assertEquals(format(constantExpression), "BOOLEAN'true'");

        // double
        constantExpression = constant(1.1, DOUBLE);
        assertEquals(format(constantExpression), "DOUBLE'1.1'");
        constantExpression = constant(Double.NaN, DOUBLE);
        assertEquals(format(constantExpression), "DOUBLE'NaN'");
        constantExpression = constant(Double.POSITIVE_INFINITY, DOUBLE);
        assertEquals(format(constantExpression), "DOUBLE'Infinity'");

        // real
        constantExpression = constant((long) floatToIntBits(1.1f), REAL);
        assertEquals(format(constantExpression), "REAL'1.1'");
        constantExpression = constant((long) floatToIntBits(Float.NaN), REAL);
        assertEquals(format(constantExpression), "REAL'NaN'");
        constantExpression = constant((long) floatToIntBits(Float.POSITIVE_INFINITY), REAL);
        assertEquals(format(constantExpression), "REAL'Infinity'");

        // string
        constantExpression = constant(utf8Slice("abcde"), VARCHAR);
        assertEquals(format(constantExpression), "VARCHAR'abcde'");
        constantExpression = constant(utf8Slice("fgh"), createCharType(3));
        assertEquals(format(constantExpression), "CHAR'fgh'");

        // integer
        constantExpression = constant(1L, TINYINT);
        assertEquals(format(constantExpression), "TINYINT'1'");
        constantExpression = constant(1L, SMALLINT);
        assertEquals(format(constantExpression), "SMALLINT'1'");
        constantExpression = constant(1L, INTEGER);
        assertEquals(format(constantExpression), "INTEGER'1'");
        constantExpression = constant(1L, BIGINT);
        assertEquals(format(constantExpression), "BIGINT'1'");

        // varbinary
        Slice value = Slices.wrappedBuffer(BaseEncoding.base16().decode("123456AB"));
        constantExpression = constant(value, VARBINARY);
        assertEquals(format(constantExpression), "X'12 34 56 ab'");

        // color
        constantExpression = constant(256L, COLOR);
        assertEquals(format(constantExpression), "COLOR'256'");

        // long and short decimals
        constantExpression = constant(decimal("1.2345678910"), DecimalType.createDecimalType(11, 10));
        assertEquals(format(constantExpression), "DECIMAL'1.2345678910'");
        constantExpression = constant(decimal("1.281734081274028174012432412423134"), DecimalType.createDecimalType(34, 33));
        assertEquals(format(constantExpression), "DECIMAL'1.281734081274028174012432412423134'");

        // time
        constantExpression = constant(662727600000L, TIMESTAMP_MILLIS);
        assertEquals(format(constantExpression), "TIMESTAMP'1970-01-08 16:05:27.600'");
        constantExpression = constant(7670L, DATE);
        assertEquals(format(constantExpression), "DATE'1991-01-01'");

        // interval
        constantExpression = constant(24L, INTERVAL_DAY_TIME);
        assertEquals(format(constantExpression), "INTERVAL DAY TO SECOND'0 00:00:00.024'");
        constantExpression = constant(25L, INTERVAL_YEAR_MONTH);
        assertEquals(format(constantExpression), "INTERVAL YEAR TO MONTH'2-1'");

        // block
        constantExpression = constant(new LongArrayBlockBuilder(null, 4).writeLong(1L).writeLong(2).build(), new ArrayType(BIGINT));
        assertEquals(format(constantExpression), "[Block: position count: 2; size: 88 bytes]");
    }

    @Test
    public void testCalls()
    {
        RowExpression callExpression;

        // arithmetic
        callExpression = createCallExpression(ADD);
        assertEquals(format(callExpression), "(c_bigint) + (BIGINT'5')");
        callExpression = createCallExpression(SUBTRACT);
        assertEquals(format(callExpression), "(c_bigint) - (BIGINT'5')");
        callExpression = createCallExpression(MULTIPLY);
        assertEquals(format(callExpression), "(c_bigint) * (BIGINT'5')");
        callExpression = createCallExpression(DIVIDE);
        assertEquals(format(callExpression), "(c_bigint) / (BIGINT'5')");
        callExpression = createCallExpression(MODULUS);
        assertEquals(format(callExpression), "(c_bigint) % (BIGINT'5')");

        // comparison
        callExpression = createCallExpression(LESS_THAN);
        assertEquals(format(callExpression), "(c_bigint) < (BIGINT'5')");
        callExpression = createCallExpression(LESS_THAN_OR_EQUAL);
        assertEquals(format(callExpression), "(c_bigint) <= (BIGINT'5')");
        callExpression = createCallExpression(EQUAL);
        assertEquals(format(callExpression), "(c_bigint) = (BIGINT'5')");
        callExpression = createCallExpression(IS_DISTINCT_FROM);
        assertEquals(format(callExpression), "(c_bigint) IS DISTINCT FROM (BIGINT'5')");

        // negation
        RowExpression expression = createCallExpression(ADD);
        callExpression = call(
                METADATA.resolveOperator(TEST_SESSION, NEGATION, List.of(expression.getType())),
                expression);
        assertEquals(format(callExpression), "-((c_bigint) + (BIGINT'5'))");

        // subscript
        ArrayType arrayType = (ArrayType) C_BIGINT_ARRAY.getType();
        Type elementType = arrayType.getElementType();
        RowExpression subscriptExpression = call(
                METADATA.resolveOperator(TEST_SESSION, SUBSCRIPT, ImmutableList.of(arrayType, elementType)),
                ImmutableList.of(C_BIGINT_ARRAY, constant(0L, INTEGER)));
        callExpression = subscriptExpression;
        assertEquals(format(callExpression), "c_bigint_array[INTEGER'0']");

        // cast
        callExpression = call(
                METADATA.getCoercion(TEST_SESSION, TINYINT, BIGINT),
                constant(1L, TINYINT));
        assertEquals(format(callExpression), "CAST(TINYINT'1' AS bigint)");

        // other
        callExpression = call(
                METADATA.resolveOperator(TEST_SESSION, HASH_CODE, ImmutableList.of(BIGINT)),
                constant(1L, BIGINT));
        assertEquals(format(callExpression), "HASH CODE(BIGINT'1')");
    }

    @Test
    public void testSpecialForm()
    {
        RowExpression specialFormExpression;

        specialFormExpression = new SpecialForm(AND, BOOLEAN, createCallExpression(EQUAL), createCallExpression(IS_DISTINCT_FROM));
        assertEquals(format(specialFormExpression), "((c_bigint) = (BIGINT'5')) AND ((c_bigint) IS DISTINCT FROM (BIGINT'5'))");

        // other
        specialFormExpression = new SpecialForm(IS_NULL, BOOLEAN, createCallExpression(ADD));
        assertEquals(format(specialFormExpression), "IS_NULL((c_bigint) + (BIGINT'5'))");
    }

    @Test
    public void testComplex()
    {
        RowExpression complexExpression;

        RowExpression expression = createCallExpression(ADD);
        complexExpression = call(
                METADATA.resolveOperator(TEST_SESSION, SUBTRACT, ImmutableList.of(BIGINT, BIGINT)),
                C_BIGINT,
                expression);
        assertEquals(format(complexExpression), "(c_bigint) - ((c_bigint) + (BIGINT'5'))");

        RowExpression expression1 = createCallExpression(ADD);
        RowExpression expression2 = call(
                METADATA.resolveOperator(TEST_SESSION, MULTIPLY, ImmutableList.of(BIGINT, BIGINT)),
                expression1,
                C_BIGINT);
        RowExpression expression3 = createCallExpression(LESS_THAN);
        complexExpression = new SpecialForm(OR, BOOLEAN, expression2, expression3);
        assertEquals(format(complexExpression), "(((c_bigint) + (BIGINT'5')) * (c_bigint)) OR ((c_bigint) < (BIGINT'5'))");

        ArrayType arrayType = (ArrayType) C_BIGINT_ARRAY.getType();
        Type elementType = arrayType.getElementType();
        expression1 = call(
                METADATA.resolveOperator(TEST_SESSION, SUBSCRIPT, ImmutableList.of(arrayType, elementType)),
                ImmutableList.of(C_BIGINT_ARRAY, constant(5L, INTEGER)));
        expression2 = call(
                METADATA.resolveOperator(TEST_SESSION, NEGATION, ImmutableList.of(expression1.getType())),
                expression1);
        expression3 = call(
                METADATA.resolveOperator(TEST_SESSION, ADD, ImmutableList.of(expression2.getType(), BIGINT)),
                expression2,
                constant(5L, BIGINT));
        assertEquals(format(expression3), "(-(c_bigint_array[INTEGER'5'])) + (BIGINT'5')");
    }

    protected static Object decimal(String decimalString)
    {
        return Decimals.parse(decimalString).getObject();
    }

    private static CallExpression createCallExpression(OperatorType type)
    {
        return call(
                METADATA.resolveOperator(TEST_SESSION, type, ImmutableList.of(BIGINT, BIGINT)),
                C_BIGINT,
                constant(5L, BIGINT));
    }

    private static String format(RowExpression expression)
    {
        return FORMATTER.formatRowExpression(TEST_SESSION.toConnectorSession(), expression);
    }
}

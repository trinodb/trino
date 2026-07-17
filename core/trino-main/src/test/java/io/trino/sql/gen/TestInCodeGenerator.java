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
package io.trino.sql.gen;

import io.airlift.slice.Slices;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.gen.InCodeGenerator.SwitchGenerationCase.DIRECT_SWITCH;
import static io.trino.sql.gen.InCodeGenerator.SwitchGenerationCase.HASH_SWITCH;
import static io.trino.sql.gen.InCodeGenerator.SwitchGenerationCase.SET_CONTAINS;
import static io.trino.sql.gen.InCodeGenerator.checkSwitchGenerationCase;
import static io.trino.sql.ir.IrExpressions.call;
import static io.trino.sql.ir.IrExpressions.constantNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestInCodeGenerator
{
    private final TestingFunctionResolution functionResolution = new TestingFunctionResolution();

    @Test
    public void testInteger()
    {
        List<Expression> values = new ArrayList<>();
        values.add(new Constant(INTEGER, (long) Integer.MIN_VALUE));
        values.add(new Constant(INTEGER, (long) Integer.MAX_VALUE));
        values.add(new Constant(INTEGER, 3L));
        assertThat(checkSwitchGenerationCase(INTEGER, values)).isEqualTo(DIRECT_SWITCH);

        values.add(constantNull(INTEGER));
        assertThat(checkSwitchGenerationCase(INTEGER, values)).isEqualTo(DIRECT_SWITCH);
        values.add(call(
                functionResolution.getCoercion(DOUBLE, INTEGER),
                new Constant(DOUBLE, 12345678901234.0)));
        assertThat(checkSwitchGenerationCase(INTEGER, values)).isEqualTo(DIRECT_SWITCH);

        values.add(new Constant(BIGINT, 6L));
        values.add(new Constant(BIGINT, 7L));
        assertThat(checkSwitchGenerationCase(INTEGER, values)).isEqualTo(DIRECT_SWITCH);

        values.add(new Constant(INTEGER, 8L));
        assertThat(checkSwitchGenerationCase(INTEGER, values)).isEqualTo(SET_CONTAINS);
    }

    @Test
    public void testBigint()
    {
        List<Expression> values = new ArrayList<>();
        values.add(new Constant(BIGINT, Integer.MAX_VALUE + 1L));
        values.add(new Constant(BIGINT, Integer.MIN_VALUE - 1L));
        values.add(new Constant(BIGINT, 3L));
        assertThat(checkSwitchGenerationCase(BIGINT, values)).isEqualTo(HASH_SWITCH);

        values.add(constantNull(BIGINT));
        assertThat(checkSwitchGenerationCase(BIGINT, values)).isEqualTo(HASH_SWITCH);
        values.add(call(
                functionResolution.getCoercion(DOUBLE, BIGINT),
                new Constant(DOUBLE, 12345678901234.0)));
        assertThat(checkSwitchGenerationCase(BIGINT, values)).isEqualTo(HASH_SWITCH);

        values.add(new Constant(BIGINT, 6L));
        values.add(new Constant(BIGINT, 7L));
        assertThat(checkSwitchGenerationCase(BIGINT, values)).isEqualTo(HASH_SWITCH);

        values.add(new Constant(BIGINT, 8L));
        assertThat(checkSwitchGenerationCase(BIGINT, values)).isEqualTo(SET_CONTAINS);
    }

    @Test
    public void testDate()
    {
        List<Expression> values = new ArrayList<>();
        values.add(new Constant(DATE, 1L));
        values.add(new Constant(DATE, 2L));
        values.add(new Constant(DATE, 3L));
        assertThat(checkSwitchGenerationCase(DATE, values)).isEqualTo(DIRECT_SWITCH);

        for (long i = 4; i <= 7; ++i) {
            values.add(new Constant(DATE, i));
        }
        assertThat(checkSwitchGenerationCase(DATE, values)).isEqualTo(DIRECT_SWITCH);

        values.add(new Constant(DATE, 33L));
        assertThat(checkSwitchGenerationCase(DATE, values)).isEqualTo(SET_CONTAINS);
    }

    @Test
    public void testDouble()
    {
        List<Expression> values = new ArrayList<>();
        values.add(new Constant(DOUBLE, 1.5));
        values.add(new Constant(DOUBLE, 2.5));
        values.add(new Constant(DOUBLE, 3.5));
        assertThat(checkSwitchGenerationCase(DOUBLE, values)).isEqualTo(HASH_SWITCH);

        values.add(constantNull(DOUBLE));
        assertThat(checkSwitchGenerationCase(DOUBLE, values)).isEqualTo(HASH_SWITCH);

        for (int i = 5; i <= 7; ++i) {
            values.add(new Constant(DOUBLE, i + 0.5));
        }
        assertThat(checkSwitchGenerationCase(DOUBLE, values)).isEqualTo(HASH_SWITCH);

        values.add(new Constant(DOUBLE, 8.5));
        assertThat(checkSwitchGenerationCase(DOUBLE, values)).isEqualTo(SET_CONTAINS);
    }

    @Test
    public void testVarchar()
    {
        List<Expression> values = new ArrayList<>();
        values.add(new Constant(VARCHAR, Slices.utf8Slice("1")));
        values.add(new Constant(VARCHAR, Slices.utf8Slice("2")));
        values.add(new Constant(VARCHAR, Slices.utf8Slice("3")));
        assertThat(checkSwitchGenerationCase(VARCHAR, values)).isEqualTo(HASH_SWITCH);

        values.add(constantNull(VARCHAR));
        assertThat(checkSwitchGenerationCase(VARCHAR, values)).isEqualTo(HASH_SWITCH);

        for (int i = 5; i <= 7; ++i) {
            values.add(new Constant(VARCHAR, Slices.utf8Slice(String.valueOf(i))));
        }
        assertThat(checkSwitchGenerationCase(VARCHAR, values)).isEqualTo(HASH_SWITCH);

        values.add(new Constant(VARCHAR, Slices.utf8Slice("8")));
        assertThat(checkSwitchGenerationCase(VARCHAR, values)).isEqualTo(SET_CONTAINS);
    }
}

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
import io.trino.metadata.Metadata;
import io.trino.metadata.MetadataManager;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.RowExpression;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
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
import static io.trino.sql.relational.Expressions.constant;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestInCodeGenerator
{
    private final Metadata metadata = MetadataManager.createTestMetadataManager();

    @Test
    public void testInteger()
    {
        List<RowExpression> values = new ArrayList<>();
        values.add(constant(Integer.MIN_VALUE, INTEGER));
        values.add(constant(Integer.MAX_VALUE, INTEGER));
        values.add(constant(3, INTEGER));
        assertEquals(DIRECT_SWITCH, checkSwitchGenerationCase(INTEGER, values));

        values.add(constant(null, INTEGER));
        assertEquals(DIRECT_SWITCH, checkSwitchGenerationCase(INTEGER, values));
        values.add(new CallExpression(
                metadata.getCoercion(DOUBLE, INTEGER),
                Collections.singletonList(constant(12345678901234.0, DOUBLE))));
        assertEquals(DIRECT_SWITCH, checkSwitchGenerationCase(INTEGER, values));

        values.add(constant(6, BIGINT));
        values.add(constant(7, BIGINT));
        assertEquals(DIRECT_SWITCH, checkSwitchGenerationCase(INTEGER, values));

        values.add(constant(8, INTEGER));
        assertEquals(SET_CONTAINS, checkSwitchGenerationCase(INTEGER, values));
    }

    @Test
    public void testBigint()
    {
        List<RowExpression> values = new ArrayList<>();
        values.add(constant(Integer.MAX_VALUE + 1L, BIGINT));
        values.add(constant(Integer.MIN_VALUE - 1L, BIGINT));
        values.add(constant(3L, BIGINT));
        assertEquals(HASH_SWITCH, checkSwitchGenerationCase(BIGINT, values));

        values.add(constant(null, BIGINT));
        assertEquals(HASH_SWITCH, checkSwitchGenerationCase(BIGINT, values));
        values.add(new CallExpression(
                metadata.getCoercion(DOUBLE, BIGINT),
                Collections.singletonList(constant(12345678901234.0, DOUBLE))));
        assertEquals(HASH_SWITCH, checkSwitchGenerationCase(BIGINT, values));

        values.add(constant(6L, BIGINT));
        values.add(constant(7L, BIGINT));
        assertEquals(HASH_SWITCH, checkSwitchGenerationCase(BIGINT, values));

        values.add(constant(8L, BIGINT));
        assertEquals(SET_CONTAINS, checkSwitchGenerationCase(BIGINT, values));
    }

    @Test
    public void testDate()
    {
        List<RowExpression> values = new ArrayList<>();
        values.add(constant(1L, DATE));
        values.add(constant(2L, DATE));
        values.add(constant(3L, DATE));
        assertEquals(DIRECT_SWITCH, checkSwitchGenerationCase(DATE, values));

        for (long i = 4; i <= 7; ++i) {
            values.add(constant(i, DATE));
        }
        assertEquals(DIRECT_SWITCH, checkSwitchGenerationCase(DATE, values));

        values.add(constant(33L, DATE));
        assertEquals(SET_CONTAINS, checkSwitchGenerationCase(DATE, values));
    }

    @Test
    public void testDouble()
    {
        List<RowExpression> values = new ArrayList<>();
        values.add(constant(1.5, DOUBLE));
        values.add(constant(2.5, DOUBLE));
        values.add(constant(3.5, DOUBLE));
        assertEquals(HASH_SWITCH, checkSwitchGenerationCase(DOUBLE, values));

        values.add(constant(null, DOUBLE));
        assertEquals(HASH_SWITCH, checkSwitchGenerationCase(DOUBLE, values));

        for (int i = 5; i <= 7; ++i) {
            values.add(constant(i + 0.5, DOUBLE));
        }
        assertEquals(HASH_SWITCH, checkSwitchGenerationCase(DOUBLE, values));

        values.add(constant(8.5, DOUBLE));
        assertEquals(SET_CONTAINS, checkSwitchGenerationCase(DOUBLE, values));
    }

    @Test
    public void testVarchar()
    {
        List<RowExpression> values = new ArrayList<>();
        values.add(constant(Slices.utf8Slice("1"), DOUBLE));
        values.add(constant(Slices.utf8Slice("2"), DOUBLE));
        values.add(constant(Slices.utf8Slice("3"), DOUBLE));
        assertEquals(HASH_SWITCH, checkSwitchGenerationCase(VARCHAR, values));

        values.add(constant(null, VARCHAR));
        assertEquals(HASH_SWITCH, checkSwitchGenerationCase(VARCHAR, values));

        for (int i = 5; i <= 7; ++i) {
            values.add(constant(Slices.utf8Slice(String.valueOf(i)), VARCHAR));
        }
        assertEquals(HASH_SWITCH, checkSwitchGenerationCase(VARCHAR, values));

        values.add(constant(Slices.utf8Slice("8"), VARCHAR));
        assertEquals(SET_CONTAINS, checkSwitchGenerationCase(VARCHAR, values));
    }
}

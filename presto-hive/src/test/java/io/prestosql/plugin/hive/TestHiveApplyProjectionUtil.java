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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.expression.Constant;
import io.prestosql.spi.expression.FieldDereference;
import io.prestosql.spi.expression.Variable;
import org.testng.annotations.Test;

import static io.prestosql.plugin.hive.HiveApplyProjectionUtil.extractSupportedProjectedColumns;
import static io.prestosql.plugin.hive.HiveApplyProjectionUtil.isPushDownSupported;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RowType.field;
import static io.prestosql.spi.type.RowType.rowType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHiveApplyProjectionUtil
{
    private static final ConnectorExpression ROW_OF_ROW_VARIABLE = new Variable("a", rowType(field("b", rowType(field("c", INTEGER)))));

    private static final ConnectorExpression ONE_LEVEL_DEREFERENCE = new FieldDereference(
            rowType(field("c", INTEGER)),
            ROW_OF_ROW_VARIABLE,
            0);

    private static final ConnectorExpression TWO_LEVEL_DEREFERENCE = new FieldDereference(
            INTEGER,
            ONE_LEVEL_DEREFERENCE,
            0);

    private static final ConnectorExpression INT_VARIABLE = new Variable("a", INTEGER);
    private static final ConnectorExpression CONSTANT = new Constant(5, INTEGER);

    @Test
    public void testIsProjectionSupported()
    {
        assertTrue(isPushDownSupported(ONE_LEVEL_DEREFERENCE));
        assertTrue(isPushDownSupported(TWO_LEVEL_DEREFERENCE));
        assertTrue(isPushDownSupported(INT_VARIABLE));
        assertFalse(isPushDownSupported(CONSTANT));
    }

    @Test
    public void testExtractSupportedProjectionColumns()
    {
        assertEquals(extractSupportedProjectedColumns(ONE_LEVEL_DEREFERENCE), ImmutableList.of(ONE_LEVEL_DEREFERENCE));
        assertEquals(extractSupportedProjectedColumns(TWO_LEVEL_DEREFERENCE), ImmutableList.of(TWO_LEVEL_DEREFERENCE));
        assertEquals(extractSupportedProjectedColumns(INT_VARIABLE), ImmutableList.of(INT_VARIABLE));
        assertEquals(extractSupportedProjectedColumns(CONSTANT), ImmutableList.of());
    }
}

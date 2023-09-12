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
package io.trino.plugin.base.projection;

import com.google.common.collect.ImmutableList;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.RowType;
import org.testng.annotations.Test;

import static io.trino.plugin.base.projection.ApplyProjectionUtil.extractSupportedProjectedColumns;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.isPushdownSupported;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestApplyProjectionUtil
{
    private static final ConnectorExpression ROW_OF_ROW_VARIABLE = new Variable("a", rowType(field("b", rowType(field("c", INTEGER)))));
    private static final ConnectorExpression LEAF_DOTTED_ROW_OF_ROW_VARIABLE = new Variable("a", rowType(field("b", rowType(field("c.x", INTEGER)))));
    private static final ConnectorExpression MID_DOTTED_ROW_OF_ROW_VARIABLE = new Variable("a", rowType(field("b.x", rowType(field("c", INTEGER)))));

    private static final ConnectorExpression ONE_LEVEL_DEREFERENCE = new FieldDereference(
            rowType(field("c", INTEGER)),
            ROW_OF_ROW_VARIABLE,
            0);

    private static final ConnectorExpression TWO_LEVEL_DEREFERENCE = new FieldDereference(
            INTEGER,
            ONE_LEVEL_DEREFERENCE,
            0);

    private static final ConnectorExpression LEAF_DOTTED_ONE_LEVEL_DEREFERENCE = new FieldDereference(
            rowType(field("c.x", INTEGER)),
            LEAF_DOTTED_ROW_OF_ROW_VARIABLE,
            0);

    private static final ConnectorExpression LEAF_DOTTED_TWO_LEVEL_DEREFERENCE = new FieldDereference(
            INTEGER,
            LEAF_DOTTED_ONE_LEVEL_DEREFERENCE,
            0);

    private static final ConnectorExpression MID_DOTTED_ONE_LEVEL_DEREFERENCE = new FieldDereference(
            rowType(field("c.x", INTEGER)),
            MID_DOTTED_ROW_OF_ROW_VARIABLE,
            0);

    private static final ConnectorExpression MID_DOTTED_TWO_LEVEL_DEREFERENCE = new FieldDereference(
            INTEGER,
            MID_DOTTED_ONE_LEVEL_DEREFERENCE,
            0);

    private static final ConnectorExpression INT_VARIABLE = new Variable("a", INTEGER);
    private static final ConnectorExpression CONSTANT = new Constant(5, INTEGER);

    @Test
    public void testIsProjectionSupported()
    {
        assertTrue(isPushdownSupported(ONE_LEVEL_DEREFERENCE, connectorExpression -> true));
        assertTrue(isPushdownSupported(TWO_LEVEL_DEREFERENCE, connectorExpression -> true));
        assertTrue(isPushdownSupported(INT_VARIABLE, connectorExpression -> true));
        assertFalse(isPushdownSupported(CONSTANT, connectorExpression -> true));

        assertFalse(isPushdownSupported(ONE_LEVEL_DEREFERENCE, connectorExpression -> false));
        assertFalse(isPushdownSupported(TWO_LEVEL_DEREFERENCE, connectorExpression -> false));
        assertFalse(isPushdownSupported(INT_VARIABLE, connectorExpression -> false));
        assertFalse(isPushdownSupported(CONSTANT, connectorExpression -> false));

        assertTrue(isPushdownSupported(LEAF_DOTTED_ONE_LEVEL_DEREFERENCE, this::isSupportedForPushDown));
        assertFalse(isPushdownSupported(LEAF_DOTTED_TWO_LEVEL_DEREFERENCE, this::isSupportedForPushDown));
        assertFalse(isPushdownSupported(MID_DOTTED_ONE_LEVEL_DEREFERENCE, this::isSupportedForPushDown));
        assertFalse(isPushdownSupported(MID_DOTTED_TWO_LEVEL_DEREFERENCE, this::isSupportedForPushDown));
    }

    @Test
    public void testExtractSupportedProjectionColumns()
    {
        assertEquals(extractSupportedProjectedColumns(ONE_LEVEL_DEREFERENCE), ImmutableList.of(ONE_LEVEL_DEREFERENCE));
        assertEquals(extractSupportedProjectedColumns(TWO_LEVEL_DEREFERENCE), ImmutableList.of(TWO_LEVEL_DEREFERENCE));
        assertEquals(extractSupportedProjectedColumns(INT_VARIABLE), ImmutableList.of(INT_VARIABLE));
        assertEquals(extractSupportedProjectedColumns(CONSTANT), ImmutableList.of());

        assertEquals(extractSupportedProjectedColumns(ONE_LEVEL_DEREFERENCE, connectorExpression -> false), ImmutableList.of());
        assertEquals(extractSupportedProjectedColumns(TWO_LEVEL_DEREFERENCE, connectorExpression -> false), ImmutableList.of());
        assertEquals(extractSupportedProjectedColumns(INT_VARIABLE, connectorExpression -> false), ImmutableList.of());
        assertEquals(extractSupportedProjectedColumns(CONSTANT, connectorExpression -> false), ImmutableList.of());

        // Partial supported projection
        assertEquals(extractSupportedProjectedColumns(LEAF_DOTTED_ONE_LEVEL_DEREFERENCE, this::isSupportedForPushDown), ImmutableList.of(LEAF_DOTTED_ONE_LEVEL_DEREFERENCE));
        assertEquals(extractSupportedProjectedColumns(LEAF_DOTTED_TWO_LEVEL_DEREFERENCE, this::isSupportedForPushDown), ImmutableList.of(LEAF_DOTTED_ONE_LEVEL_DEREFERENCE));
        assertEquals(extractSupportedProjectedColumns(MID_DOTTED_ONE_LEVEL_DEREFERENCE, this::isSupportedForPushDown), ImmutableList.of(MID_DOTTED_ROW_OF_ROW_VARIABLE));
        assertEquals(extractSupportedProjectedColumns(MID_DOTTED_TWO_LEVEL_DEREFERENCE, this::isSupportedForPushDown), ImmutableList.of(MID_DOTTED_ROW_OF_ROW_VARIABLE));
    }

    /**
     * This method is used to simulate the behavior when the field passed in the connectorExpression might not supported for pushdown.
     */
    private boolean isSupportedForPushDown(ConnectorExpression connectorExpression)
    {
        if (connectorExpression instanceof FieldDereference fieldDereference) {
            RowType rowType = (RowType) fieldDereference.getTarget().getType();
            RowType.Field field = rowType.getFields().get(fieldDereference.getField());
            String fieldName = field.getName().get();
            if (fieldName.contains(".") || fieldName.contains("$")) {
                return false;
            }
        }
        return true;
    }
}

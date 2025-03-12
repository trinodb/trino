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
package io.trino.spi.type;

import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlRow;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.BLOCK_BUILDER;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRowType
{
    private final TypeOperators typeOperators = new TypeOperators();

    @Test
    public void testEmptyRowType()
            throws Throwable
    {
        RowType emptyRowType = RowType.rowType();
        assertThat(emptyRowType).isEqualTo(emptyRowType);
        assertThat(emptyRowType).isEqualTo(RowType.rowType());
        assertThat(emptyRowType.getFields()).isEmpty();

        RowBlockBuilder blockBuilder = emptyRowType.createBlockBuilder(null, 1);
        blockBuilder.buildEntry(fieldBuilders -> {
            assertThat(fieldBuilders).isEmpty();
        });
        Block singleEmptyRow = blockBuilder.build();
        assertThat(singleEmptyRow).isInstanceOf(RowBlock.class);
        assertThat(singleEmptyRow.getPositionCount()).isEqualTo(1);
        SqlRow emptyRow = emptyRowType.getObject(singleEmptyRow, 0);
        assertThat(emptyRow.getFieldCount()).isEqualTo(0);

        assertThat((boolean) typeOperators.getIndeterminateOperator(emptyRowType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL))
                .invokeExact(singleEmptyRow, 0)).isFalse();
        assertThat((Boolean) typeOperators.getEqualOperator(emptyRowType, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL))
                .invokeExact(singleEmptyRow, 0, singleEmptyRow, 0)).isTrue();
        assertThat((boolean) typeOperators.getIdenticalOperator(emptyRowType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL))
                .invokeExact(singleEmptyRow, 0, singleEmptyRow, 0)).isTrue();
        assertThat((boolean) typeOperators.getLessThanOperator(emptyRowType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL))
                .invokeExact(singleEmptyRow, 0, singleEmptyRow, 0)).isFalse();
        assertThat((boolean) typeOperators.getLessThanOrEqualOperator(emptyRowType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL))
                .invokeExact(singleEmptyRow, 0, singleEmptyRow, 0)).isTrue();
        assertThat((long) typeOperators.getComparisonUnorderedFirstOperator(emptyRowType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL))
                .invokeExact(singleEmptyRow, 0, singleEmptyRow, 0)).isZero();
        assertThat((long) typeOperators.getComparisonUnorderedLastOperator(emptyRowType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL))
                .invokeExact(singleEmptyRow, 0, singleEmptyRow, 0)).isZero();

        assertThat((SqlRow) typeOperators.getReadValueOperator(emptyRowType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL)).invokeExact(singleEmptyRow, 0))
                .matches(row -> row.getFieldCount() == 0);

        // write data into a block builder
        blockBuilder = emptyRowType.createBlockBuilder(null, 1);
        typeOperators.getReadValueOperator(emptyRowType, simpleConvention(BLOCK_BUILDER, NEVER_NULL)).invoke(emptyRow, blockBuilder);
        Block newBlock = blockBuilder.build();
        assertThat(newBlock).isInstanceOf(RowBlock.class);
        assertThat(newBlock.getPositionCount()).isEqualTo(1);
        SqlRow newRow = emptyRowType.getObject(newBlock, 0);
        assertThat(newRow.getFieldCount()).isEqualTo(0);
    }

    @Test
    public void testRowDisplayName()
    {
        List<RowType.Field> fields = asList(
                RowType.field("bool_col", BOOLEAN),
                RowType.field("double_col", DOUBLE),
                RowType.field("array_col", new ArrayType(VARCHAR)),
                RowType.field("map_col", new MapType(BOOLEAN, DOUBLE, typeOperators)));

        RowType row = RowType.from(fields);
        assertThat(row.getDisplayName()).isEqualTo("row(bool_col boolean, double_col double, array_col array(varchar), map_col map(boolean, double))");
    }

    @Test
    public void testRowDisplayNoColumnNames()
    {
        List<Type> types = asList(
                BOOLEAN,
                DOUBLE,
                new ArrayType(VARCHAR),
                new MapType(BOOLEAN, DOUBLE, typeOperators));
        RowType row = RowType.anonymous(types);
        assertThat(row.getDisplayName()).isEqualTo("row(boolean, double, array(varchar), map(boolean, double))");
    }

    @Test
    public void testRowDisplayMixedUnnamedColumns()
    {
        List<RowType.Field> fields = asList(
                RowType.field(BOOLEAN),
                RowType.field("double_col", DOUBLE),
                RowType.field(new ArrayType(VARCHAR)),
                RowType.field("map_col", new MapType(BOOLEAN, DOUBLE, typeOperators)));

        RowType row = RowType.from(fields);
        assertThat(row.getDisplayName()).isEqualTo("row(boolean, double_col double, array(varchar), map_col map(boolean, double))");
    }
}

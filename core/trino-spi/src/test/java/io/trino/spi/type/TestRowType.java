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

import org.testng.annotations.Test;

import java.util.List;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

public class TestRowType
{
    private final TypeOperators typeOperators = new TypeOperators();

    @Test
    public void testRowDisplayName()
    {
        List<RowType.Field> fields = asList(
                RowType.field("bool_col", BOOLEAN),
                RowType.field("double_col", DOUBLE),
                RowType.field("array_col", new ArrayType(VARCHAR)),
                RowType.field("map_col", new MapType(BOOLEAN, DOUBLE, typeOperators)));

        RowType row = RowType.from(fields);
        assertEquals(
                row.getDisplayName(),
                "row(bool_col boolean, double_col double, array_col array(varchar), map_col map(boolean, double))");
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
        assertEquals(
                row.getDisplayName(),
                "row(boolean, double, array(varchar), map(boolean, double))");
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
        assertEquals(
                row.getDisplayName(),
                "row(boolean, double_col double, array(varchar), map_col map(boolean, double))");
    }
}

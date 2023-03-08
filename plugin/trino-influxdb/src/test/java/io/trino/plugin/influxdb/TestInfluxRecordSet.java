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

package io.trino.plugin.influxdb;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.plugin.influxdb.InfluxConstant.ColumnKind.FIELD;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestInfluxRecordSet
{
    InfluxRecord sourceData;

    @BeforeClass
    public void setup()
    {
        sourceData = new InfluxRecord(ImmutableList.of(
                "col1", "col2", "col3"
        ), ImmutableList.of(
                ImmutableList.of("text1", -10L, false),
                ImmutableList.of("text2", 0L, false),
                ImmutableList.of("text3", 10L, true)));
    }

    @Test
    public void testFieldRequires()
    {
        assertThatThrownBy(() -> new InfluxRecordSet(null, ImmutableList.of(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableHandle is null");
        assertThatThrownBy(() -> new InfluxRecordSet(new InfluxTableHandle("a", "b"), null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("columnHandles is null");
    }

    @Test
    public void testCursorSimple()
    {
        try (InfluxRecordCursor cursor = new InfluxRecordCursor(
                ImmutableList.of(
                        new InfluxColumnHandle("col1", VARCHAR, FIELD),
                        new InfluxColumnHandle("col2", BIGINT, FIELD),
                        new InfluxColumnHandle("col3", BOOLEAN, FIELD)),
                sourceData)) {
            assertEquals(cursor.getType(0), createUnboundedVarcharType());
            assertEquals(cursor.getType(1), BIGINT);

            int idx = 0;
            while (cursor.advanceNextPosition()) {
                idx++;
                if (idx == 1) {
                    assertEquals(ImmutableList.of("text1", -10L, false),
                            ImmutableList.of(cursor.getSlice(0).toStringUtf8(), cursor.getLong(1), cursor.getBoolean(2)));
                }
                if (idx == 2) {
                    assertEquals(ImmutableList.of("text2", 0L, false),
                            ImmutableList.of(cursor.getSlice(0).toStringUtf8(), cursor.getLong(1), cursor.getBoolean(2)));
                }
                if (idx == 3) {
                    assertEquals(ImmutableList.of("text3", 10L, true),
                            ImmutableList.of(cursor.getSlice(0).toStringUtf8(), cursor.getLong(1), cursor.getBoolean(2)));
                }
            }
        }
    }

    @Test
    public void testCursorMixedOrder()
    {
        try (InfluxRecordCursor cursor = new InfluxRecordCursor(
                ImmutableList.of(
                        new InfluxColumnHandle("col1", VARCHAR, FIELD),
                        new InfluxColumnHandle("col2", BIGINT, FIELD),
                        new InfluxColumnHandle("col3", BOOLEAN, FIELD)),
                sourceData)) {
            int idx = 0;
            while (cursor.advanceNextPosition()) {
                idx++;
                if (idx == 1) {
                    assertEquals(ImmutableList.of(-10L, "text1", false),
                            ImmutableList.of(cursor.getLong(1), cursor.getSlice(0).toStringUtf8(), cursor.getBoolean(2)));
                }
                if (idx == 2) {
                    assertEquals(ImmutableList.of(0L, false, "text2"),
                            ImmutableList.of(cursor.getLong(1), cursor.getBoolean(2), cursor.getSlice(0).toStringUtf8()));
                }
                if (idx == 3) {
                    assertEquals(ImmutableList.of("text3", 10L, true),
                            ImmutableList.of(cursor.getSlice(0).toStringUtf8(), cursor.getLong(1), cursor.getBoolean(2)));
                }
            }
        }
    }
}

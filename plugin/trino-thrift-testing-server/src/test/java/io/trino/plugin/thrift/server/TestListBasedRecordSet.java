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
package io.trino.plugin.thrift.server;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.spi.connector.RecordCursor;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestListBasedRecordSet
{
    @Test
    public void testEmptyCursor()
    {
        ListBasedRecordSet recordSet = new ListBasedRecordSet(ImmutableList.of(), ImmutableList.of(BIGINT, INTEGER));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(BIGINT, INTEGER));
        RecordCursor cursor = recordSet.cursor();
        assertFalse(cursor.advanceNextPosition());
    }

    @Test
    public void testCursor()
    {
        ListBasedRecordSet recordSet = new ListBasedRecordSet(
                ImmutableList.of(
                        Arrays.asList("1", null, "3"),
                        Arrays.asList("ab", "c", null)),
                ImmutableList.of(BIGINT, VARCHAR));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(BIGINT, VARCHAR));
        RecordCursor cursor = recordSet.cursor();
        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getType(0), BIGINT);
        assertEquals(cursor.getType(1), VARCHAR);
        assertThatThrownBy(() -> cursor.getLong(2))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessage("Index 2 out of bounds for length 2");
        assertEquals(cursor.getLong(0), 1L);
        assertEquals(cursor.getSlice(1), Slices.utf8Slice("ab"));
        assertTrue(cursor.advanceNextPosition());
        assertTrue(cursor.isNull(0));
        assertEquals(cursor.getSlice(1), Slices.utf8Slice("c"));
        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getLong(0), 3L);
        assertTrue(cursor.isNull(1));
        assertFalse(cursor.advanceNextPosition());
        assertThatThrownBy(() -> cursor.getLong(0))
                .isInstanceOf(IndexOutOfBoundsException.class)
                .hasMessage("Index 3 out of bounds for length 3");
    }
}

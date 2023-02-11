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
package io.trino.plugin.barb;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnMetadata;
import org.testng.annotations.Test;

import java.net.URI;

import static io.trino.plugin.barb.MetadataUtil.TABLE_CODEC;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static org.testng.Assert.assertEquals;

public class TestBARBTable
{
    private final BARBTable barbTable = new BARBTable("tableName",
            ImmutableList.of(new BARBColumn("a", createUnboundedVarcharType()), new BARBColumn("b", BIGINT)),
            ImmutableList.of(URI.create("file://table-1.json"), URI.create("file://table-2.json")));

    @Test
    public void testColumnMetadata()
    {
        assertEquals(barbTable.getColumnsMetadata(), ImmutableList.of(
                new ColumnMetadata("a", createUnboundedVarcharType()),
                new ColumnMetadata("b", BIGINT)));
    }

    @Test
    public void testRoundTrip()
    {
        String json = TABLE_CODEC.toJson(barbTable);
        BARBTable barbTableCopy = TABLE_CODEC.fromJson(json);

        assertEquals(barbTableCopy.getName(), barbTable.getName());
        assertEquals(barbTableCopy.getColumns(), barbTable.getColumns());
        assertEquals(barbTableCopy.getSources(), barbTable.getSources());
    }
}

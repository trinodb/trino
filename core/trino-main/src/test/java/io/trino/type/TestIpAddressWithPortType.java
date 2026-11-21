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
package io.trino.type;

import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.block.ValueBlock;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.type.IpAddressOperators.castFromVarcharToIpAddress;
import static io.trino.type.IpAddressType.IPADDRESS;
import static io.trino.type.IpAddressWithPortType.IPADDRESS_WITH_PORT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIpAddressWithPortType
        extends AbstractTestType
{
    public TestIpAddressWithPortType()
    {
        super(IPADDRESS_WITH_PORT, List.class, createTestBlock());
    }

    private static ValueBlock createTestBlock()
    {
        RowBlockBuilder blockBuilder = IPADDRESS_WITH_PORT.createBlockBuilder(null, 3);

        blockBuilder.buildEntry(fieldBuilders -> {
            IPADDRESS.writeSlice(fieldBuilders.get(0), castFromVarcharToIpAddress(utf8Slice("2001:db8::ff00:42:8320")));
            INTEGER.writeLong(fieldBuilders.get(1), 80);
        });

        blockBuilder.buildEntry(fieldBuilders -> {
            IPADDRESS.writeSlice(fieldBuilders.get(0), castFromVarcharToIpAddress(utf8Slice("2001:db8::ff00:42:8321")));
            INTEGER.writeLong(fieldBuilders.get(1), 443);
        });

        blockBuilder.buildEntry(fieldBuilders -> {
            IPADDRESS.writeSlice(fieldBuilders.get(0), castFromVarcharToIpAddress(utf8Slice("2001:db8::ff00:42:8322")));
            INTEGER.writeLong(fieldBuilders.get(1), 8080);
        });

        return blockBuilder.buildValueBlock();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        SqlRow row = (SqlRow) value;
        int rawIndex = row.getRawIndex();
        var address = IPADDRESS.getSlice(row.getRawFieldBlock(0), rawIndex);
        long port = INTEGER.getLong(row.getRawFieldBlock(1), rawIndex);

        return buildRowValue(IPADDRESS_WITH_PORT, fieldBuilders -> {
            IPADDRESS.writeSlice(fieldBuilders.get(0), address);
            INTEGER.writeLong(fieldBuilders.get(1), port + 1);
        });
    }

    @Override
    protected Object getNonNullValue()
    {
        return buildRowValue(IPADDRESS_WITH_PORT, fieldBuilders -> {
            IPADDRESS.writeSlice(fieldBuilders.get(0), castFromVarcharToIpAddress(utf8Slice("0.0.0.0")));
            INTEGER.writeLong(fieldBuilders.get(1), 0);
        });
    }

    @Test
    public void testDisplayName()
    {
        assertThat(type.getDisplayName()).isEqualTo("ipaddress_with_port");
    }

    @Test
    public void testFields()
    {
        assertThat(IPADDRESS_WITH_PORT.getFields()).hasSize(2);
        assertThat(IPADDRESS_WITH_PORT.getFieldTypes()).containsExactly(IPADDRESS, INTEGER);
    }
}

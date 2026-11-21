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
import static io.trino.type.IpAddressWithSubnetType.IPADDRESS_WITH_SUBNET;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIpAddressWithSubnetType
        extends AbstractTestType
{
    public TestIpAddressWithSubnetType()
    {
        super(IPADDRESS_WITH_SUBNET, List.class, createTestBlock());
    }

    private static ValueBlock createTestBlock()
    {
        RowBlockBuilder blockBuilder = IPADDRESS_WITH_SUBNET.createBlockBuilder(null, 3);

        blockBuilder.buildEntry(fieldBuilders -> {
            IPADDRESS.writeSlice(fieldBuilders.get(0), castFromVarcharToIpAddress(utf8Slice("2001:db8:1::")));
            INTEGER.writeLong(fieldBuilders.get(1), 32);
        });

        blockBuilder.buildEntry(fieldBuilders -> {
            IPADDRESS.writeSlice(fieldBuilders.get(0), castFromVarcharToIpAddress(utf8Slice("2001:db8:2::")));
            INTEGER.writeLong(fieldBuilders.get(1), 48);
        });

        blockBuilder.buildEntry(fieldBuilders -> {
            IPADDRESS.writeSlice(fieldBuilders.get(0), castFromVarcharToIpAddress(utf8Slice("2001:db8:3::")));
            INTEGER.writeLong(fieldBuilders.get(1), 64);
        });

        return blockBuilder.buildValueBlock();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        SqlRow row = (SqlRow) value;
        int rawIndex = row.getRawIndex();
        var address = IPADDRESS.getSlice(row.getRawFieldBlock(0), rawIndex);
        long prefixLength = INTEGER.getLong(row.getRawFieldBlock(1), rawIndex);

        return buildRowValue(IPADDRESS_WITH_SUBNET, fieldBuilders -> {
            IPADDRESS.writeSlice(fieldBuilders.get(0), address);
            INTEGER.writeLong(fieldBuilders.get(1), prefixLength + 1);
        });
    }

    @Override
    protected Object getNonNullValue()
    {
        return buildRowValue(IPADDRESS_WITH_SUBNET, fieldBuilders -> {
            IPADDRESS.writeSlice(fieldBuilders.get(0), castFromVarcharToIpAddress(utf8Slice("0.0.0.0")));
            INTEGER.writeLong(fieldBuilders.get(1), 0);
        });
    }

    @Test
    public void testDisplayName()
    {
        assertThat(type.getDisplayName()).isEqualTo("ipaddress_with_subnet");
    }

    @Test
    public void testFields()
    {
        assertThat(IPADDRESS_WITH_SUBNET.getFields()).hasSize(2);
        assertThat(IPADDRESS_WITH_SUBNET.getFieldTypes()).containsExactly(IPADDRESS, INTEGER);
        assertThat(IPADDRESS_WITH_SUBNET.getFields().get(0).getName().orElseThrow()).isEqualTo("ip_address");
        assertThat(IPADDRESS_WITH_SUBNET.getFields().get(1).getName().orElseThrow()).isEqualTo("prefix_length");
    }
}

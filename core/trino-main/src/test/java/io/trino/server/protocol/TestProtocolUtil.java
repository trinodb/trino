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
package io.trino.server.protocol;

import io.trino.client.Column;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.server.protocol.ProtocolUtil.createColumn;
import static io.trino.spi.type.IntervalField.DAY;
import static io.trino.spi.type.IntervalField.SECOND;
import static io.trino.type.IntervalDayTimeType.createIntervalDayTimeType;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static org.assertj.core.api.Assertions.assertThat;

class TestProtocolUtil
{
    @Test
    void testIntervalCapability()
    {
        for (Type type : List.of(createIntervalDayTimeType(DAY, SECOND, 9, 6), createIntervalDayTimeType(DAY, SECOND, 9, 12), INTERVAL_YEAR_MONTH)) {
            Column legacy = createColumn("value", type, true, true, true, true, false);
            assertThat(legacy.getTypeSignature().getArguments()).isEmpty();
            assertThat(legacy.getType()).isEqualTo(legacy.getTypeSignature().getRawType());
            Column precise = createColumn("value", type, true, true, true, true, true);
            assertThat(precise.getTypeSignature().getArguments()).hasSize(type.getTypeDescriptor().getParameters().size());
            assertThat(precise.getType()).isEqualToIgnoringCase(type.getDisplayName());
        }
    }

    @Test
    void testNestedIntervalCapability()
    {
        Type type = RowType.anonymous(List.of(new ArrayType(createIntervalDayTimeType(DAY, SECOND, 9, 12))));
        Column legacy = createColumn("value", type, true, true, true, true, false);
        assertThat(legacy.getType()).isEqualTo("row(array(interval day to second))");
        assertThat(legacy.getTypeSignature().getArguments().getFirst().getNamedTypeSignature().getTypeSignature()
                .getArguments().getFirst().getTypeSignature().getArguments()).isEmpty();
        Column precise = createColumn("value", type, true, true, true, true, true);
        assertThat(precise.getTypeSignature().getArguments().getFirst().getNamedTypeSignature().getTypeSignature()
                .getArguments().getFirst().getTypeSignature().getArguments()).hasSize(4);
    }
}

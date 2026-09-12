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
package io.trino.lib.type;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TypeConstructorTest
{
    @Test
    void testPrimitiveConstructorsInstantiateMatchingValueType()
    {
        assertThat(UnknownType.CONSTRUCTOR.newInstance(List.of())).isInstanceOf(UnknownType.class);
        assertThat(BooleanType.CONSTRUCTOR.newInstance(List.of())).isInstanceOf(BooleanType.class);
        assertThat(TinyintType.CONSTRUCTOR.newInstance(List.of())).isInstanceOf(TinyintType.class);
        assertThat(SmallintType.CONSTRUCTOR.newInstance(List.of())).isInstanceOf(SmallintType.class);
        assertThat(IntegerType.CONSTRUCTOR.newInstance(List.of())).isInstanceOf(IntegerType.class);
        assertThat(BigintType.CONSTRUCTOR.newInstance(List.of())).isInstanceOf(BigintType.class);
        assertThat(RealType.CONSTRUCTOR.newInstance(List.of())).isInstanceOf(RealType.class);
        assertThat(DoubleType.CONSTRUCTOR.newInstance(List.of())).isInstanceOf(DoubleType.class);
        assertThat(NumberType.CONSTRUCTOR.newInstance(List.of())).isInstanceOf(NumberType.class);
        assertThat(VarbinaryType.CONSTRUCTOR.newInstance(List.of())).isInstanceOf(VarbinaryType.class);
        assertThat(UnboundedVarcharType.CONSTRUCTOR.newInstance(List.of())).isInstanceOf(UnboundedVarcharType.class);
        assertThat(DateType.CONSTRUCTOR.newInstance(List.of())).isInstanceOf(DateType.class);
        assertThat(IntervalDayToSecondType.CONSTRUCTOR.newInstance(List.of())).isInstanceOf(IntervalDayToSecondType.class);
        assertThat(IntervalYearToMonthType.CONSTRUCTOR.newInstance(List.of())).isInstanceOf(IntervalYearToMonthType.class);
        assertThat(JsonType.CONSTRUCTOR.newInstance(List.of())).isInstanceOf(JsonType.class);
        assertThat(UuidType.CONSTRUCTOR.newInstance(List.of())).isInstanceOf(UuidType.class);
        assertThat(IpAddressType.CONSTRUCTOR.newInstance(List.of())).isInstanceOf(IpAddressType.class);
    }
}

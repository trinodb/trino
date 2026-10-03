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

import io.trino.FeaturesConfig;
import io.trino.metadata.TypeRegistry;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeNotFoundException;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static io.trino.spi.type.StandardTypes.INTERVAL_DAY_TO_SECOND;
import static io.trino.spi.type.StandardTypes.INTERVAL_YEAR_TO_MONTH;
import static io.trino.spi.type.TypeParameter.numericParameter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTypeRegistry
{
    private final TypeRegistry typeRegistry = new TypeRegistry(new TypeOperators(), new FeaturesConfig());

    @Test
    public void testNonexistentType()
    {
        assertThatThrownBy(() -> typeRegistry.getType(new TypeDescriptor("not_a_real_type")))
                .isInstanceOf(TypeNotFoundException.class)
                .hasMessage("Unknown type: not_a_real_type");
    }

    @Test
    public void testIntervalNumericParameterRange()
    {
        for (String base : List.of(INTERVAL_DAY_TO_SECOND, INTERVAL_YEAR_TO_MONTH)) {
            long[] valid = base.equals(INTERVAL_DAY_TO_SECOND) ? new long[] {5, 5, 2, 12} : new long[] {0, 1, 2};
            TypeDescriptor descriptor = new TypeDescriptor(base, Arrays.stream(valid).mapToObj(value -> numericParameter(value)).toList());
            assertThat(typeRegistry.getType(descriptor).getTypeDescriptor()).isEqualTo(descriptor);
            for (int position = 0; position < valid.length; position++) {
                for (long offset : new long[] {1L << 32, -(1L << 32)}) {
                    long[] invalid = valid.clone();
                    invalid[position] += offset;
                    TypeDescriptor invalidDescriptor = new TypeDescriptor(base, Arrays.stream(invalid).mapToObj(value -> numericParameter(value)).toList());
                    assertThatThrownBy(() -> typeRegistry.getType(invalidDescriptor))
                            .isInstanceOf(TypeNotFoundException.class)
                            .hasCauseInstanceOf(IllegalArgumentException.class);
                }
            }
        }
    }

    @Test
    public void testOperatorsImplemented()
    {
        typeRegistry.verifyTypes();
    }
}

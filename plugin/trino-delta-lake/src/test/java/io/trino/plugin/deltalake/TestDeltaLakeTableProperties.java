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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.session.PropertyMetadata;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static io.trino.plugin.deltalake.DeltaLakeTableProperties.EXTRA_PROPERTIES_PROPERTY;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestDeltaLakeTableProperties
{
    private final PropertyMetadata<?> extraProperties = new DeltaLakeTableProperties(new DeltaLakeConfig(), TESTING_TYPE_MANAGER).getTableProperties().stream()
            .filter(property -> property.getName().equals(EXTRA_PROPERTIES_PROPERTY))
            .findFirst()
            .orElseThrow();

    @Test
    void testExtraPropertiesPreserveCaseAndAreImmutable()
    {
        Map<String, String> input = new HashMap<>();
        input.put("custom.CaseSensitive", "value");

        Map<?, ?> decoded = (Map<?, ?>) extraProperties.decode(input);
        assertThat(decoded).isEqualTo(ImmutableMap.of("custom.CaseSensitive", "value"));
        assertThatThrownBy(decoded::clear).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testNullExtraPropertyKey()
    {
        Map<String, String> input = new HashMap<>();
        input.put(null, "value");

        assertThatThrownBy(() -> extraProperties.decode(input))
                .hasMessage("Extra table property key cannot be null '{null=value}'");
    }

    @Test
    void testNullExtraPropertyValue()
    {
        Map<String, String> input = new HashMap<>();
        input.put("property", null);

        assertThatThrownBy(() -> extraProperties.decode(input))
                .hasMessage("Extra table property value cannot be null '{property=null}'");
    }
}

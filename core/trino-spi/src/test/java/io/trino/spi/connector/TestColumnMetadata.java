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
package io.trino.spi.connector;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;

class TestColumnMetadata
{
    @Test
    public void testBuilderFrom()
    {
        ColumnMetadata originColumnMetadata = new ColumnMetadata(
                "test_column",
                INTEGER,
                Optional.of("1"),
                false,
                "test_comment",
                "test_extra_info",
                false,
                ImmutableMap.of("test_key", "test_value"));

        ColumnMetadata buildColumnMetadata = ColumnMetadata.builderFrom(originColumnMetadata).build();

        assertThat(buildColumnMetadata).isEqualTo(originColumnMetadata);
        assertThat(buildColumnMetadata.getDefaultValue()).isEqualTo(originColumnMetadata.getDefaultValue());
        assertThat(buildColumnMetadata.getProperties()).isEqualTo(originColumnMetadata.getProperties());
    }
}

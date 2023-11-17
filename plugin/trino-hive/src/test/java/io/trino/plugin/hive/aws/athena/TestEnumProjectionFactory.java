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
package io.trino.plugin.hive.aws.athena;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.Domain;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_VALUES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestEnumProjectionFactory
{
    @Test
    void testIsSupported()
    {
        assertThat(new EnumProjectionFactory().isSupportedColumnType(VARCHAR)).isTrue();
        assertThat(new EnumProjectionFactory().isSupportedColumnType(BIGINT)).isFalse();
    }

    @Test
    void testCreate()
    {
        Projection projection = new EnumProjectionFactory().create("test", VARCHAR, ImmutableMap.of(COLUMN_PROJECTION_VALUES, ImmutableList.of("a", "b", "c")));
        assertThat(projection.getProjectedValues(Optional.empty())).containsExactly("a", "b", "c");
        assertThat(projection.getProjectedValues(Optional.of(Domain.all(VARCHAR)))).containsExactly("a", "b", "c");
        assertThat(projection.getProjectedValues(Optional.of(Domain.none(VARCHAR)))).isEmpty();
        assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice("b"))))).containsExactly("b");
        assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice("x"))))).isEmpty();

        assertThatThrownBy(() -> new EnumProjectionFactory().create("test", VARCHAR, ImmutableMap.of("ignored", ImmutableList.of("a", "b", "c"))))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Column projection for column 'test' failed. Missing required property: 'partition_projection_values'");

        assertThatThrownBy(() -> new EnumProjectionFactory().create("test", VARCHAR, ImmutableMap.of(COLUMN_PROJECTION_VALUES, "invalid")))
                .isInstanceOf(ClassCastException.class);
    }
}

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

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.spi.predicate.Domain;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.TimestampType.TIMESTAMP_PICOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestInjectedProjectionFactory
{
    @Test
    void testIsSupported()
    {
        assertThat(new InjectedProjectionFactory().isSupportedColumnType(VARCHAR)).isTrue();
        assertThat(new InjectedProjectionFactory().isSupportedColumnType(createCharType(10))).isTrue();
        assertThat(new InjectedProjectionFactory().isSupportedColumnType(BIGINT)).isTrue();
        assertThat(new InjectedProjectionFactory().isSupportedColumnType(TIMESTAMP_SECONDS)).isFalse();
        assertThat(new InjectedProjectionFactory().isSupportedColumnType(TIMESTAMP_PICOS)).isFalse();
    }

    @Test
    void testCreate()
    {
        Projection projection = new InjectedProjectionFactory().create("test", VARCHAR, ImmutableMap.of());
        assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice("b"))))).containsExactly("b");
        assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice("x"))))).containsExactly("x");

        assertThatThrownBy(() -> assertThat(projection.getProjectedValues(Optional.empty())).containsExactly("a", "b", "c"))
                .isInstanceOf(InvalidProjectionException.class)
                .hasMessage("Column projection for column 'test' failed. Injected projection requires single predicate for it's column in where clause");
        assertThatThrownBy(() -> assertThat(projection.getProjectedValues(Optional.of(Domain.all(VARCHAR)))).containsExactly("a", "b", "c"))
                .isInstanceOf(InvalidProjectionException.class)
                .hasMessage("Column projection for column 'test' failed. Injected projection requires single predicate for it's column in where clause. Currently provided can't be converted to single partition.");
        assertThatThrownBy(() -> assertThat(projection.getProjectedValues(Optional.of(Domain.none(VARCHAR)))).isEmpty())
                .isInstanceOf(InvalidProjectionException.class)
                .hasMessage("Column projection for column 'test' failed. Injected projection requires single predicate for it's column in where clause. Currently provided can't be converted to single partition.");
    }
}

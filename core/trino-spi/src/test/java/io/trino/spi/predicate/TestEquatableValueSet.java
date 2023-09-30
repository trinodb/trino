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
package io.trino.spi.predicate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.json.ObjectMapperProvider;
import io.trino.spi.block.Block;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.block.TestingBlockJsonSerde;
import io.trino.spi.type.TestingTypeDeserializer;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.type.TestingIdType.ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestEquatableValueSet
{
    @Test
    public void testEmptySet()
    {
        EquatableValueSet equatables = EquatableValueSet.none(ID);
        assertThat(equatables.getType()).isEqualTo(ID);
        assertThat(equatables.isNone()).isTrue();
        assertThat(equatables.isAll()).isFalse();
        assertThat(equatables.isSingleValue()).isFalse();
        assertThat(equatables.inclusive()).isTrue();
        assertThat(equatables.getValues().size()).isEqualTo(0);
        assertThat(equatables.complement()).isEqualTo(EquatableValueSet.all(ID));
        assertThat(equatables.containsValue(0L)).isFalse();
        assertThat(equatables.containsValue(1L)).isFalse();
        assertThat(equatables.toString()).isEqualTo("EquatableValueSet[type=id, values=0, {}]");
    }

    @Test
    public void testEntireSet()
    {
        EquatableValueSet equatables = EquatableValueSet.all(ID);
        assertThat(equatables.getType()).isEqualTo(ID);
        assertThat(equatables.isNone()).isFalse();
        assertThat(equatables.isAll()).isTrue();
        assertThat(equatables.isSingleValue()).isFalse();
        assertThat(equatables.inclusive()).isFalse();
        assertThat(equatables.getValues().size()).isEqualTo(0);
        assertThat(equatables.complement()).isEqualTo(EquatableValueSet.none(ID));
        assertThat(equatables.containsValue(0L)).isTrue();
        assertThat(equatables.containsValue(1L)).isTrue();
    }

    @Test
    public void testSingleValue()
    {
        EquatableValueSet equatables = EquatableValueSet.of(ID, 10L);

        EquatableValueSet complement = (EquatableValueSet) EquatableValueSet.all(ID).subtract(equatables);

        // inclusive
        assertThat(equatables.getType()).isEqualTo(ID);
        assertThat(equatables.isNone()).isFalse();
        assertThat(equatables.isAll()).isFalse();
        assertThat(equatables.isSingleValue()).isTrue();
        assertThat(equatables.inclusive()).isTrue();
        assertThat(Iterables.elementsEqual(equatables.getValues(), ImmutableList.of(10L))).isTrue();
        assertThat(equatables.complement()).isEqualTo(complement);
        assertThat(equatables.containsValue(0L)).isFalse();
        assertThat(equatables.containsValue(1L)).isFalse();
        assertThat(equatables.containsValue(10L)).isTrue();

        // exclusive
        assertThat(complement.getType()).isEqualTo(ID);
        assertThat(complement.isNone()).isFalse();
        assertThat(complement.isAll()).isFalse();
        assertThat(complement.isSingleValue()).isFalse();
        assertThat(complement.inclusive()).isFalse();
        assertThat(Iterables.elementsEqual(complement.getValues(), ImmutableList.of(10L))).isTrue();
        assertThat(complement.complement()).isEqualTo(equatables);
        assertThat(complement.containsValue(0L)).isTrue();
        assertThat(complement.containsValue(1L)).isTrue();
        assertThat(complement.containsValue(10L)).isFalse();
    }

    @Test
    public void testMultipleValues()
    {
        EquatableValueSet equatables = EquatableValueSet.of(ID, 1L, 2L, 3L, 1L);

        EquatableValueSet complement = (EquatableValueSet) EquatableValueSet.all(ID).subtract(equatables);

        // inclusive
        assertThat(equatables.getType()).isEqualTo(ID);
        assertThat(equatables.isNone()).isFalse();
        assertThat(equatables.isAll()).isFalse();
        assertThat(equatables.isSingleValue()).isFalse();
        assertThat(equatables.inclusive()).isTrue();
        assertThat(Iterables.elementsEqual(equatables.getValues(), ImmutableList.of(1L, 2L, 3L))).isTrue();
        assertThat(equatables.complement()).isEqualTo(complement);
        assertThat(equatables.containsValue(0L)).isFalse();
        assertThat(equatables.containsValue(1L)).isTrue();
        assertThat(equatables.containsValue(2L)).isTrue();
        assertThat(equatables.containsValue(3L)).isTrue();
        assertThat(equatables.containsValue(4L)).isFalse();
        assertThat(equatables.toString()).isEqualTo("EquatableValueSet[type=id, values=3, {1, 2, 3}]");
        assertThat(equatables.toString(ToStringSession.INSTANCE, 2)).isEqualTo("EquatableValueSet[type=id, values=3, {1, 2, ...}]");

        // exclusive
        assertThat(complement.getType()).isEqualTo(ID);
        assertThat(complement.isNone()).isFalse();
        assertThat(complement.isAll()).isFalse();
        assertThat(complement.isSingleValue()).isFalse();
        assertThat(complement.inclusive()).isFalse();
        assertThat(Iterables.elementsEqual(complement.getValues(), ImmutableList.of(1L, 2L, 3L))).isTrue();
        assertThat(complement.complement()).isEqualTo(equatables);
        assertThat(complement.containsValue(0L)).isTrue();
        assertThat(complement.containsValue(1L)).isFalse();
        assertThat(complement.containsValue(2L)).isFalse();
        assertThat(complement.containsValue(3L)).isFalse();
        assertThat(complement.containsValue(4L)).isTrue();
        assertThat(complement.toString()).isEqualTo("EquatableValueSet[type=id, values=3, EXCLUDES{1, 2, 3}]");
        assertThat(complement.toString(ToStringSession.INSTANCE, 2)).isEqualTo("EquatableValueSet[type=id, values=3, EXCLUDES{1, 2, ...}]");
    }

    @Test
    public void testGetSingleValue()
    {
        assertThat(EquatableValueSet.of(ID, 0L).getSingleValue()).isEqualTo(0L);
        assertThatThrownBy(() -> EquatableValueSet.all(ID).getSingleValue())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("EquatableValueSet does not have just a single value");
    }

    @Test
    public void testOverlaps()
    {
        assertThat(EquatableValueSet.all(ID).overlaps(EquatableValueSet.all(ID))).isTrue();
        assertThat(EquatableValueSet.all(ID).overlaps(EquatableValueSet.none(ID))).isFalse();
        assertThat(EquatableValueSet.all(ID).overlaps(EquatableValueSet.of(ID, 0L))).isTrue();
        assertThat(EquatableValueSet.all(ID).overlaps(EquatableValueSet.of(ID, 0L, 1L))).isTrue();
        assertThat(EquatableValueSet.all(ID).overlaps(EquatableValueSet.of(ID, 0L, 1L).complement())).isTrue();

        assertThat(EquatableValueSet.none(ID).overlaps(EquatableValueSet.all(ID))).isFalse();
        assertThat(EquatableValueSet.none(ID).overlaps(EquatableValueSet.none(ID))).isFalse();
        assertThat(EquatableValueSet.none(ID).overlaps(EquatableValueSet.of(ID, 0L))).isFalse();
        assertThat(EquatableValueSet.none(ID).overlaps(EquatableValueSet.of(ID, 0L, 1L))).isFalse();
        assertThat(EquatableValueSet.none(ID).overlaps(EquatableValueSet.of(ID, 0L, 1L).complement())).isFalse();

        assertThat(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.all(ID))).isTrue();
        assertThat(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.none(ID))).isFalse();
        assertThat(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.of(ID, 0L))).isTrue();
        assertThat(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.of(ID, 1L))).isFalse();
        assertThat(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.of(ID, 0L, 1L))).isTrue();
        assertThat(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.of(ID, 0L, 1L).complement())).isFalse();
        assertThat(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.of(ID, 0L).complement())).isFalse();
        assertThat(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.of(ID, 1L).complement())).isTrue();

        assertThat(EquatableValueSet.of(ID, 0L, 1L).overlaps(EquatableValueSet.all(ID))).isTrue();
        assertThat(EquatableValueSet.of(ID, 0L, 1L).overlaps(EquatableValueSet.none(ID))).isFalse();
        assertThat(EquatableValueSet.of(ID, 0L, 1L).overlaps(EquatableValueSet.of(ID, 0L))).isTrue();
        assertThat(EquatableValueSet.of(ID, 0L, 1L).overlaps(EquatableValueSet.of(ID, -1L))).isFalse();
        assertThat(EquatableValueSet.of(ID, 0L, 1L).overlaps(EquatableValueSet.of(ID, 0L, 1L))).isTrue();
        assertThat(EquatableValueSet.of(ID, 0L, 1L).overlaps(EquatableValueSet.of(ID, -1L).complement())).isTrue();

        assertThat(EquatableValueSet.of(ID, 0L, 1L).complement().overlaps(EquatableValueSet.all(ID))).isTrue();
        assertThat(EquatableValueSet.of(ID, 0L, 1L).complement().overlaps(EquatableValueSet.none(ID))).isFalse();
        assertThat(EquatableValueSet.of(ID, 0L, 1L).complement().overlaps(EquatableValueSet.of(ID, 0L))).isFalse();
        assertThat(EquatableValueSet.of(ID, 0L, 1L).complement().overlaps(EquatableValueSet.of(ID, -1L))).isTrue();
        assertThat(EquatableValueSet.of(ID, 0L, 1L).complement().overlaps(EquatableValueSet.of(ID, 0L, 1L))).isFalse();
        assertThat(EquatableValueSet.of(ID, 0L, 1L).complement().overlaps(EquatableValueSet.of(ID, -1L).complement())).isTrue();
    }

    @Test
    public void testContains()
    {
        assertThat(EquatableValueSet.all(ID).contains(EquatableValueSet.all(ID))).isTrue();
        assertThat(EquatableValueSet.all(ID).contains(EquatableValueSet.none(ID))).isTrue();
        assertThat(EquatableValueSet.all(ID).contains(EquatableValueSet.of(ID, 0L))).isTrue();
        assertThat(EquatableValueSet.all(ID).contains(EquatableValueSet.of(ID, 0L, 1L))).isTrue();
        assertThat(EquatableValueSet.all(ID).contains(EquatableValueSet.of(ID, 0L, 1L).complement())).isTrue();

        assertThat(EquatableValueSet.none(ID).contains(EquatableValueSet.all(ID))).isFalse();
        assertThat(EquatableValueSet.none(ID).contains(EquatableValueSet.none(ID))).isTrue();
        assertThat(EquatableValueSet.none(ID).contains(EquatableValueSet.of(ID, 0L))).isFalse();
        assertThat(EquatableValueSet.none(ID).contains(EquatableValueSet.of(ID, 0L, 1L))).isFalse();
        assertThat(EquatableValueSet.none(ID).contains(EquatableValueSet.of(ID, 0L, 1L).complement())).isFalse();

        assertThat(EquatableValueSet.of(ID, 0L).contains(EquatableValueSet.all(ID))).isFalse();
        assertThat(EquatableValueSet.of(ID, 0L).contains(EquatableValueSet.none(ID))).isTrue();
        assertThat(EquatableValueSet.of(ID, 0L).contains(EquatableValueSet.of(ID, 0L))).isTrue();
        assertThat(EquatableValueSet.of(ID, 0L).contains(EquatableValueSet.of(ID, 0L, 1L))).isFalse();
        assertThat(EquatableValueSet.of(ID, 0L).contains(EquatableValueSet.of(ID, 0L, 1L).complement())).isFalse();
        assertThat(EquatableValueSet.of(ID, 0L).contains(EquatableValueSet.of(ID, 0L).complement())).isFalse();
        assertThat(EquatableValueSet.of(ID, 0L).contains(EquatableValueSet.of(ID, 1L).complement())).isFalse();

        assertThat(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.all(ID))).isFalse();
        assertThat(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.none(ID))).isTrue();
        assertThat(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.of(ID, 0L))).isTrue();
        assertThat(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.of(ID, 0L, 1L))).isTrue();
        assertThat(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.of(ID, 0L, 2L))).isFalse();
        assertThat(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.of(ID, 0L, 1L).complement())).isFalse();
        assertThat(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.of(ID, 0L).complement())).isFalse();
        assertThat(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.of(ID, 1L).complement())).isFalse();

        assertThat(EquatableValueSet.of(ID, 0L, 1L).complement().contains(EquatableValueSet.all(ID))).isFalse();
        assertThat(EquatableValueSet.of(ID, 0L, 1L).complement().contains(EquatableValueSet.none(ID))).isTrue();
        assertThat(EquatableValueSet.of(ID, 0L, 1L).complement().contains(EquatableValueSet.of(ID, 0L))).isFalse();
        assertThat(EquatableValueSet.of(ID, 0L, 1L).complement().contains(EquatableValueSet.of(ID, -1L))).isTrue();
        assertThat(EquatableValueSet.of(ID, 0L, 1L).complement().contains(EquatableValueSet.of(ID, 0L, 1L))).isFalse();
        assertThat(EquatableValueSet.of(ID, 0L, 1L).complement().contains(EquatableValueSet.of(ID, -1L).complement())).isFalse();
    }

    @Test
    public void testIntersect()
    {
        assertThat(EquatableValueSet.none(ID).intersect(EquatableValueSet.none(ID))).isEqualTo(EquatableValueSet.none(ID));
        assertThat(EquatableValueSet.all(ID).intersect(EquatableValueSet.all(ID))).isEqualTo(EquatableValueSet.all(ID));
        assertThat(EquatableValueSet.none(ID).intersect(EquatableValueSet.all(ID))).isEqualTo(EquatableValueSet.none(ID));
        assertThat(EquatableValueSet.none(ID).intersect(EquatableValueSet.of(ID, 0L))).isEqualTo(EquatableValueSet.none(ID));
        assertThat(EquatableValueSet.all(ID).intersect(EquatableValueSet.of(ID, 0L))).isEqualTo(EquatableValueSet.of(ID, 0L));
        assertThat(EquatableValueSet.of(ID, 0L).intersect(EquatableValueSet.of(ID, 0L))).isEqualTo(EquatableValueSet.of(ID, 0L));
        assertThat(EquatableValueSet.of(ID, 0L, 1L).intersect(EquatableValueSet.of(ID, 0L))).isEqualTo(EquatableValueSet.of(ID, 0L));
        assertThat(EquatableValueSet.of(ID, 0L).complement().intersect(EquatableValueSet.of(ID, 0L))).isEqualTo(EquatableValueSet.none(ID));
        assertThat(EquatableValueSet.of(ID, 0L).complement().intersect(EquatableValueSet.of(ID, 1L))).isEqualTo(EquatableValueSet.of(ID, 1L));
        assertThat(EquatableValueSet.of(ID, 0L).intersect(EquatableValueSet.of(ID, 1L).complement())).isEqualTo(EquatableValueSet.of(ID, 0L));
        assertThat(EquatableValueSet.of(ID, 0L, 1L).intersect(EquatableValueSet.of(ID, 0L, 2L))).isEqualTo(EquatableValueSet.of(ID, 0L));
        assertThat(EquatableValueSet.of(ID, 0L, 1L).complement().intersect(EquatableValueSet.of(ID, 0L, 2L))).isEqualTo(EquatableValueSet.of(ID, 2L));
        assertThat(EquatableValueSet.of(ID, 0L, 1L).complement().intersect(EquatableValueSet.of(ID, 0L, 2L).complement())).isEqualTo(EquatableValueSet.of(ID, 0L, 1L, 2L).complement());
    }

    @Test
    public void testUnion()
    {
        assertThat(EquatableValueSet.none(ID).union(EquatableValueSet.none(ID))).isEqualTo(EquatableValueSet.none(ID));
        assertThat(EquatableValueSet.all(ID).union(EquatableValueSet.all(ID))).isEqualTo(EquatableValueSet.all(ID));
        assertThat(EquatableValueSet.none(ID).union(EquatableValueSet.all(ID))).isEqualTo(EquatableValueSet.all(ID));
        assertThat(EquatableValueSet.none(ID).union(EquatableValueSet.of(ID, 0L))).isEqualTo(EquatableValueSet.of(ID, 0L));
        assertThat(EquatableValueSet.all(ID).union(EquatableValueSet.of(ID, 0L))).isEqualTo(EquatableValueSet.all(ID));
        assertThat(EquatableValueSet.of(ID, 0L).union(EquatableValueSet.of(ID, 0L))).isEqualTo(EquatableValueSet.of(ID, 0L));
        assertThat(EquatableValueSet.of(ID, 0L, 1L).union(EquatableValueSet.of(ID, 0L))).isEqualTo(EquatableValueSet.of(ID, 0L, 1L));
        assertThat(EquatableValueSet.of(ID, 0L).complement().union(EquatableValueSet.of(ID, 0L))).isEqualTo(EquatableValueSet.all(ID));
        assertThat(EquatableValueSet.of(ID, 0L).complement().union(EquatableValueSet.of(ID, 1L))).isEqualTo(EquatableValueSet.of(ID, 0L).complement());
        assertThat(EquatableValueSet.of(ID, 0L).union(EquatableValueSet.of(ID, 1L).complement())).isEqualTo(EquatableValueSet.of(ID, 1L).complement());
        assertThat(EquatableValueSet.of(ID, 0L, 1L).union(EquatableValueSet.of(ID, 0L, 2L))).isEqualTo(EquatableValueSet.of(ID, 0L, 1L, 2L));
        assertThat(EquatableValueSet.of(ID, 0L, 1L).complement().union(EquatableValueSet.of(ID, 0L, 2L))).isEqualTo(EquatableValueSet.of(ID, 1L).complement());
        assertThat(EquatableValueSet.of(ID, 0L, 1L).complement().union(EquatableValueSet.of(ID, 0L, 2L).complement())).isEqualTo(EquatableValueSet.of(ID, 0L).complement());
    }

    @Test
    public void testSubtract()
    {
        assertThat(EquatableValueSet.all(ID).subtract(EquatableValueSet.all(ID))).isEqualTo(EquatableValueSet.none(ID));
        assertThat(EquatableValueSet.all(ID).subtract(EquatableValueSet.none(ID))).isEqualTo(EquatableValueSet.all(ID));
        assertThat(EquatableValueSet.all(ID).subtract(EquatableValueSet.of(ID, 0L))).isEqualTo(EquatableValueSet.of(ID, 0L).complement());
        assertThat(EquatableValueSet.all(ID).subtract(EquatableValueSet.of(ID, 0L, 1L))).isEqualTo(EquatableValueSet.of(ID, 0L, 1L).complement());
        assertThat(EquatableValueSet.all(ID).subtract(EquatableValueSet.of(ID, 0L, 1L).complement())).isEqualTo(EquatableValueSet.of(ID, 0L, 1L));

        assertThat(EquatableValueSet.none(ID).subtract(EquatableValueSet.all(ID))).isEqualTo(EquatableValueSet.none(ID));
        assertThat(EquatableValueSet.none(ID).subtract(EquatableValueSet.none(ID))).isEqualTo(EquatableValueSet.none(ID));
        assertThat(EquatableValueSet.none(ID).subtract(EquatableValueSet.of(ID, 0L))).isEqualTo(EquatableValueSet.none(ID));
        assertThat(EquatableValueSet.none(ID).subtract(EquatableValueSet.of(ID, 0L, 1L))).isEqualTo(EquatableValueSet.none(ID));
        assertThat(EquatableValueSet.none(ID).subtract(EquatableValueSet.of(ID, 0L, 1L).complement())).isEqualTo(EquatableValueSet.none(ID));

        assertThat(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.all(ID))).isEqualTo(EquatableValueSet.none(ID));
        assertThat(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.none(ID))).isEqualTo(EquatableValueSet.of(ID, 0L));
        assertThat(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.of(ID, 0L))).isEqualTo(EquatableValueSet.none(ID));
        assertThat(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.of(ID, 0L).complement())).isEqualTo(EquatableValueSet.of(ID, 0L));
        assertThat(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.of(ID, 1L))).isEqualTo(EquatableValueSet.of(ID, 0L));
        assertThat(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.of(ID, 1L).complement())).isEqualTo(EquatableValueSet.none(ID));
        assertThat(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.of(ID, 0L, 1L))).isEqualTo(EquatableValueSet.none(ID));
        assertThat(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.of(ID, 0L, 1L).complement())).isEqualTo(EquatableValueSet.of(ID, 0L));

        assertThat(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.all(ID))).isEqualTo(EquatableValueSet.none(ID));
        assertThat(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.none(ID))).isEqualTo(EquatableValueSet.of(ID, 0L).complement());
        assertThat(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.of(ID, 0L))).isEqualTo(EquatableValueSet.of(ID, 0L).complement());
        assertThat(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.of(ID, 0L).complement())).isEqualTo(EquatableValueSet.none(ID));
        assertThat(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.of(ID, 1L))).isEqualTo(EquatableValueSet.of(ID, 0L, 1L).complement());
        assertThat(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.of(ID, 1L).complement())).isEqualTo(EquatableValueSet.of(ID, 1L));
        assertThat(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.of(ID, 0L, 1L))).isEqualTo(EquatableValueSet.of(ID, 0L, 1L).complement());
        assertThat(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.of(ID, 0L, 1L).complement())).isEqualTo(EquatableValueSet.of(ID, 1L));
    }

    @Test
    public void testUnmodifiableCollection()
    {
        Collection<Object> values = EquatableValueSet.of(ID, 1L).getValues();
        assertThatThrownBy(values::clear)
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testUnmodifiableValueEntries()
    {
        Set<EquatableValueSet.ValueEntry> entries = EquatableValueSet.of(ID, 1L).getEntries();
        assertThatThrownBy(entries::clear)
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testUnmodifiableIterator()
    {
        Iterator<Object> iterator = EquatableValueSet.of(ID, 1L).getValues().iterator();
        iterator.next();
        assertThatThrownBy(iterator::remove)
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testUnmodifiableValueEntryIterator()
    {
        Iterator<EquatableValueSet.ValueEntry> iterator = EquatableValueSet.of(ID, 1L).getEntries().iterator();
        iterator.next();
        assertThatThrownBy(iterator::remove)
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();

        ObjectMapper mapper = new ObjectMapperProvider().get()
                .registerModule(new SimpleModule()
                        .addDeserializer(Type.class, new TestingTypeDeserializer(typeManager))
                        .addSerializer(Block.class, new TestingBlockJsonSerde.Serializer(blockEncodingSerde))
                        .addDeserializer(Block.class, new TestingBlockJsonSerde.Deserializer(blockEncodingSerde)));

        EquatableValueSet set = EquatableValueSet.all(ID);
        assertThat(set).isEqualTo(mapper.readValue(mapper.writeValueAsString(set), EquatableValueSet.class));

        set = EquatableValueSet.none(ID);
        assertThat(set).isEqualTo(mapper.readValue(mapper.writeValueAsString(set), EquatableValueSet.class));

        set = EquatableValueSet.of(ID, 1L);
        assertThat(set).isEqualTo(mapper.readValue(mapper.writeValueAsString(set), EquatableValueSet.class));

        set = EquatableValueSet.of(ID, 1L, 2L);
        assertThat(set).isEqualTo(mapper.readValue(mapper.writeValueAsString(set), EquatableValueSet.class));

        set = EquatableValueSet.of(ID, 1L, 2L).complement();
        assertThat(set).isEqualTo(mapper.readValue(mapper.writeValueAsString(set), EquatableValueSet.class));
    }

    @Test
    public void testExpandRanges()
    {
        assertThat(ValueSet.all(ID).tryExpandRanges(10)).isEqualTo(Optional.empty());
        assertThat(ValueSet.none(ID).tryExpandRanges(10)).isEqualTo(Optional.of(List.of()));
        assertThat(ValueSet.none(ID).tryExpandRanges(1)).isEqualTo(Optional.of(List.of()));
        assertThat(ValueSet.none(ID).tryExpandRanges(0)).isEqualTo(Optional.of(List.of()));
        assertThat(ValueSet.of(ID, 1L, 2L).tryExpandRanges(3)).isEqualTo(Optional.of(List.of(1L, 2L)));
        assertThat(ValueSet.of(ID, 1L, 2L).tryExpandRanges(2)).isEqualTo(Optional.of(List.of(1L, 2L)));
        assertThat(ValueSet.of(ID, 1L, 2L).tryExpandRanges(1)).isEqualTo(Optional.empty());
        assertThat(ValueSet.of(ID, 1L, 2L).tryExpandRanges(0)).isEqualTo(Optional.empty());
    }
}

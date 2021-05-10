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
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import static io.trino.spi.type.TestingIdType.ID;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestEquatableValueSet
{
    @Test
    public void testEmptySet()
    {
        EquatableValueSet equatables = EquatableValueSet.none(ID);
        assertEquals(equatables.getType(), ID);
        assertTrue(equatables.isNone());
        assertFalse(equatables.isAll());
        assertFalse(equatables.isSingleValue());
        assertTrue(equatables.inclusive());
        assertEquals(equatables.getValues().size(), 0);
        assertEquals(equatables.complement(), EquatableValueSet.all(ID));
        assertFalse(equatables.containsValue(0L));
        assertFalse(equatables.containsValue(1L));
        assertEquals(equatables.toString(), "EquatableValueSet[type=id, values=0, {}]");
    }

    @Test
    public void testEntireSet()
    {
        EquatableValueSet equatables = EquatableValueSet.all(ID);
        assertEquals(equatables.getType(), ID);
        assertFalse(equatables.isNone());
        assertTrue(equatables.isAll());
        assertFalse(equatables.isSingleValue());
        assertFalse(equatables.inclusive());
        assertEquals(equatables.getValues().size(), 0);
        assertEquals(equatables.complement(), EquatableValueSet.none(ID));
        assertTrue(equatables.containsValue(0L));
        assertTrue(equatables.containsValue(1L));
    }

    @Test
    public void testSingleValue()
    {
        EquatableValueSet equatables = EquatableValueSet.of(ID, 10L);

        EquatableValueSet complement = (EquatableValueSet) EquatableValueSet.all(ID).subtract(equatables);

        // inclusive
        assertEquals(equatables.getType(), ID);
        assertFalse(equatables.isNone());
        assertFalse(equatables.isAll());
        assertTrue(equatables.isSingleValue());
        assertTrue(equatables.inclusive());
        assertTrue(Iterables.elementsEqual(equatables.getValues(), ImmutableList.of(10L)));
        assertEquals(equatables.complement(), complement);
        assertFalse(equatables.containsValue(0L));
        assertFalse(equatables.containsValue(1L));
        assertTrue(equatables.containsValue(10L));

        // exclusive
        assertEquals(complement.getType(), ID);
        assertFalse(complement.isNone());
        assertFalse(complement.isAll());
        assertFalse(complement.isSingleValue());
        assertFalse(complement.inclusive());
        assertTrue(Iterables.elementsEqual(complement.getValues(), ImmutableList.of(10L)));
        assertEquals(complement.complement(), equatables);
        assertTrue(complement.containsValue(0L));
        assertTrue(complement.containsValue(1L));
        assertFalse(complement.containsValue(10L));
    }

    @Test
    public void testMultipleValues()
    {
        EquatableValueSet equatables = EquatableValueSet.of(ID, 1L, 2L, 3L, 1L);

        EquatableValueSet complement = (EquatableValueSet) EquatableValueSet.all(ID).subtract(equatables);

        // inclusive
        assertEquals(equatables.getType(), ID);
        assertFalse(equatables.isNone());
        assertFalse(equatables.isAll());
        assertFalse(equatables.isSingleValue());
        assertTrue(equatables.inclusive());
        assertTrue(Iterables.elementsEqual(equatables.getValues(), ImmutableList.of(1L, 2L, 3L)));
        assertEquals(equatables.complement(), complement);
        assertFalse(equatables.containsValue(0L));
        assertTrue(equatables.containsValue(1L));
        assertTrue(equatables.containsValue(2L));
        assertTrue(equatables.containsValue(3L));
        assertFalse(equatables.containsValue(4L));
        assertEquals(equatables.toString(), "EquatableValueSet[type=id, values=3, {1, 2, 3}]");
        assertEquals(
                equatables.toString(ToStringSession.INSTANCE, 2),
                "EquatableValueSet[type=id, values=3, {1, 2, ...}]");

        // exclusive
        assertEquals(complement.getType(), ID);
        assertFalse(complement.isNone());
        assertFalse(complement.isAll());
        assertFalse(complement.isSingleValue());
        assertFalse(complement.inclusive());
        assertTrue(Iterables.elementsEqual(complement.getValues(), ImmutableList.of(1L, 2L, 3L)));
        assertEquals(complement.complement(), equatables);
        assertTrue(complement.containsValue(0L));
        assertFalse(complement.containsValue(1L));
        assertFalse(complement.containsValue(2L));
        assertFalse(complement.containsValue(3L));
        assertTrue(complement.containsValue(4L));
        assertEquals(complement.toString(), "EquatableValueSet[type=id, values=3, EXCLUDES{1, 2, 3}]");
        assertEquals(
                complement.toString(ToStringSession.INSTANCE, 2),
                "EquatableValueSet[type=id, values=3, EXCLUDES{1, 2, ...}]");
    }

    @Test
    public void testGetSingleValue()
    {
        assertEquals(EquatableValueSet.of(ID, 0L).getSingleValue(), 0L);
        assertThatThrownBy(() -> EquatableValueSet.all(ID).getSingleValue())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("EquatableValueSet does not have just a single value");
    }

    @Test
    public void testOverlaps()
    {
        assertTrue(EquatableValueSet.all(ID).overlaps(EquatableValueSet.all(ID)));
        assertFalse(EquatableValueSet.all(ID).overlaps(EquatableValueSet.none(ID)));
        assertTrue(EquatableValueSet.all(ID).overlaps(EquatableValueSet.of(ID, 0L)));
        assertTrue(EquatableValueSet.all(ID).overlaps(EquatableValueSet.of(ID, 0L, 1L)));
        assertTrue(EquatableValueSet.all(ID).overlaps(EquatableValueSet.of(ID, 0L, 1L).complement()));

        assertFalse(EquatableValueSet.none(ID).overlaps(EquatableValueSet.all(ID)));
        assertFalse(EquatableValueSet.none(ID).overlaps(EquatableValueSet.none(ID)));
        assertFalse(EquatableValueSet.none(ID).overlaps(EquatableValueSet.of(ID, 0L)));
        assertFalse(EquatableValueSet.none(ID).overlaps(EquatableValueSet.of(ID, 0L, 1L)));
        assertFalse(EquatableValueSet.none(ID).overlaps(EquatableValueSet.of(ID, 0L, 1L).complement()));

        assertTrue(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.all(ID)));
        assertFalse(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.none(ID)));
        assertTrue(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.of(ID, 0L)));
        assertFalse(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.of(ID, 1L)));
        assertTrue(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.of(ID, 0L, 1L)));
        assertFalse(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.of(ID, 0L, 1L).complement()));
        assertFalse(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.of(ID, 0L).complement()));
        assertTrue(EquatableValueSet.of(ID, 0L).overlaps(EquatableValueSet.of(ID, 1L).complement()));

        assertTrue(EquatableValueSet.of(ID, 0L, 1L).overlaps(EquatableValueSet.all(ID)));
        assertFalse(EquatableValueSet.of(ID, 0L, 1L).overlaps(EquatableValueSet.none(ID)));
        assertTrue(EquatableValueSet.of(ID, 0L, 1L).overlaps(EquatableValueSet.of(ID, 0L)));
        assertFalse(EquatableValueSet.of(ID, 0L, 1L).overlaps(EquatableValueSet.of(ID, -1L)));
        assertTrue(EquatableValueSet.of(ID, 0L, 1L).overlaps(EquatableValueSet.of(ID, 0L, 1L)));
        assertTrue(EquatableValueSet.of(ID, 0L, 1L).overlaps(EquatableValueSet.of(ID, -1L).complement()));

        assertTrue(EquatableValueSet.of(ID, 0L, 1L).complement().overlaps(EquatableValueSet.all(ID)));
        assertFalse(EquatableValueSet.of(ID, 0L, 1L).complement().overlaps(EquatableValueSet.none(ID)));
        assertFalse(EquatableValueSet.of(ID, 0L, 1L).complement().overlaps(EquatableValueSet.of(ID, 0L)));
        assertTrue(EquatableValueSet.of(ID, 0L, 1L).complement().overlaps(EquatableValueSet.of(ID, -1L)));
        assertFalse(EquatableValueSet.of(ID, 0L, 1L).complement().overlaps(EquatableValueSet.of(ID, 0L, 1L)));
        assertTrue(EquatableValueSet.of(ID, 0L, 1L).complement().overlaps(EquatableValueSet.of(ID, -1L).complement()));
    }

    @Test
    public void testContains()
    {
        assertTrue(EquatableValueSet.all(ID).contains(EquatableValueSet.all(ID)));
        assertTrue(EquatableValueSet.all(ID).contains(EquatableValueSet.none(ID)));
        assertTrue(EquatableValueSet.all(ID).contains(EquatableValueSet.of(ID, 0L)));
        assertTrue(EquatableValueSet.all(ID).contains(EquatableValueSet.of(ID, 0L, 1L)));
        assertTrue(EquatableValueSet.all(ID).contains(EquatableValueSet.of(ID, 0L, 1L).complement()));

        assertFalse(EquatableValueSet.none(ID).contains(EquatableValueSet.all(ID)));
        assertTrue(EquatableValueSet.none(ID).contains(EquatableValueSet.none(ID)));
        assertFalse(EquatableValueSet.none(ID).contains(EquatableValueSet.of(ID, 0L)));
        assertFalse(EquatableValueSet.none(ID).contains(EquatableValueSet.of(ID, 0L, 1L)));
        assertFalse(EquatableValueSet.none(ID).contains(EquatableValueSet.of(ID, 0L, 1L).complement()));

        assertFalse(EquatableValueSet.of(ID, 0L).contains(EquatableValueSet.all(ID)));
        assertTrue(EquatableValueSet.of(ID, 0L).contains(EquatableValueSet.none(ID)));
        assertTrue(EquatableValueSet.of(ID, 0L).contains(EquatableValueSet.of(ID, 0L)));
        assertFalse(EquatableValueSet.of(ID, 0L).contains(EquatableValueSet.of(ID, 0L, 1L)));
        assertFalse(EquatableValueSet.of(ID, 0L).contains(EquatableValueSet.of(ID, 0L, 1L).complement()));
        assertFalse(EquatableValueSet.of(ID, 0L).contains(EquatableValueSet.of(ID, 0L).complement()));
        assertFalse(EquatableValueSet.of(ID, 0L).contains(EquatableValueSet.of(ID, 1L).complement()));

        assertFalse(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.all(ID)));
        assertTrue(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.none(ID)));
        assertTrue(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.of(ID, 0L)));
        assertTrue(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.of(ID, 0L, 1L)));
        assertFalse(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.of(ID, 0L, 2L)));
        assertFalse(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.of(ID, 0L, 1L).complement()));
        assertFalse(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.of(ID, 0L).complement()));
        assertFalse(EquatableValueSet.of(ID, 0L, 1L).contains(EquatableValueSet.of(ID, 1L).complement()));

        assertFalse(EquatableValueSet.of(ID, 0L, 1L).complement().contains(EquatableValueSet.all(ID)));
        assertTrue(EquatableValueSet.of(ID, 0L, 1L).complement().contains(EquatableValueSet.none(ID)));
        assertFalse(EquatableValueSet.of(ID, 0L, 1L).complement().contains(EquatableValueSet.of(ID, 0L)));
        assertTrue(EquatableValueSet.of(ID, 0L, 1L).complement().contains(EquatableValueSet.of(ID, -1L)));
        assertFalse(EquatableValueSet.of(ID, 0L, 1L).complement().contains(EquatableValueSet.of(ID, 0L, 1L)));
        assertFalse(EquatableValueSet.of(ID, 0L, 1L).complement().contains(EquatableValueSet.of(ID, -1L).complement()));
    }

    @Test
    public void testIntersect()
    {
        assertEquals(EquatableValueSet.none(ID).intersect(EquatableValueSet.none(ID)), EquatableValueSet.none(ID));
        assertEquals(EquatableValueSet.all(ID).intersect(EquatableValueSet.all(ID)), EquatableValueSet.all(ID));
        assertEquals(EquatableValueSet.none(ID).intersect(EquatableValueSet.all(ID)), EquatableValueSet.none(ID));
        assertEquals(EquatableValueSet.none(ID).intersect(EquatableValueSet.of(ID, 0L)), EquatableValueSet.none(ID));
        assertEquals(EquatableValueSet.all(ID).intersect(EquatableValueSet.of(ID, 0L)), EquatableValueSet.of(ID, 0L));
        assertEquals(EquatableValueSet.of(ID, 0L).intersect(EquatableValueSet.of(ID, 0L)), EquatableValueSet.of(ID, 0L));
        assertEquals(EquatableValueSet.of(ID, 0L, 1L).intersect(EquatableValueSet.of(ID, 0L)), EquatableValueSet.of(ID, 0L));
        assertEquals(EquatableValueSet.of(ID, 0L).complement().intersect(EquatableValueSet.of(ID, 0L)), EquatableValueSet.none(ID));
        assertEquals(EquatableValueSet.of(ID, 0L).complement().intersect(EquatableValueSet.of(ID, 1L)), EquatableValueSet.of(ID, 1L));
        assertEquals(EquatableValueSet.of(ID, 0L).intersect(EquatableValueSet.of(ID, 1L).complement()), EquatableValueSet.of(ID, 0L));
        assertEquals(EquatableValueSet.of(ID, 0L, 1L).intersect(EquatableValueSet.of(ID, 0L, 2L)), EquatableValueSet.of(ID, 0L));
        assertEquals(EquatableValueSet.of(ID, 0L, 1L).complement().intersect(EquatableValueSet.of(ID, 0L, 2L)), EquatableValueSet.of(ID, 2L));
        assertEquals(EquatableValueSet.of(ID, 0L, 1L).complement().intersect(EquatableValueSet.of(ID, 0L, 2L).complement()), EquatableValueSet.of(ID, 0L, 1L, 2L).complement());
    }

    @Test
    public void testUnion()
    {
        assertEquals(EquatableValueSet.none(ID).union(EquatableValueSet.none(ID)), EquatableValueSet.none(ID));
        assertEquals(EquatableValueSet.all(ID).union(EquatableValueSet.all(ID)), EquatableValueSet.all(ID));
        assertEquals(EquatableValueSet.none(ID).union(EquatableValueSet.all(ID)), EquatableValueSet.all(ID));
        assertEquals(EquatableValueSet.none(ID).union(EquatableValueSet.of(ID, 0L)), EquatableValueSet.of(ID, 0L));
        assertEquals(EquatableValueSet.all(ID).union(EquatableValueSet.of(ID, 0L)), EquatableValueSet.all(ID));
        assertEquals(EquatableValueSet.of(ID, 0L).union(EquatableValueSet.of(ID, 0L)), EquatableValueSet.of(ID, 0L));
        assertEquals(EquatableValueSet.of(ID, 0L, 1L).union(EquatableValueSet.of(ID, 0L)), EquatableValueSet.of(ID, 0L, 1L));
        assertEquals(EquatableValueSet.of(ID, 0L).complement().union(EquatableValueSet.of(ID, 0L)), EquatableValueSet.all(ID));
        assertEquals(EquatableValueSet.of(ID, 0L).complement().union(EquatableValueSet.of(ID, 1L)), EquatableValueSet.of(ID, 0L).complement());
        assertEquals(EquatableValueSet.of(ID, 0L).union(EquatableValueSet.of(ID, 1L).complement()), EquatableValueSet.of(ID, 1L).complement());
        assertEquals(EquatableValueSet.of(ID, 0L, 1L).union(EquatableValueSet.of(ID, 0L, 2L)), EquatableValueSet.of(ID, 0L, 1L, 2L));
        assertEquals(EquatableValueSet.of(ID, 0L, 1L).complement().union(EquatableValueSet.of(ID, 0L, 2L)), EquatableValueSet.of(ID, 1L).complement());
        assertEquals(EquatableValueSet.of(ID, 0L, 1L).complement().union(EquatableValueSet.of(ID, 0L, 2L).complement()), EquatableValueSet.of(ID, 0L).complement());
    }

    @Test
    public void testSubtract()
    {
        assertEquals(EquatableValueSet.all(ID).subtract(EquatableValueSet.all(ID)), EquatableValueSet.none(ID));
        assertEquals(EquatableValueSet.all(ID).subtract(EquatableValueSet.none(ID)), EquatableValueSet.all(ID));
        assertEquals(EquatableValueSet.all(ID).subtract(EquatableValueSet.of(ID, 0L)), EquatableValueSet.of(ID, 0L).complement());
        assertEquals(EquatableValueSet.all(ID).subtract(EquatableValueSet.of(ID, 0L, 1L)), EquatableValueSet.of(ID, 0L, 1L).complement());
        assertEquals(EquatableValueSet.all(ID).subtract(EquatableValueSet.of(ID, 0L, 1L).complement()), EquatableValueSet.of(ID, 0L, 1L));

        assertEquals(EquatableValueSet.none(ID).subtract(EquatableValueSet.all(ID)), EquatableValueSet.none(ID));
        assertEquals(EquatableValueSet.none(ID).subtract(EquatableValueSet.none(ID)), EquatableValueSet.none(ID));
        assertEquals(EquatableValueSet.none(ID).subtract(EquatableValueSet.of(ID, 0L)), EquatableValueSet.none(ID));
        assertEquals(EquatableValueSet.none(ID).subtract(EquatableValueSet.of(ID, 0L, 1L)), EquatableValueSet.none(ID));
        assertEquals(EquatableValueSet.none(ID).subtract(EquatableValueSet.of(ID, 0L, 1L).complement()), EquatableValueSet.none(ID));

        assertEquals(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.all(ID)), EquatableValueSet.none(ID));
        assertEquals(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.none(ID)), EquatableValueSet.of(ID, 0L));
        assertEquals(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.of(ID, 0L)), EquatableValueSet.none(ID));
        assertEquals(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.of(ID, 0L).complement()), EquatableValueSet.of(ID, 0L));
        assertEquals(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.of(ID, 1L)), EquatableValueSet.of(ID, 0L));
        assertEquals(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.of(ID, 1L).complement()), EquatableValueSet.none(ID));
        assertEquals(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.of(ID, 0L, 1L)), EquatableValueSet.none(ID));
        assertEquals(EquatableValueSet.of(ID, 0L).subtract(EquatableValueSet.of(ID, 0L, 1L).complement()), EquatableValueSet.of(ID, 0L));

        assertEquals(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.all(ID)), EquatableValueSet.none(ID));
        assertEquals(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.none(ID)), EquatableValueSet.of(ID, 0L).complement());
        assertEquals(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.of(ID, 0L)), EquatableValueSet.of(ID, 0L).complement());
        assertEquals(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.of(ID, 0L).complement()), EquatableValueSet.none(ID));
        assertEquals(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.of(ID, 1L)), EquatableValueSet.of(ID, 0L, 1L).complement());
        assertEquals(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.of(ID, 1L).complement()), EquatableValueSet.of(ID, 1L));
        assertEquals(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.of(ID, 0L, 1L)), EquatableValueSet.of(ID, 0L, 1L).complement());
        assertEquals(EquatableValueSet.of(ID, 0L).complement().subtract(EquatableValueSet.of(ID, 0L, 1L).complement()), EquatableValueSet.of(ID, 1L));
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
        assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), EquatableValueSet.class));

        set = EquatableValueSet.none(ID);
        assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), EquatableValueSet.class));

        set = EquatableValueSet.of(ID, 1L);
        assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), EquatableValueSet.class));

        set = EquatableValueSet.of(ID, 1L, 2L);
        assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), EquatableValueSet.class));

        set = EquatableValueSet.of(ID, 1L, 2L).complement();
        assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), EquatableValueSet.class));
    }
}

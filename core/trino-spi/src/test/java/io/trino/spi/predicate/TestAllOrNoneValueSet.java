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
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slices;
import io.trino.spi.type.TestingTypeDeserializer;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAllOrNoneValueSet
{
    @Test
    public void testAll()
    {
        AllOrNoneValueSet valueSet = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        assertThat(valueSet.getType()).isEqualTo(HYPER_LOG_LOG);
        assertThat(valueSet.isNone()).isFalse();
        assertThat(valueSet.isAll()).isTrue();
        assertThat(valueSet.isSingleValue()).isFalse();
        assertThat(valueSet.containsValue(Slices.EMPTY_SLICE)).isTrue();

        assertThatThrownBy(valueSet::getSingleValue)
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testNone()
    {
        AllOrNoneValueSet valueSet = AllOrNoneValueSet.none(HYPER_LOG_LOG);
        assertThat(valueSet.getType()).isEqualTo(HYPER_LOG_LOG);
        assertThat(valueSet.isNone()).isTrue();
        assertThat(valueSet.isAll()).isFalse();
        assertThat(valueSet.isSingleValue()).isFalse();
        assertThat(valueSet.containsValue(Slices.EMPTY_SLICE)).isFalse();

        assertThatThrownBy(valueSet::getSingleValue)
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testIntersect()
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        AllOrNoneValueSet none = AllOrNoneValueSet.none(HYPER_LOG_LOG);

        assertThat(all.intersect(all)).isEqualTo(all);
        assertThat(all.intersect(none)).isEqualTo(none);
        assertThat(none.intersect(all)).isEqualTo(none);
        assertThat(none.intersect(none)).isEqualTo(none);
    }

    @Test
    public void testUnion()
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        AllOrNoneValueSet none = AllOrNoneValueSet.none(HYPER_LOG_LOG);

        assertThat(all.union(all)).isEqualTo(all);
        assertThat(all.union(none)).isEqualTo(all);
        assertThat(none.union(all)).isEqualTo(all);
        assertThat(none.union(none)).isEqualTo(none);
    }

    @Test
    public void testComplement()
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        AllOrNoneValueSet none = AllOrNoneValueSet.none(HYPER_LOG_LOG);

        assertThat(all.complement()).isEqualTo(none);
        assertThat(none.complement()).isEqualTo(all);
    }

    @Test
    public void testOverlaps()
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        AllOrNoneValueSet none = AllOrNoneValueSet.none(HYPER_LOG_LOG);

        assertThat(all.overlaps(all)).isTrue();
        assertThat(all.overlaps(none)).isFalse();
        assertThat(none.overlaps(all)).isFalse();
        assertThat(none.overlaps(none)).isFalse();
    }

    @Test
    public void testSubtract()
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        AllOrNoneValueSet none = AllOrNoneValueSet.none(HYPER_LOG_LOG);

        assertThat(all.subtract(all)).isEqualTo(none);
        assertThat(all.subtract(none)).isEqualTo(all);
        assertThat(none.subtract(all)).isEqualTo(none);
        assertThat(none.subtract(none)).isEqualTo(none);
    }

    @Test
    public void testContains()
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        AllOrNoneValueSet none = AllOrNoneValueSet.none(HYPER_LOG_LOG);

        assertThat(all.contains(all)).isTrue();
        assertThat(all.contains(none)).isTrue();
        assertThat(none.contains(all)).isFalse();
        assertThat(none.contains(none)).isTrue();
    }

    @Test
    public void testContainsValue()
    {
        assertThat(AllOrNoneValueSet.all(BIGINT).containsValue(42L)).isTrue();
        assertThat(AllOrNoneValueSet.none(BIGINT).containsValue(42L)).isFalse();
    }

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        TestingTypeManager typeManager = new TestingTypeManager();

        ObjectMapper mapper = new ObjectMapperProvider().get()
                .registerModule(new SimpleModule().addDeserializer(Type.class, new TestingTypeDeserializer(typeManager)));

        AllOrNoneValueSet all = AllOrNoneValueSet.all(HYPER_LOG_LOG);
        assertThat(all).isEqualTo(mapper.readValue(mapper.writeValueAsString(all), AllOrNoneValueSet.class));

        AllOrNoneValueSet none = AllOrNoneValueSet.none(HYPER_LOG_LOG);
        assertThat(none).isEqualTo(mapper.readValue(mapper.writeValueAsString(none), AllOrNoneValueSet.class));
    }

    @Test
    public void testExpandRanges()
    {
        // HyperLogLogType is non-comparable and non-orderable
        assertThat(ValueSet.all(HYPER_LOG_LOG).tryExpandRanges(10)).isEqualTo(Optional.empty());
        assertThat(ValueSet.none(HYPER_LOG_LOG).tryExpandRanges(10)).isEqualTo(Optional.of(List.of()));
        assertThat(ValueSet.none(HYPER_LOG_LOG).tryExpandRanges(1)).isEqualTo(Optional.of(List.of()));
        assertThat(ValueSet.none(HYPER_LOG_LOG).tryExpandRanges(0)).isEqualTo(Optional.of(List.of()));
    }
}

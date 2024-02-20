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
package io.trino.sql.planner.optimizations;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.ObjectMapperProvider;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConstantProperty;
import io.trino.spi.connector.GroupingProperty;
import io.trino.spi.connector.LocalProperty;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.connector.SortingProperty;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static io.trino.sql.planner.optimizations.LocalProperties.extractLeadingConstants;
import static io.trino.sql.planner.optimizations.LocalProperties.stripLeadingConstants;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLocalProperties
{
    @Test
    public void testConstantProcessing()
    {
        assertThat(stripLeadingConstants(ImmutableList.of())).isEqualTo(ImmutableList.of());
        assertThat(extractLeadingConstants(ImmutableList.of())).isEqualTo(ImmutableSet.of());

        List<LocalProperty<String>> input = ImmutableList.of(grouped("a"));
        assertThat(stripLeadingConstants(input)).isEqualTo(ImmutableList.of(grouped("a")));
        assertThat(extractLeadingConstants(input)).isEqualTo(ImmutableSet.of());

        input = ImmutableList.of(constant("b"), grouped("a"));
        assertThat(stripLeadingConstants(input)).isEqualTo(ImmutableList.of(grouped("a")));
        assertThat(extractLeadingConstants(input)).isEqualTo(ImmutableSet.of("b"));

        input = ImmutableList.of(constant("a"), grouped("a"));
        assertThat(stripLeadingConstants(input)).isEqualTo(ImmutableList.of(grouped("a")));
        assertThat(extractLeadingConstants(input)).isEqualTo(ImmutableSet.of("a"));

        input = ImmutableList.of(grouped("a"), constant("b"));
        assertThat(stripLeadingConstants(input)).isEqualTo(input);
        assertThat(extractLeadingConstants(input)).isEqualTo(ImmutableSet.of());

        input = ImmutableList.of(constant("a"));
        assertThat(stripLeadingConstants(input)).isEqualTo(ImmutableList.of());
        assertThat(extractLeadingConstants(input)).isEqualTo(ImmutableSet.of("a"));

        input = ImmutableList.of(constant("a"), constant("b"));
        assertThat(stripLeadingConstants(input)).isEqualTo(ImmutableList.of());
        assertThat(extractLeadingConstants(input)).isEqualTo(ImmutableSet.of("a", "b"));
    }

    @Test
    public void testTranslate()
    {
        Map<String, String> map = ImmutableMap.of();
        List<LocalProperty<String>> input = ImmutableList.of();
        assertThat(LocalProperties.translate(input, translateWithMap(map))).isEqualTo(ImmutableList.of());

        map = ImmutableMap.of();
        input = ImmutableList.of(grouped("a"));
        assertThat(LocalProperties.translate(input, translateWithMap(map))).isEqualTo(ImmutableList.of());

        map = ImmutableMap.of("a", "a1");
        input = ImmutableList.of(grouped("a"));
        assertThat(LocalProperties.translate(input, translateWithMap(map))).isEqualTo(ImmutableList.of(grouped("a1")));

        map = ImmutableMap.of();
        input = ImmutableList.of(constant("a"));
        assertThat(LocalProperties.translate(input, translateWithMap(map))).isEqualTo(ImmutableList.of());

        map = ImmutableMap.of();
        input = ImmutableList.of(constant("a"), grouped("b"));
        assertThat(LocalProperties.translate(input, translateWithMap(map))).isEqualTo(ImmutableList.of());

        map = ImmutableMap.of("b", "b1");
        input = ImmutableList.of(constant("a"), grouped("b"));
        assertThat(LocalProperties.translate(input, translateWithMap(map))).isEqualTo(ImmutableList.of(grouped("b1")));

        map = ImmutableMap.of("a", "a1", "b", "b1");
        input = ImmutableList.of(constant("a"), grouped("b"));
        assertThat(LocalProperties.translate(input, translateWithMap(map))).isEqualTo(ImmutableList.of(constant("a1"), grouped("b1")));

        map = ImmutableMap.of("a", "a1", "b", "b1");
        input = ImmutableList.of(grouped("a", "b"));
        assertThat(LocalProperties.translate(input, translateWithMap(map))).isEqualTo(ImmutableList.of(grouped("a1", "b1")));

        map = ImmutableMap.of("a", "a1", "c", "c1");
        input = ImmutableList.of(constant("a"), grouped("b"), grouped("c"));
        assertThat(LocalProperties.translate(input, translateWithMap(map))).isEqualTo(ImmutableList.of(constant("a1")));

        map = ImmutableMap.of("a", "a1", "c", "c1");
        input = ImmutableList.of(grouped("a", "b"), grouped("c"));
        assertThat(LocalProperties.translate(input, translateWithMap(map))).isEqualTo(ImmutableList.of());

        map = ImmutableMap.of("a", "a1", "c", "c1");
        input = ImmutableList.of(grouped("a"), grouped("b"), grouped("c"));
        assertThat(LocalProperties.translate(input, translateWithMap(map))).isEqualTo(ImmutableList.of(grouped("a1")));

        map = ImmutableMap.of("a", "a1", "c", "c1");
        input = ImmutableList.of(constant("b"), grouped("a", "b"), grouped("c")); // Because b is constant, we can rewrite (a, b)
        assertThat(LocalProperties.translate(input, translateWithMap(map))).isEqualTo(ImmutableList.of(grouped("a1"), grouped("c1")));

        map = ImmutableMap.of("a", "a1", "c", "c1");
        input = ImmutableList.of(grouped("a"), constant("b"), grouped("c")); // Don't fail c translation due to a failed constant translation
        assertThat(LocalProperties.translate(input, translateWithMap(map))).isEqualTo(ImmutableList.of(grouped("a1"), grouped("c1")));

        map = ImmutableMap.of("a", "a1", "b", "b1", "c", "c1");
        input = ImmutableList.of(grouped("a"), constant("b"), grouped("c"));
        assertThat(LocalProperties.translate(input, translateWithMap(map))).isEqualTo(ImmutableList.of(grouped("a1"), constant("b1"), grouped("c1")));
    }

    private static <X, Y> Function<X, Optional<Y>> translateWithMap(Map<X, Y> translateMap)
    {
        return input -> Optional.ofNullable(translateMap.get(input));
    }

    @Test
    public void testNormalizeEmpty()
    {
        List<LocalProperty<String>> localProperties = builder().build();
        assertNormalize(localProperties);
        assertNormalizeAndFlatten(localProperties);
    }

    @Test
    public void testNormalizeSingleSmbolGroup()
    {
        List<LocalProperty<String>> localProperties = builder().grouped("a").build();
        assertNormalize(localProperties, Optional.of(grouped("a")));
        assertNormalizeAndFlatten(localProperties, grouped("a"));
    }

    @Test
    public void testNormalizeOverlappingSymbol()
    {
        List<LocalProperty<String>> localProperties = builder()
                .grouped("a")
                .sorted("a", SortOrder.ASC_NULLS_FIRST)
                .constant("a")
                .build();
        assertNormalize(
                localProperties,
                Optional.of(grouped("a")),
                Optional.empty(),
                Optional.empty());
        assertNormalizeAndFlatten(
                localProperties,
                grouped("a"));
    }

    @Test
    public void testNormalizeComplexWithLeadingConstant()
    {
        List<LocalProperty<String>> localProperties = builder()
                .constant("a")
                .grouped("a", "b")
                .grouped("b", "c")
                .sorted("c", SortOrder.ASC_NULLS_FIRST)
                .build();
        assertNormalize(
                localProperties,
                Optional.of(constant("a")),
                Optional.of(grouped("b")),
                Optional.of(grouped("c")),
                Optional.empty());
        assertNormalizeAndFlatten(
                localProperties,
                constant("a"),
                grouped("b"),
                grouped("c"));
    }

    @Test
    public void testNormalizeComplexWithMiddleConstant()
    {
        List<LocalProperty<String>> localProperties = builder()
                .sorted("a", SortOrder.ASC_NULLS_FIRST)
                .grouped("a", "b")
                .grouped("c")
                .constant("a")
                .build();
        assertNormalize(
                localProperties,
                Optional.of(sorted("a", SortOrder.ASC_NULLS_FIRST)),
                Optional.of(grouped("b")),
                Optional.of(grouped("c")),
                Optional.empty());
        assertNormalizeAndFlatten(
                localProperties,
                sorted("a", SortOrder.ASC_NULLS_FIRST),
                grouped("b"),
                grouped("c"));
    }

    @Test
    public void testNormalizeDifferentSorts()
    {
        List<LocalProperty<String>> localProperties = builder()
                .sorted("a", SortOrder.ASC_NULLS_FIRST)
                .sorted("a", SortOrder.DESC_NULLS_LAST)
                .build();
        assertNormalize(
                localProperties,
                Optional.of(sorted("a", SortOrder.ASC_NULLS_FIRST)),
                Optional.empty());
        assertNormalizeAndFlatten(
                localProperties,
                sorted("a", SortOrder.ASC_NULLS_FIRST));
    }

    @Test
    public void testMatchedGroupHierarchy()
    {
        List<LocalProperty<String>> actual = builder()
                .grouped("a")
                .grouped("b")
                .grouped("c")
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d").build(),
                Optional.of(grouped("d")));

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("b").build(),
                Optional.of(grouped("b")));

        assertMatch(
                actual,
                builder().grouped("b", "c").build(),
                Optional.of(grouped("b", "c")));

        assertMatch(
                actual,
                builder().grouped("a", "c").build(),
                Optional.of(grouped("c")));

        assertMatch(
                actual,
                builder().grouped("c").build(),
                Optional.of(grouped("c")));

        assertMatch(
                actual,
                builder()
                        .grouped("a")
                        .grouped("a")
                        .grouped("a")
                        .grouped("a")
                        .grouped("b")
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    @Test
    public void testGroupedTuple()
    {
        List<LocalProperty<String>> actual = builder()
                .grouped("a", "b", "c")
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d").build(),
                Optional.of(grouped("d")));

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.of(grouped("a", "b")));

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.of(grouped("a")));

        assertMatch(
                actual,
                builder().grouped("a").grouped("b").build(),
                Optional.of(grouped("a")),
                Optional.of(grouped("b")));
    }

    @Test
    public void testGroupedDoubleThenSingle()
    {
        List<LocalProperty<String>> actual = builder()
                .grouped("a", "b")
                .grouped("c")
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d").build(),
                Optional.of(grouped("d")));

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "c").build(),
                Optional.of(grouped("a", "c")));

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.of(grouped("a")));

        assertMatch(
                actual,
                builder().grouped("c").build(),
                Optional.of(grouped("c")));
    }

    @Test
    public void testGroupedDoubleThenDouble()
    {
        List<LocalProperty<String>> actual = builder()
                .grouped("a", "b")
                .grouped("c", "a")
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d").build(),
                Optional.of(grouped("d")));

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("b", "c").build(),
                Optional.of(grouped("b", "c")));

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.of(grouped("a")));

        assertMatch(
                actual,
                builder().grouped("c").build(),
                Optional.of(grouped("c")));
    }

    @Test
    public void testSortProperties()
    {
        List<LocalProperty<String>> actual = builder()
                .sorted("a", SortOrder.ASC_NULLS_FIRST)
                .sorted("b", SortOrder.ASC_NULLS_FIRST)
                .sorted("c", SortOrder.ASC_NULLS_FIRST)
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d").build(),
                Optional.of(grouped("d")));

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("b", "c").build(),
                Optional.of(grouped("b", "c")));

        assertMatch(
                actual,
                builder().grouped("b").build(),
                Optional.of(grouped("b")));

        assertMatch(
                actual,
                builder()
                        .sorted("a", SortOrder.ASC_NULLS_FIRST)
                        .sorted("c", SortOrder.ASC_NULLS_FIRST)
                        .build(),
                Optional.empty(),
                Optional.of(sorted("c", SortOrder.ASC_NULLS_FIRST)));
    }

    @Test
    public void testSortGroupSort()
    {
        List<LocalProperty<String>> actual = builder()
                .sorted("a", SortOrder.ASC_NULLS_FIRST)
                .grouped("b", "c")
                .sorted("d", SortOrder.ASC_NULLS_FIRST)
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d", "e").build(),
                Optional.of(grouped("e")));

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.of(grouped("b")));

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("b").build(),
                Optional.of(grouped("b")));

        assertMatch(
                actual,
                builder().grouped("d").build(),
                Optional.of(grouped("d")));

        assertMatch(
                actual,
                builder()
                        .sorted("a", SortOrder.ASC_NULLS_FIRST)
                        .sorted("b", SortOrder.ASC_NULLS_FIRST)
                        .build(),
                Optional.empty(),
                Optional.of(sorted("b", SortOrder.ASC_NULLS_FIRST)));

        assertMatch(
                actual,
                builder()
                        .sorted("a", SortOrder.ASC_NULLS_FIRST)
                        .grouped("b", "c", "d")
                        .grouped("e", "a")
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(grouped("e")));
    }

    @Test
    public void testPartialConstantGroup()
    {
        List<LocalProperty<String>> actual = builder()
                .constant("a")
                .grouped("a", "b")
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.of(grouped("c")));

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("b").build(),
                Optional.empty());
    }

    @Test
    public void testNonoverlappingConstantGroup()
    {
        List<LocalProperty<String>> actual = builder()
                .constant("a")
                .grouped("b")
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.of(grouped("c")));

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder()
                        .grouped("b")
                        .grouped("a")
                        .build(),
                Optional.empty(),
                Optional.empty());
    }

    @Test
    public void testConstantWithMultiGroup()
    {
        List<LocalProperty<String>> actual = builder()
                .constant("a")
                .grouped("a", "b")
                .grouped("a", "c")
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "c", "d").build(),
                Optional.of(grouped("d")));

        assertMatch(
                actual,
                builder().grouped("a", "b", "c").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("a", "c").build(),
                Optional.of(grouped("c")));

        assertMatch(
                actual,
                builder().grouped("b").build(),
                Optional.empty());

        assertMatch(
                actual,
                builder().grouped("b", "c").build(),
                Optional.empty());
    }

    @Test
    public void testConstantWithSort()
    {
        List<LocalProperty<String>> actual = builder()
                .constant("b")
                .sorted("a", SortOrder.ASC_NULLS_FIRST)
                .sorted("b", SortOrder.ASC_NULLS_FIRST)
                .sorted("c", SortOrder.ASC_NULLS_FIRST)
                .build();

        assertMatch(
                actual,
                builder().grouped("a", "b", "d").build(),
                Optional.of(grouped("d")));

        assertMatch(
                actual,
                builder().grouped("a", "c").build(),
                Optional.empty());
    }

    @Test
    public void testMoreRequiredGroupsThanActual()
    {
        List<LocalProperty<String>> actual = builder()
                .constant("b")
                .grouped("a")
                .grouped("d")
                .build();

        assertMatch(
                actual,
                builder()
                        .grouped("a")
                        .grouped("b")
                        .grouped("c")
                        .grouped("d")
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(grouped("c")),
                Optional.of(grouped("d")));
    }

    @Test
    public void testDifferentSortOrders()
    {
        List<LocalProperty<String>> actual = builder()
                .sorted("a", SortOrder.ASC_NULLS_FIRST)
                .build();

        assertMatch(
                actual,
                builder()
                        .sorted("a", SortOrder.ASC_NULLS_LAST)
                        .build(),
                Optional.of(sorted("a", SortOrder.ASC_NULLS_LAST)));
    }

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        ObjectMapper mapper = new ObjectMapperProvider().get()
                .registerModule(new SimpleModule()
                        .addDeserializer(ColumnHandle.class, new JsonDeserializer<>()
                        {
                            @Override
                            public ColumnHandle deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                                    throws IOException
                            {
                                return new ObjectMapperProvider().get().readValue(jsonParser, TestingColumnHandle.class);
                            }
                        }));

        TestingColumnHandle columnHandle = new TestingColumnHandle("a");

        LocalProperty<ColumnHandle> property1 = new ConstantProperty<>(columnHandle);
        assertThat(property1).isEqualTo(mapper.readValue(mapper.writeValueAsString(property1), new TypeReference<LocalProperty<ColumnHandle>>() { }));

        LocalProperty<ColumnHandle> property2 = new SortingProperty<>(columnHandle, SortOrder.ASC_NULLS_FIRST);
        assertThat(property2).isEqualTo(mapper.readValue(mapper.writeValueAsString(property2), new TypeReference<LocalProperty<ColumnHandle>>() { }));

        LocalProperty<ColumnHandle> property3 = new GroupingProperty<>(ImmutableList.of(columnHandle));
        assertThat(property3).isEqualTo(mapper.readValue(mapper.writeValueAsString(property3), new TypeReference<LocalProperty<ColumnHandle>>() { }));
    }

    @SafeVarargs
    private static <T> void assertMatch(List<LocalProperty<T>> actual, List<LocalProperty<T>> wanted, Optional<LocalProperty<T>>... match)
    {
        assertThat(LocalProperties.match(actual, wanted)).isEqualTo(Arrays.asList(match));
    }

    @SafeVarargs
    private static <T> void assertNormalize(List<LocalProperty<T>> localProperties, Optional<LocalProperty<T>>... normalized)
    {
        assertThat(LocalProperties.normalize(localProperties)).isEqualTo(Arrays.asList(normalized));
    }

    @SafeVarargs
    private static <T> void assertNormalizeAndFlatten(List<LocalProperty<T>> localProperties, LocalProperty<T>... normalized)
    {
        assertThat(LocalProperties.normalizeAndPrune(localProperties)).isEqualTo(Arrays.asList(normalized));
    }

    private static ConstantProperty<String> constant(String column)
    {
        return new ConstantProperty<>(column);
    }

    private static GroupingProperty<String> grouped(String... columns)
    {
        return new GroupingProperty<>(Arrays.asList(columns));
    }

    private static SortingProperty<String> sorted(String column, SortOrder order)
    {
        return new SortingProperty<>(column, order);
    }

    private static Builder builder()
    {
        return new Builder();
    }

    private static class Builder
    {
        private final List<LocalProperty<String>> properties = new ArrayList<>();

        public Builder grouped(String... columns)
        {
            properties.add(new GroupingProperty<>(Arrays.asList(columns)));
            return this;
        }

        public Builder sorted(String column, SortOrder order)
        {
            properties.add(new SortingProperty<>(column, order));
            return this;
        }

        public Builder constant(String column)
        {
            properties.add(new ConstantProperty<>(column));
            return this;
        }

        public List<LocalProperty<String>> build()
        {
            return new ArrayList<>(properties);
        }
    }
}

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
package io.trino.operator.dynamicfiltering;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.LongStream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.unmodifiableFuture;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.block.BlockAssertions.createBlockOfReals;
import static io.trino.block.BlockAssertions.createLongDictionaryBlock;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createRepeatedValuesBlock;
import static io.trino.block.BlockAssertions.createSlicesBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.block.BlockAssertions.createTypedLongsBlock;
import static io.trino.operator.dynamicfiltering.DictionaryAwarePageFilter.BlockFilterStats;
import static io.trino.operator.dynamicfiltering.DictionaryAwarePageFilter.SelectedPositionsWithStats;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Domain.onlyNull;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.predicate.Range.greaterThan;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDynamicPageFilter
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final IsolatedBlockFilterFactory BLOCK_FILTER_FACTORY = new IsolatedBlockFilterFactory();

    @Test
    public void testAllPageFilter()
    {
        assertThat(createBlockFilters(TupleDomain.all(), ImmutableMap.of())).isEmpty();
    }

    @Test
    public void testNonePageFilter()
    {
        Optional<List<DynamicPageFilter.BlockFilter>> blockFilters = createBlockFilters(TupleDomain.none(), ImmutableMap.of());
        assertThat(blockFilters).isEqualTo(Optional.of(DynamicPageFilter.NONE_BLOCK_FILTER));
        // NONE_BLOCK_FILTER case is handled outside of DynamicRowFilteringPageSource#filterPage
        // It is verified in TestDynamicRowFilteringPageSource#testNoneDynamicFilter
    }

    @Test
    public void testUnsupportedTypePageFilter()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        Optional<List<DynamicPageFilter.BlockFilter>> blockFilters = createBlockFilters(
                TupleDomain.withColumnDomains(ImmutableMap.of(column, singleValue(VARBINARY, utf8Slice("abc")))),
                ImmutableMap.of(column, 0));
        assertThat(blockFilters).isEmpty();

        blockFilters = createBlockFilters(
                TupleDomain.withColumnDomains(ImmutableMap.of(column, Domain.create(
                        ValueSet.ofRanges(greaterThan(createCharType(10), utf8Slice("abc"))),
                        false))),
                ImmutableMap.of(column, 0));
        assertThat(blockFilters).isEmpty();
    }

    @Test
    public void testSliceBlockFilter()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        Optional<List<DynamicPageFilter.BlockFilter>> blockFilters = createBlockFilters(
                TupleDomain.withColumnDomains(ImmutableMap.of(column, onlyNull(VARCHAR))),
                ImmutableMap.of(column, 0));
        assertThat(blockFilters).isPresent();
        Page page = new Page(
                createStringsBlock("ab", "bc", null, "cd", null),
                createStringsBlock(null, "de", "ef", null, "fg"));
        verifySelectedPositions(filterPage(page, blockFilters.get()).getPositions(), new int[] {2, 4});

        blockFilters = createBlockFilters(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        column,
                        multipleValues(VARCHAR, ImmutableList.of("bc", "cd")))),
                ImmutableMap.of(column, 0));
        assertThat(blockFilters).isPresent();
        verifySelectedPositions(filterPage(page, blockFilters.get()).getPositions(), new int[] {1, 3});

        blockFilters = createBlockFilters(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        column,
                        Domain.create(ValueSet.of(VARCHAR, utf8Slice("ab")), true))),
                ImmutableMap.of(column, 0));
        assertThat(blockFilters).isPresent();
        verifySelectedPositions(filterPage(page, blockFilters.get()).getPositions(), new int[] {0, 2, 4});
    }

    @Test
    public void testLongBlockFilter()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        Optional<List<DynamicPageFilter.BlockFilter>> blockFilters = createBlockFilters(
                TupleDomain.withColumnDomains(ImmutableMap.of(column, onlyNull(INTEGER))),
                ImmutableMap.of(column, 0));
        assertThat(blockFilters).isPresent();
        Page page = new Page(
                createTypedLongsBlock(INTEGER, 1L, 2L, null, 5L, null),
                createTypedLongsBlock(INTEGER, null, 102L, 135L, null, 3L));
        verifySelectedPositions(
                filterPage(page, blockFilters.get()).getPositions(),
                new int[] {2, 4});

        blockFilters = createBlockFilters(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        column,
                        multipleValues(INTEGER, ImmutableList.of(2L, 5L)))),
                ImmutableMap.of(column, 0));
        assertThat(blockFilters).isPresent();
        verifySelectedPositions(
                filterPage(page, blockFilters.get()).getPositions(),
                new int[] {1, 3});

        blockFilters = createBlockFilters(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        column,
                        Domain.create(ValueSet.of(INTEGER, 1L), true))),
                ImmutableMap.of(column, 0));
        assertThat(blockFilters).isPresent();
        verifySelectedPositions(
                filterPage(page, blockFilters.get()).getPositions(),
                new int[] {0, 2, 4});
    }

    @Test
    public void testSelectivePageFilter()
    {
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        Optional<List<DynamicPageFilter.BlockFilter>> blockFilters = createBlockFilters(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(columnB, multipleValues(BIGINT, ImmutableList.of(-10L, 5L, 15L, 135L, 185L, 250L)))),
                ImmutableMap.of(columnB, 1));
        assertThat(blockFilters).isPresent();

        // page without null
        Page page = new Page(createLongSequenceBlock(0, 101), createLongSequenceBlock(100, 201));
        verifySelectedPositions(filterPage(page, blockFilters.get()).getPositions(), new int[] {35, 85});

        // page with null
        page = new Page(
                createLongsBlock(1L, 2L, null, 5L, null),
                createLongsBlock(null, 102L, 135L, null, 3L));
        verifySelectedPositions(filterPage(page, blockFilters.get()).getPositions(), new int[] {2});
    }

    @Test
    public void testNonSelectivePageFilter()
    {
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        List<Long> filterValues = LongStream.range(-5, 205).boxed().collect(toImmutableList());
        Optional<List<DynamicPageFilter.BlockFilter>> blockFilters = createBlockFilters(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(columnB, multipleValues(BIGINT, filterValues))),
                ImmutableMap.of(columnB, 1));
        assertThat(blockFilters).isPresent();

        // page without null
        Page page = new Page(
                createLongSequenceBlock(0, 101),
                createLongSequenceBlock(100, 201));
        verifySelectedPositions(filterPage(page, blockFilters.get()).getPositions(), 101);

        // page with null
        page = new Page(
                createLongsBlock(1L, 2L, null, 5L, null),
                createLongsBlock(null, 102L, 135L, null, 3L));
        verifySelectedPositions(
                filterPage(page, blockFilters.get()).getPositions(),
                new int[] {1, 2, 4});
    }

    @Test
    public void testPageFilterWithNullsAllowed()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        Optional<List<DynamicPageFilter.BlockFilter>> blockFilters = createBlockFilters(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                column,
                                Domain.create(ValueSet.of(INTEGER, 1L, 2L, 3L), true))),
                ImmutableMap.of(column, 0));
        assertThat(blockFilters).isPresent();

        Block blockWithNulls = createTypedLongsBlock(INTEGER, 3L, null, 4L);
        verifySelectedPositions(
                filterPage(new Page(blockWithNulls), blockFilters.get()).getPositions(),
                new int[] {0, 1});

        // select all values in block
        verifySelectedPositions(
                filterPage(new Page(createTypedLongsBlock(INTEGER, 3L, null, 1L)), blockFilters.get()).getPositions(),
                3);

        Block blockWithoutNulls = createTypedLongsBlock(INTEGER, 3L, 4L, 5L);
        verifySelectedPositions(
                filterPage(new Page(blockWithoutNulls), blockFilters.get()).getPositions(),
                new int[] {0});
    }

    @Test
    public void testFilterNullBlockWithPositionsList()
    {
        ColumnHandle columnA = new TestingColumnHandle("columnA");
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        Optional<List<DynamicPageFilter.BlockFilter>> blockFilters = createBlockFilters(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                columnA, Domain.create(ValueSet.of(BIGINT, 1L, 2L, 3L), true),
                                columnB, Domain.create(ValueSet.of(BIGINT, 1L, 2L, 3L), true))),
                ImmutableMap.of(columnA, 0, columnB, 1));
        assertThat(blockFilters).isPresent();

        // block with nulls is second column (positions list instead of range)
        verifySelectedPositions(
                filterPage(
                        new Page(
                                createLongsBlock(3, 1, 5),
                                createLongsBlock(3L, null, 1L)),
                        blockFilters.get()).getPositions(),
                new int[] {0, 1});
    }

    @Test
    public void testPageFilterWithRealNaN()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        Optional<List<DynamicPageFilter.BlockFilter>> blockFilters = createBlockFilters(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                column,
                                // Domain cannot contain floating point NaN
                                multipleValues(REAL, ImmutableList.of((long) floatToRawIntBits(32.0f), (long) floatToRawIntBits(54.6f))))),
                ImmutableMap.of(column, 0));
        assertThat(blockFilters).isPresent();

        verifySelectedPositions(
                filterPage(
                        new Page(createBlockOfReals(42.0f, Float.NaN, 32.0f, null, 53.1f)),
                        blockFilters.get()).getPositions(),
                new int[] {2});
    }

    @Test
    public void testBooleanPageFilter()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        Optional<List<DynamicPageFilter.BlockFilter>> blockFilters = createBlockFilters(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(column, singleValue(BOOLEAN, false))),
                ImmutableMap.of(column, 0));
        assertThat(blockFilters).isEmpty();
    }

    @Test
    public void testRleBlock()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        Optional<List<DynamicPageFilter.BlockFilter>> blockFilters = createBlockFilters(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(column, multipleValues(BIGINT, ImmutableList.of(-10L, 5L, 15L, 135L)))),
                ImmutableMap.of(column, 0));
        assertThat(blockFilters).isPresent();

        Page page = new Page(createRepeatedValuesBlock(15, 50));
        verifySelectedPositions(filterPage(page, blockFilters.get()).getPositions(), 50);

        page = new Page(createRepeatedValuesBlock(10, 50));
        assertThat(filterPage(page, blockFilters.get()).getPositions().isEmpty()).isTrue();

        page = new Page(RunLengthEncodedBlock.create(createLongsBlock((Long) null), 20));
        assertThat(filterPage(page, blockFilters.get()).getPositions().isEmpty()).isTrue();
    }

    @Test
    public void testRleBlockWithNullsAllowed()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        Optional<List<DynamicPageFilter.BlockFilter>> blockFilters = createBlockFilters(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(column, Domain.create(ValueSet.of(BIGINT, 1L, 2L, 3L), true))),
                ImmutableMap.of(column, 0));
        assertThat(blockFilters).isPresent();

        Page page = new Page(RunLengthEncodedBlock.create(createLongsBlock((Long) null), 20));
        verifySelectedPositions(filterPage(page, blockFilters.get()).getPositions(), 20);

        page = new Page(createRepeatedValuesBlock(10, 50));
        assertThat(filterPage(page, blockFilters.get()).getPositions().isEmpty()).isTrue();

        page = new Page(createRepeatedValuesBlock(1, 50));
        verifySelectedPositions(filterPage(page, blockFilters.get()).getPositions(), 50);
    }

    @Test
    public void testMultipleColumnsWithRleBlockFirst()
    {
        ColumnHandle columnA = new TestingColumnHandle("columnA");
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        Optional<List<DynamicPageFilter.BlockFilter>> blockFilters = createBlockFilters(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        columnA, multipleValues(BIGINT, ImmutableList.of(-10L, 5L, 15L, 135L)),
                        columnB, multipleValues(BIGINT, ImmutableList.of(5L, 15L)))),
                ImmutableMap.of(columnA, 0, columnB, 1));
        assertThat(blockFilters).isPresent();

        Page page = new Page(
                RunLengthEncodedBlock.create(createLongsBlock(5), 20),
                createLongSequenceBlock(0, 20));
        verifySelectedPositions(
                filterPage(page, blockFilters.get()).getPositions(),
                new int[] {5, 15});

        page = new Page(
                RunLengthEncodedBlock.create(createLongsBlock(1000), 20),
                createLongSequenceBlock(0, 20));
        assertThat(filterPage(page, blockFilters.get()).getPositions().isEmpty()).isTrue();
    }

    @Test
    public void testMultipleColumnsWithRleBlockLast()
    {
        ColumnHandle columnA = new TestingColumnHandle("columnA");
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        Optional<List<DynamicPageFilter.BlockFilter>> blockFilters = createBlockFilters(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        columnA, multipleValues(BIGINT, ImmutableList.of(5L, 10L, 13L, 15L)),
                        columnB, multipleValues(BIGINT, ImmutableList.of(5L, 15L)))),
                ImmutableMap.of(columnA, 0, columnB, 1));
        assertThat(blockFilters).isPresent();

        Page page = new Page(
                createLongSequenceBlock(0, 20),
                RunLengthEncodedBlock.create(createLongsBlock(5), 20));
        verifySelectedPositions(
                filterPage(page, blockFilters.get()).getPositions(),
                new int[] {5, 10, 13, 15});

        page = new Page(
                createLongSequenceBlock(0, 20),
                RunLengthEncodedBlock.create(createLongsBlock(1000), 20));
        assertThat(filterPage(page, blockFilters.get()).getPositions().isEmpty()).isTrue();
    }

    @Test
    public void testDictionaryBlock()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        Optional<List<DynamicPageFilter.BlockFilter>> blockFiltersOptional = createBlockFilters(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(column, multipleValues(BIGINT, ImmutableList.of(-10L, 5L, 12L, 135L)))),
                ImmutableMap.of(column, 0));
        assertThat(blockFiltersOptional).isPresent();
        DynamicPageFilter.BlockFilter[] blockFilters = blockFiltersOptional.get().toArray(new DynamicPageFilter.BlockFilter[0]);

        DictionaryAwarePageFilter filter = new DictionaryAwarePageFilter(1);
        // DictionaryBlock will contain values from 0-9 repeated 6 times
        Block dictionary = createLongSequenceBlock(0, 20);
        int[] ids = new int[60];
        Arrays.setAll(ids, i -> i % 10);
        Block dictionaryBlock = DictionaryBlock.create(ids.length, dictionary, ids);
        verifySelectedPositions(
                filter.filterPage(new Page(dictionaryBlock), blockFilters, blockFilters.length).getPositions(),
                new int[] {5, 15, 25, 35, 45, 55});
        // Wrap dictionary in lazy block
        LazyBlock lazyBlock = new LazyBlock(ids.length, () -> dictionaryBlock);
        verifySelectedPositions(
                filter.filterPage(new Page(lazyBlock), blockFilters, blockFilters.length).getPositions(),
                new int[] {5, 15, 25, 35, 45, 55});

        // Another DictionaryBlock with same dictionary but different ids
        // Contains values from 5-15 repeated 6 times
        Arrays.setAll(ids, i -> (i % 10) + 5);
        verifySelectedPositions(
                filter.filterPage(new Page(DictionaryBlock.create(ids.length, dictionary, ids)), blockFilters, blockFilters.length).getPositions(),
                new int[] {0, 7, 10, 17, 20, 27, 30, 37, 40, 47, 50, 57});

        // No position is selected
        Page page = new Page(createLongDictionaryBlock(200, 50));
        assertThat(filter.filterPage(page, blockFilters, blockFilters.length).getPositions().isEmpty()).isTrue();

        // All positions are selected
        Arrays.fill(ids, 5);
        verifySelectedPositions(
                filter.filterPage(new Page(DictionaryBlock.create(ids.length, dictionary, ids)), blockFilters, blockFilters.length).getPositions(),
                ids.length);
    }

    @Test
    public void testDictionaryBlockWithNulls()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        Optional<List<DynamicPageFilter.BlockFilter>> blockFiltersOptional = createBlockFilters(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(column, multipleValues(VARCHAR, ImmutableList.of("bc", "cd")))),
                ImmutableMap.of(column, 0));
        assertThat(blockFiltersOptional).isPresent();
        DynamicPageFilter.BlockFilter[] blockFilters = blockFiltersOptional.get().toArray(new DynamicPageFilter.BlockFilter[0]);

        DictionaryAwarePageFilter filter = new DictionaryAwarePageFilter(1);
        // DictionaryBlock will contain below values repeated 10 times
        Block dictionary = createSlicesBlock(
                utf8Slice("ab"),
                utf8Slice("bc"),
                null,
                utf8Slice("cc"));
        int[] ids = new int[20];
        Arrays.setAll(ids, i -> i % 4);
        Block dictionaryBlock = DictionaryBlock.create(ids.length, dictionary, ids);
        verifySelectedPositions(
                filter.filterPage(new Page(dictionaryBlock), blockFilters, blockFilters.length).getPositions(),
                new int[] {1, 5, 9, 13, 17});

        // All nulls
        ids = new int[20];
        Arrays.fill(ids, 2);
        Block allNulls = DictionaryBlock.create(ids.length, dictionary, ids);
        assertThat(
                filter.filterPage(new Page(allNulls), blockFilters, blockFilters.length).getPositions().isEmpty())
                .isTrue();

        filter = new DictionaryAwarePageFilter(1);
        blockFiltersOptional = createBlockFilters(
                TupleDomain.withColumnDomains(ImmutableMap.of(column, Domain.create(
                        ValueSet.of(VARCHAR, utf8Slice("bc"), utf8Slice("cd")),
                        true))),
                ImmutableMap.of(column, 0));
        blockFilters = blockFiltersOptional.orElseThrow().toArray(new DynamicPageFilter.BlockFilter[0]);
        verifySelectedPositions(
                filter.filterPage(new Page(dictionaryBlock), blockFilters, blockFilters.length).getPositions(),
                new int[] {1, 2, 5, 6, 9, 10, 13, 14, 17, 18});

        verifySelectedPositions(
                filter.filterPage(new Page(allNulls), blockFilters, blockFilters.length).getPositions(),
                allNulls.getPositionCount());
    }

    @Test
    public void testDictionaryProcessingEnableDisable()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        Optional<List<DynamicPageFilter.BlockFilter>> blockFiltersOptional = createBlockFilters(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(column, multipleValues(BIGINT, ImmutableList.of(-10L, 4L, 12L)))),
                ImmutableMap.of(column, 0));
        assertThat(blockFiltersOptional).isPresent();
        DynamicPageFilter.BlockFilter blockFilter = blockFiltersOptional.get().toArray(new DynamicPageFilter.BlockFilter[0])[0];

        SelectedPositions allPositions = positionsRange(0, 10);
        DictionaryAwareBlockFilter dictionaryAwareFilter = new DictionaryAwareBlockFilter();
        // DictionaryBlock will contain 10 values from 0-4
        Block dictionary = createLongSequenceBlock(0, 30);
        int[] ids = new int[allPositions.size()];
        Arrays.setAll(ids, i -> i % 5);
        // Dictionary is bigger than the block
        Block dictionaryBlock = DictionaryBlock.create(ids.length, dictionary, ids);
        // Dictionary is always processed for first block
        verifySelectedPositions(
                dictionaryAwareFilter.filter(dictionaryBlock, blockFilter, allPositions),
                new int[] {4, 9});
        assertThat(dictionaryAwareFilter.wasLastBlockDictionaryProcessed()).isTrue();

        // Dictionary is smaller than the block
        dictionary = createLongSequenceBlock(0, 5);
        dictionaryBlock = DictionaryBlock.create(ids.length, dictionary, ids);
        verifySelectedPositions(
                dictionaryAwareFilter.filter(dictionaryBlock, blockFilter, allPositions),
                new int[] {4, 9});
        assertThat(dictionaryAwareFilter.wasLastBlockDictionaryProcessed()).isTrue();

        // New dictionary is bigger than block but last dictionary was well utilized
        dictionary = createLongSequenceBlock(0, 15);
        dictionaryBlock = DictionaryBlock.create(ids.length, dictionary, ids);
        verifySelectedPositions(
                dictionaryAwareFilter.filter(dictionaryBlock, blockFilter, allPositions),
                new int[] {4, 9});
        assertThat(dictionaryAwareFilter.wasLastBlockDictionaryProcessed()).isTrue();

        // New dictionary is bigger than block and last dictionary was not well utilized
        dictionary = createLongSequenceBlock(0, 15);
        dictionaryBlock = DictionaryBlock.create(ids.length, dictionary, ids);
        verifySelectedPositions(
                dictionaryAwareFilter.filter(dictionaryBlock, blockFilter, allPositions),
                new int[] {4, 9});
        assertThat(dictionaryAwareFilter.wasLastBlockDictionaryProcessed()).isFalse();

        // Get usage count up
        verifySelectedPositions(
                dictionaryAwareFilter.filter(dictionaryBlock, blockFilter, allPositions),
                new int[] {4, 9});
        assertThat(dictionaryAwareFilter.wasLastBlockDictionaryProcessed()).isFalse();
        // New dictionary is bigger than block but last dictionary was well utilized
        dictionary = createLongSequenceBlock(0, 20);
        dictionaryBlock = DictionaryBlock.create(ids.length, dictionary, ids);
        verifySelectedPositions(
                dictionaryAwareFilter.filter(dictionaryBlock, blockFilter, allPositions),
                new int[] {4, 9});
        assertThat(dictionaryAwareFilter.wasLastBlockDictionaryProcessed()).isTrue();
    }

    @Test
    public void testMultipleDictionaryBlocks()
    {
        ColumnHandle columnA = new TestingColumnHandle("columnA");
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        Optional<List<DynamicPageFilter.BlockFilter>> blockFiltersOptional = createBlockFilters(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                columnA, multipleValues(BIGINT, LongStream.range(3, 8).boxed().collect(toImmutableList())),
                                columnB, singleValue(BIGINT, 5L))),
                ImmutableMap.of(columnA, 0, columnB, 1));
        assertThat(blockFiltersOptional).isPresent();
        DynamicPageFilter.BlockFilter[] blockFilters = blockFiltersOptional.get().toArray(new DynamicPageFilter.BlockFilter[0]);

        DictionaryAwarePageFilter filter = new DictionaryAwarePageFilter(2);
        Page page = new Page(
                createLongDictionaryBlock(0, 50),
                createLongDictionaryBlock(0, 50));
        verifySelectedPositions(
                filter.filterPage(page, blockFilters, blockFilters.length).getPositions(),
                new int[] {5, 15, 25, 35, 45});

        page = new Page(
                createLongDictionaryBlock(0, 50),
                createLongDictionaryBlock(0, 50));
        verifySelectedPositions(
                filter.filterPage(page, blockFilters, blockFilters.length).getPositions(),
                new int[] {5, 15, 25, 35, 45});
    }

    @Test
    public void testMultipleColumnsOverlap()
    {
        ColumnHandle columnA = new TestingColumnHandle("columnA");
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        ColumnHandle columnC = new TestingColumnHandle("columnC");
        Optional<List<DynamicPageFilter.BlockFilter>> blockFilters = createBlockFilters(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                columnA, multipleValues(BIGINT, ImmutableList.of(-10L, 5L, 15L, 35L, 50L, 85L, 95L, 105L)),
                                columnB, multipleValues(BIGINT, LongStream.range(100, 200).boxed().collect(toImmutableList())),
                                columnC, multipleValues(BIGINT, LongStream.range(150, 250).boxed().collect(toImmutableList())))),
                ImmutableMap.of(columnA, 0, columnB, 1, columnC, 2));
        assertThat(blockFilters).isPresent();

        Page page = new Page(
                createLongSequenceBlock(0, 101),
                createLongSequenceBlock(100, 201),
                createLongSequenceBlock(200, 301));
        verifySelectedPositions(
                filterPage(page, blockFilters.get()).getPositions(),
                new int[] {5, 15, 35});
    }

    @Test
    public void testMultipleColumnsShortCircuit()
    {
        ColumnHandle columnA = new TestingColumnHandle("columnA");
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        ColumnHandle columnC = new TestingColumnHandle("columnC");
        Optional<List<DynamicPageFilter.BlockFilter>> blockFilters = createBlockFilters(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                columnA, multipleValues(BIGINT, ImmutableList.of(-10L, 5L, 15L, 35L, 50L, 85L, 95L, 105L)),
                                columnB, singleValue(BIGINT, 0L),
                                columnC, multipleValues(BIGINT, LongStream.range(150, 250).boxed().collect(toImmutableList())))),
                ImmutableMap.of(columnA, 0, columnB, 1, columnC, 2));
        assertThat(blockFilters).isPresent();

        Page page = new Page(
                createLongSequenceBlock(0, 101),
                createLongSequenceBlock(100, 201),
                createLongSequenceBlock(200, 301));
        SelectedPositionsWithStats positionsWithStats = filterPage(page, blockFilters.get());
        assertThat(positionsWithStats.getPositions()).isEqualTo(DynamicPageFilter.BlockFilter.EMPTY);
        assertThat(positionsWithStats.getBlockFilterStats()).isEqualTo(ImmutableList.of(
                new BlockFilterStats(101, 6, 0),
                new BlockFilterStats(6, 0, 1)));
    }

    @Test
    public void testDynamicFilterUpdates()
    {
        ColumnHandle columnA = new TestingColumnHandle("columnA");
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        ColumnHandle columnC = new TestingColumnHandle("columnC");
        TestingDynamicFilter dynamicFilter = new TestingDynamicFilter(4);
        DynamicPageFilter pageFilter = new DynamicPageFilter(
                dynamicFilter,
                ImmutableMap.of(columnA, 0, columnB, 1, columnC, 2),
                TYPE_OPERATORS,
                BLOCK_FILTER_FACTORY,
                directExecutor());
        assertThat(pageFilter.getBlockFilters()).isEmpty();

        dynamicFilter.update(TupleDomain.withColumnDomains(
                ImmutableMap.of(columnB, multipleValues(BIGINT, ImmutableList.of(131L, 142L)))));
        Optional<List<DynamicPageFilter.BlockFilter>> blockFilters = pageFilter.getBlockFilters();
        assertThat(blockFilters).isPresent();
        Page page = new Page(
                createLongSequenceBlock(0, 101),
                createLongSequenceBlock(100, 201),
                createLongSequenceBlock(200, 301));
        verifySelectedPositions(
                filterPage(page, blockFilters.get()).getPositions(),
                new int[] {31, 42});

        dynamicFilter.update(TupleDomain.all());
        assertThat(pageFilter.getBlockFilters()).isEqualTo(blockFilters);

        dynamicFilter.update(TupleDomain.withColumnDomains(
                ImmutableMap.of(columnC, singleValue(BIGINT, 231L))));
        assertThat(pageFilter.getBlockFilters()).isNotEqualTo(blockFilters);
        blockFilters = pageFilter.getBlockFilters();
        assertThat(blockFilters).isPresent();
        verifySelectedPositions(filterPage(page, blockFilters.get()).getPositions(), new int[] {31});

        dynamicFilter.update(TupleDomain.all());
        assertThat(pageFilter.getBlockFilters().get()).isEqualTo(blockFilters.get());
    }

    private static Optional<List<DynamicPageFilter.BlockFilter>> createBlockFilters(
            TupleDomain<ColumnHandle> tupleDomain,
            Map<ColumnHandle, Integer> channels)
    {
        return DynamicPageFilter.createPageFilter(tupleDomain, channels, TYPE_OPERATORS, BLOCK_FILTER_FACTORY);
    }

    private static SelectedPositionsWithStats filterPage(Page page, List<DynamicPageFilter.BlockFilter> blockFilters)
    {
        DynamicPageFilter.BlockFilter[] effectiveBlockFilters = blockFilters.toArray(new DynamicPageFilter.BlockFilter[0]);
        return filterPage(page, effectiveBlockFilters);
    }

    private static SelectedPositionsWithStats filterPage(Page page, DynamicPageFilter.BlockFilter[] blockFilters)
    {
        DictionaryAwarePageFilter filter = new DictionaryAwarePageFilter(page.getChannelCount());
        return filter.filterPage(page, blockFilters, blockFilters.length);
    }

    private static void verifySelectedPositions(SelectedPositions selectedPositions, int[] positions)
    {
        assertThat(selectedPositions.isList()).isTrue();
        assertThat(selectedPositions.getOffset()).isEqualTo(0);
        assertThat(selectedPositions.size()).isEqualTo(positions.length);
        assertThat(Arrays.copyOf(selectedPositions.getPositions(), positions.length)).isEqualTo(positions);
    }

    private static void verifySelectedPositions(SelectedPositions selectedPositions, int rangeSize)
    {
        assertThat(selectedPositions.isList()).isFalse();
        assertThat(selectedPositions.getOffset()).isEqualTo(0);
        assertThat(selectedPositions.size()).isEqualTo(rangeSize);
    }

    private static class TestingDynamicFilter
            implements DynamicFilter
    {
        private CompletableFuture<?> isBlocked;
        private TupleDomain<ColumnHandle> currentPredicate;
        private int futuresLeft;

        private TestingDynamicFilter(int expectedFilters)
        {
            this.futuresLeft = expectedFilters;
            this.isBlocked = expectedFilters == 0 ? NOT_BLOCKED : new CompletableFuture<>();
            this.currentPredicate = TupleDomain.all();
        }

        public void update(TupleDomain<ColumnHandle> predicate)
        {
            futuresLeft -= 1;
            verify(futuresLeft >= 0);
            currentPredicate = currentPredicate.intersect(predicate);
            CompletableFuture<?> currentFuture = isBlocked;
            // create next blocking future (if needed)
            isBlocked = isComplete() ? NOT_BLOCKED : new CompletableFuture<>();
            verify(currentFuture.complete(null));
        }

        @Override
        public Set<ColumnHandle> getColumnsCovered()
        {
            return currentPredicate.getDomains().orElseThrow().keySet();
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return unmodifiableFuture(isBlocked);
        }

        @Override
        public boolean isComplete()
        {
            return futuresLeft == 0;
        }

        @Override
        public boolean isAwaitable()
        {
            return futuresLeft > 0;
        }

        @Override
        public TupleDomain<ColumnHandle> getCurrentPredicate()
        {
            return currentPredicate;
        }

        @Override
        public OptionalLong getPreferredDynamicFilterTimeout()
        {
            return OptionalLong.of(0L);
        }
    }
}

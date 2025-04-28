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
package io.trino.plugin.hudi.query.index;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.plugin.hudi.query.index.HudiRecordLevelIndexSupport.extractPredicatesForColumns;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class HudiRecordLevelIndexSupportTest
{
    /**
     * Example unit test case on how to initialize domains and run tests.
     */
    @Test
    public void testExtractPredicatesForColumns()
    {
        // Define column symbols
        TestingColumnHandle idColumn = new TestingColumnHandle("id");
        TestingColumnHandle nameColumn = new TestingColumnHandle("name");
        TestingColumnHandle valueColumn = new TestingColumnHandle("value");
        TestingColumnHandle timestampColumn = new TestingColumnHandle("timestamp");

        // Create domains for columns
        Domain idDomain = singleValue(BIGINT, 42L);
        Domain nameDomain = singleValue(VARCHAR, Slices.utf8Slice("test"));
        Domain valueDomain = Domain.create(
                ValueSet.ofRanges(range(BIGINT, 10L, true, 20L, false)),
                false);
        Domain timestampDomain = singleValue(BIGINT, 1715882800000L);

        // Build TupleDomain with all columns
        Map<TestingColumnHandle, Domain> domains = new HashMap<>();
        domains.put(idColumn, idDomain);
        domains.put(nameColumn, nameDomain);
        domains.put(valueColumn, valueDomain);
        domains.put(timestampColumn, timestampDomain);
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(domains).transformKeys(TestingColumnHandle::getName);

        // Define the columns we want to extract
        List<String> columnsToExtract = List.of(idColumn.getName(), nameColumn.getName());

        // Extract predicates
        TupleDomain<String> extractedDomain = extractPredicatesForColumns(tupleDomain, columnsToExtract);

        // Verify the result
        Map<String, Domain> extractedDomains = extractedDomain.getDomains().get();
        assertThat(extractedDomains).hasSize(2);
        assertThat(extractedDomains).containsKey(idColumn.getName());
        assertThat(extractedDomains).containsKey(nameColumn.getName());
        assertThat(extractedDomains).doesNotContainKey(valueColumn.getName());
        assertThat(extractedDomains).doesNotContainKey(timestampColumn.getName());

        assertThat(extractedDomains.get(idColumn.getName())).isEqualTo(idDomain);
        assertThat(extractedDomains.get(nameColumn.getName())).isEqualTo(nameDomain);
    }

    @Test
    public void testExtractPredicatesForColumns_None()
    {
        // When TupleDomain is None
        TupleDomain<String> tupleDomain = TupleDomain.none();
        List<String> columns = List.of();

        TupleDomain<String> result = HudiRecordLevelIndexSupport.extractPredicatesForColumns(tupleDomain, columns);

        assertThat(result.isNone()).isTrue();
    }

    @Test
    public void testExtractPredicatesForColumns_All()
    {
        // When TupleDomain is All
        TupleDomain<String> tupleDomain = TupleDomain.all();
        List<String> columns = List.of("col1");

        TupleDomain<String> result = HudiRecordLevelIndexSupport.extractPredicatesForColumns(tupleDomain, columns);

        assertThat(result.isAll()).isTrue();
    }

    @Test
    public void testExtractPredicatesForColumns_WithInClause()
    {
        // Setup columns
        TestingColumnHandle col1 = new TestingColumnHandle("col1");
        TestingColumnHandle col2 = new TestingColumnHandle("col2");

        // Create a TupleDomain with col1 having multiple values (IN clause)
        Map<TestingColumnHandle, Domain> domains = new HashMap<>();
        domains.put(col1, createMultiValueDomain(List.of("value1", "value2", "value3"))); // IN ('value1', 'value2', 'value3')
        domains.put(col2, createStringDomain("value4")); // = 'value4'
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(domains).transformKeys(TestingColumnHandle::getName);

        // Request predicates for col1 only
        List<String> columns = List.of(col1.getName());

        // Should return TupleDomain with only col1 containing all its values
        TupleDomain<String> result = HudiRecordLevelIndexSupport.extractPredicatesForColumns(tupleDomain, columns);

        assertThat(result.isNone()).isFalse();
        assertThat(result.isAll()).isFalse();

        Map<String, Domain> resultDomains = result.getDomains().get();
        assertThat(resultDomains).hasSize(1);
        assertThat(resultDomains).containsKey(col1.getName());
        assertThat(resultDomains).doesNotContainKey(col2.getName());

        List<String> values = getMultiValue(resultDomains.get(col1.getName()));
        assertThat(values).hasSize(3);
        assertThat(values).contains("value1", "value2", "value3");
    }

    @Test
    public void testExtractPredicatesForColumns_MixedPredicates()
    {
        // Setup columns
        TestingColumnHandle col1 = new TestingColumnHandle("col1"); // Simple equality
        TestingColumnHandle col2 = new TestingColumnHandle("col2"); // IN clause
        TestingColumnHandle col3 = new TestingColumnHandle("col3"); // Range predicate
        TestingColumnHandle col4 = new TestingColumnHandle("col4"); // Not part of record key set

        // Create domains with different predicate types
        Map<TestingColumnHandle, Domain> domains = new HashMap<>();
        domains.put(col1, createStringDomain("value1"));
        domains.put(col2, createMultiValueDomain(List.of("a", "b", "c")));

        // Create a simple range domain simulation from 10 (inclusive) to 50 (exclusive)
        Range range = Range.range(IntegerType.INTEGER, 10L, true, 50L, false);
        ValueSet valueSet = ValueSet.ofRanges(range);
        Domain rangeDomain = Domain.create(valueSet, false);
        domains.put(col3, rangeDomain);

        domains.put(col4, createStringDomain("value4"));

        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(domains).transformKeys(TestingColumnHandle::getName);

        // Request predicates for col1, col2, and col3
        List<String> columns = List.of(col1.getName(), col2.getName(), col3.getName());

        TupleDomain<String> result = HudiRecordLevelIndexSupport.extractPredicatesForColumns(tupleDomain, columns);

        Map<String, Domain> resultDomains = result.getDomains().get();
        assertThat(resultDomains).hasSize(3);

        // Check col1 (equality)
        List<String> valuesCol1 = getMultiValue(resultDomains.get(col1.getName()));
        assertThat(resultDomains).containsKey(col1.getName());
        assertThat(valuesCol1).hasSize(1);
        assertThat(valuesCol1.getFirst()).isEqualTo("value1");

        // Check col2 (IN clause)
        List<String> valuesCol2 = getMultiValue(resultDomains.get(col2.getName()));
        assertThat(resultDomains).containsKey(col2.getName());
        assertThat(valuesCol2).hasSize(3);
        assertThat(valuesCol2).containsAll(Arrays.asList("a", "b", "c"));

        // Check col3 (range)
        assertThat(resultDomains).containsKey(col3.getName());
        assertThat(resultDomains.get(col3.getName()).getValues().getRanges().getSpan().getLowValue().get()).isEqualTo(10L);
        assertThat(resultDomains.get(col3.getName()).getValues().getRanges().getSpan().getHighValue().get()).isEqualTo(50L);

        // Check col4 (not requested)
        assertThat(resultDomains).doesNotContainKey(col4.getName());
    }

    @Test
    public void testExtractPredicatesForColumns_NoMatchingColumns()
    {
        // Setup columns that don't match
        TestingColumnHandle col1 = new TestingColumnHandle("col1");
        TestingColumnHandle col2 = new TestingColumnHandle("col2");
        TestingColumnHandle col3 = new TestingColumnHandle("col3");

        // Create a TupleDomain with col1 and col2
        Map<TestingColumnHandle, Domain> domains = new HashMap<>();
        domains.put(col1, createStringDomain("value1"));
        domains.put(col2, createStringDomain("value2"));
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(domains).transformKeys(TestingColumnHandle::getName);

        // Request predicates for col3 only
        List<String> columns = List.of(col3.getName());

        // Since no matching columns, should return TupleDomain.all()
        TupleDomain<String> result = HudiRecordLevelIndexSupport.extractPredicatesForColumns(tupleDomain, columns);

        // domains.isPresent() && domains.get().isEmpty()
        assertThat(result.isAll()).isTrue();
    }

    @Test
    public void testExtractPredicatesForColumns_PartialMatchingColumns()
    {
        // Setup columns
        TestingColumnHandle col1 = new TestingColumnHandle("col1");
        TestingColumnHandle col2 = new TestingColumnHandle("col2");
        TestingColumnHandle col3 = new TestingColumnHandle("col3");

        // Create a TupleDomain with col1, col2, and col3
        Map<TestingColumnHandle, Domain> domains = new HashMap<>();
        domains.put(col1, createStringDomain("value1"));
        domains.put(col2, createStringDomain("value2"));
        domains.put(col3, createStringDomain("value3"));
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(domains).transformKeys(TestingColumnHandle::getName);

        // Request predicates for col1 and col3 only
        List<String> columns = List.of(col1.getName(), col3.getName());

        // Should return TupleDomain with only col1 and col3
        TupleDomain<String> result = HudiRecordLevelIndexSupport.extractPredicatesForColumns(tupleDomain, columns);

        assertThat(result.isNone()).isFalse();
        assertThat(result.isAll()).isFalse();

        Map<String, Domain> resultDomains = result.getDomains().get();
        assertThat(resultDomains).hasSize(2);
        assertThat(resultDomains).containsKey(col1.getName());
        assertThat(resultDomains).containsKey(col3.getName());
        assertThat(resultDomains).doesNotContainKey(col2.getName());

        assertThat(getSingleValue(resultDomains.get(col1.getName()))).isEqualTo("value1");
        assertThat(getSingleValue(resultDomains.get(col3.getName()))).isEqualTo("value3");
    }

    @Test
    public void testExtractPredicatesForColumns_AllMatchingColumns()
    {
        // Setup columns
        TestingColumnHandle col1 = new TestingColumnHandle("col1");
        TestingColumnHandle col2 = new TestingColumnHandle("col2");

        // Create a TupleDomain with col1 and col2
        Map<TestingColumnHandle, Domain> domains = new HashMap<>();
        domains.put(col1, createStringDomain("value1"));
        domains.put(col2, createStringDomain("value2"));
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(domains).transformKeys(TestingColumnHandle::getName);

        // Request predicates for all columns
        List<String> columns = List.of(col1.getName(), col2.getName());

        // Should return the original TupleDomain
        TupleDomain<String> result = HudiRecordLevelIndexSupport.extractPredicatesForColumns(tupleDomain, columns);

        assertThat(result.isNone()).isFalse();
        assertThat(result.isAll()).isFalse();

        Map<String, Domain> resultDomains = result.getDomains().get();
        assertThat(resultDomains).hasSize(2);
        assertThat(resultDomains).containsKey(col1.getName());
        assertThat(resultDomains).containsKey(col2.getName());

        assertThat(getSingleValue(resultDomains.get(col1.getName()))).isEqualTo("value1");
        assertThat(getSingleValue(resultDomains.get(col2.getName()))).isEqualTo("value2");
    }

    @Test
    public void testConstructRecordKeys_WithInClause()
    {
        // Domain with multiple values for a key simulating an IN clause
        Map<String, Domain> domains = new HashMap<>();
        domains.put("key1", createMultiValueDomain(List.of("value1", "value2", "value3")));
        TupleDomain<String> recordKeyDomains = TupleDomain.withColumnDomains(domains);
        List<String> recordKeys = List.of("key1");

        List<String> result = HudiRecordLevelIndexSupport.constructRecordKeys(recordKeyDomains, recordKeys);

        // The code should take the first value from the IN list for building the key
        assertThat(result).hasSize(3);
        assertThat(result).isEqualTo(List.of("value1", "value2", "value3"));
    }

    @Test
    public void testConstructRecordKeys_ComplexKeyWithInClause()
    {
        Map<String, Domain> domains = new HashMap<>();
        // Domain with multiple values for the first key
        domains.put("part1", createMultiValueDomain(List.of("val1a", "val1b", "val1c")));
        // Regular single-value domain for second key
        domains.put("part2", createStringDomain("value2"));

        TupleDomain<String> recordKeyDomains = TupleDomain.withColumnDomains(domains);
        List<String> recordKeys = List.of("part1", "part2");

        List<String> result = HudiRecordLevelIndexSupport.constructRecordKeys(recordKeyDomains, recordKeys);

        assertThat(result).hasSize(3);
        // Expecting the first value of the IN clause to be used
        assertThat(result.get(0)).isEqualTo("part1:val1a,part2:value2");
    }

    @Test
    public void testConstructRecordKeys_MultipleKeysWithMultipleValues()
    {
        Map<String, Domain> domains = new HashMap<>();
        // Multiple IN clauses
        domains.put("part1", createMultiValueDomain(List.of("val1a", "val1b", "val1c")));
        domains.put("part2", createMultiValueDomain(List.of("val2a", "val2b")));

        TupleDomain<String> recordKeyDomains = TupleDomain.withColumnDomains(domains);
        List<String> recordKeys = List.of("part1", "part2");

        List<String> result = HudiRecordLevelIndexSupport.constructRecordKeys(recordKeyDomains, recordKeys);

        // Verify only the first value from each IN clause is used
        assertThat(result).hasSize(6);
        assertThat(result).isEqualTo(
                List.of("part1:val1a,part2:val2a", "part1:val1a,part2:val2b",
                        "part1:val1b,part2:val2a", "part1:val1b,part2:val2b",
                        "part1:val1c,part2:val2a", "part1:val1c,part2:val2b"));
    }

    @Test
    public void testConstructRecordKeys_MultipleKeysWithRange()
    {
        Map<String, Domain> domains = new HashMap<>();
        // Multiple IN clauses
        domains.put("part1", createMultiValueDomain(List.of("val1a", "val1b", "val1c")));
        domains.put("part2", createMultiValueDomain(List.of("val2a", "val2b")));

        Range range = Range.range(IntegerType.INTEGER, 10L, true, 50L, false);
        ValueSet valueSet = ValueSet.ofRanges(range);
        Domain rangeDomain = Domain.create(valueSet, false);
        domains.put("part3", rangeDomain);

        TupleDomain<String> recordKeyDomains = TupleDomain.withColumnDomains(domains);
        List<String> recordKeys = List.of("part1", "part2", "part3");

        List<String> result = HudiRecordLevelIndexSupport.constructRecordKeys(recordKeyDomains, recordKeys);

        // Can only handle IN and EQUAL cases
        assertThat(result).isEmpty();
    }

    @Test
    public void testConstructRecordKeys_NullRecordKeys()
    {
        TupleDomain<String> recordKeyDomains = createStringTupleDomain(Map.of("key1", "value1"));
        List<String> recordKeys = null;

        List<String> result = HudiRecordLevelIndexSupport.constructRecordKeys(recordKeyDomains, recordKeys);

        assertThat(result).isEmpty();
    }

    @Test
    public void testConstructRecordKeys_EmptyRecordKeys()
    {
        TupleDomain<String> recordKeyDomains = createStringTupleDomain(Map.of("key1", "value1"));
        List<String> recordKeys = Collections.emptyList();

        List<String> result = HudiRecordLevelIndexSupport.constructRecordKeys(recordKeyDomains, recordKeys);

        assertThat(result).isEmpty();
    }

    @Test
    public void testConstructRecordKeys_EmptyDomains()
    {
        TupleDomain<String> recordKeyDomains = TupleDomain.withColumnDomains(Collections.emptyMap());
        List<String> recordKeys = List.of("key1");

        List<String> result = HudiRecordLevelIndexSupport.constructRecordKeys(recordKeyDomains, recordKeys);

        assertThat(result).isEmpty();
    }

    @Test
    public void testConstructRecordKeys_MissingDomainForKey()
    {
        TupleDomain<String> recordKeyDomains = createStringTupleDomain(Map.of("key1", "value1"));
        List<String> recordKeys = List.of("key2"); // Key not in domains

        List<String> result = HudiRecordLevelIndexSupport.constructRecordKeys(recordKeyDomains, recordKeys);

        assertThat(result).isEmpty();
    }

    @Test
    public void testConstructRecordKeys_SingleKey()
    {
        TupleDomain<String> recordKeyDomains = createStringTupleDomain(Map.of("key1", "value1"));
        List<String> recordKeys = List.of("key1");

        List<String> result = HudiRecordLevelIndexSupport.constructRecordKeys(recordKeyDomains, recordKeys);

        assertThat(result).hasSize(1);
        assertThat(result.getFirst()).isEqualTo("value1");
    }

    @Test
    public void testConstructRecordKeys_ComplexKey()
    {
        Map<String, String> keyValues = new HashMap<>();
        keyValues.put("part1", "value1");
        keyValues.put("part2", "value2");
        keyValues.put("part3", "value3");

        TupleDomain<String> recordKeyDomains = createStringTupleDomain(keyValues);
        List<String> recordKeys = List.of("part1", "part2", "part3");

        List<String> result = HudiRecordLevelIndexSupport.constructRecordKeys(recordKeyDomains, recordKeys);

        assertThat(result).hasSize(1);
        assertThat(result.getFirst()).isEqualTo("part1:value1,part2:value2,part3:value3");
    }

    @Test
    public void testConstructRecordKeys_ComplexKeyWithMissingPart()
    {
        Map<String, String> keyValues = new HashMap<>();
        keyValues.put("part1", "value1");
        // part2 is missing
        keyValues.put("part3", "value3");

        TupleDomain<String> recordKeyDomains = createStringTupleDomain(keyValues);
        List<String> recordKeys = List.of("part1", "part2", "part3");

        List<String> result = HudiRecordLevelIndexSupport.constructRecordKeys(recordKeyDomains, recordKeys);

        // Since one key is missing, should return empty list
        assertThat(result).isEmpty();
    }

    // Helper methods for test data creation
    private Domain createStringDomain(String value)
    {
        return Domain.singleValue(VARCHAR, Slices.utf8Slice(value));
    }

    private Domain createMultiValueDomain(List<String> values)
    {
        return Domain.multipleValues(VARCHAR, values.stream().map(Slices::utf8Slice).collect(Collectors.toList()));
    }

    private TupleDomain<String> createStringTupleDomain(Map<String, String> keyValues)
    {
        Map<String, Domain> domains = keyValues.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> createStringDomain(entry.getValue())));
        return TupleDomain.withColumnDomains(domains);
    }

    private String getSingleValue(Domain domain)
    {
        return ((Slice) domain.getSingleValue()).toStringUtf8();
    }

    private List<String> getMultiValue(Domain domain)
    {
        return domain.getNullableDiscreteSet().getNonNullValues().stream()
                .map(x -> ((Slice) x)
                        .toStringUtf8()).toList();
    }
}

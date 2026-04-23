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
package io.trino.plugin.hudi.util;

import io.airlift.slice.Slices;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class TestTupleDomainUtilsTest
{
    @Test
    void testGetReferencedColumnsEmpty()
    {
        // Test with an empty TupleDomain
        TupleDomain<String> emptyTupleDomain = TupleDomain.none();
        assertThat(TupleDomainUtils.getReferencedColumns(emptyTupleDomain)).isEmpty();

        // Test with a TupleDomain that effectively has no column domains
        TupleDomain<String> effectivelyEmptyTupleDomain = TupleDomain.all();
        assertThat(TupleDomainUtils.getReferencedColumns(effectivelyEmptyTupleDomain)).isEmpty();
    }

    @Test
    void testGetReferencedColumnsNonEmpty()
    {
        // Test with a TupleDomain containing domains for specific columns
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of(
                "col_a", Domain.singleValue(BIGINT, 1L),
                "col_b", Domain.onlyNull(VARCHAR)));
        assertThat(TupleDomainUtils.getReferencedColumns(tupleDomain))
                .containsExactlyInAnyOrder("col_a", "col_b");
    }

    @Test
    void testAreAllFieldsReferencedAllMatch()
    {
        // Test when all source fields are present in the TupleDomain
        // 'all' counts as referenced and when passed into the builder, it will be ignored
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of(
                "id", Domain.singleValue(BIGINT, 100L),
                "name", Domain.singleValue(VARCHAR, Slices.utf8Slice("test")),
                "value", Domain.all(BIGINT)));
        List<String> sourceFields = List.of("id", "name");
        assertThat(TupleDomainUtils.areAllFieldsReferenced(tupleDomain, sourceFields)).isTrue();

        // The constructed TupleDomain will hence not have any constraint on the domain with 'all'
        List<String> sourceFieldsIncludingAll = List.of("id", "name", "value");
        assertThat(TupleDomainUtils.areAllFieldsReferenced(tupleDomain, sourceFieldsIncludingAll)).isFalse();
    }

    @Test
    void testAreAllFieldsReferencedSomeMatch()
    {
        // Test when only some source fields are present
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of(
                "id", Domain.singleValue(BIGINT, 100L),
                "value", Domain.all(BIGINT)));
        // "name" is missing
        List<String> sourceFields = List.of("id", "name");
        assertThat(TupleDomainUtils.areAllFieldsReferenced(tupleDomain, sourceFields)).isFalse();
    }

    @Test
    void testAreAllFieldsReferencedNoneMatch()
    {
        // Test when none of the source fields are present
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of(
                "id", Domain.singleValue(BIGINT, 100L),
                "value", Domain.all(BIGINT)));
        // All provided sourceFields are absent
        List<String> sourceFields = List.of("field_x", "field_y");
        assertThat(TupleDomainUtils.areAllFieldsReferenced(tupleDomain, sourceFields)).isFalse();
    }

    @Test
    void testAreAllFieldsReferencedEmptySourceFields()
    {
        // Test with an empty list of source fields (should technically be true)
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of(
                "id", Domain.singleValue(BIGINT, 100L)));
        List<String> sourceFields = List.of();
        // An empty set is a subset of any set
        assertThat(TupleDomainUtils.areAllFieldsReferenced(tupleDomain, sourceFields)).isTrue();
    }

    @Test
    void testAreAllFieldsReferencedEmptyTupleDomain()
    {
        // Test with an empty TupleDomain
        TupleDomain<String> emptyTupleDomain = TupleDomain.none();
        List<String> sourceFields = List.of("id", "name");
        assertThat(TupleDomainUtils.areAllFieldsReferenced(emptyTupleDomain, sourceFields)).isFalse();

        // Test with an empty source list and empty tuple domain
        List<String> emptySourceFields = List.of();
        assertThat(TupleDomainUtils.areAllFieldsReferenced(emptyTupleDomain, emptySourceFields)).isTrue();
    }

    @Test
    void testAreSomeFieldsReferencedSomeMatch()
    {
        // Test when at least one source field is present
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of(
                "id", Domain.singleValue(BIGINT, 100L),
                "value", Domain.all(BIGINT)));
        // Only "id" is present
        List<String> sourceFields = List.of("id", "name");
        assertThat(TupleDomainUtils.areSomeFieldsReferenced(tupleDomain, sourceFields)).isTrue();
    }

    @Test
    void testAreSomeFieldsReferencedAllMatch()
    {
        // Test when all source fields are present
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of(
                "id", Domain.singleValue(BIGINT, 100L),
                "name", Domain.singleValue(VARCHAR, Slices.utf8Slice("test"))));
        List<String> sourceFields = List.of("id", "name");
        assertThat(TupleDomainUtils.areSomeFieldsReferenced(tupleDomain, sourceFields)).isTrue();
    }

    @Test
    void testAreSomeFieldsReferencedNoneMatch()
    {
        // Test when none of the source fields are present
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of(
                "id", Domain.singleValue(BIGINT, 100L)));
        List<String> sourceFields = List.of("name", "value"); // None are present
        assertThat(TupleDomainUtils.areSomeFieldsReferenced(tupleDomain, sourceFields)).isFalse();
    }

    @Test
    void testAreSomeFieldsReferencedEmptySourceFields()
    {
        // Test with an empty list of source fields
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of(
                "id", Domain.singleValue(BIGINT, 100L)));
        List<String> sourceFields = List.of();
        assertThat(TupleDomainUtils.areSomeFieldsReferenced(tupleDomain, sourceFields)).isFalse();
    }

    @Test
    void testAreSomeFieldsReferencedEmptyTupleDomain()
    {
        // Test with an empty TupleDomain
        TupleDomain<String> emptyTupleDomain = TupleDomain.none();
        List<String> sourceFields = List.of("id", "name");
        assertThat(TupleDomainUtils.areSomeFieldsReferenced(emptyTupleDomain, sourceFields)).isFalse();
    }

    @Test
    void testAreDomainsInOrEqualOnlyAllMatch()
    {
        // Test when all referenced source fields have IN or EQUALS domains
        // "other_col" is an irrelevant column
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of(
                "key1", Domain.singleValue(BIGINT, 1L), // EQUALS
                "key2", Domain.multipleValues(VARCHAR, List.of("a", "b")), // IN
                "other_col", Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 10L)), false)));
        List<String> sourceFields = List.of("key1", "key2");
        assertThat(TupleDomainUtils.areDomainsInOrEqualOnly(tupleDomain, sourceFields)).isTrue();
    }

    @Test
    void testAreDomainsInOrEqualOnlySomeMatch()
    {
        // Test when one source field has a non-IN/EQUALS domain (e.g. RANGE)
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of(
                "key1", Domain.singleValue(BIGINT, 1L), // EQUALS
                "key2", Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 10L)), false)));
        List<String> sourceFields = List.of("key1", "key2");
        assertThat(TupleDomainUtils.areDomainsInOrEqualOnly(tupleDomain, sourceFields)).isFalse();
    }

    @Test
    void testAreDomainsInOrEqualOnlySomeMatchWithAll()
    {
        // Test when one source field has Domain.all()
        // After creation, `key2` wil not be in the TupleDomain
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of(
                "key1", Domain.singleValue(BIGINT, 1L), // EQUALS
                // ALL type (neither single value nor discrete set)
                "key2", Domain.all(VARCHAR)));
        List<String> sourceFields = List.of("key1", "key2");
        assertThat(TupleDomainUtils.areDomainsInOrEqualOnly(tupleDomain, sourceFields)).isFalse();
    }

    @Test
    void testAreDomainsInOrEqualOnlySomeMatchWithNull()
    {
        // Test when one source field has Domain.onlyNull()
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of(
                "key1", Domain.singleValue(BIGINT, 1L), // EQUALS
                // onlyNull (neither single value nor discrete set)
                "key2", Domain.onlyNull(VARCHAR)));
        List<String> sourceFields = List.of("key1", "key2");
        assertThat(TupleDomainUtils.areDomainsInOrEqualOnly(tupleDomain, sourceFields)).isFalse();
    }

    @Test
    void testAreDomainsInOrEqualOnlySourceFieldMissing()
    {
        // Test when one of the source fields is not present in the TupleDomain
        // key2 is missing
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of(
                "key1", Domain.singleValue(BIGINT, 1L)));
        List<String> sourceFields = List.of("key1", "key2");
        assertThat(TupleDomainUtils.areDomainsInOrEqualOnly(tupleDomain, sourceFields)).isFalse();
    }

    @Test
    void testAreDomainsInOrEqualOnlyEmptySourceFields()
    {
        // Test with an empty list of source fields
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of(
                "key1", Domain.singleValue(BIGINT, 1L)));
        List<String> sourceFields = List.of();
        assertThat(TupleDomainUtils.areDomainsInOrEqualOnly(tupleDomain, sourceFields)).isFalse(); // As per implementation check
    }

    @Test
    void testAreDomainsInOrEqualOnlyNullSourceFields()
    {
        // Test with null source fields list
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of(
                "key1", Domain.singleValue(BIGINT, 1L)));
        List<String> sourceFields = null;
        assertThat(TupleDomainUtils.areDomainsInOrEqualOnly(tupleDomain, sourceFields)).isFalse(); // As per implementation check
    }

    @Test
    void testAreDomainsInOrEqualOnlyTupleDomainAll()
    {
        // Test with TupleDomain.all()
        TupleDomain<String> tupleDomain = TupleDomain.all();
        List<String> sourceFields = List.of("key1", "key2");
        assertThat(TupleDomainUtils.areDomainsInOrEqualOnly(tupleDomain, sourceFields)).isFalse(); // As per implementation check
    }

    @Test
    void testAreDomainsInOrEqualOnlyTupleDomainNone()
    {
        // Test with TupleDomain.none()
        TupleDomain<String> tupleDomain = TupleDomain.none();
        List<String> sourceFields = List.of("key1", "key2");
        assertThat(TupleDomainUtils.areDomainsInOrEqualOnly(tupleDomain, sourceFields)).isFalse();
    }
}

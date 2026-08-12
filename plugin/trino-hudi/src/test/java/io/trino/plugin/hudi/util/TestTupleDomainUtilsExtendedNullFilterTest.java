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

import java.util.Map;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class TestTupleDomainUtilsExtendedNullFilterTest
{
    @Test
    void testHasSimpleNullCheck_withOnlyNullConstraint()
    {
        // Create a Domain that represents exactly "IS NULL" for a column.
        // Domain.onlyNull(Type) creates a domain where null is allowed and the value set is 'none'.
        Domain isNullDomain = Domain.onlyNull(VARCHAR);
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of("col_a", isNullDomain));

        boolean result = TupleDomainUtils.hasSimpleNullCheck(tupleDomain);

        assertThat(result)
                .as("Check failed: TupleDomain with only an 'IS NULL' constraint should return true.")
                .isTrue();
    }

    @Test
    void testHasSimpleNullCheck_withOnlyNotNullConstraint()
    {
        // Create a Domain that represents exactly "IS NOT NULL" for a column.
        // Domain.notNull(Type) creates a domain where null is *NOT* allowed and the value set is 'all'.
        Domain isNotNullDomain = Domain.notNull(VARCHAR);
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of("col_a", isNotNullDomain));

        boolean result = TupleDomainUtils.hasSimpleNullCheck(tupleDomain);

        assertThat(result)
                .as("Check failed: TupleDomain with only an 'IS NOT NULL' constraint should return true.")
                .isTrue();
    }

    @Test
    void testHasSimpleNullCheck_withMixedConstraintsIncludingNull()
    {
        // Create a TupleDomain with multiple columns, where one column has an "IS NULL" constraint and others have different constraints.
        Domain isNullDomain = Domain.onlyNull(VARCHAR);
        // Example of another constraint: col_b > 'abc', null allowed
        Domain rangeDomain = Domain.create(
                ValueSet.ofRanges(Range.greaterThan(VARCHAR, Slices.utf8Slice("abc"))),
                true);
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of(
                "col_a", isNullDomain,
                "col_b", rangeDomain));

        boolean result = TupleDomainUtils.hasSimpleNullCheck(tupleDomain);

        assertThat(result)
                .as("Check failed: TupleDomain with mixed constraints including 'IS NULL' should return true.")
                .isTrue();
    }

    @Test
    void testHasSimpleNullCheck_withMixedConstraintsIncludingNotNull()
    {
        // Create a TupleDomain with multiple columns, where one column has an "IS NOT NULL" constraint.
        Domain isNotNullDomain = Domain.notNull(VARCHAR);
        // Add another constraint: col_b < 'xyz', null not allowed
        Domain rangeDomain = Domain.create(
                ValueSet.ofRanges(Range.lessThan(VARCHAR, Slices.utf8Slice("xyz"))),
                false);
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of(
                "col_a", isNotNullDomain,
                "col_b", rangeDomain));

        boolean result = TupleDomainUtils.hasSimpleNullCheck(tupleDomain);

        assertThat(result)
                .as("Check failed: TupleDomain with mixed constraints including 'IS NOT NULL' should return true.")
                .isTrue();
    }

    @Test
    void testHasSimpleNullCheck_withNonNullAndNotNullConstraint()
    {
        // Create a domain that allows specific non-null values AND disallows null.
        // This is *NOT* exclusively an "IS NOT NULL" constraint because ValueSet is not 'all'.
        Domain specificValuesNotNullDomain = Domain.create(
                // Only allows 'value1'
                ValueSet.of(VARCHAR, Slices.utf8Slice("value1")),
                false);
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of(
                "col_a", specificValuesNotNullDomain));

        boolean result = TupleDomainUtils.hasSimpleNullCheck(tupleDomain);

        assertThat(result).isFalse();
    }

    @Test
    void testHasSimpleNullCheck_withNonNullAndNullConstraint()
    {
        // Create a domain that allows specific non-null values AND allows null.
        // This is *NOT* exclusively an "IS NULL" constraint because ValueSet is not 'none'.
        Domain specificValuesNullDomain = Domain.create(
                // Only allows 'value1'
                ValueSet.of(VARCHAR, Slices.utf8Slice("value1")),
                true);
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of(
                "col_a", specificValuesNullDomain));

        boolean result = TupleDomainUtils.hasSimpleNullCheck(tupleDomain);

        assertThat(result).isFalse();
    }

    @Test
    void testHasSimpleNullCheck_withNoSimpleNullChecks()
    {
        // Create a TupleDomain where columns have constraints, but none are *only* IS NULL or IS NOT NULL.
        // col_a > 'abc', null allowed
        Domain rangeDomain1 = Domain.create(
                ValueSet.ofRanges(Range.greaterThan(VARCHAR, Slices.utf8Slice("abc"))),
                true);
        // col_b < 'xyz', null not allowed
        Domain rangeDomain2 = Domain.create(
                ValueSet.ofRanges(Range.lessThan(VARCHAR, Slices.utf8Slice("xyz"))),
                false);
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of(
                "col_a", rangeDomain1,
                "col_b", rangeDomain2));

        boolean result = TupleDomainUtils.hasSimpleNullCheck(tupleDomain);

        assertThat(result).isFalse();
    }

    @Test
    void testHasSimpleNullCheck_withAllTupleDomain()
    {
        // Create an 'All' TupleDomain, which represents no constraints.
        TupleDomain<String> tupleDomain = TupleDomain.all();

        boolean result = TupleDomainUtils.hasSimpleNullCheck(tupleDomain);

        assertThat(result)
                .as("Check failed: TupleDomain.all() should return false as it has no constraints.")
                .isFalse();
    }

    @Test
    void testHasSimpleNullCheck_withNoneTupleDomain()
    {
        // Create a 'None' TupleDomain, which represents a contradiction (always false).
        TupleDomain<String> tupleDomain = TupleDomain.none();

        boolean result = TupleDomainUtils.hasSimpleNullCheck(tupleDomain);

        assertThat(result)
                .as("Check failed: TupleDomain.none() should return false.")
                .isFalse();
    }

    @Test
    void testHasSimpleNullCheck_withEmptyTupleDomain()
    {
        // Create a TupleDomain using an empty map of column domains.
        // This usually results in an 'All' TupleDomain.
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of());

        boolean result = TupleDomainUtils.hasSimpleNullCheck(tupleDomain);

        assertThat(result)
                .as("Check failed: TupleDomain created with an empty map (effectively All) should return false.")
                .isFalse();
    }
}

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
package io.trino.plugin.varada.util;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class DomainUtilsTest
{
    @Test
    public void testSimplify()
    {
        int predicateThreshold = 1;
        List<Range> tooManyRanges = ImmutableList.of(Range.lessThan(VarcharType.VARCHAR, Slices.utf8Slice("a")),
                Range.greaterThan(VarcharType.VARCHAR, Slices.utf8Slice("s")));

        Domain tooBigDomain = Domain.create(SortedRangeSet.copyOf(VarcharType.VARCHAR, tooManyRanges), true);
        Domain validDomain = Domain.singleValue(VarcharType.VARCHAR, Slices.utf8Slice("e"));
        TupleDomain<String> tupleDomain = TupleDomain.withColumnDomains(Map.of(
                "columnHandle1", tooBigDomain,
                "columnHandle2", validDomain));

        SimplifyResult<String> simplifyResult = DomainUtils.simplify(tupleDomain, predicateThreshold);

        TupleDomain<String> expectedTupleDomain = TupleDomain.withColumnDomains(Map.of(
                "columnHandle1", createSimplifyExpectedResult(tooBigDomain),
                "columnHandle2", validDomain));
        Set<String> expectedSimplifiedColumns = Set.of("columnHandle1");
        assertThat(simplifyResult).isEqualTo(new SimplifyResult<>(expectedTupleDomain, expectedSimplifiedColumns));
    }

    private Domain createSimplifyExpectedResult(Domain domain)
    {
        return Domain.create(SortedRangeSet.copyOf(domain.getType(),
                        Collections.singletonList(domain.getValues().getRanges().getSpan())),
                domain.isNullAllowed());
    }
}

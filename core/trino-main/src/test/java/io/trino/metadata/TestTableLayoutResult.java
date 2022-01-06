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

package io.trino.metadata;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.testng.annotations.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.iterative.rule.PushPredicateIntoTableScan.computeEnforced;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.lang.String.format;
import static org.testng.Assert.fail;

public class TestTableLayoutResult
{
    @Test
    public void testComputeEnforced()
    {
        assertComputeEnforced(TupleDomain.all(), TupleDomain.all(), TupleDomain.all());
        assertComputeEnforcedFails(TupleDomain.all(), TupleDomain.none());
        assertComputeEnforced(TupleDomain.none(), TupleDomain.all(), TupleDomain.none());
        assertComputeEnforced(TupleDomain.none(), TupleDomain.none(), TupleDomain.all());

        assertComputeEnforced(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L))),
                TupleDomain.all(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L))));
        assertComputeEnforced(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L))),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L))),
                TupleDomain.all());
        assertComputeEnforcedFails(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L))),
                TupleDomain.none());
        assertComputeEnforcedFails(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L))),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 9999L))));
        assertComputeEnforcedFails(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L))),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c9999"), Domain.singleValue(BIGINT, 1L))));

        assertComputeEnforced(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L),
                        new TestingColumnHandle("c2"), Domain.singleValue(BIGINT, 2L))),
                TupleDomain.all(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L),
                        new TestingColumnHandle("c2"), Domain.singleValue(BIGINT, 2L))));
        assertComputeEnforced(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L),
                        new TestingColumnHandle("c2"), Domain.singleValue(BIGINT, 2L))),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L))),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c2"), Domain.singleValue(BIGINT, 2L))));
        assertComputeEnforced(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L),
                        new TestingColumnHandle("c2"), Domain.singleValue(BIGINT, 2L))),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c2"), Domain.singleValue(BIGINT, 2L))),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L))));
        assertComputeEnforced(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L),
                        new TestingColumnHandle("c2"), Domain.singleValue(BIGINT, 2L))),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        new TestingColumnHandle("c1"), Domain.singleValue(BIGINT, 1L),
                        new TestingColumnHandle("c2"), Domain.singleValue(BIGINT, 2L))),
                TupleDomain.all());
    }

    private void assertComputeEnforcedFails(TupleDomain<ColumnHandle> predicate, TupleDomain<ColumnHandle> unenforced)
    {
        try {
            TupleDomain<ColumnHandle> enforced = computeEnforced(predicate, unenforced);
            fail(format("expected IllegalArgumentException but found [%s]", enforced.toString(SESSION)));
        }
        catch (IllegalArgumentException e) {
            // do nothing
        }
    }

    private void assertComputeEnforced(TupleDomain<ColumnHandle> predicate, TupleDomain<ColumnHandle> unenforced, TupleDomain<ColumnHandle> expectedEnforced)
    {
        TupleDomain<ColumnHandle> enforced = computeEnforced(predicate, unenforced);
        if (!enforced.equals(expectedEnforced)) {
            fail(format("expected [%s] but found [%s]", expectedEnforced.toString(SESSION), enforced.toString(SESSION)));
        }
    }
}

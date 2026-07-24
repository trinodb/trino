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
package io.trino.plugin.doris;

import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDorisSplitManager
{
    @Test
    void testViewUsesSingleSyntheticSplit()
            throws Exception
    {
        AtomicInteger plannerCalls = new AtomicInteger();
        DorisSplitManager splitManager = new DorisSplitManager(_ -> {
            plannerCalls.incrementAndGet();
            return List.of();
        });

        DorisTableHandle tableHandle = new DorisTableHandle("tpch", "revenue0", "tpch", "revenue0", DorisRelationType.VIEW);

        ConnectorSplitSource splitSource = splitManager.getSplits(
                DorisTransactionHandle.INSTANCE,
                SESSION,
                tableHandle,
                DynamicFilter.EMPTY,
                new Constraint(TupleDomain.all()));

        ConnectorSplitSource.ConnectorSplitBatch batch = splitSource.getNextBatch(10).get();
        assertThat(plannerCalls.get()).isEqualTo(0);
        assertThat(batch.getSplits()).hasSize(1);
        assertThat(((DorisSplit) batch.getSplits().getFirst()).getAddresses()).isEmpty();
    }
}

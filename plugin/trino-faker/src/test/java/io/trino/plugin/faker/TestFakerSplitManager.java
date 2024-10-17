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
package io.trino.plugin.faker;

import io.trino.client.NodeVersion;
import io.trino.metadata.InternalNode;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.TestingNodeManager;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.trino.plugin.faker.FakerSplitManager.MAX_ROWS_PER_SPLIT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFakerSplitManager
{
    @Test
    public void testSplits()
            throws ExecutionException, InterruptedException
    {
        URI uriA = URI.create("http://a");
        URI uriB = URI.create("http://b");
        HostAddress hostA = HostAddress.fromUri(uriA);
        HostAddress hostB = HostAddress.fromUri(uriB);
        TestingNodeManager nodeManager = new TestingNodeManager(List.of(
                new InternalNode("a", uriA, NodeVersion.UNKNOWN, false),
                new InternalNode("b", uriB, NodeVersion.UNKNOWN, false)));
        long expectedRows = 123 + 5 * MAX_ROWS_PER_SPLIT;
        ConnectorSplitSource splitSource = new FakerSplitManager(nodeManager).getSplits(
                TestingTransactionHandle.create(),
                TestingConnectorSession.SESSION,
                new FakerTableHandle(1L, new SchemaTableName("schema", "table"), TupleDomain.all(), expectedRows),
                DynamicFilter.EMPTY,
                Constraint.alwaysTrue());
        List<ConnectorSplit> splits = splitSource.getNextBatch(1_000_000).get().getSplits();

        assertThat(splitSource.isFinished()).withFailMessage("split source is not finished").isTrue();
        assertThat(splits)
                .hasSize(6)
                .containsOnly(
                        new FakerSplit(List.of(hostA), MAX_ROWS_PER_SPLIT),
                        new FakerSplit(List.of(hostB), MAX_ROWS_PER_SPLIT),
                        new FakerSplit(List.of(hostA), 123));
        long actualRows = splits.stream().mapToLong(split -> ((FakerSplit) split).limit()).sum();
        assertThat(actualRows).isEqualTo(expectedRows);
    }
}

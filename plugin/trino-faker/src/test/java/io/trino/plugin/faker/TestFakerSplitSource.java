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

import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.trino.plugin.faker.FakerSplitSource.MAX_ROWS_PER_SPLIT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFakerSplitSource
{
    @Test
    public void testSplits()
            throws ExecutionException, InterruptedException
    {
        HostAddress hostA = HostAddress.fromString("a");
        HostAddress hostB = HostAddress.fromString("b");
        long expectedRows = 1_123;
        FakerSplitSource splitSource = new FakerSplitSource(List.of(hostA, hostB), 9, expectedRows);
        List<ConnectorSplit> splits = splitSource.getNextBatch(1_000_000).get().getSplits();

        assertThat(splitSource.isFinished()).withFailMessage("split source is not finished").isTrue();
        assertThat(splits)
                .hasSize(18)
                .containsOnly(
                        new FakerSplit(List.of(hostA), 63),
                        new FakerSplit(List.of(hostB), 63),
                        new FakerSplit(List.of(hostB), 52));
        long actualRows = splits.stream().mapToLong(split -> ((FakerSplit) split).limit()).sum();
        assertThat(actualRows).isEqualTo(expectedRows);
    }

    @Test
    public void testMaxRowsPerSplit()
            throws ExecutionException, InterruptedException
    {
        HostAddress hostA = HostAddress.fromString("a");
        long expectedRows = 123 + 5 * MAX_ROWS_PER_SPLIT;
        FakerSplitSource splitSource = new FakerSplitSource(List.of(hostA), 2, expectedRows);
        List<ConnectorSplit> splits = splitSource.getNextBatch(1_000_000).get().getSplits();

        assertThat(splitSource.isFinished()).withFailMessage("split source is not finished").isTrue();
        assertThat(splits)
                .containsExactly(
                        new FakerSplit(List.of(hostA), MAX_ROWS_PER_SPLIT),
                        new FakerSplit(List.of(hostA), MAX_ROWS_PER_SPLIT),
                        new FakerSplit(List.of(hostA), MAX_ROWS_PER_SPLIT),
                        new FakerSplit(List.of(hostA), MAX_ROWS_PER_SPLIT),
                        new FakerSplit(List.of(hostA), MAX_ROWS_PER_SPLIT),
                        new FakerSplit(List.of(hostA), 123));
    }
}

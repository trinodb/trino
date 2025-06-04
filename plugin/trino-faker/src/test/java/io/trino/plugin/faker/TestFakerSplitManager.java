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

import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.trino.plugin.faker.FakerSplitManager.MAX_ROWS_PER_SPLIT;
import static org.assertj.core.api.Assertions.assertThat;

final class TestFakerSplitManager
{
    @Test
    void testSplits()
            throws ExecutionException, InterruptedException
    {
        long expectedRows = 123 + 5 * MAX_ROWS_PER_SPLIT;
        ConnectorSplitSource splitSource = new FakerSplitManager().getSplits(
                TestingTransactionHandle.create(),
                TestingConnectorSession.SESSION,
                new FakerTableHandle(new SchemaTableName("schema", "table"), expectedRows),
                DynamicFilter.EMPTY,
                Constraint.alwaysTrue());
        List<ConnectorSplit> splits = splitSource.getNextBatch(1_000_000).get().getSplits();

        assertThat(splitSource.isFinished()).withFailMessage("split source is not finished").isTrue();
        assertThat(splits)
                .hasSize(6)
                .map(split -> (FakerSplit) split)
                .satisfies(splitsList -> assertThat(splitsList)
                        .filteredOn(split -> split.rowsCount() == MAX_ROWS_PER_SPLIT)
                        .hasSize(5))
                .satisfies(splitsList -> assertThat(splitsList)
                        .filteredOn(split -> split.rowsCount() == 123)
                        .hasSize(1));

        long actualRows = splits.stream().mapToLong(split -> ((FakerSplit) split).rowsCount()).sum();
        assertThat(actualRows).isEqualTo(expectedRows);
    }
}

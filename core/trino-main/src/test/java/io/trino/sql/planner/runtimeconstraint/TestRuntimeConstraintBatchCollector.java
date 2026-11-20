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
package io.trino.sql.planner.runtimeconstraint;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRuntimeConstraintBatchCollector
{
    @Test
    public void testSplitsPendingValuesWithoutAcknowledgingUnsentUpdates()
    {
        RuntimeConstraintBatchCollector<Character, String> collector = new RuntimeConstraintBatchCollector<>(7, value -> value.charAt(0));
        collector.update("alpha", 60);
        collector.update("beta", 60);
        collector.update("charlie", 10);

        RuntimeConstraintBatchCollector.Batch<String> first = collector.getPendingBatch(100, 10);
        assertThat(first.values()).containsExactly("alpha");
        assertThat(first.sequence()).isEqualTo(1);
        assertThat(first.hasMore()).isTrue();
        assertThat(collector.getPendingBatch(100, 10)).isEqualTo(first);

        collector.acknowledge(first.sequence());
        RuntimeConstraintBatchCollector.Batch<String> second = collector.getPendingBatch(100, 10);
        assertThat(second.values()).containsExactly("beta", "charlie");
        assertThat(second.sequence()).isEqualTo(3);
        assertThat(second.hasMore()).isFalse();

        collector.acknowledge(second.sequence());
        assertThat(collector.getPendingValues()).isEmpty();
        assertThat(collector.getRetainedBytes()).isZero();
    }

    @Test
    public void testLimitsEntryCountAndAlwaysMakesProgress()
    {
        RuntimeConstraintBatchCollector<Character, String> collector = new RuntimeConstraintBatchCollector<>(7, value -> value.charAt(0));
        collector.update("alpha", 200);
        collector.update("beta", 0);

        assertThat(collector.getPendingBatch(100, 10).values()).containsExactly("alpha");
        assertThat(collector.getPendingBatch(Long.MAX_VALUE, 1).values()).containsExactly("alpha");
    }
}

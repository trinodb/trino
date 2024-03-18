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
package io.trino.plugin.varada.warmup.model;

import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionValueWarmupPredicateRuleTest
{
    private static final RegularColumn COLUMN_NAME = new RegularColumn("col1");
    private static final String VALUE = "value1";

    @Test
    public void testValueEquals()
    {
        PartitionValueWarmupPredicateRule rule = new PartitionValueWarmupPredicateRule(COLUMN_NAME.getName(), VALUE);
        assertThat(rule.test(Collections.singletonMap(COLUMN_NAME, VALUE))).isTrue();
    }

    @Test
    public void testValueNotEquals()
    {
        PartitionValueWarmupPredicateRule rule = new PartitionValueWarmupPredicateRule(COLUMN_NAME.getName(), "another-value");
        assertThat(rule.test(Collections.singletonMap(COLUMN_NAME, VALUE))).isFalse();
    }

    @Test
    public void testValueNonExistColumn()
    {
        PartitionValueWarmupPredicateRule rule = new PartitionValueWarmupPredicateRule("another-column", VALUE);
        assertThat(rule.test(Collections.singletonMap(COLUMN_NAME, VALUE))).isFalse();
    }

    @Test
    public void testValueMultiPartitions()
    {
        Map<RegularColumn, String> partitionKeys = new HashMap<>();
        partitionKeys.put(COLUMN_NAME, VALUE);
        partitionKeys.put(new RegularColumn("another-column"), "another-value");

        PartitionValueWarmupPredicateRule rule = new PartitionValueWarmupPredicateRule(COLUMN_NAME.getName(), VALUE);
        assertThat(rule.test(partitionKeys)).isTrue();
        PartitionValueWarmupPredicateRule rule2 = new PartitionValueWarmupPredicateRule("another-column", "another-value");
        assertThat(rule2.test(partitionKeys)).isTrue();
    }

    @Test
    public void testValueNoPartitions()
    {
        PartitionValueWarmupPredicateRule rule = new PartitionValueWarmupPredicateRule(COLUMN_NAME.getName(), VALUE);
        assertThat(rule.test(Collections.emptyMap())).isFalse();
    }
}

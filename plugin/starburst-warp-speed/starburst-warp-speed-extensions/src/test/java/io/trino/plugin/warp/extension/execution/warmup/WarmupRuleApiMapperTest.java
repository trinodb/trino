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
package io.trino.plugin.warp.extension.execution.warmup;

import io.trino.plugin.varada.api.warmup.WarmupColRuleData;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.warmup.WarmupRuleApiMapper;
import io.trino.plugin.varada.warmup.model.DateSlidingWindowWarmupPredicateRule;
import io.trino.plugin.varada.warmup.model.PartitionValueWarmupPredicateRule;
import io.trino.plugin.varada.warmup.model.WarmupRule;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class WarmupRuleApiMapperTest
{
    @Test
    public void testMapper()
    {
        WarmupRule warmupRule = WarmupRule.builder()
                .schema("schema")
                .table("table")
                .varadaColumn(new RegularColumn("col1"))
                .warmUpType(WarmUpType.WARM_UP_TYPE_LUCENE)
                .priority(0)
                .ttl(0)
                .predicates(Set.of(new PartitionValueWarmupPredicateRule("col2", "2"),
                        new DateSlidingWindowWarmupPredicateRule("col3", 30, "XXX", "")))
                .build();
        WarmupColRuleData warmupColRuleData = WarmupRuleApiMapper.fromModel(warmupRule);
        assertThat(warmupRule).isEqualTo(WarmupRuleApiMapper.toModel(warmupColRuleData));
    }
}

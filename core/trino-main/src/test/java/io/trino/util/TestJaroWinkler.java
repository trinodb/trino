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
package io.trino.util;

import org.junit.jupiter.api.Test;

import static io.trino.util.JaroWinkler.similarity;
import static org.assertj.core.api.Assertions.assertThat;

final class TestJaroWinkler
{
    // Expected values produced by info.debatty:java-string-similarity 2.0.0, which this implementation replaces
    @Test
    void testSimilarity()
    {
        assertThat(similarity("My string", "My string")).isEqualTo(1.0);
        assertThat(similarity("", "")).isEqualTo(1.0);
        assertThat(similarity("abc", "xyz")).isEqualTo(0.0);
        assertThat(similarity("a", "")).isEqualTo(0.0);
        assertThat(similarity("My string", "My tsring")).isEqualTo(0.9740740478038787);
        assertThat(similarity("My string", "My ntrisg")).isEqualTo(0.8962963163852692);
        assertThat(similarity("query_max_memory", "query_max_total_memory")).isEqualTo(0.9276859543540261);
        assertThat(similarity("spill_enabled", "spil_enabled")).isEqualTo(0.924556186565986);
        assertThat(similarity("task_concurrency", "task_concurency")).isEqualTo(0.9934895895421505);
        assertThat(similarity("join_distribution_type", "join_distribution_typo")).isEqualTo(0.9986225908452814);
    }

    @Test
    void testSymmetry()
    {
        assertThat(similarity("spill_enabled", "spil_enabled")).isEqualTo(similarity("spil_enabled", "spill_enabled"));
        assertThat(similarity("query_max_memory", "query_max_total_memory")).isEqualTo(similarity("query_max_total_memory", "query_max_memory"));
    }
}

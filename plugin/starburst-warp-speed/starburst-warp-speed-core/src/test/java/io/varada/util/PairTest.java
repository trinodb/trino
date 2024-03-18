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
package io.varada.util;

import io.varada.tools.util.Pair;
import org.junit.jupiter.api.Test;

import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

public class PairTest
{
    @Test
    public void test()
    {
        Pair<String, Integer> pair = Pair.of("a", 1);
        assertThat(pair.getKey()).isEqualTo("a");
        assertThat(pair.getValue()).isEqualTo(1);
        assertThat(pair.getLeft()).isEqualTo("a");
        assertThat(pair.getRight()).isEqualTo(1);
        assertThat(pair.hashCode()).isEqualTo(Objects.hash("a", 1));
        assertThat(pair).isEqualTo(Pair.of("a", 1));
    }
}

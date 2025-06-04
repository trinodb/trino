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
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestLikePredicate
{
    @Test
    public void testGetChildren()
    {
        StringLiteral value = new StringLiteral("a");
        StringLiteral pattern = new StringLiteral("b");
        StringLiteral escape = new StringLiteral("c");

        assertThat(new LikePredicate(value, pattern, escape).getChildren()).isEqualTo(ImmutableList.of(value, pattern, escape));
        assertThat(new LikePredicate(value, pattern, Optional.empty()).getChildren()).isEqualTo(ImmutableList.of(value, pattern));
    }
}

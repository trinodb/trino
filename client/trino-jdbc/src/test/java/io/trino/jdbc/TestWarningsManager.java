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
package io.trino.jdbc;

import com.google.common.collect.ImmutableList;
import io.trino.client.Warning;
import org.junit.jupiter.api.Test;

import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestWarningsManager
{
    private static final Warning.Code DEPRECATED = new Warning.Code(1, "DEPRECATED");
    private static final Warning.Code PERFORMANCE = new Warning.Code(2, "PERFORMANCE");

    @Test
    public void testKeepsDistinctWarningsWithSameCode()
    {
        WarningsManager manager = new WarningsManager();
        Warning first = new Warning(DEPRECATED, "first deprecated feature");
        Warning second = new Warning(DEPRECATED, "second deprecated feature");
        Warning third = new Warning(PERFORMANCE, "slow plan");

        // the server sends all warnings collected so far with every response
        manager.addWarnings(ImmutableList.of(first));
        manager.addWarnings(ImmutableList.of(first, second));
        manager.addWarnings(ImmutableList.of(first, second, third));

        assertThat(messages(manager.getWarnings()))
                .containsExactly("first deprecated feature", "second deprecated feature", "slow plan");
    }

    private static List<String> messages(SQLWarning warning)
    {
        List<String> messages = new ArrayList<>();
        for (SQLWarning current = warning; current != null; current = current.getNextWarning()) {
            messages.add(current.getMessage());
        }
        return messages;
    }
}

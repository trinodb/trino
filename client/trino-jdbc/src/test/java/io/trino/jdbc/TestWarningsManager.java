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

import static io.trino.jdbc.TestingJdbcUtils.warningMessages;
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

        assertThat(warningMessages(manager.getWarnings()))
                .containsExactly("first deprecated feature", "second deprecated feature", "slow plan");
    }

    @Test
    public void testKeepsSameMessageWithDifferentCodes()
    {
        WarningsManager manager = new WarningsManager();
        manager.addWarnings(ImmutableList.of(new Warning(DEPRECATED, "same message"), new Warning(PERFORMANCE, "same message")));

        SQLWarning warnings = manager.getWarnings();
        assertThat(warningMessages(warnings)).containsExactly("same message", "same message");
        assertThat(warnings.getSQLState()).isEqualTo("DEPRECATED");
        assertThat(warnings.getNextWarning().getSQLState()).isEqualTo("PERFORMANCE");
    }

    @Test
    public void testDropsDuplicateWithinOneResponse()
    {
        WarningsManager manager = new WarningsManager();
        manager.addWarnings(ImmutableList.of(new Warning(PERFORMANCE, "slow plan"), new Warning(PERFORMANCE, "slow plan")));

        assertThat(warningMessages(manager.getWarnings())).containsExactly("slow plan");
    }
}

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
package io.trino.execution.warnings;

import io.trino.spi.TrinoWarning;
import io.trino.spi.WarningCode;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDefaultWarningCollector
{
    @Test
    public void testNoWarnings()
    {
        WarningCollector warningCollector = new DefaultWarningCollector(new WarningCollectorConfig().setMaxWarnings(0));
        warningCollector.add(new TrinoWarning(new WarningCode(1, "1"), "warning 1"));
        assertThat(warningCollector.getWarnings()).isEmpty();
    }

    @Test
    public void testMaxWarnings()
    {
        WarningCollector warningCollector = new DefaultWarningCollector(new WarningCollectorConfig().setMaxWarnings(2));
        warningCollector.add(new TrinoWarning(new WarningCode(1, "1"), "warning 1"));
        warningCollector.add(new TrinoWarning(new WarningCode(2, "2"), "warning 2"));
        warningCollector.add(new TrinoWarning(new WarningCode(3, "3"), "warning 3"));
        assertThat(warningCollector.getWarnings()).hasSize(2);
    }
}

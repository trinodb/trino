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
package io.trino.cli;

import org.junit.jupiter.api.Test;

import static io.trino.cli.Theme.AUTO;
import static io.trino.cli.Theme.DARK;
import static io.trino.cli.Theme.NONE;
import static org.assertj.core.api.Assertions.assertThat;

class TestTheme
{
    @Test
    void testExplicitThemeAlwaysResolvesToItself()
    {
        for (Theme theme : Theme.values()) {
            if (theme == AUTO) {
                continue;
            }
            for (boolean realTerminal : new boolean[] {true, false}) {
                for (boolean noColor : new boolean[] {true, false}) {
                    assertThat(theme.resolve(realTerminal, noColor)).isSameAs(theme);
                }
            }
        }
    }

    @Test
    void testAutoUsesDarkForInteractiveTerminal()
    {
        assertThat(AUTO.resolve(true, false)).isSameAs(DARK);
    }

    @Test
    void testAutoDisablesColorForDumbTerminal()
    {
        assertThat(AUTO.resolve(false, false)).isSameAs(NONE);
    }

    @Test
    void testAutoDisablesColorWhenNoColorIsSet()
    {
        assertThat(AUTO.resolve(true, true)).isSameAs(NONE);
        assertThat(AUTO.resolve(false, true)).isSameAs(NONE);
    }
}

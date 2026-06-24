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

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

public class TestPromptColor
{
    private static final char ESC = (char) 27;
    // ESC + "[1m": SGR 1 = bold, applied to the terminal's default foreground.
    private static final String BOLD = ESC + "[1m";
    // ESC + "[90m": SGR 90 = "bright black" (dark gray), which has poor contrast on dark backgrounds.
    private static final String GRAY_FOREGROUND = ESC + "[90m";

    @Test
    public void testPromptIsBoldAndNotLowContrastGray()
            throws Exception
    {
        String prompt = colored("trino> ");

        assertThat(prompt)
                .as("CLI prompt should be bold over the terminal's default foreground")
                .startsWith(BOLD)
                .contains("trino> ");
        assertThat(prompt)
                .as("CLI prompt must not use the dark-gray foreground that is hard to read on dark backgrounds")
                .doesNotContain(GRAY_FOREGROUND);
    }

    private static String colored(String value)
            throws Exception
    {
        Method method = InputReader.class.getDeclaredMethod("colored", String.class);
        method.setAccessible(true);
        return (String) method.invoke(null, value);
    }
}

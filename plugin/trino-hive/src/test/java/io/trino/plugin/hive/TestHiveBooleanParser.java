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
package io.trino.plugin.hive;

import org.junit.jupiter.api.Test;

import static io.trino.plugin.hive.HiveBooleanParser.parseHiveBoolean;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveBooleanParser
{
    @Test
    public void testParse()
    {
        assertThat(parseBoolean("true")).isTrue();
        assertThat(parseBoolean("TRUE")).isTrue();
        assertThat(parseBoolean("tRuE")).isTrue();

        assertThat(parseBoolean("false")).isFalse();
        assertThat(parseBoolean("FALSE")).isFalse();
        assertThat(parseBoolean("fAlSe")).isFalse();

        assertThat(parseBoolean("true ")).isNull();
        assertThat(parseBoolean(" true")).isNull();
        assertThat(parseBoolean("false ")).isNull();
        assertThat(parseBoolean(" false")).isNull();
        assertThat(parseBoolean("t")).isNull();
        assertThat(parseBoolean("f")).isNull();
        assertThat(parseBoolean("")).isNull();
        assertThat(parseBoolean("blah")).isNull();
    }

    private static Boolean parseBoolean(String s)
    {
        return parseHiveBoolean(s.getBytes(US_ASCII), 0, s.length());
    }
}

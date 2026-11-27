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

import io.trino.client.ClientSession;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.ZoneId;
import java.util.Locale;

import static io.trino.cli.Console.buildPrompt;
import static org.assertj.core.api.Assertions.assertThat;

public class TestConsole
{
    @Test
    public void testPrompt()
    {
        assertThat(buildPrompt(session().build())).isEqualTo("trino> ");
        assertThat(buildPrompt(session().catalog("hive").build())).isEqualTo("trino:hive> ");
        assertThat(buildPrompt(session().catalog("hive").schema("config").build())).isEqualTo("trino:hive.config> ");
        assertThat(buildPrompt(session().catalog("hive").schema("100%").build())).isEqualTo("trino:hive.100%%> ");
    }

    private static ClientSession.Builder session()
    {
        return ClientSession.builder()
                .server(URI.create("http://localhost:8080"))
                .source("test")
                .timeZone(ZoneId.of("UTC"))
                .locale(Locale.ENGLISH);
    }
}

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
package io.trino.server;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.assertj.core.api.Assertions.assertThat;

class TestPluginReader
{
    @Test
    void testCall()
    {
        PluginReader pluginReader = new PluginReader();
        StringWriter writer = new StringWriter();
        CommandLine cmd = new CommandLine(pluginReader)
                .setOut(new PrintWriter(writer))
                .setErr(new PrintWriter(writer));

        int exitCode = cmd.execute(
                "--impacted-modules", "src/test/resources/gib-impacted.log",
                "--plugin-dir", "src/test/resources/server-plugins",
                "--root-pom", "src/test/resources/pom.xml");
        assertThat(exitCode).isEqualTo(0);
        assertThat(writer.toString()).isEqualTo("");
    }
}

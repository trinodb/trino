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

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;

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

        String[] command = ImmutableList.<String>builder()
                .add("--impacted-modules", "src/test/resources/gib-impacted.log")
                .add("--plugin-dir", "src/test/resources/server-plugins")
                .add("--root-pom", "src/test/resources/pom.xml")
                .build()
                .toArray(String[]::new);

        int exitCode = cmd.execute(command);
        assertThat(exitCode).isEqualTo(0);
        assertThat(writer.toString()).isEqualTo("");
    }

    @Test
    void testCallMultiplePluginDirs(@TempDir Path tempDir)
    {
        PluginReader pluginReader = new PluginReader();
        StringWriter writer = new StringWriter();
        CommandLine cmd = new CommandLine(pluginReader)
                .setOut(new PrintWriter(writer))
                .setErr(new PrintWriter(writer));

        String[] command = ImmutableList.<String>builder()
                .add("--impacted-modules", "src/test/resources/gib-impacted.log")
                .add("--plugin-dir", "src/test/resources/server-plugins")
                .add("--plugin-dir", tempDir.toString())
                .add("--root-pom", "src/test/resources/pom.xml")
                .build()
                .toArray(String[]::new);

        int exitCode = cmd.execute(command);
        assertThat(exitCode).isEqualTo(0);
        assertThat(writer.toString()).isEqualTo("");
    }
}

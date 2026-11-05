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
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.trino.cli.TerminalUtils.mayBeFile;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTerminalUtils
{
    @Test
    public void testMayBeFile(@TempDir Path directory)
            throws IOException
    {
        assertThat(mayBeFile(Files.createFile(directory.resolve("result.csv")))).isTrue();
        // a path that does not exist, as /dev/stdout on Windows, leaves open that the output is a file
        assertThat(mayBeFile(directory.resolve("missing"))).isTrue();
        // an existing path that is not a regular file, such as a pipe or a device
        assertThat(mayBeFile(directory)).isFalse();
    }
}

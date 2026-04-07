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
package io.trino.plugin.exasol;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class TestExasolConfig
{
    ExasolConfig exasolConfig;

    @BeforeEach
    void setup()
    {
        exasolConfig = new ExasolConfig();
    }

    @Test
    void testDefaultLogDirEmpty()
    {
        assertThat(exasolConfig.getJdbcDriverLogDir()).isEmpty();
    }

    @Test
    void testCustomLogDir()
    {
        Path jdbcDriverLogDir = Path.of("log-dir");
        exasolConfig.setJdbcDriverLogDir(jdbcDriverLogDir);
        assertThat(exasolConfig.getJdbcDriverLogDir()).contains(jdbcDriverLogDir);
    }
}

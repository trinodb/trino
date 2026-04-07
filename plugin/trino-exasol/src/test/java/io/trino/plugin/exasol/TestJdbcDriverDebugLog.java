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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJdbcDriverDebugLog
        extends AbstractTestQueryFramework
{
    @TempDir
    static Path logDir;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingExasolServer exasolServer = closeAfterClass(new TestingExasolServer());
        return ExasolQueryRunner.builder(exasolServer)
                .setInitialTables(List.of(NATION))
                .addConnectorProperty("exasol.jdbc-driver.log-dir", logDir.toString())
                .build();
    }

    @Test
    void testJdbcDriverCreatesDebugLogFiles()
            throws IOException
    {
        assertQuery("SELECT count(*) FROM nation", "SELECT 25");
        try (Stream<Path> logFiles = Files.list(logDir)) {
            assertThat(logFiles).hasSizeGreaterThan(0);
        }
    }
}

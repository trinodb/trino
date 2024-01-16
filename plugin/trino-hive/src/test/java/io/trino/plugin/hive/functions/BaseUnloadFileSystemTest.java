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
package io.trino.plugin.hive.functions;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.testing.AbstractTestQueryFramework;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseUnloadFileSystemTest
        extends AbstractTestQueryFramework
{
    protected abstract TrinoFileSystemFactory getFileSystemFactory()
            throws IOException;

    protected abstract String getLocation(String path);

    @Test
    public void testUnload()
            throws Exception
    {
        String tableName = "test_unload_" + randomNameSuffix();
        String location = getLocation(tableName);

        TrinoFileSystem fileSystem = getFileSystemFactory().create(getSession().toConnectorSession());
        fileSystem.createDirectory(Location.of(location));

        try {
            assertQuerySucceeds("SELECT * FROM TABLE(hive.system.unload(" +
                    "input => TABLE(tpch.tiny.region)," +
                    "location => '" + location + "'," +
                    "format => 'ORC'))");

            assertUpdate("CREATE TABLE " + tableName + "(LIKE tpch.tiny.region) WITH (external_location = '" + location + "', format = 'ORC')");
            assertThat(query("SELECT * FROM " + tableName)).matches("SELECT * FROM tpch.tiny.region");

            assertUpdate("DROP TABLE " + tableName);
        }
        finally {
            fileSystem.deleteDirectory(Location.of(location));
        }
    }

    static String requireEnv(String variable)
    {
        return requireNonNull(System.getenv(variable), () -> "environment variable not set: " + variable);
    }
}

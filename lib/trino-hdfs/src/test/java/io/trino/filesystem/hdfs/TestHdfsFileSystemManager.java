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
package io.trino.filesystem.hdfs;

import com.google.common.collect.ImmutableMap;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingNodeManager;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class TestHdfsFileSystemManager
{
    @Test
    void testManager()
            throws IOException
    {
        HdfsFileSystemManager manager = new HdfsFileSystemManager(
                ImmutableMap.<String, String>builder()
                        .put("unused-property", "ignored")
                        .put("hive.dfs.verify-checksum", "false")
                        .put("hive.s3.region", "us-west-1")
                        .buildOrThrow(),
                true,
                true,
                true,
                "test",
                new TestingNodeManager(),
                OpenTelemetry.noop());

        Set<String> used = manager.configure();
        assertThat(used).containsExactly("hive.dfs.verify-checksum", "hive.s3.region");

        TrinoFileSystemFactory factory = manager.create();
        TrinoFileSystem fileSystem = factory.create(ConnectorIdentity.ofUser("test"));

        Location location = Location.of("/tmp/" + UUID.randomUUID());
        assertThat(fileSystem.newInputFile(location).exists()).isFalse();

        manager.stop();
    }
}

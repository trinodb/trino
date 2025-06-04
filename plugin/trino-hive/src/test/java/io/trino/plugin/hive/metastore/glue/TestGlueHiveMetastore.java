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
package io.trino.plugin.hive.metastore.glue;

import io.trino.metastore.HiveMetastore;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.plugin.hive.metastore.AbstractTestHiveMetastore;
import org.junit.jupiter.api.AfterAll;

import java.io.IOException;
import java.nio.file.Path;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.metastore.glue.TestingGlueHiveMetastore.createTestingGlueHiveMetastore;
import static java.nio.file.Files.createTempDirectory;

final class TestGlueHiveMetastore
        extends AbstractTestHiveMetastore
{
    private final AutoCloseableCloser closer = AutoCloseableCloser.create();
    private final Path tempDir;
    private final GlueHiveMetastore metastore;

    TestGlueHiveMetastore()
            throws IOException
    {
        tempDir = createTempDirectory("test");
        metastore = createTestingGlueHiveMetastore(tempDir, closer::register);
    }

    @AfterAll
    void tearDown()
            throws Exception
    {
        metastore.shutdown();
        deleteRecursively(tempDir, ALLOW_INSECURE);
        closer.close();
    }

    @Override
    protected HiveMetastore getMetastore()
    {
        return metastore;
    }
}

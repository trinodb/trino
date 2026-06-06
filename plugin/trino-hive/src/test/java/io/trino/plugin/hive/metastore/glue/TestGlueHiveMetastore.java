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
import io.trino.plugin.hive.FlociS3AndGlue;
import io.trino.plugin.hive.metastore.AbstractTestHiveMetastore;
import org.junit.jupiter.api.AfterAll;

import java.net.URI;

import static io.trino.plugin.hive.metastore.glue.TestingGlueHiveMetastore.createTestingGlueHiveMetastore;
import static io.trino.testing.TestingNames.randomNameSuffix;

final class TestGlueHiveMetastore
        extends AbstractTestHiveMetastore
{
    private final AutoCloseableCloser closer = AutoCloseableCloser.create();
    private final GlueHiveMetastore metastore;

    TestGlueHiveMetastore()
            throws Exception
    {
        FlociS3AndGlue floci = closer.register(new FlociS3AndGlue());
        String bucketName = "test-glue-hive-metastore-" + randomNameSuffix();
        floci.createBucket(bucketName);
        metastore = createTestingGlueHiveMetastore(URI.create("s3://%s/".formatted(bucketName)), closer::register, floci, false);
    }

    @AfterAll
    void tearDown()
            throws Exception
    {
        closer.close();
    }

    @Override
    protected HiveMetastore getMetastore()
    {
        return metastore;
    }
}

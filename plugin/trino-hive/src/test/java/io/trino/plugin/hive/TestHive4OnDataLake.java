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
package io.trino.plugin.hive;

import io.trino.plugin.hive.containers.Hive4MinioDataLake;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.junit.jupiter.api.Assumptions.abort;

@Execution(ExecutionMode.SAME_THREAD) // TODO Make custom hive4 image to support running queries concurrently
class TestHive4OnDataLake
        extends BaseTestHiveOnDataLake
{
    private static final String BUCKET_NAME = "test-hive-insert-overwrite-" + randomNameSuffix();

    public TestHive4OnDataLake()
    {
        super(BUCKET_NAME, new Hive4MinioDataLake(BUCKET_NAME));
    }

    @Override
    @Test
    public void testSyncPartitionOnBucketRoot()
    {
        // https://github.com/trinodb/trino/issues/24453
        abort("Fails with `location must not be root path`");
    }

    @Override
    @Test
    public void testUnpartitionedTableExternalLocationOnTopOfTheBucket()
    {
        // https://github.com/trinodb/trino/issues/24453
        abort("Fails with `location must not be root path`");
    }

    @Override
    @Test
    public void testPartitionedTableExternalLocationOnTopOfTheBucket()
    {
        // https://github.com/trinodb/trino/issues/24453
        abort("Fails with `location must not be root path`");
    }

    @Override
    @Test
    public void testInsertOverwritePartitionedAndBucketedAcidTable()
    {
        // https://github.com/trinodb/trino/issues/24454
        abort("Fails with `Processor has no capabilities, cannot create an ACID table`");
    }
}

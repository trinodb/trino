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
package io.trino.plugin.hive.metastore;

import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestBridgingHiveMetastore
        extends AbstractTestHiveMetastore
{
    private final HiveHadoop hiveHadoop;

    TestBridgingHiveMetastore()
    {
        hiveHadoop = HiveHadoop.builder().build();
        hiveHadoop.start();

        setMetastore(new BridgingHiveMetastore(testingThriftHiveMetastoreBuilder()
                .metastoreClient(hiveHadoop.getHiveMetastoreEndpoint())
                .thriftMetastoreConfig(new ThriftMetastoreConfig().setDeleteFilesOnDrop(true))
                .build()));
    }

    @AfterAll
    void afterAll()
    {
        hiveHadoop.stop();
    }
}

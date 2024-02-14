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
package io.trino.plugin.hive.metastore.thrift;

import io.trino.hive.thrift.metastore.TableMeta;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveWithDisabledBatchFetch
{
    @Test
    public void testBatchEnabled()
    {
        ThriftMetastore thriftMetastore = prepareThriftMetastore(true);
        assertThat(thriftMetastore.getAllTables()).isPresent();
    }

    @Test
    public void testBatchDisabled()
    {
        ThriftMetastore thriftMetastore = prepareThriftMetastore(false);
        assertThat(thriftMetastore.getAllTables()).isEmpty();
    }

    @Test
    public void testFallbackInCaseOfMetastoreFailure()
    {
        ThriftMetastore thriftMetastore = testingThriftHiveMetastoreBuilder()
                .thriftMetastoreConfig(new ThriftMetastoreConfig().setBatchMetadataFetchEnabled(true))
                .metastoreClient(createFailingMetastoreClient())
                .build();

        assertThat(thriftMetastore.getAllTables()).isEmpty();
    }

    private static ThriftMetastore prepareThriftMetastore(boolean enabled)
    {
        return testingThriftHiveMetastoreBuilder()
                .thriftMetastoreConfig(new ThriftMetastoreConfig().setBatchMetadataFetchEnabled(enabled))
                .metastoreClient(new MockThriftMetastoreClient())
                .build();
    }

    private static ThriftMetastoreClient createFailingMetastoreClient()
    {
        return new MockThriftMetastoreClient()
        {
            @Override
            public List<TableMeta> getTableMeta(Optional<String> databaseName)
                    throws TException
            {
                throw new TTransportException();
            }
        };
    }
}

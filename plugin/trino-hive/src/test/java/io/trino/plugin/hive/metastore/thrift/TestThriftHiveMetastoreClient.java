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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.filesystem.Location;
import io.trino.filesystem.memory.MemoryFileSystem;
import io.trino.hive.thrift.metastore.FieldSchema;
import io.trino.hive.thrift.metastore.NoSuchObjectException;
import io.trino.hive.thrift.metastore.SerDeInfo;
import io.trino.hive.thrift.metastore.StorageDescriptor;
import io.trino.hive.thrift.metastore.Table;
import io.trino.plugin.hive.metastore.thrift.ThriftHiveMetastoreClient.AlternativeCall;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestThriftHiveMetastoreClient
{
    @Test
    public void testAlternativeCall()
            throws TException
    {
        AtomicInteger connectionCount = new AtomicInteger();
        AtomicInteger chosenOption = new AtomicInteger(Integer.MAX_VALUE);

        AlternativeCall<String> failure = () -> {
            throw new RuntimeException("yay! exception");
        };
        AlternativeCall<String> success1 = () -> "first";
        AlternativeCall<String> success2 = () -> "second";

        ThriftHiveMetastoreClient client = new ThriftHiveMetastoreClient(
                () -> {
                    connectionCount.incrementAndGet();
                    return new TTransportMock();
                },
                "dummy",
                Optional.empty(),
                new MetastoreSupportsDateStatistics(),
                new AtomicInteger(),
                new AtomicInteger(),
                new AtomicInteger(),
                new AtomicInteger());
        assertThat(connectionCount.get()).isEqualTo(1);

        assertThat(client.alternativeCall(_ -> false, chosenOption, success1, success2, failure))
                .isEqualTo("first");
        assertThat(connectionCount.get()).isEqualTo(1);
        assertThat(chosenOption.get()).isEqualTo(0);

        chosenOption.set(Integer.MAX_VALUE);
        assertThat(client.alternativeCall(_ -> false, chosenOption, failure, success1, success2))
                .isEqualTo("first");
        assertThat(connectionCount.get()).isEqualTo(2);
        assertThat(chosenOption.get()).isEqualTo(1);

        assertThat(client.alternativeCall(_ -> false, chosenOption, failure, success1, success2))
                .isEqualTo("first");
        // Alternative call should use chosenOption
        assertThat(connectionCount.get()).isEqualTo(2);
    }

    private static class TTransportMock
            extends TTransport
    {
        @Override
        public boolean isOpen()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void open()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        {
            // Do nothing
        }

        @Override
        public int read(byte[] bytes, int i, int i1)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void write(byte[] bytes, int i, int i1)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TConfiguration getConfiguration()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateKnownMessageSize(long size)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkReadBytesAvailable(long numBytes)
        {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    void testDropTableWhenThrowExceptionDuringRemoveDataFromS3()
    {
        AtomicInteger getTableCalls = new AtomicInteger();
        AtomicBoolean directoryDeleted = new AtomicBoolean();

        ThriftMetastoreClient client = createClient(getTableCalls);

        ThriftHiveMetastore metastore = new ThriftHiveMetastore(
                Optional.empty(),
                _ -> new MemoryFileSystem() {
                    @Override
                    public void deleteDirectory(Location location)
                    {
                        directoryDeleted.set(true);
                    }
                },
                _ -> client,
                2.0,
                new Duration(1, MILLISECONDS),
                new Duration(10, MILLISECONDS),
                new Duration(30, SECONDS),
                new Duration(10, SECONDS),
                3,
                true,
                false,
                false,
                new ThriftMetastoreStats(),
                newSingleThreadExecutor());

        metastore.dropTable("test_schema", "test_table", true);

        assertThat(directoryDeleted.get())
                .describedAs("Directory must be deleted on retry when HMS already dropped the table "
                        + "but Trino got TTransportException (socket timeout)")
                .isTrue();
    }

    private static ThriftMetastoreClient createClient(AtomicInteger getTableCalls)
    {
        // We want to reproduce case
        // 1. call getTable -> return successfully that table exists
        // 2. call dropTable -> the table successfully deleted from database, but throw exception 'Socket is closed by peer' during removing data from s3
        // 3. call getTable -> return that table not found because was deleted on the previous step
        InvocationHandler handler = (_, method, _) -> switch (method.getName()) {
            case "getTable" -> {
                if (getTableCalls.incrementAndGet() == 1) {
                    yield managedTable();
                }
                throw new NoSuchObjectException("table not found");
            }
            case "dropTable" -> {
                throw new TTransportException("Socket is closed by peer.");
            }
            case "close" -> null;
            default -> throw new UnsupportedOperationException(method.getName());
        };

        return (ThriftMetastoreClient) Proxy.newProxyInstance(
                ThriftMetastoreClient.class.getClassLoader(),
                new Class<?>[] {ThriftMetastoreClient.class},
                handler);
    }

    private static Table managedTable()
    {
        Table table = new Table();
        table.setDbName("test_db");
        table.setTableName("test_table");
        table.setTableType(MANAGED_TABLE.name());
        table.setSd(new StorageDescriptor(
                ImmutableList.of(new FieldSchema("id", "bigint", "")),
                "s3a://trino-sandbox/test_schema/test_table",
                null, null, false, 0,
                new SerDeInfo("test_table", null, ImmutableMap.of()),
                null, null, ImmutableMap.of()));
        table.setOwner("test");
        table.setParameters(ImmutableMap.of());
        return table;
    }
}

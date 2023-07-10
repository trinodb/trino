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

import io.trino.plugin.hive.metastore.thrift.ThriftHiveMetastoreClient.AlternativeCall;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

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
        AlternativeCall<String> sucesss1 = () -> "first";
        AlternativeCall<String> sucesss2 = () -> "second";

        ThriftHiveMetastoreClient client = new ThriftHiveMetastoreClient(
                () -> {
                    connectionCount.incrementAndGet();
                    return new TTransportMock();
                },
                "dummy",
                new MetastoreSupportsDateStatistics(),
                new AtomicInteger(),
                new AtomicInteger(),
                new AtomicInteger(),
                new AtomicInteger(),
                new AtomicInteger(),
                new AtomicInteger(),
                new AtomicInteger());
        assertThat(connectionCount.get()).isEqualTo(1);

        assertThat(client.alternativeCall(e -> false, chosenOption, sucesss1, sucesss2, failure))
                .isEqualTo("first");
        assertThat(connectionCount.get()).isEqualTo(1);
        assertThat(chosenOption.get()).isEqualTo(0);

        chosenOption.set(Integer.MAX_VALUE);
        assertThat(client.alternativeCall(e -> false, chosenOption, failure, sucesss1, sucesss2))
                .isEqualTo("first");
        assertThat(connectionCount.get()).isEqualTo(2);
        assertThat(chosenOption.get()).isEqualTo(1);

        assertThat(client.alternativeCall(e -> false, chosenOption, failure, sucesss1, sucesss2))
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
}

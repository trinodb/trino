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

import io.trino.filesystem.Location;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.HdfsAuthentication;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHdfsOutputStreamAuthentication
{
    @Test
    public void testWritesRunThroughHdfsEnvironmentAuthentication()
            throws IOException
    {
        CountingHdfsAuthentication authentication = new CountingHdfsAuthentication();
        HdfsEnvironment environment = new HdfsEnvironment(
                new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(new HdfsConfig()), emptySet()),
                new HdfsConfig(),
                authentication);

        ByteArrayOutputStream delegate = new ByteArrayOutputStream();
        try (HdfsOutputStream output = new HdfsOutputStream(
                Location.of("hdfs://hadoop-master/test"),
                new FSDataOutputStream(delegate, null),
                environment,
                new HdfsContext(ConnectorIdentity.ofUser("test")))) {
            output.write(1);
            output.write(new byte[] {2, 3, 4}, 1, 2);
        }

        assertThat(delegate.toByteArray()).containsExactly(1, 3, 4);
        assertThat(authentication.getInvocations()).isEqualTo(2);
    }

    private static class CountingHdfsAuthentication
            implements HdfsAuthentication
    {
        private final AtomicInteger invocations = new AtomicInteger();

        @Override
        public <T> T doAs(ConnectorIdentity identity, ExceptionAction<T> action)
                throws IOException
        {
            invocations.incrementAndGet();
            return action.run();
        }

        public int getInvocations()
        {
            return invocations.get();
        }
    }
}

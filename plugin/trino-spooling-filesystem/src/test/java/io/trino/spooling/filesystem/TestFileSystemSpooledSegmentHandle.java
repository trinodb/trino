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
package io.trino.spooling.filesystem;

import io.trino.spi.QueryId;
import io.trino.spi.protocol.SpoolingContext;
import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.Arrays;

import static java.time.temporal.ChronoUnit.MILLIS;
import static org.assertj.core.api.Assertions.assertThat;

class TestFileSystemSpooledSegmentHandle
{
    private static final SecureRandom random = new SecureRandom();
    private static final QueryId queryId = new QueryId("query_id");
    private static final SpoolingContext context = new SpoolingContext("encoding", queryId, 100, 1000);

    private final Instant now = Instant.now()
            .truncatedTo(MILLIS); // ULID retains millisecond precision

    @Test
    public void testStorageObjectNameStability()
    {
        Instant expireAt = Instant.ofEpochMilli(90000);
        FileSystemSpooledSegmentHandle handle = FileSystemSpooledSegmentHandle.random(new NotARandomAtAll(), context, expireAt);
        assertThat(handle.storageObjectName())
                .isEqualTo("0000002QWG0G2081040G208104::query_id");
    }

    @Test
    public void testLexicalOrdering()
    {
        FileSystemSpooledSegmentHandle handle1 = FileSystemSpooledSegmentHandle.random(random, context, now.plusMillis(1));
        FileSystemSpooledSegmentHandle handle2 = FileSystemSpooledSegmentHandle.random(random, context, now.plusMillis(3));
        FileSystemSpooledSegmentHandle handle3 = FileSystemSpooledSegmentHandle.random(random, context, now.plusMillis(2));

        assertThat(handle2.storageObjectName())
                .isGreaterThan(handle1.storageObjectName());

        assertThat(handle3.storageObjectName())
                .isLessThan(handle2.storageObjectName())
                .isGreaterThan(handle1.storageObjectName());

        assertThat(handle1.storageObjectName())
                .isLessThan(handle2.storageObjectName())
                .isLessThan(handle3.storageObjectName());
    }

    private static class NotARandomAtAll
            extends SecureRandom
    {
        @Override
        public void nextBytes(byte[] bytes)
        {
            Arrays.fill(bytes, (byte) 4); // chosen by fair dice roll, guaranteed to be random
        }
    }
}

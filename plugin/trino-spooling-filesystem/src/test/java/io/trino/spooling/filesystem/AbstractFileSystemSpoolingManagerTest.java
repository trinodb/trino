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

import io.azam.ulidj.ULID;
import io.trino.filesystem.encryption.EncryptionKey;
import io.trino.spi.QueryId;
import io.trino.spi.protocol.SpooledLocation;
import io.trino.spi.protocol.SpooledSegmentHandle;
import io.trino.spi.protocol.SpoolingContext;
import io.trino.spi.protocol.SpoolingManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;

import static io.trino.filesystem.encryption.EncryptionKey.randomAes256;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public abstract class AbstractFileSystemSpoolingManagerTest
{
    @Test
    public void testRetrieveSpooledSegment()
            throws Exception
    {
        SpoolingManager manager = getSpoolingManager();
        SpoolingContext context = new SpoolingContext("json", QueryId.valueOf("a"), 0, 0);
        SpooledSegmentHandle spooledSegmentHandle = manager.create(context);
        try (OutputStream segment = manager.createOutputStream(spooledSegmentHandle)) {
            segment.write("data".getBytes(UTF_8));
        }

        try (InputStream output = manager.openInputStream(spooledSegmentHandle)) {
            byte[] buffer = new byte[4];
            assertThat(output.read(buffer)).isEqualTo(buffer.length);
            assertThat(buffer).isEqualTo("data".getBytes(UTF_8));
        }
    }

    @Test
    public void testAcknowledgedSegmentCantBeRetrievedAgain()
            throws Exception
    {
        SpoolingManager manager = getSpoolingManager();
        SpoolingContext context = new SpoolingContext("json", QueryId.valueOf("a"), 0, 0);
        SpooledSegmentHandle spooledSegmentHandle = manager.create(context);
        try (OutputStream segment = manager.createOutputStream(spooledSegmentHandle)) {
            segment.write("data".getBytes(UTF_8));
        }

        try (InputStream output = manager.openInputStream(spooledSegmentHandle)) {
            byte[] buffer = new byte[4];
            assertThat(output.read(buffer)).isEqualTo(buffer.length);
            assertThat(buffer).isEqualTo("data".getBytes(UTF_8));
        }

        manager.acknowledge(spooledSegmentHandle);
        assertThatThrownBy(() -> manager.openInputStream(spooledSegmentHandle).read())
                .isInstanceOf(IOException.class)
                .hasMessage("Segment not found or expired");
    }

    @Test
    public void testHandleRoundTrip()
    {
        EncryptionKey key = randomAes256();
        FileSystemSpooledSegmentHandle handle = new FileSystemSpooledSegmentHandle("json", QueryId.valueOf("a"), ULID.randomBinary(), Optional.of(key));
        SpooledLocation location = getSpoolingManager().location(handle);
        FileSystemSpooledSegmentHandle handle2 = (FileSystemSpooledSegmentHandle) getSpoolingManager().handle(location);

        assertThat(handle.queryId()).isEqualTo(handle2.queryId());
        assertThat(handle.storageObjectName()).isEqualTo(handle2.storageObjectName());
        assertThat(handle.uuid()).isEqualTo(handle2.uuid());
        assertThat(handle.expirationTime()).isEqualTo(handle2.expirationTime());
        assertThat(handle2.encryptionKey()).isPresent().hasValue(key);
    }

    protected abstract SpoolingManager getSpoolingManager();
}

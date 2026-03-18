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
package io.trino.exchange;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.BufferState;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.execution.buffer.OutputBufferInfo;
import io.trino.execution.buffer.OutputBufferStatus;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
import io.trino.operator.OperatorInfo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.util.Ciphers.createRandomAesEncryptionKey;
import static io.trino.util.Ciphers.serializeAesEncryptionKey;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExchangeEncryptionKey
{
    private static final Slice ENCRYPTION_KEY = serializeAesEncryptionKey(createRandomAesEncryptionKey());

    @Test
    public void testSpoolingBufferAlwaysEncrypted()
    {
        Session session = createSessionWithEncryption();
        Optional<Slice> key = ExchangeEncryptionKey.keyFor(session, spoolingBuffer());
        assertThat(key).hasValue(ENCRYPTION_KEY);
    }

    @Test
    public void testDirectBufferSkipsEncryption()
    {
        Session session = createSessionWithEncryption();
        Optional<Slice> key = ExchangeEncryptionKey.keyFor(session, directBuffer());
        assertThat(key).isEmpty();
    }

    @Test
    public void testNoKeyReturnsEmpty()
    {
        Session session = createSessionWithoutEncryption();
        assertThat(ExchangeEncryptionKey.keyFor(session, spoolingBuffer())).isEmpty();
        assertThat(ExchangeEncryptionKey.keyFor(session, directBuffer())).isEmpty();
        assertThat(ExchangeEncryptionKey.keyForDirectExchange(session)).isEmpty();
    }

    @Test
    public void testSpoolingDataSourceAlwaysEncrypted()
    {
        Session session = createSessionWithEncryption();
        Optional<Slice> key = ExchangeEncryptionKey.keyFor(session, spoolingDataSource());
        assertThat(key).hasValue(ENCRYPTION_KEY);
    }

    @Test
    public void testDirectDataSourceSkipsEncryption()
    {
        Session session = createSessionWithEncryption();
        Optional<Slice> key = ExchangeEncryptionKey.keyFor(session, directDataSource());
        assertThat(key).isEmpty();
    }

    @Test
    public void testKeyForDirectExchangeSkipsEncryption()
    {
        Session session = createSessionWithEncryption();
        assertThat(ExchangeEncryptionKey.keyForDirectExchange(session)).isEmpty();
    }

    private static Session createSessionWithEncryption()
    {
        return testSessionBuilder()
                .build()
                .withExchangeEncryption(ENCRYPTION_KEY);
    }

    private static Session createSessionWithoutEncryption()
    {
        return testSessionBuilder()
                .build();
    }

    private static OutputBuffer spoolingBuffer()
    {
        return new StubOutputBuffer(true);
    }

    private static OutputBuffer directBuffer()
    {
        return new StubOutputBuffer(false);
    }

    private static ExchangeDataSource spoolingDataSource()
    {
        return new StubExchangeDataSource(true);
    }

    private static ExchangeDataSource directDataSource()
    {
        return new StubExchangeDataSource(false);
    }

    private record StubOutputBuffer(boolean usesExternalStorage)
            implements OutputBuffer
    {
        @Override
        public OutputBufferInfo getInfo()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public BufferState getState()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getUtilization()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public OutputBufferStatus getStatus()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addStateChangeListener(StateChangeListener<BufferState> stateChangeListener) {}

        @Override
        public void setOutputBuffers(OutputBuffers newOutputBuffers) {}

        @Override
        public ListenableFuture<BufferResult> get(OutputBufferId bufferId, long token, DataSize maxSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void acknowledge(OutputBufferId bufferId, long token) {}

        @Override
        public void destroy(OutputBufferId bufferId) {}

        @Override
        public ListenableFuture<Void> isFull()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void enqueue(List<Slice> pages) {}

        @Override
        public void enqueue(int partition, List<Slice> pages) {}

        @Override
        public void setNoMorePages() {}

        @Override
        public void destroy() {}

        @Override
        public void abort() {}

        @Override
        public long getPeakMemoryUsage()
        {
            return 0;
        }

        @Override
        public Optional<Throwable> getFailureCause()
        {
            return Optional.empty();
        }
    }

    private record StubExchangeDataSource(boolean usesExternalStorage)
            implements ExchangeDataSource
    {
        @Override
        public Slice pollPage()
        {
            return null;
        }

        @Override
        public boolean isFinished()
        {
            return true;
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            return immediateVoidFuture();
        }

        @Override
        public void addInput(ExchangeInput input) {}

        @Override
        public void noMoreInputs() {}

        @Override
        public OperatorInfo getInfo()
        {
            return null;
        }

        @Override
        public boolean usesExternalStorage()
        {
            return usesExternalStorage;
        }

        @Override
        public void close() {}
    }
}

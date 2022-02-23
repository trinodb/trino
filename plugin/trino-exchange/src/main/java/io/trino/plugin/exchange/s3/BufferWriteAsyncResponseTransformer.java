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
package io.trino.plugin.exchange.s3;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.core.internal.async.ByteArrayAsyncResponseTransformer;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.System.arraycopy;
import static java.util.Objects.requireNonNull;
import static software.amazon.awssdk.utils.FunctionalUtils.invokeSafely;

/**
 *  An implementation of {@link AsyncResponseTransformer} that writes data to the specified range of the given buffer.
 *  This class mimics the implementation of {@link ByteArrayAsyncResponseTransformer} but avoids memory copying.
 *
 * {@link AsyncResponseTransformer} that writes the data to the specified range of the given buffer
 *
 * @param <ResponseT> Response POJO type.
 */
public final class BufferWriteAsyncResponseTransformer<ResponseT>
        implements AsyncResponseTransformer<ResponseT, ResponseT>
{
    private final byte[] buffer;
    private final int offset;

    private volatile CompletableFuture<Void> cf;
    private volatile ResponseT response;

    public BufferWriteAsyncResponseTransformer(byte[] buffer, int offset)
    {
        checkArgument(offset < buffer.length, "Buffer offset should be smaller than buffer length");
        this.buffer = requireNonNull(buffer, "buffer is null");
        this.offset = offset;
    }

    @Override
    public CompletableFuture<ResponseT> prepare()
    {
        cf = new CompletableFuture<>();
        return cf.thenApply(ignored -> response);
    }

    @Override
    public void onResponse(ResponseT response)
    {
        this.response = response;
    }

    @Override
    public void onStream(SdkPublisher<ByteBuffer> publisher)
    {
        publisher.subscribe(new BufferSubscriber(buffer, offset, cf));
    }

    @Override
    public void exceptionOccurred(Throwable throwable)
    {
        cf.completeExceptionally(throwable);
    }

    /**
     * {@link Subscriber} implementation that writes chunks to given range of a buffer.
     */
    static class BufferSubscriber
            implements Subscriber<ByteBuffer>
    {
        private int offset;

        private final byte[] buffer;
        private final CompletableFuture<Void> future;

        private Subscription subscription;

        BufferSubscriber(byte[] buffer, int offset, CompletableFuture<Void> future)
        {
            // this assignment is expected to be followed by an assignment of a final field to ensure safe publication
            this.offset = offset;

            this.buffer = requireNonNull(buffer, "buffer is null");
            this.future = requireNonNull(future, "future is null");
        }

        @Override
        public void onSubscribe(Subscription s)
        {
            if (this.subscription != null) {
                s.cancel();
                return;
            }
            this.subscription = s;
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(ByteBuffer byteBuffer)
        {
            invokeSafely(() -> {
                int readableBytes = byteBuffer.remaining();
                if (byteBuffer.hasArray()) {
                    arraycopy(byteBuffer.array(), byteBuffer.arrayOffset() + byteBuffer.position(), buffer, offset, readableBytes);
                }
                else {
                    byteBuffer.asReadOnlyBuffer().get(buffer, offset, readableBytes);
                }
                offset += readableBytes;
            });
            subscription.request(1);
        }

        @Override
        public void onError(Throwable throwable)
        {
            future.completeExceptionally(throwable);
        }

        @Override
        public void onComplete()
        {
            future.complete(null);
        }
    }

    static <ResponseT> AsyncResponseTransformer<ResponseT, ResponseT> toBufferWrite(byte[] buffer, int offset)
    {
        return new BufferWriteAsyncResponseTransformer<>(buffer, offset);
    }
}

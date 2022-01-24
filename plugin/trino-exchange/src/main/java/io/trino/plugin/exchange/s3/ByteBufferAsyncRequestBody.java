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

import io.airlift.log.Logger;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.internal.async.ByteArrayAsyncRequestBody;
import software.amazon.awssdk.core.internal.util.Mimetype;

import java.nio.ByteBuffer;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * This class mimics the implementation of {@link ByteArrayAsyncRequestBody} except for we use a ByteBuffer
 * to avoid unnecessary memory copy
 *
 * An implementation of {@link AsyncRequestBody} for providing data from memory. This is created using static
 * methods on {@link AsyncRequestBody}
 *
 * @see AsyncRequestBody#fromBytes(byte[])
 * @see AsyncRequestBody#fromByteBuffer(ByteBuffer)
 * @see AsyncRequestBody#fromString(String)
 */
public final class ByteBufferAsyncRequestBody
        implements AsyncRequestBody
{
    private static final Logger log = Logger.get(ByteBufferAsyncRequestBody.class);

    private final ByteBuffer byteBuffer;

    private final String mimetype;

    public ByteBufferAsyncRequestBody(ByteBuffer byteBuffer, String mimetype)
    {
        this.byteBuffer = requireNonNull(byteBuffer, "byteBuffer is null");
        this.mimetype = requireNonNull(mimetype, "mimetype is null");
    }

    @Override
    public Optional<Long> contentLength()
    {
        return Optional.of((long) byteBuffer.remaining());
    }

    @Override
    public String contentType()
    {
        return mimetype;
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> s)
    {
        // As per rule 1.9 we must throw NullPointerException if the subscriber parameter is null
        if (s == null) {
            throw new NullPointerException("Subscription MUST NOT be null.");
        }

        // As per 2.13, this method must return normally (i.e. not throw).
        try {
            s.onSubscribe(
                    new Subscription() {
                        private boolean done;

                        @Override
                        public void request(long n)
                        {
                            if (done) {
                                return;
                            }
                            if (n > 0) {
                                done = true;
                                s.onNext(byteBuffer.asReadOnlyBuffer());
                                s.onComplete();
                            }
                            else {
                                s.onError(new IllegalArgumentException("§3.9: non-positive requests are not allowed!"));
                            }
                        }

                        @Override
                        public void cancel()
                        {
                            synchronized (this) {
                                if (!done) {
                                    done = true;
                                }
                            }
                        }
                    });
        }
        catch (Throwable ex) {
            log.error(ex, " violated the Reactive Streams rule 2.13 by throwing an exception from onSubscribe.");
        }
    }

    static AsyncRequestBody fromByteBuffer(ByteBuffer byteBuffer)
    {
        return new ByteBufferAsyncRequestBody(byteBuffer, Mimetype.MIMETYPE_OCTET_STREAM);
    }
}

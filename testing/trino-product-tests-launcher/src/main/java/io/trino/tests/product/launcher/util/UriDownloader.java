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
package io.trino.tests.product.launcher.util;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;
import okio.BufferedSink;
import okio.BufferedSource;
import okio.ForwardingSource;
import okio.Okio;
import okio.Source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.function.Consumer;

import static java.lang.Math.toIntExact;
import static okio.Okio.buffer;

// Based on https://github.com/square/okhttp/blob/f9901627431be098ad73abd725fbb3738747461c/samples/guide/src/main/java/okhttp3/recipes/Progress.java
public class UriDownloader
{
    private UriDownloader() {}

    public static void download(String location, Path target, Consumer<Integer> progressListener)
    {
        OkHttpClient client = clientWithProgressListener(progressListener);
        Request request = new Request.Builder().url(location).build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Could not download file " + location + "(response: " + response + ")");
            }

            try (BufferedSink bufferedSink = Okio.buffer(Okio.sink(target))) {
                bufferedSink.writeAll(response.body().source());
                bufferedSink.flush();
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static OkHttpClient clientWithProgressListener(Consumer<Integer> progressListener)
    {
        return new OkHttpClient.Builder()
                .addNetworkInterceptor(chain -> {
                    Response originalResponse = chain.proceed(chain.request());
                    return originalResponse.newBuilder()
                            .body(new ProgressResponseBody(originalResponse.body(), listener(progressListener)))
                            .build();
                }).build();
    }

    private static ProgressListener listener(Consumer<Integer> progressConsumer)
    {
        return new ProgressListener()
        {
            boolean firstUpdate = true;

            @Override
            public void update(long bytesRead, long contentLength, boolean done)
            {
                if (done) {
                    progressConsumer.accept(100);
                }
                else {
                    if (firstUpdate) {
                        progressConsumer.accept(0);
                        firstUpdate = false;
                    }

                    if (contentLength != -1) {
                        progressConsumer.accept(toIntExact((100 * bytesRead) / contentLength));
                    }
                }
            }
        };
    }

    private static class ProgressResponseBody
            extends ResponseBody
    {
        private final ResponseBody responseBody;
        private final ProgressListener progressListener;
        private BufferedSource bufferedSource;

        ProgressResponseBody(ResponseBody responseBody, ProgressListener progressListener)
        {
            this.responseBody = responseBody;
            this.progressListener = progressListener;
        }

        @Override
        public MediaType contentType()
        {
            return responseBody.contentType();
        }

        @Override
        public long contentLength()
        {
            return responseBody.contentLength();
        }

        @Override
        public BufferedSource source()
        {
            if (bufferedSource == null) {
                bufferedSource = buffer(source(responseBody.source()));
            }
            return bufferedSource;
        }

        private Source source(Source source)
        {
            return new ForwardingSource(source) {
                long totalBytesRead;

                @Override
                public long read(Buffer sink, long byteCount)
                        throws IOException
                {
                    long bytesRead = super.read(sink, byteCount);
                    // read() returns the number of bytes read, or -1 if this source is exhausted.
                    totalBytesRead += bytesRead != -1 ? bytesRead : 0;
                    progressListener.update(totalBytesRead, responseBody.contentLength(), bytesRead == -1);
                    return bytesRead;
                }
            };
        }
    }

    @FunctionalInterface
    private interface ProgressListener
    {
        void update(long bytesRead, long contentLength, boolean done);
    }
}

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
package io.trino.client;

import io.trino.client.spooling.SegmentLoader;
import io.trino.client.spooling.SpooledSegment;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.google.common.net.HttpHeaders.ACCEPT_ENCODING;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OkHttpSegmentLoader
        implements SegmentLoader
{
    private static final Logger logger = Logger.getLogger(SegmentLoader.class.getPackage().getName());

    private final Call.Factory callFactory;

    public OkHttpSegmentLoader()
    {
        this(new OkHttpClient());
    }

    public OkHttpSegmentLoader(Call.Factory callFactory)
    {
        this.callFactory = requireNonNull(callFactory, "callFactory is null");
    }

    @Override
    public InputStream load(SpooledSegment segment)
            throws IOException
    {
        Request request = new Request.Builder()
                .url(segment.getDataUri().toString())
                .headers(toHeaders(segment.getHeaders()))
                .addHeader(ACCEPT_ENCODING, "identity")
                .build();

        Response response = callFactory.newCall(request).execute();
        if (response.body() == null) {
            throw new IOException("Could not open segment for streaming, got empty body");
        }

        if (response.isSuccessful()) {
            return response.body().byteStream();
        }
        throw new IOException(format("Could not open segment for streaming, got error '%s' with code %d", response.message(), response.code()));
    }

    @Override
    public void acknowledge(SpooledSegment segment)
    {
        Request ackRequest = new Request.Builder()
                .get()
                .url(segment.getAckUri().toString())
                .headers(toHeaders(segment.getHeaders()))
                .build();

        callFactory.newCall(ackRequest).enqueue(new Callback()
        {
            @Override
            public void onFailure(Call call, IOException cause)
            {
                logger.log(Level.WARNING, "Could not acknowledge spooled segment", cause);
            }

            @Override
            public void onResponse(Call call, Response response)
            {
                response.close();
            }
        });
    }

    private static Headers toHeaders(Map<String, List<String>> headers)
    {
        Headers.Builder builder = new Headers.Builder();
        headers.forEach((key, values) -> values.forEach(value -> builder.add(key, value)));
        return builder.build();
    }

    @Override
    public void close()
    {
        if (callFactory instanceof OkHttpClient) {
            ((OkHttpClient) callFactory).dispatcher().executorService().shutdown();
        }
    }
}

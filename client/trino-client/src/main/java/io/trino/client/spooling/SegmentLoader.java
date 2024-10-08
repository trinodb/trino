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
package io.trino.client.spooling;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SegmentLoader
        implements AutoCloseable
{
    private static final Logger logger = Logger.getLogger(SegmentLoader.class.getPackage().getName());

    private final Call.Factory callFactory;

    public SegmentLoader()
    {
        this(new OkHttpClient());
    }

    public SegmentLoader(Call.Factory callFactory)
    {
        this.callFactory = requireNonNull(callFactory, "callFactory is null");
    }

    public InputStream load(SpooledSegment segment)
            throws IOException
    {
        return loadFromURI(segment.getDataUri(), segment.getHeaders());
    }

    public InputStream loadFromURI(URI segmentUri, Map<String, List<String>> headers)
            throws IOException
    {
        Headers requestHeaders = toHeaders(headers);
        Request request = new Request.Builder()
                .url(segmentUri.toString())
                .headers(requestHeaders)
                .build();

        Response response = callFactory.newCall(request).execute();
        if (response.body() == null) {
            throw new IOException("Could not open segment for streaming, got empty body");
        }

        if (response.isSuccessful()) {
            return delegatingInputStream(response, response.body().byteStream(), segmentUri, requestHeaders);
        }
        throw new IOException(format("Could not open segment for streaming, got error '%s' with code %d", response.message(), response.code()));
    }

    private void delete(URI segmentUri, Headers headers)
    {
        Request deleteRequest = new Request.Builder()
                .delete()
                .url(segmentUri.toString())
                .headers(headers)
                .build();

        callFactory.newCall(deleteRequest).enqueue(new Callback()
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

    private InputStream delegatingInputStream(Response response, InputStream delegate, URI segmentUri, Headers headers)
    {
        return new FilterInputStream(delegate)
        {
            @Override
            public void close()
                    throws IOException
            {
                try (Response ignored = response; InputStream ignored2 = delegate) {
                    delete(segmentUri, headers);
                }
            }
        };
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

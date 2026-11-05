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
package io.trino.plugin.opensearch.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.opensearch.AwsSecurityConfig.DeploymentType;
import org.apache.hc.client5.http.async.AsyncExecCallback;
import org.apache.hc.client5.http.async.AsyncExecChain;
import org.apache.hc.client5.http.async.AsyncExecChainHandler;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.nio.AsyncEntityProducer;
import org.apache.hc.core5.http.nio.DataStreamChannel;
import org.apache.hc.core5.net.URIAuthority;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.stream;
import static org.apache.hc.core5.http.HttpHeaders.HOST;
import static software.amazon.awssdk.http.ContentStreamProvider.fromByteArrayUnsafe;
import static software.amazon.awssdk.http.auth.aws.signer.AwsV4FamilyHttpSigner.SERVICE_SIGNING_NAME;
import static software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner.REGION_NAME;

class AwsRequestSigner
        implements AsyncExecChainHandler
{
    private final String serviceName;
    private final String region;
    private final AwsCredentialsProvider credentialsProvider;
    private final AwsV4HttpSigner signer;

    public AwsRequestSigner(String region, DeploymentType deploymentType, AwsCredentialsProvider credentialsProvider)
    {
        this.credentialsProvider = credentialsProvider;
        this.signer = AwsV4HttpSigner.create();
        this.serviceName = switch (deploymentType) {
            case SERVERLESS -> "aoss";
            case PROVISIONED -> "es";
        };
        this.region = region;
    }

    @Override
    public void execute(HttpRequest request, AsyncEntityProducer entityProducer, AsyncExecChain.Scope scope, AsyncExecChain chain, AsyncExecCallback asyncExecCallback)
            throws HttpException, IOException
    {
        URI uri;
        try {
            uri = request.getUri();
        }
        catch (URISyntaxException e) {
            throw new IOException("Invalid request URI", e);
        }

        Map<String, List<String>> parameters = parseQueryParameters(uri.getRawQuery());

        Map<String, List<String>> headers = stream(request.getHeaders())
                .collect(toImmutableMap(Header::getName, header -> ImmutableList.of(header.getValue())));

        SdkHttpRequest.Builder awsRequest = SdkHttpRequest.builder()
                .rawQueryParameters(parameters)
                .method(SdkHttpMethod.fromValue(request.getMethod()))
                .protocol(uri.getScheme() != null ? uri.getScheme() : "https")
                .encodedPath(uri.getRawPath())
                .headers(headers)
                .rawQueryParameters(parameters);

        if (uri.getHost() != null) {
            try {
                awsRequest.uri(new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), null, null, null));
            }
            catch (URISyntaxException e) {
                throw new IOException("Invalid host URI", e);
            }
        }

        // AWS OpenSearch Service rejects UNSIGNED-PAYLOAD, so the body must be hashed
        ContentStreamProvider payload = entityProducer == null ? null : fromByteArrayUnsafe(readContent(entityProducer));

        SignedRequest signedRequest = signer.sign(builder -> builder
                .identity(credentialsProvider.resolveCredentials())
                .request(awsRequest.build())
                .payload(payload)
                .putProperty(SERVICE_SIGNING_NAME, serviceName)
                .putProperty(REGION_NAME, region));

        // Clear existing headers and set signed ones
        for (Header existingHeader : request.getHeaders()) {
            request.removeHeaders(existingHeader.getName());
        }
        for (Map.Entry<String, List<String>> entry : signedRequest.request().headers().entrySet()) {
            request.setHeader(entry.getKey(), entry.getValue().getFirst());
        }

        // HTTP/2 sends the authority (which may include the default port) instead of Host, so we must align it with the signed Host
        Header host = request.getFirstHeader(HOST);
        if (host != null) {
            try {
                request.setAuthority(URIAuthority.create(host.getValue()));
            }
            catch (URISyntaxException e) {
                throw new IOException("Invalid signed host: " + host.getValue(), e);
            }
        }

        chain.proceed(request, entityProducer, scope, asyncExecCallback);
    }

    @VisibleForTesting
    static Map<String, List<String>> parseQueryParameters(String rawQuery)
    {
        Map<String, List<String>> parameters = new TreeMap<>(CASE_INSENSITIVE_ORDER);
        if (rawQuery == null) {
            return parameters;
        }
        for (String param : rawQuery.split("&")) {
            String[] parts = param.split("=", 2);
            // The signer encodes parameters itself, so pass decoded values to avoid double encoding
            String name = URLDecoder.decode(parts[0], UTF_8);
            String value = parts.length > 1 ? URLDecoder.decode(parts[1], UTF_8) : "";
            parameters.computeIfAbsent(name, _ -> new ArrayList<>()).add(value);
        }
        return parameters;
    }

    @VisibleForTesting
    static byte[] readContent(AsyncEntityProducer entityProducer)
            throws IOException
    {
        // The same producer is sent after signing, so it must support being produced twice
        if (!entityProducer.isRepeatable()) {
            throw new IOException("Cannot sign non-repeatable request entity");
        }

        ByteArrayOutputStream content = new ByteArrayOutputStream();
        AtomicBoolean ended = new AtomicBoolean();
        DataStreamChannel channel = new DataStreamChannel()
        {
            @Override
            public void requestOutput() {}

            @Override
            public int write(ByteBuffer source)
            {
                int length = source.remaining();
                byte[] bytes = new byte[length];
                source.get(bytes);
                content.writeBytes(bytes);
                return length;
            }

            @Override
            public void endStream()
            {
                ended.set(true);
            }

            @Override
            public void endStream(List<? extends Header> trailers)
            {
                ended.set(true);
            }
        };

        try {
            while (!ended.get()) {
                entityProducer.produce(channel);
            }
        }
        finally {
            entityProducer.releaseResources();
        }
        return content.toByteArray();
    }
}

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

import com.google.common.collect.ImmutableList;
import org.apache.hc.client5.http.async.AsyncExecCallback;
import org.apache.hc.client5.http.async.AsyncExecChain;
import org.apache.hc.client5.http.async.AsyncExecChainHandler;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.nio.AsyncEntityProducer;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.opensearch.AwsSecurityConfig.DeploymentType;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.util.Arrays.stream;
import static software.amazon.awssdk.http.auth.aws.signer.AwsV4FamilyHttpSigner.PAYLOAD_SIGNING_ENABLED;
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

        String query = uri.getRawQuery();
        Map<String, List<String>> parameters = new TreeMap<>(CASE_INSENSITIVE_ORDER);
        if (query != null) {
            for (String param : query.split("&")) {
                String[] parts = param.split("=", 2);
                String name = parts[0];
                String value = parts.length > 1 ? parts[1] : "";
                parameters.computeIfAbsent(name, key -> new ArrayList<>()).add(value);
            }
        }

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

        SignedRequest signedRequest = signer.sign(builder -> builder
                .identity(credentialsProvider.resolveCredentials())
                .request(awsRequest.build())
                .putProperty(SERVICE_SIGNING_NAME, serviceName)
                .putProperty(REGION_NAME, region)
                // AsyncEntityProducer is a streaming API with no synchronous way to read content.
                // Disable payload signing when body is present, which uses UNSIGNED-PAYLOAD.
                // This is supported by both AWS OpenSearch Service and OpenSearch Serverless.
                .putProperty(PAYLOAD_SIGNING_ENABLED, entityProducer == null));

        // Clear existing headers and set signed ones
        for (Header existingHeader : request.getHeaders()) {
            request.removeHeaders(existingHeader.getName());
        }
        for (Map.Entry<String, List<String>> entry : signedRequest.request().headers().entrySet()) {
            request.setHeader(entry.getKey(), entry.getValue().getFirst());
        }

        chain.proceed(request, entityProducer, scope, asyncExecCallback);
    }
}

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
package io.trino.plugin.elasticsearch.client;

import com.google.common.collect.ImmutableList;
import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.internal.SignerConstant;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.auth.signer.params.SignerChecksumParams;
import software.amazon.awssdk.core.checksums.Algorithm;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static org.apache.http.protocol.HttpCoreContext.HTTP_TARGET_HOST;

@SuppressWarnings("deprecation")
class AwsRequestSigner
        implements HttpRequestInterceptor
{
    private static final String EMPTY_BODY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    private static final String SERVICE_NAME = "es";
    private final AwsCredentialsProvider credentialsProvider;
    private final Aws4Signer signer;
    private final String region;

    public AwsRequestSigner(String region, AwsCredentialsProvider credentialsProvider)
    {
        this.credentialsProvider = credentialsProvider;
        this.signer = Aws4Signer.create();
        this.region = region;
    }

    @Override
    public void process(HttpRequest request, HttpContext context)
            throws IOException
    {
        String method = request.getRequestLine().getMethod();

        URI uri = URI.create(request.getRequestLine().getUri());
        URIBuilder uriBuilder = new URIBuilder(uri);

        Map<String, List<String>> parameters = new TreeMap<>(CASE_INSENSITIVE_ORDER);
        for (NameValuePair parameter : uriBuilder.getQueryParams()) {
            parameters.computeIfAbsent(parameter.getName(), key -> new ArrayList<>())
                    .add(parameter.getValue());
        }

        InputStream content = null;
        if (request instanceof HttpEntityEnclosingRequest enclosingRequest) {
            if (enclosingRequest.getEntity() != null) {
                content = enclosingRequest.getEntity().getContent();
            }
        }

        Map<String, List<String>> headers = Arrays.stream(request.getAllHeaders())
                .collect(toImmutableMap(Header::getName, header -> ImmutableList.of(header.getValue())));
        SdkHttpFullRequest.Builder awsRequest = SdkHttpFullRequest.builder()
                .rawQueryParameters(parameters)
                .method(SdkHttpMethod.fromValue(method))
                .protocol("https")
                .encodedPath(uri.getPath())
                .headers(headers)
                .rawQueryParameters(parameters);

        Aws4SignerParams aws4SignerParams = Aws4SignerParams.builder()
                .signingName(SERVICE_NAME)
                .signingRegion(Region.of(region))
                .awsCredentials(credentialsProvider.resolveCredentials())
                .checksumParams(
                        SignerChecksumParams.builder()
                                .algorithm(Algorithm.SHA256)
                                .isStreamingRequest(false)
                                .checksumHeaderName(SignerConstant.X_AMZ_CONTENT_SHA256)
                                .build())
                .build();
        HttpHost host = (HttpHost) context.getAttribute(HTTP_TARGET_HOST);
        if (host != null) {
            awsRequest.uri(URI.create(host.toURI()));
        }

        if (content == null) {
            // https://github.com/aws/aws-sdk-java-v2/issues/3807 workaround is to set header when content is null.
            awsRequest.putHeader(SignerConstant.X_AMZ_CONTENT_SHA256, EMPTY_BODY_SHA256);
        }
        else {
            awsRequest.contentStreamProvider(ContentStreamProvider.fromInputStream(content));
        }

        SdkHttpFullRequest signedRequest = signer.sign(awsRequest.build(), aws4SignerParams);

        Header[] newHeaders = signedRequest.headers().entrySet().stream()
                .map(entry -> new BasicHeader(entry.getKey(), entry.getValue().getFirst()))
                .toArray(Header[]::new);

        request.setHeaders(newHeaders);

        InputStream newContent = signedRequest.contentStreamProvider().stream().map(ContentStreamProvider::newStream).findAny().orElse(null);
        checkState(newContent == null || request instanceof HttpEntityEnclosingRequest);
        if (newContent != null) {
            BasicHttpEntity entity = new BasicHttpEntity();
            entity.setContent(newContent);
            ((HttpEntityEnclosingRequest) request).setEntity(entity);
        }
    }
}

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
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.checksums.DefaultChecksumAlgorithm;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4FamilyHttpSigner;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.util.Arrays.stream;
import static org.apache.http.protocol.HttpCoreContext.HTTP_TARGET_HOST;

class AwsRequestSigner
        implements HttpRequestInterceptor
{
    private static final String SERVICE_NAME = "es";
    private final AwsCredentialsProvider credentialsProvider;
    private final AwsV4HttpSigner signer;
    private final String region;

    public AwsRequestSigner(String region, AwsCredentialsProvider credentialsProvider)
    {
        this.credentialsProvider = credentialsProvider;
        this.signer = AwsV4HttpSigner.create();
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

        HttpHost host = (HttpHost) context.getAttribute(HTTP_TARGET_HOST);

        SdkHttpRequest.Builder sdkHttpRequest =
                SdkHttpRequest.builder()
                        .method(SdkHttpMethod.fromValue(method))
                        .rawQueryParameters(parameters);

        stream(request.getAllHeaders())
                .forEach(header -> sdkHttpRequest.appendHeader(header.getName(), header.getValue()));

        if (host != null) {
            sdkHttpRequest
                    .uri(uri)
                    .protocol(uri.getScheme());
        }

        SignRequest.Builder<AwsCredentials> signRequestBuilder =
                SignRequest.builder(credentialsProvider.resolveCredentials())
                        .request(sdkHttpRequest.build())
                        .putProperty(AwsV4FamilyHttpSigner.CHECKSUM_ALGORITHM, DefaultChecksumAlgorithm.SHA256)
                        .putProperty(AwsV4FamilyHttpSigner.SERVICE_SIGNING_NAME, SERVICE_NAME)
                        .putProperty(AwsV4HttpSigner.REGION_NAME, region);

        if (content == null) {
            signRequestBuilder.payload(null);
        }
        else {
            signRequestBuilder.payload(ContentStreamProvider.fromInputStream(content));
        }

        SignedRequest signedRequest = signer.sign(signRequestBuilder.build());

        Header[] newHeaders = signedRequest.request().headers().entrySet().stream()
                .map(entry -> new BasicHeader(entry.getKey(), entry.getValue().getFirst()))
                .toArray(Header[]::new);

        request.setHeaders(newHeaders);

        InputStream newContent = signedRequest.payload().stream().map(ContentStreamProvider::newStream).findAny().orElse(null);
        checkState(newContent == null || request instanceof HttpEntityEnclosingRequest);
        if (newContent != null) {
            BasicHttpEntity entity = new BasicHttpEntity();
            entity.setContent(newContent);
            ((HttpEntityEnclosingRequest) request).setEntity(entity);
        }
    }
}

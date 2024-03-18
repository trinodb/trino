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
package io.trino.tests.product.warp.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.log.Logger;
import jakarta.ws.rs.HttpMethod;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Objects;

import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.trino.tests.product.warp.utils.DemoterUtils.objectMapper;
import static org.assertj.core.api.Assertions.assertThat;

public class RestUtils
{
    private static final Logger logger = Logger.get(RuleUtils.class);

    public RestUtils() {}

    public String executeTrinoCommand(String prefix, String ext, Object inObj, String httpMethod, int responseCode)
            throws IOException
    {
        URI baseUrl = URI.create("http://presto-master:8080");
        return executeCommand(baseUrl, prefix, ext, inObj, httpMethod, responseCode);
    }

    public void executeDeleteCommand(String prefix, String ext, Object inObj)
            throws IOException
    {
        executeRestCommand(prefix, ext, inObj, HttpMethod.DELETE, HttpURLConnection.HTTP_NO_CONTENT);
    }

    public String executeGetCommand(String prefix, String ext)
            throws IOException
    {
        return executeRestCommand(prefix, ext, null, HttpMethod.GET, HttpURLConnection.HTTP_OK);
    }

    public void executePostCommand(String prefix, String ext, Object inObj)
            throws IOException
    {
        executeRestCommand(prefix, ext, inObj, HttpMethod.POST, HttpURLConnection.HTTP_NO_CONTENT);
    }

    public String executePostCommandWithReturnValue(String prefix, String ext, Object inObj)
            throws IOException
    {
        return executeRestCommand(prefix, ext, inObj, HttpMethod.POST, HttpURLConnection.HTTP_OK);
    }

    public String executeRestCommand(String prefix, String ext, Object inObj, String httpMethod, int responseCode)
            throws IOException
    {
        URI baseUrl = URI.create("http://presto-master:8089");
        return executeCommand(baseUrl, prefix, ext, inObj, httpMethod, responseCode);
    }

    public String executeWorkerRestCommand(String prefix, String ext, Object inObj, String httpMethod, int responseCode)
            throws IOException
    {
        URI baseUrl = URI.create("http://presto-worker:8089");
        return executeCommand(baseUrl, prefix, ext, inObj, httpMethod, responseCode);
    }

    public String executeCommand(URI baseUrl, String prefix, String ext, Object inObj, String httpMethod, int responseCode)
            throws JsonProcessingException
    {
        Request.Builder request;
        if (HttpMethod.GET.equals(httpMethod)) {
            request = prepareGet();
        }
        else if (HttpMethod.DELETE.equals(httpMethod)) {
            request = prepareDelete();
        }
        else {
            request = preparePost();
        }
        request.setHeader("Content-Type", "application/json");
        request.setHeader("X-Trino-User", "warpSpeed-automation");

        try {
            prefix = prefix.endsWith("/") ? prefix : prefix + "/";
            prefix = prefix.startsWith("/") ? prefix : "/" + prefix;

            URL url = new URI(baseUrl.getScheme() + "://" + baseUrl.getHost() + ":" + baseUrl.getPort() + prefix + ext).toURL();
            request.setUri(url.toURI());
        }
        catch (URISyntaxException | MalformedURLException e) {
            throw new RuntimeException(e);
        }

        HttpClient client = new JettyHttpClient();
        if (Objects.nonNull(inObj)) {
            String input = objectMapper.writeValueAsString(inObj);
            request.setBodyGenerator(createStaticBodyGenerator(input.getBytes(Charset.defaultCharset())));
        }
        StringResponseHandler stringResponseHandler = StringResponseHandler.createStringResponseHandler();
        logger.info("request %s", request.build());
        StringResponseHandler.StringResponse response = client.execute(request.build(), stringResponseHandler);
        assertThat(response.getStatusCode()).describedAs(response.getBody()).isEqualTo(responseCode);
        client.close();
        return response.getBody();
    }
}

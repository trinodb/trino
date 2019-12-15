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
package io.prestosql.plugin.salesforce.driver.soap;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import io.prestosql.plugin.salesforce.driver.oauth.ForceClientException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;

import static java.lang.Math.toIntExact;

public class ForceSoapValidator
{
    private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    private static final String SOAP_FAULT = "<soapenv:Fault>";
    private static final String BAD_TOKEN_SF_ERROR_CODE = "INVALID_SESSION_ID";
    private final long connectTimeout;
    private final long readTimeout;

    public ForceSoapValidator(long connectTimeout, long readTimeout)
    {
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
    }

    public boolean validateForceToken(String partnerUrl, String accessToken)
    {
        HttpRequestFactory requestFactory = buildHttpRequestFactory();
        ClassLoader classLoader = getClass().getClassLoader();
        try (InputStream is = classLoader.getResourceAsStream("forceSoapBody")) {
            String requestBody = IOUtils.toString(is);

            HttpRequest request = requestFactory.buildPostRequest(new GenericUrl(partnerUrl), ByteArrayContent.fromString("text/xml", requestBody.replace("{sessionId}", accessToken)));
            HttpHeaders headers = request.getHeaders();
            headers.set("SOAPAction", "some");
            HttpResponse result = request.execute();
            return result.getStatusCode() == HttpStatusCodes.STATUS_CODE_OK;
        }
        catch (HttpResponseException e) {
            if (e.getStatusCode() == HttpStatusCodes.STATUS_CODE_SERVER_ERROR && StringUtils.containsIgnoreCase(e.getContent(), SOAP_FAULT) && StringUtils.containsIgnoreCase(e.getContent(), BAD_TOKEN_SF_ERROR_CODE)) {
                return false;
            }
            throw new ForceClientException("Response error: " + e.getStatusCode() + " " + e.getContent());
        }
        catch (IOException e) {
            throw new ForceClientException("IO error: " + e.getMessage(), e);
        }
    }

    private HttpRequestFactory buildHttpRequestFactory()
    {
        return HTTP_TRANSPORT.createRequestFactory(request -> {
            request.setConnectTimeout(toIntExact(connectTimeout));
            request.setReadTimeout(toIntExact(readTimeout));
        });
    }
}

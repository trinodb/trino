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

package io.trino.server.security.jwt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.oauth2.sdk.util.StringUtils;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;

import javax.ws.rs.core.MediaType;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class AzureJwtTokenValidator
        extends JwtTokenValidator
{
    public static final String AZURE_VALIDATION_URL = "http-server.authentication.azure.validation.url";
    public static final String AZURE_PARAM_KEY = "http-server.authentication.azure.param.key";
    public static final String AZURE_PARAM_VALUE = "http-server.authentication.azure.param.value";
    public static final String AUTHORIZATION = "Authorization";
    public static final String TOKEN_PREFIX = "Bearer";
    private static final Logger log = Logger.get(AzureJwtTokenValidator.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private String azureValidationUrl;
    private String httpRequestParam;
    private String httpRequestParamValue;

    @Override
    public Optional<String> validateAndCreatePrincipal(String token, HttpClient httpClient)
    {
        String principalField = this.principalField != null ? this.principalField : "sub";
        String authToken = TOKEN_PREFIX + " " + token;
        HttpUriBuilder httpUriBuilder = uriBuilderFrom(URI.create(azureValidationUrl));
        if (StringUtils.isNotBlank(httpRequestParam) && StringUtils.isBlank(httpRequestParamValue)) {
            httpUriBuilder.addParameter(httpRequestParam, httpRequestParamValue);
        }
        Request request = prepareGet()
                .addHeader(ACCEPT, MediaType.APPLICATION_JSON)
                .addHeader(CONTENT_TYPE, MediaType.APPLICATION_JSON)
                .addHeader(AUTHORIZATION, authToken)
                .setUri(httpUriBuilder.build())
                .build();
        Optional<String> principal = Optional.empty();
        try {
            StringResponseHandler.StringResponse response = httpClient.execute(request, createStringResponseHandler());
            if (response != null) {
                if (response.getStatusCode() != HttpStatus.OK.code()) {
                    log.error("Unexpected response code " + response.getStatusCode() + " from Azure JWT authenticator at " + response.getBody());
                }
                else {
                    Map<String, String> responseMap = StringUtils.isNotBlank(response.getBody()) ? OBJECT_MAPPER.readValue(response.getBody(), Map.class) : null;
                    principal = (responseMap != null && responseMap.containsKey(principalField)) ? Optional.of(responseMap.get(principalField)) : Optional.empty();
                }
            }
            else {
                log.error("Unexpected null response received");
            }
        }
        catch (Exception e) {
            log.error("Error validating azure token using api. Error is : " + e.getMessage());
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }

        return principal;
    }

    @Override
    protected void setRequiredProperties(Map<String, Object> properties, File configFile)
    {
        super.setRequiredProperties(properties, configFile);
        String validationUrl = (String) properties.get(AZURE_VALIDATION_URL);
        checkState(!isNullOrEmpty(validationUrl), "Configuration does not contain '%s' property: %s", AZURE_VALIDATION_URL, configFile);
        this.azureValidationUrl = validationUrl;
    }

    @Override
    protected void setOptionalProperties(Map<String, Object> properties)
    {
        super.setOptionalProperties(properties);
        this.httpRequestParam = (String) properties.get(AZURE_PARAM_KEY);
        this.httpRequestParamValue = (String) properties.get(AZURE_PARAM_VALUE);
    }
}

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
package io.prestosql.plugin.salesforce.driver.oauth;

import com.google.api.client.auth.oauth2.BearerToken;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpIOExceptionHandler;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

import static java.lang.Math.toIntExact;

public class ForceOAuthClient
{
    private static final String LOGIN_URL = "https://login.salesforce.com/services/oauth2/userinfo";
    private static final String TEST_LOGIN_URL = "https://test.salesforce.com/services/oauth2/userinfo";
    private static final String API_VERSION = "43";

    private static final String BAD_TOKEN_SF_ERROR_CODE = "Bad_OAuth_Token";
    private static final String MISSING_TOKEN_SF_ERROR_CODE = "Missing_OAuth_Token";
    private static final String WRONG_ORG_SF_ERROR_CODE = "Wrong_Org";
    private static final String BAD_ID_SF_ERROR_CODE = "Bad_Id";
    private static final String INTERNAL_SERVER_ERROR_SF_ERROR_CODE = "Internal Error";
    private static final int MAX_RETRIES = 5;

    private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    private static final JsonFactory JSON_FACTORY = new JacksonFactory();

    private final long connectTimeout;
    private final long readTimeout;

    public ForceOAuthClient(long connectTimeout, long readTimeout)
    {
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
    }

    private static void extractPartnerUrl(ForceUserInfo userInfo)
    {
        if (userInfo.getUrls() == null || !userInfo.getUrls().containsKey("partner")) {
            throw new IllegalStateException("User info doesn't contain partner URL: " + userInfo.getUrls());
        }
        userInfo.setPartnerUrl(userInfo.getUrls().get("partner").replace("{version}", API_VERSION));
    }

    private static void extractInstance(ForceUserInfo userInfo)
    {
        String profileUrl = userInfo.getPartnerUrl();

        if (StringUtils.isBlank(profileUrl)) {
            return;
        }

        profileUrl = profileUrl.replace("https://", "");

        String instance = StringUtils.split(profileUrl, '.')[0];
        userInfo.setInstance(instance);
    }

    public ForceUserInfo getUserInfo(String accessToken, boolean sandbox)
    {
        GenericUrl loginUrl = new GenericUrl(sandbox ? TEST_LOGIN_URL : LOGIN_URL);
        HttpRequestFactory requestFactory = buildHttpRequestFactory(accessToken);
        int tryCount = 0;
        while (true) {
            try {
                HttpResponse result = requestFactory.buildGetRequest(loginUrl).execute();
                ForceUserInfo forceUserInfo = result.parseAs(ForceUserInfo.class);
                extractPartnerUrl(forceUserInfo);
                extractInstance(forceUserInfo);

                return forceUserInfo;
            }
            catch (HttpResponseException e) {
                if (isForceInternalError(e) && tryCount < MAX_RETRIES) {
                    tryCount++;
                    continue; //try one more time
                }
                if (isBadTokenError(e)) {
                    throw new BadOAuthTokenException("Bad OAuth Token: " + accessToken);
                }
                throw new ForceClientException("Response error: " + e.getStatusCode() + " " + e.getContent());
            }
            catch (IOException e) {
                throw new ForceClientException("IO error: " + e.getMessage(), e);
            }
        }
    }

    private HttpRequestFactory buildHttpRequestFactory(String accessToken)
    {
        Credential credential = new Credential(BearerToken.authorizationHeaderAccessMethod()).setAccessToken(accessToken);

        return HTTP_TRANSPORT.createRequestFactory(request -> {
            request.setConnectTimeout(toIntExact(connectTimeout));
            request.setReadTimeout(toIntExact(readTimeout));
            request.setParser(JSON_FACTORY.createJsonObjectParser());
            request.setInterceptor(credential);
            request.setUnsuccessfulResponseHandler(buildUnsuccessfulResponseHandler());
            request.setIOExceptionHandler(buildIOExceptionHandler());
            request.setNumberOfRetries(MAX_RETRIES);
        });
    }

    private boolean isBadTokenError(HttpResponseException e)
    {
        return ((e.getStatusCode() == HttpStatusCodes.STATUS_CODE_FORBIDDEN) && StringUtils.equalsAnyIgnoreCase(e.getContent(), BAD_TOKEN_SF_ERROR_CODE, MISSING_TOKEN_SF_ERROR_CODE, WRONG_ORG_SF_ERROR_CODE)) || (e.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND && StringUtils.equalsIgnoreCase(e.getContent(), BAD_ID_SF_ERROR_CODE));
    }

    private boolean isForceInternalError(HttpResponseException e)
    {
        return e.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND && StringUtils.equalsIgnoreCase(e.getContent(), INTERNAL_SERVER_ERROR_SF_ERROR_CODE);
    }

    private BackOff getBackOff()
    {
        return new ExponentialBackOff.Builder().setInitialIntervalMillis(500).setMaxElapsedTimeMillis(30000).setMaxIntervalMillis(10000).setMultiplier(1.5).setRandomizationFactor(0.5).build();
    }

    private HttpBackOffUnsuccessfulResponseHandler buildUnsuccessfulResponseHandler()
    {
        return new HttpBackOffUnsuccessfulResponseHandler(getBackOff());
    }

    private HttpIOExceptionHandler buildIOExceptionHandler()
    {
        return new HttpBackOffIOExceptionHandler(getBackOff());
    }
}

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
package com.yahoo.trino.server.security;

import com.google.inject.Inject;
import io.trino.client.ProtocolDetectionException;
import io.trino.client.ProtocolHeaders;
import io.trino.server.HttpRequestSessionContextFactory;
import io.trino.server.ProtocolConfig;
import io.trino.server.security.AuthenticationException;
import io.trino.server.security.Authenticator;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import jakarta.ws.rs.container.ContainerRequestContext;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Strings.emptyToNull;
import static io.trino.client.ProtocolHeaders.detectProtocol;
import static software.amazon.awssdk.profiles.ProfileProperty.AWS_ACCESS_KEY_ID;
import static software.amazon.awssdk.profiles.ProfileProperty.AWS_SECRET_ACCESS_KEY;
import static software.amazon.awssdk.profiles.ProfileProperty.AWS_SESSION_TOKEN;

public class AwsSessionCredentialsAuthenticator
        implements Authenticator
{
    private final Optional<String> alternateHeaderName;
    private final Set<String> allowedAwsAccounts;

    @Inject
    public AwsSessionCredentialsAuthenticator(ProtocolConfig protocolConfig, AwsSessionCredentialsAuthenticatorConfig awsSessionCredentialsAuthenticatorConfig)
    {
        this.alternateHeaderName = protocolConfig.getAlternateHeaderName();
        this.allowedAwsAccounts = awsSessionCredentialsAuthenticatorConfig.getAllowedAwsAccounts();
    }

    @Override
    public Identity authenticate(ContainerRequestContext request)
            throws AuthenticationException
    {
        try {
            ProtocolHeaders protocolHeaders = detectProtocol(alternateHeaderName, request.getHeaders().keySet());
            Map<String, String> extraCredentials = HttpRequestSessionContextFactory.parseExtraCredentials(protocolHeaders, request.getHeaders());

            AwsSessionCredentials sessionCredentials = new AwsSessionCredentials.Builder().accessKeyId(extraCredentials.get(AWS_ACCESS_KEY_ID))
                    .secretAccessKey(extraCredentials.get(AWS_SECRET_ACCESS_KEY))
                    .sessionToken(extraCredentials.get(AWS_SESSION_TOKEN))
                    .build();

            AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(sessionCredentials);
            StsClient stsClient = StsClient.builder()
                    .credentialsProvider(credentialsProvider)
                    .build();

            GetCallerIdentityRequest getCallerIdentityRequest = GetCallerIdentityRequest.builder().build();
            GetCallerIdentityResponse getCallerIdentityResponse = stsClient.getCallerIdentity(getCallerIdentityRequest);

            if (getCallerIdentityResponse.sdkHttpResponse().isSuccessful()) {
                String requestAccountId = emptyToNull(getCallerIdentityResponse.account());

                if (requestAccountId == null || !allowedAwsAccounts.contains(requestAccountId)) {
                    throw new AuthenticationException("Requested account is not allowed to access the Trino cluster");
                }

                String principal = emptyToNull(getCallerIdentityResponse.arn());
                if (principal != null) {
                    String user;
                    String userId = emptyToNull(getCallerIdentityResponse.userId());
                    if (userId != null) {
                        // Hardcoding userId to index 1 ref https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_variables.html#principaltable
                        user = userId.contains(":") ? userId.split(":")[1] : userId;
                    }
                    else {
                        user = principal.substring(principal.lastIndexOf("/") + 1);
                    }
                    return Identity.forUser(user)
                            .withPrincipal(new BasicPrincipal(principal))
                            .withExtraCredentials(extraCredentials)
                            .build();
                }
            }
            else {
                throw new AuthenticationException("Error while authentication the sesion credentials: " + getCallerIdentityResponse.sdkHttpResponse().statusText().orElse("UNKNOWN"));
            }
        }
        catch (ProtocolDetectionException | RuntimeException e) {
            throw new AuthenticationException(e.getMessage());
        }
        throw new AuthenticationException(null);
    }
}

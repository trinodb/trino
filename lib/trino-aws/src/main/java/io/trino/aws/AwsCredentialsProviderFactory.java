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
package io.trino.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.AwsRegionProvider;
import com.amazonaws.regions.AwsRegionProviderChain;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;

import java.net.URI;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.amazonaws.regions.Regions.US_EAST_1;
import static java.lang.String.format;

/**
 * This factory can be used to create an {@link AWSCredentialsProvider}
 * based on an arbitrary {@link AwsCredentialsProviderConfig configuration}.
 * <p>
 * The {@link AWSCredentialsProvider} returned by this factory will depend on
 * the configuration in the following way:
 * <ul>
 *     <li>Static credentials embedded in the {@link AwsCredentialsProviderConfig#getServiceUri()},
 *     if present,</li>
 *     <li>Or, a custom implementation if {@link AwsCredentialsProviderConfig#getCustomCredentialsProvider()}
 *     is present and the class named by it implements the {@link AWSCredentialsProvider}
 *     interface. A custom {@link CustomProviderResolver} can be provided via
 *     {@link AwsCredentialsProviderConfig#getCustomProviderResolver()}; the default one
 *     requires a no-argument constructor and uses plain {@link Class#forName(String)}.</li>
 * </ul>
 * Otherwise,
 * <ul>
 *     <li>If {@link AwsCredentialsProviderConfig#getAccessKey()} and
 *     {@link AwsCredentialsProviderConfig#getSecretKey()} both are present, they
 *     will be returned as static credentials. These credentials may also include
 *     an optional {@link AwsCredentialsProviderConfig#getSessionToken()}.</li>
 *     <li>Otherwise, the default credentials provider will be returned,
 *     which will try to obtain the credentials in the following order:
 *     <ol>
 *         <li>Environment variables</li>
 *         <li>System properties</li>
 *         <li>Web Identity token</li>
 *         <li>Local profile ({@code ~/.aws/credentials})</li>
 *         <li>Local EC2 instance metadata</li>
 *     </ol>
 *     </li>
 *     <li>In addition, if {@link AwsCredentialsProviderConfig#getIamRole()}
 *     is present, the returned credential provider will retrieve the credentials
 *     having assumed the named role; it can also use the optional
 *     {@link AwsCredentialsProviderConfig#getExternalId()} while assuming the role.
 *     If that is the case, then {@link AwsCredentialsProviderConfig#getRegion()}
 *     can also be provided.</li>
 * </ul>
 */
public class AwsCredentialsProviderFactory
{
    private static final AwsRegionProviderChain REGION_PROVIDER_CHAIN = new AwsRegionProviderChain(
            new DefaultAwsRegionProviderChain(),
            // fallback:
            new AwsRegionProvider()
            {
                @Override
                public String getRegion()
                {
                    return US_EAST_1.getName();
                }
            });
    private static final Pattern EMBEDDED_CREDENTIALS_PATTERN = Pattern.compile("([^:]+):(.+)");

    public AWSCredentialsProvider createAwsCredentialsProvider(AwsCredentialsProviderConfig config)
    {
        return config.getServiceUri()
                .flatMap(AwsCredentialsProviderFactory::getEmbeddedAwsCredentials)
                .<AWSCredentialsProvider>map(AWSStaticCredentialsProvider::new)
                .or(() -> config.getCustomCredentialsProvider()
                        .map(providerClass -> createCustomAWSCredentialsProvider(config.getCustomProviderResolver(), providerClass)))
                .orElseGet(() -> createStandardAwsCredentialsProvider(config));
    }

    private static Optional<AWSCredentials> getEmbeddedAwsCredentials(URI uri)
    {
        return Optional.ofNullable(uri.getUserInfo())
                .map(EMBEDDED_CREDENTIALS_PATTERN::matcher)
                .filter(Matcher::matches)
                .map(matcher -> new BasicAWSCredentials(matcher.group(1), matcher.group(2)));
    }

    private static AWSCredentialsProvider createCustomAWSCredentialsProvider(CustomProviderResolver customProviderResolver, String providerClass)
    {
        try {
            return customProviderResolver.resolve(providerClass);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(format("Error creating an instance of %s", providerClass), e);
        }
    }

    private static AWSCredentialsProvider createStandardAwsCredentialsProvider(AwsCredentialsProviderConfig config)
    {
        AWSCredentialsProvider provider = getFixedAwsCredentials(config)
                .<AWSCredentialsProvider>map(AWSStaticCredentialsProvider::new)
                .orElseGet(DefaultAWSCredentialsProviderChain::getInstance);

        return config.getIamRole()
                .<AWSCredentialsProvider>map(iamRole -> new STSAssumeRoleSessionCredentialsProvider.Builder(iamRole, config.getIamRoleSessionName())
                        .withExternalId(config.getExternalId().orElse(null))
                        .withStsClient(getStsClient(config, provider))
                        .build())
                .orElse(provider);
    }

    private static AWSSecurityTokenService getStsClient(AwsCredentialsProviderConfig config, AWSCredentialsProvider provider)
    {
        // we need to get the regions directly, because we need a fallback,
        // and it's not possible to specify fallback with the default region provider chain:
        String region = config.getStsRegion()
                // TODO: pinClientToCurrentRegion
                .or(config::getRegion)
                .orElseGet(REGION_PROVIDER_CHAIN::getRegion);

        AWSSecurityTokenServiceClientBuilder builder = AWSSecurityTokenServiceClientBuilder.standard()
                .withCredentials(provider);
        config.getStsEndpoint()
                .map(endpoint -> new AwsClientBuilder.EndpointConfiguration(endpoint, region))
                // endpoint configuration and region are mutually exclusive:
                .ifPresentOrElse(
                        builder::setEndpointConfiguration,
                        () -> builder.setRegion(region));
        return builder.build();
    }

    private static Optional<AWSCredentials> getFixedAwsCredentials(AwsCredentialsProviderConfig config)
    {
        return config.getAccessKey()
                .map(accessKey -> config.getSecretKey()
                        .map(secretKey -> config.getSessionToken()
                                .<AWSCredentials>map(sessionToken -> new BasicSessionCredentials(accessKey, secretKey, sessionToken))
                                .orElseGet(() -> new BasicAWSCredentials(accessKey, secretKey)))
                        .orElseThrow(() -> new IllegalArgumentException("AWS Secret Key is required when access key is present")))
                .or(() -> {
                    if (config.getSecretKey().isPresent()) {
                        throw new IllegalArgumentException("AWS Access Key is required when secret key is present");
                    }
                    return Optional.empty();
                });
    }
}

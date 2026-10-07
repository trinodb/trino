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
package io.trino.filesystem.s3;

import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.checksums.SdkChecksum;
import software.amazon.awssdk.checksums.spi.ChecksumAlgorithm;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.auth.aws.scheme.AwsV4AuthScheme;
import software.amazon.awssdk.http.auth.spi.scheme.AuthScheme;
import software.amazon.awssdk.http.auth.spi.scheme.AuthSchemeOption;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignRequest;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignedRequest;
import software.amazon.awssdk.http.auth.spi.signer.HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignerProperty;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.IdentityProviders;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeProvider;

import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Locale.ENGLISH;
import static software.amazon.awssdk.http.auth.aws.signer.AwsV4FamilyHttpSigner.CHECKSUM_ALGORITHM;

final class S3RemoteSigningAuthScheme
        implements AuthScheme<AwsCredentialsIdentity>, HttpSigner<AwsCredentialsIdentity>
{
    static final SignerProperty<HttpSigner<AwsCredentialsIdentity>> REQUEST_SIGNER = SignerProperty.create(S3RemoteSigningAuthScheme.class, "RequestSigner");

    private static final AwsV4AuthScheme DEFAULT = AwsV4AuthScheme.create();

    static void addSignerOverride(AwsRequestOverrideConfiguration.Builder request, HttpSigner<AwsCredentialsIdentity> signer)
    {
        // S3 clients are shared across tables; keep each table's signer in its request options.
        request.addPlugin(configuration -> {
            S3ServiceClientConfiguration.Builder s3Configuration = (S3ServiceClientConfiguration.Builder) configuration;
            S3AuthSchemeProvider delegate = s3Configuration.authSchemeProvider();
            s3Configuration.authSchemeProvider(parameters -> {
                List<AuthSchemeOption> options = delegate.resolveAuthScheme(parameters).stream()
                        .filter(option -> option.schemeId().equals(AwsV4AuthScheme.SCHEME_ID))
                        .map(option -> option.toBuilder().putSignerProperty(REQUEST_SIGNER, signer).build())
                        .collect(toImmutableList());
                if (options.isEmpty()) {
                    throw SdkClientException.create("Remote signing requires the SigV4 authentication scheme");
                }
                return options;
            });
        });
    }

    @Override
    public String schemeId()
    {
        return DEFAULT.schemeId();
    }

    @Override
    public IdentityProvider<AwsCredentialsIdentity> identityProvider(IdentityProviders providers)
    {
        return DEFAULT.identityProvider(providers);
    }

    @Override
    public HttpSigner<AwsCredentialsIdentity> signer()
    {
        return this;
    }

    @Override
    public SignedRequest sign(SignRequest<? extends AwsCredentialsIdentity> request)
    {
        HttpSigner<AwsCredentialsIdentity> remoteSigner = request.property(REQUEST_SIGNER);
        if (remoteSigner == null) {
            return DEFAULT.signer().sign(request);
        }
        return remoteSigner.sign(withChecksum(request));
    }

    private static <T extends AwsCredentialsIdentity> SignRequest<T> withChecksum(SignRequest<T> request)
    {
        ChecksumAlgorithm algorithm = request.property(CHECKSUM_ALGORITHM);
        if (algorithm == null) {
            return request;
        }
        String header = "x-amz-checksum-" + algorithm.algorithmId().toLowerCase(ENGLISH);
        if (request.request().firstMatchingHeader(header).isPresent()) {
            return request;
        }

        // The SDK selects the checksum before signing, but its default signer normally calculates it.
        SdkChecksum checksum = SdkChecksum.forAlgorithm(algorithm);
        if (request.payload().isPresent()) {
            try (InputStream input = request.payload().orElseThrow().newStream()) {
                byte[] buffer = new byte[8192];
                for (int read = input.read(buffer); read != -1; read = input.read(buffer)) {
                    checksum.update(buffer, 0, read);
                }
            }
            catch (IOException e) {
                throw SdkClientException.create("Failed to calculate checksum for remote signing", e);
            }
        }
        String value = Base64.getEncoder().encodeToString(checksum.getChecksumBytes());
        return request.toBuilder()
                .request(request.request().toBuilder().putHeader(header, value).build())
                .build();
    }

    @Override
    public CompletableFuture<AsyncSignedRequest> signAsync(AsyncSignRequest<? extends AwsCredentialsIdentity> request)
    {
        return request.requireProperty(REQUEST_SIGNER, DEFAULT.signer()).signAsync(request);
    }
}

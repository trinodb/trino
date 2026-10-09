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

import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.s3.S3FileSystemConfig.SignerType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.http.auth.aws.scheme.AwsV4AuthScheme;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.scheme.AuthSchemeOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeParams;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeProvider;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.filesystem.s3.S3FileSystemConfig.SignerType.Aws4Signer;
import static io.trino.filesystem.s3.S3FileSystemConfig.SignerType.Aws4UnsignedPayloadSigner;
import static io.trino.filesystem.s3.S3FileSystemConfig.SignerType.AwsS3V4Signer;
import static io.trino.filesystem.s3.S3FileSystemLoader.createAuthSchemeProvider;
import static org.assertj.core.api.Assertions.assertThat;

final class TestS3FileSystemSigner
{
    @Test
    void testAwsS3V4SignerKeepsDefaultSigning()
    {
        // The S3 client already uses the S3-specific SigV4 signer by default, so the resolved
        // signer properties must match the unmodified default auth scheme.
        AuthSchemeOption option = resolveSigV4(createAuthSchemeProvider(AwsS3V4Signer));
        AuthSchemeOption defaultOption = resolveSigV4(S3AuthSchemeProvider.defaultProvider());
        assertThat(option.signerProperty(AwsV4HttpSigner.DOUBLE_URL_ENCODE))
                .isEqualTo(defaultOption.signerProperty(AwsV4HttpSigner.DOUBLE_URL_ENCODE));
        assertThat(option.signerProperty(AwsV4HttpSigner.NORMALIZE_PATH))
                .isEqualTo(defaultOption.signerProperty(AwsV4HttpSigner.NORMALIZE_PATH));
    }

    @Test
    void testAws4SignerUsesGenericSigning()
    {
        AuthSchemeOption option = resolveSigV4(createAuthSchemeProvider(Aws4Signer));
        assertThat(option.signerProperty(AwsV4HttpSigner.DOUBLE_URL_ENCODE)).isTrue();
        assertThat(option.signerProperty(AwsV4HttpSigner.NORMALIZE_PATH)).isTrue();
    }

    @Test
    void testAws4UnsignedPayloadSignerDisablesPayloadSigning()
    {
        AuthSchemeOption option = resolveSigV4(createAuthSchemeProvider(Aws4UnsignedPayloadSigner));
        assertThat(option.signerProperty(AwsV4HttpSigner.DOUBLE_URL_ENCODE)).isTrue();
        assertThat(option.signerProperty(AwsV4HttpSigner.NORMALIZE_PATH)).isTrue();
        assertThat(option.signerProperty(AwsV4HttpSigner.PAYLOAD_SIGNING_ENABLED)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"PutObject", "UploadPart"})
    void testChunkedEncodingEnabledByDefault(String operation)
    {
        S3FileSystemConfig config = new S3FileSystemConfig();
        assertThat(resolveSigV4(createClientAuthSchemeProvider(config), operation).signerProperty(AwsV4HttpSigner.CHUNK_ENCODING_ENABLED)).isTrue();
        assertThat(resolveSigV4(createClientAuthSchemeProvider(config.setSignerType(Aws4Signer)), operation).signerProperty(AwsV4HttpSigner.CHUNK_ENCODING_ENABLED)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"PutObject", "UploadPart"})
    void testChunkedEncodingDisabled(String operation)
    {
        S3FileSystemConfig config = new S3FileSystemConfig().setChunkedEncodingEnabled(false);
        assertThat(resolveSigV4(createClientAuthSchemeProvider(config), operation).signerProperty(AwsV4HttpSigner.CHUNK_ENCODING_ENABLED)).isFalse();
        // chunked encoding must also be disabled when a custom signer is configured
        for (SignerType signerType : SignerType.values()) {
            AuthSchemeOption option = resolveSigV4(createClientAuthSchemeProvider(config.setSignerType(signerType)), operation);
            assertThat(option.signerProperty(AwsV4HttpSigner.CHUNK_ENCODING_ENABLED))
                    .as("chunked encoding for %s with %s signer", operation, signerType)
                    .isFalse();
        }
    }

    private static S3AuthSchemeProvider createClientAuthSchemeProvider(S3FileSystemConfig config)
    {
        S3FileSystemLoader loader = new S3FileSystemLoader(OpenTelemetry.noop(), config.setRegion(Region.US_EAST_1.id()), new S3FileSystemStats());
        try (S3Client client = loader.createClient()) {
            return client.serviceClientConfiguration().authSchemeProvider();
        }
        finally {
            loader.destroy();
        }
    }

    private static AuthSchemeOption resolveSigV4(S3AuthSchemeProvider provider)
    {
        return resolveSigV4(provider, "GetObject");
    }

    private static AuthSchemeOption resolveSigV4(S3AuthSchemeProvider provider, String operation)
    {
        return provider.resolveAuthScheme(S3AuthSchemeParams.builder()
                        .operation(operation)
                        .region(Region.US_EAST_1)
                        .build()).stream()
                .filter(option -> option.schemeId().equals(AwsV4AuthScheme.SCHEME_ID))
                .collect(onlyElement());
    }
}

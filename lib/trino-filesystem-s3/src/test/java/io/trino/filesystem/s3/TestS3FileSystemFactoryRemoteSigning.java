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

import com.google.common.collect.ImmutableMap;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.switching.SwitchingFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignRequest;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignedRequest;
import software.amazon.awssdk.http.auth.spi.signer.HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static software.amazon.awssdk.regions.Region.US_EAST_1;

final class TestS3FileSystemFactoryRemoteSigning
{
    private static final Location LOCATION = Location.of("s3://bucket/data/test.csv");

    enum CreateMethod
    {
        IDENTITY,
        IDENTITY_SUPPLIER,
        // File system managers call the factory through the switching file system, which uses an identity supplier
        SWITCHING_FILE_SYSTEM,
    }

    @ParameterizedTest
    @EnumSource(CreateMethod.class)
    void testRemoteSignerReplacesIdentityCredentials(CreateMethod createMethod)
    {
        List<SignRequest<? extends AwsCredentialsIdentity>> signRequests = new CopyOnWriteArrayList<>();
        HttpSigner<AwsCredentialsIdentity> signer = new HttpSigner<>()
        {
            @Override
            public SignedRequest sign(SignRequest<? extends AwsCredentialsIdentity> request)
            {
                signRequests.add(request);
                // Fail before the request is sent
                throw new IllegalStateException("remote signer called");
            }

            @Override
            public CompletableFuture<AsyncSignedRequest> signAsync(AsyncSignRequest<? extends AwsCredentialsIdentity> request)
            {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
        };
        S3FileSystemFactory factory = new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setRegion(US_EAST_1.id())
                        // Unreachable, so that a request signed without the remote signer fails fast
                        .setEndpoint("http://localhost:1")
                        .setPathStyleAccess(true),
                new S3FileSystemStats(),
                Optional.of(_ -> Optional.of(signer)));
        try {
            // Static credentials that the remote signer must replace
            ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                    .withExtraCredentials(ImmutableMap.of(
                            EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, "local-access-key",
                            EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, "local-secret-key",
                            EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, "local-session-token"))
                    .build();
            TrinoFileSystem fileSystem = switch (createMethod) {
                case IDENTITY -> factory.create(identity);
                case IDENTITY_SUPPLIER -> factory.create(() -> identity);
                case SWITCHING_FILE_SYSTEM -> new SwitchingFileSystemFactory(_ -> factory).create(identity);
            };

            assertThatThrownBy(() -> fileSystem.newOutputFile(LOCATION).createOrOverwrite(new byte[] {1}))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("remote signer called");

            assertThat(signRequests).singleElement().satisfies(request -> {
                assertThat(request.identity().accessKeyId()).isNull();
                assertThat(request.identity().secretAccessKey()).isNull();
            });
        }
        finally {
            factory.destroy();
        }
    }
}

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
import com.google.inject.Key;
import io.airlift.bootstrap.Bootstrap;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.s3.keystore.KeyStoreTestFixture;
import io.trino.filesystem.switching.SwitchingFileSystemFactory;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;

import static com.google.common.io.Resources.getResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestS3FileSystemModule
{
    @Test
    public void testDefaultFactoryBinding()
    {
        TrinoFileSystemFactory factory = bootstrap(Map.of(
                "s3.region", "us-east-1",
                "s3.aws-access-key", "access",
                "s3.aws-secret-key", "secret"))
                .initialize()
                .getInstance(Key.get(TrinoFileSystemFactory.class, FileSystemS3.class));

        assertThat(factory).isInstanceOf(S3FileSystemFactory.class);
    }

    @Test
    public void testGlobalAliasKeystoreFactoryBinding()
            throws Exception
    {
        Path keystorePath = KeyStoreTestFixture.createKeyStore(
                Map.of(
                        "s3.access.key", "access",
                        "s3.secret.key", "secret"),
                "none");

        TrinoFileSystemFactory factory = bootstrap(Map.of(
                "s3.region", "us-east-1",
                "s3.keystore.path", keystorePath.toString(),
                "s3.keystore.password", "none",
                "s3.aws-access-key-alias", "s3.access.key",
                "s3.aws-secret-key-alias", "s3.secret.key"))
                .initialize()
                .getInstance(Key.get(TrinoFileSystemFactory.class, FileSystemS3.class));

        assertThat(factory).isInstanceOf(S3FileSystemFactory.class);
    }

    @Test
    public void testBucketPrefixFactoryBinding()
            throws Exception
    {
        Path keystorePath = KeyStoreTestFixture.createKeyStore(
                Map.of(
                        "fs.s3a.bucket.test-bucket.access.key", "access",
                        "fs.s3a.bucket.test-bucket.secret.key", "secret"),
                "none");

        TrinoFileSystemFactory factory = bootstrap(Map.of(
                "s3.region", "us-east-1",
                "s3.keystore.path", keystorePath.toString(),
                "s3.keystore.password", "none",
                "s3.keystore.bucket-key-prefix", "fs.s3a.bucket."))
                .initialize()
                .getInstance(Key.get(TrinoFileSystemFactory.class, FileSystemS3.class));

        assertThat(factory).isInstanceOf(SwitchingFileSystemFactory.class);
        assertThat(factory).isNotInstanceOf(S3FileSystemFactory.class);
    }

    @Test
    public void testSecurityMappingFactoryBinding()
    {
        TrinoFileSystemFactory factory = bootstrap(Map.of(
                "s3.region", "us-east-1",
                "s3.security-mapping.enabled", "true",
                "s3.security-mapping.config-file", securityMappingConfigFile()))
                .initialize()
                .getInstance(Key.get(TrinoFileSystemFactory.class, FileSystemS3.class));

        assertThat(factory).isInstanceOf(SwitchingFileSystemFactory.class);
        assertThat(factory).isNotInstanceOf(S3FileSystemFactory.class);
    }

    @Test
    public void testSecurityMappingWithBucketPrefixFailsStartup()
            throws Exception
    {
        Path keystorePath = KeyStoreTestFixture.createKeyStore(
                Map.of(
                        "fs.s3a.bucket.test-bucket.access.key", "access",
                        "fs.s3a.bucket.test-bucket.secret.key", "secret"),
                "none");

        assertThatThrownBy(() -> bootstrap(ImmutableMap.<String, String>builder()
                .put("s3.region", "us-east-1")
                .put("s3.security-mapping.enabled", "true")
                .put("s3.security-mapping.config-file", securityMappingConfigFile())
                .put("s3.keystore.path", keystorePath.toString())
                .put("s3.keystore.password", "none")
                .put("s3.keystore.bucket-key-prefix", "fs.s3a.bucket.")
                .buildOrThrow())
                .initialize())
                .hasMessageContaining("s3.keystore.bucket-key-prefix cannot be used with s3.security-mapping.enabled");
    }

    private static String securityMappingConfigFile()
    {
        return new File(getResource(TestS3FileSystemModule.class, "security-mapping.json").getFile()).getPath();
    }

    private static Bootstrap bootstrap(Map<String, String> properties)
    {
        return new Bootstrap(
                binder -> binder.bind(OpenTelemetry.class).toInstance(OpenTelemetry.noop()),
                new S3FileSystemModule())
                .setRequiredConfigurationProperties(properties)
                .doNotInitializeLogging()
                .quiet();
    }
}

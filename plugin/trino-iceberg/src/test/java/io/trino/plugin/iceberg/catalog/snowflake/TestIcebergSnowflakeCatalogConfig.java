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
package io.trino.plugin.iceberg.catalog.snowflake;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergSnowflakeCatalogConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(IcebergSnowflakeCatalogConfig.class)
                .setUser(null)
                .setPassword(null)
                .setPrivateKey(null)
                .setPrivateKeyFile(null)
                .setPrivateKeyPassphrase(null)
                .setDatabase(null)
                .setUri(null)
                .setRole(null));
    }

    @Test
    public void testExplicitPropertyMapping()
            throws IOException
    {
        Path keyFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("iceberg.snowflake-catalog.password", "password")
                .put("iceberg.snowflake-catalog.private-key", "key")
                .put("iceberg.snowflake-catalog.private-key-file", keyFile.toString())
                .put("iceberg.snowflake-catalog.private-key.passphrase", "passphrase")
                .put("iceberg.snowflake-catalog.user", "user")
                .put("iceberg.snowflake-catalog.role", "role")
                .put("iceberg.snowflake-catalog.account-uri", "jdbc:snowflake://sample.url")
                .put("iceberg.snowflake-catalog.database", "database")
                .buildOrThrow();

        IcebergSnowflakeCatalogConfig expected = new IcebergSnowflakeCatalogConfig()
                .setPassword("password")
                .setPrivateKey("key")
                .setPrivateKeyFile(keyFile.toString())
                .setPrivateKeyPassphrase("passphrase")
                .setUser("user")
                .setRole("role")
                .setUri(URI.create("jdbc:snowflake://sample.url"))
                .setDatabase("database");

        assertFullMapping(properties, expected);
    }

    @Test
    public void testInvalidSnowflakeUrl()
            throws SQLException
    {
        IcebergSnowflakeCatalogConfig config = new IcebergSnowflakeCatalogConfig()
                .setPrivateKey("key")
                .setUser("user")
                .setRole("role")
                .setUri(URI.create("foobar"))
                .setDatabase("database");
        assertThat(config.isUrlValid()).isFalse();
    }

    @Test
    public void testInvalidSetting()
    {
        IcebergSnowflakeCatalogConfig keyAndPasswordConfig = new IcebergSnowflakeCatalogConfig();
        keyAndPasswordConfig.setPassword("password");
        keyAndPasswordConfig.setPrivateKey("key");
        assertThatThrownBy(keyAndPasswordConfig::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Either password or private key must be set, but not both");

        IcebergSnowflakeCatalogConfig bothKeyFileOptionsConfig = new IcebergSnowflakeCatalogConfig();
        bothKeyFileOptionsConfig.setPrivateKey("key");
        bothKeyFileOptionsConfig.setPrivateKeyFile("key-file");
        assertThatThrownBy(bothKeyFileOptionsConfig::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("iceberg.snowflake-catalog.private-key and iceberg.snowflake-catalog.private-key-file cannot be set simultaneously");

        IcebergSnowflakeCatalogConfig passwordAndPassphraseConfig = new IcebergSnowflakeCatalogConfig();
        passwordAndPassphraseConfig.setPassword("password");
        passwordAndPassphraseConfig.setPrivateKeyPassphrase("passphrase");
        assertThatThrownBy(passwordAndPassphraseConfig::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("iceberg.snowflake-catalog.private-key.passphrase is set, but iceberg.snowflake-catalog.private-key or iceberg.snowflake-catalog.private-key-file is missing");
    }
}

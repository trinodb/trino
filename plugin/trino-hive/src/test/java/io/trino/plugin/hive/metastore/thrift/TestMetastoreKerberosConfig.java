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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.ConfigurationFactory;
import jakarta.validation.constraints.AssertTrue;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMetastoreKerberosConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(MetastoreKerberosConfig.class)
                .setHiveMetastoreServicePrincipal(null)
                .setHiveMetastoreClientPrincipal(null)
                .setHiveMetastoreClientKeytab(null)
                .setHiveMetastoreClientCredentialCacheLocation(null));
    }

    @Test
    public void testExplicitPropertyMappingsForKeytab()
            throws Exception
    {
        Path clientKeytabFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore.service.principal", "hive/_HOST@EXAMPLE.COM")
                .put("hive.metastore.client.principal", "metastore@EXAMPLE.COM")
                .put("hive.metastore.client.keytab", clientKeytabFile.toString())
                .buildOrThrow();

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        MetastoreKerberosConfig config = configurationFactory.build(MetastoreKerberosConfig.class);

        MetastoreKerberosConfig expected = new MetastoreKerberosConfig()
                .setHiveMetastoreServicePrincipal("hive/_HOST@EXAMPLE.COM")
                .setHiveMetastoreClientPrincipal("metastore@EXAMPLE.COM")
                .setHiveMetastoreClientKeytab(clientKeytabFile.toString());

        assertThat(config.getHiveMetastoreServicePrincipal())
                .isEqualTo(expected.getHiveMetastoreServicePrincipal());
        assertThat(config.getHiveMetastoreClientPrincipal())
                .isEqualTo(expected.getHiveMetastoreClientPrincipal());
        assertThat(config.getHiveMetastoreClientKeytab())
                .isEqualTo(expected.getHiveMetastoreClientKeytab());
    }

    @Test
    public void testExplicitPropertyMappingsForCredentialCache()
            throws Exception
    {
        Path credentialCacheLocation = Files.createTempFile("credentialCache", null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore.service.principal", "hive/_HOST@EXAMPLE.COM")
                .put("hive.metastore.client.principal", "metastore@EXAMPLE.COM")
                .put("hive.metastore.client.credential-cache.location", credentialCacheLocation.toString())
                .buildOrThrow();

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        MetastoreKerberosConfig config = configurationFactory.build(MetastoreKerberosConfig.class);

        MetastoreKerberosConfig expected = new MetastoreKerberosConfig()
                .setHiveMetastoreServicePrincipal("hive/_HOST@EXAMPLE.COM")
                .setHiveMetastoreClientPrincipal("metastore@EXAMPLE.COM")
                .setHiveMetastoreClientCredentialCacheLocation(credentialCacheLocation.toString());

        assertThat(config.getHiveMetastoreServicePrincipal())
                .isEqualTo(expected.getHiveMetastoreServicePrincipal());
        assertThat(config.getHiveMetastoreClientPrincipal())
                .isEqualTo(expected.getHiveMetastoreClientPrincipal());
        assertThat(config.getHiveMetastoreClientCredentialCacheLocation())
                .isEqualTo(expected.getHiveMetastoreClientCredentialCacheLocation());
    }

    @Test
    public void testValidation()
            throws Exception
    {
        assertFailsValidation(
                new MetastoreKerberosConfig()
                        .setHiveMetastoreServicePrincipal("hive/_HOST@EXAMPLE.COM")
                        .setHiveMetastoreClientPrincipal("metastore@EXAMPLE.COM"),
                "configValid",
                "Exactly one of `hive.metastore.client.keytab` or `hive.metastore.client.credential-cache.location` must be specified",
                AssertTrue.class);

        Path clientKeytabFile = Files.createTempFile(null, null);
        Path credentialCacheLocation = Files.createTempFile("credentialCache", null);

        assertFailsValidation(
                new MetastoreKerberosConfig()
                        .setHiveMetastoreServicePrincipal("hive/_HOST@EXAMPLE.COM")
                        .setHiveMetastoreClientPrincipal("metastore@EXAMPLE.COM")
                        .setHiveMetastoreClientKeytab(clientKeytabFile.toString())
                        .setHiveMetastoreClientCredentialCacheLocation(credentialCacheLocation.toString()),
                "configValid",
                "Exactly one of `hive.metastore.client.keytab` or `hive.metastore.client.credential-cache.location` must be specified",
                AssertTrue.class);
    }
}

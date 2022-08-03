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
package io.trino.hdfs.authentication;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.ConfigurationFactory;
import org.testng.annotations.Test;

import javax.validation.constraints.AssertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHdfsKerberosConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HdfsKerberosConfig.class)
                .setHdfsTrinoPrincipal(null)
                .setHdfsTrinoKeytab(null)
                .setHdfsTrinoCredentialCacheLocation(null));
    }

    @Test
    public void testExplicitPropertyMappingsForKeytab()
            throws Exception
    {
        Path keytab = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.hdfs.trino.principal", "trino@EXAMPLE.COM")
                .put("hive.hdfs.trino.keytab", keytab.toString())
                .buildOrThrow();

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        HdfsKerberosConfig config = configurationFactory.build(HdfsKerberosConfig.class);

        HdfsKerberosConfig expected = new HdfsKerberosConfig()
                .setHdfsTrinoPrincipal("trino@EXAMPLE.COM")
                .setHdfsTrinoKeytab(keytab.toString());

        assertThat(config.getHdfsTrinoPrincipal())
                .isEqualTo(expected.getHdfsTrinoPrincipal());
        assertThat(config.getHdfsTrinoKeytab())
                .isEqualTo(expected.getHdfsTrinoKeytab());
    }

    @Test
    public void testExplicitPropertyMappingsForCredentialCache()
            throws Exception
    {
        Path credentialCacheLocation = Files.createTempFile("credentialCache", null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.hdfs.trino.principal", "trino@EXAMPLE.COM")
                .put("hive.hdfs.trino.credential-cache.location", credentialCacheLocation.toString())
                .buildOrThrow();

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        HdfsKerberosConfig config = configurationFactory.build(HdfsKerberosConfig.class);

        HdfsKerberosConfig expected = new HdfsKerberosConfig()
                .setHdfsTrinoPrincipal("trino@EXAMPLE.COM")
                .setHdfsTrinoCredentialCacheLocation(credentialCacheLocation.toString());

        assertThat(config.getHdfsTrinoPrincipal())
                .isEqualTo(expected.getHdfsTrinoPrincipal());
        assertThat(config.getHdfsTrinoCredentialCacheLocation())
                .isEqualTo(expected.getHdfsTrinoCredentialCacheLocation());
    }

    @Test
    public void testValidation()
            throws Exception
    {
        assertFailsValidation(
                new HdfsKerberosConfig()
                        .setHdfsTrinoPrincipal("trino@EXAMPLE.COM"),
                "configValid",
                "Exactly one of `hive.hdfs.trino.keytab` or `hive.hdfs.trino.credential-cache.location` must be specified",
                AssertTrue.class);

        Path keytab = Files.createTempFile(null, null);
        Path credentialCacheLocation = Files.createTempFile("credentialCache", null);

        assertFailsValidation(
                new HdfsKerberosConfig()
                        .setHdfsTrinoPrincipal("trino@EXAMPLE.COM")
                        .setHdfsTrinoKeytab(keytab.toString())
                        .setHdfsTrinoCredentialCacheLocation(credentialCacheLocation.toString()),
                "configValid",
                "Exactly one of `hive.hdfs.trino.keytab` or `hive.hdfs.trino.credential-cache.location` must be specified",
                AssertTrue.class);
    }
}

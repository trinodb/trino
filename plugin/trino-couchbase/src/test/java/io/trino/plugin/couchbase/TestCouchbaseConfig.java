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
package io.trino.plugin.couchbase;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestCouchbaseConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(
                recordDefaults(CouchbaseConfig.class)
                        .setCluster("localhost")
                        .setUsername("Administrator")
                        .setPassword("password")
                        .setBucket("default")
                        .setScope("_default")
                        .setTlsCertificate(null)
                        .setTlsKeyPassword(null)
                        .setTlsKey(null)
                        .setTimeouts("60")
                        .setSchemaFolder("couchbase-schema")
                        .setPageSize("10000"));
    }

    @Test
    void testExplicitPropertyMappings()
            throws IOException
    {
        Path tls = Files.createTempFile(null, null);
        Path keystoreFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("couchbase.cluster", "remote-host")
                .put("couchbase.username", "some-user")
                .put("couchbase.password", "some-password")
                .put("couchbase.bucket", "some-bucket")
                .put("couchbase.scope", "some-scope")
                .put("couchbase.tls-certificate", tls.toString())
                .put("couchbase.tls-key", keystoreFile.toString())
                .put("couchbase.tls-key-password", "some-keystore-password")
                .put("couchbase.schema-folder", "some-folder")
                .put("couchbase.timeouts", "10")
                .put("couchbase.page-size", "1")
                .buildOrThrow();

        CouchbaseConfig expected = new CouchbaseConfig()
                .setCluster("remote-host")
                .setUsername("some-user")
                .setPassword("some-password")
                .setBucket("some-bucket")
                .setScope("some-scope")
                .setTlsCertificate(tls.toString())
                .setTlsKey(keystoreFile.toString())
                .setTlsKeyPassword("some-keystore-password")
                .setSchemaFolder("some-folder")
                .setTimeouts("10")
                .setPageSize("1");

        assertFullMapping(properties, expected);
    }
}

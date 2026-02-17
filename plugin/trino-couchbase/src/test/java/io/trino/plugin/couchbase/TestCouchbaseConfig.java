package io.trino.plugin.couchbase;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.*;

public class TestCouchbaseConfig {

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
                        .setSchemaFolder("couchbase-schema")
        );
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
                .put("couchbase.tls-keystore", keystoreFile.toString())
                .put("couchbase.tls-keystore-password", "some-keystore-password")
                .put("couchbase.schema-folder", "some-folder")
                .build();

        CouchbaseConfig expected = new CouchbaseConfig()
                .setCluster("remote-host")
                .setUsername("some-user")
                .setPassword("some-password")
                .setBucket("some-bucket")
                .setScope("some-scope")
                .setTlsCertificate(tls.toString())
                .setTlsKey(keystoreFile.toString())
                .setTlsKeyPassword("some-keystore-password")
                .setSchemaFolder("some-folder");

        assertFullMapping(properties, expected);
    }
}

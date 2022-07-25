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
package io.trino.plugin.pinot;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.trino.plugin.pinot.client.PinotGrpcServerQueryClientTlsConfig;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.plugin.pinot.client.PinotKeystoreTrustStoreType.JKS;
import static io.trino.plugin.pinot.client.PinotKeystoreTrustStoreType.PKCS12;

public class TestPinotGrpcServerQueryClientTlsConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(
                ConfigAssertions.recordDefaults(PinotGrpcServerQueryClientTlsConfig.class)
                        .setKeystoreType(JKS)
                        .setKeystorePath(null)
                        .setKeystorePassword(null)
                        .setTruststoreType(JKS)
                        .setTruststorePath(null)
                        .setTruststorePassword(null)
                        .setSslProvider("JDK"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("pinot.grpc.tls.keystore-type", "PKCS12")
                .put("pinot.grpc.tls.keystore-path", "/root")
                .put("pinot.grpc.tls.keystore-password", "password")
                .put("pinot.grpc.tls.truststore-type", "PKCS12")
                .put("pinot.grpc.tls.truststore-path", "/root")
                .put("pinot.grpc.tls.truststore-password", "password")
                .put("pinot.grpc.tls.ssl-provider", "OPENSSL")
                .buildOrThrow();
        PinotGrpcServerQueryClientTlsConfig expected = new PinotGrpcServerQueryClientTlsConfig()
                .setKeystoreType(PKCS12)
                .setKeystorePath("/root")
                .setKeystorePassword("password")
                .setTruststoreType(PKCS12)
                .setTruststorePath("/root")
                .setTruststorePassword("password")
                .setSslProvider("OPENSSL");
        ConfigAssertions.assertFullMapping(properties, expected);
    }
}

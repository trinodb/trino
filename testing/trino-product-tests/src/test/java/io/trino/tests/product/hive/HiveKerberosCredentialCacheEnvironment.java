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
package io.trino.tests.product.hive;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * Kerberos-enabled Hive environment using credential cache instead of keytab.
 * <p>
 * This environment demonstrates Trino's ability to use pre-obtained Kerberos
 * tickets (credential cache) instead of keytabs for authentication. The credential
 * cache is created by running {@code kinit} with a keytab before the test starts.
 * <p>
 * <b>Credential Cache vs Keytab:</b>
 * <ul>
 *   <li>Keytab: Contains the principal's long-term secret key, can be used to
 *       obtain new tickets at any time</li>
 *   <li>Credential Cache: Contains pre-obtained tickets with limited lifetime,
 *       no secret key exposure</li>
 * </ul>
 * <p>
 * <b>Configuration differences from base Kerberos environment:</b>
 * <ul>
 *   <li>{@code hive.metastore.client.credential-cache.location} instead of
 *       {@code hive.metastore.client.keytab}</li>
 *   <li>{@code hive.hdfs.trino.credential-cache.location} instead of
 *       {@code hive.hdfs.trino.keytab}</li>
 * </ul>
 */
public class HiveKerberosCredentialCacheEnvironment
        extends HiveKerberosEnvironment
{
    @Override
    protected void writeCredentialCaches(Path keytabDir)
            throws IOException
    {
        // Create credential cache from trino keytab using kinit
        String principal = TRINO_PRINCIPAL + "@" + kdc.getRealm();
        byte[] credentialCache = kdc.createCredentialCache(principal, KDC_TRINO_KEYTAB_PATH);
        Path credentialCachePath = keytabDir.resolve("trino-krbcc");
        Files.write(credentialCachePath, credentialCache);
        credentialCachePath.toFile().setReadable(true, false);
    }

    @Override
    protected Map<String, String> getMetastoreAuthenticationProperties()
    {
        return Map.of("hive.metastore.client.credential-cache.location", TRINO_CREDENTIAL_CACHE);
    }

    @Override
    protected Map<String, String> getHdfsAuthenticationProperties()
    {
        return Map.of("hive.hdfs.trino.credential-cache.location", TRINO_CREDENTIAL_CACHE);
    }

    @Override
    protected Map<String, Map<String, String>> getAdditionalCatalogs()
    {
        return Map.of("tpch", Map.of("connector.name", "tpch"));
    }
}

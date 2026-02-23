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
 * Kerberos-enabled Hive environment with impersonation using credential cache instead of keytab.
 * <p>
 * This environment combines:
 * <ul>
 *   <li>Credential cache authentication (instead of keytabs)</li>
 *   <li>HDFS impersonation - Trino impersonates the end user for HDFS access</li>
 *   <li>Hive Metastore thrift impersonation - Trino impersonates for HMS access</li>
 *   <li>SQL-standard security - Hive authorization based on SQL permissions</li>
 *   <li>Hive views enabled - Support for querying Hive views</li>
 * </ul>
 * <p>
 * <b>Configuration differences from {@link HiveKerberosImpersonationEnvironment}:</b>
 * <ul>
 *   <li>{@code hive.metastore.client.credential-cache.location} instead of
 *       {@code hive.metastore.client.keytab}</li>
 *   <li>{@code hive.hdfs.trino.credential-cache.location} instead of
 *       {@code hive.hdfs.trino.keytab}</li>
 * </ul>
 */
public class HiveKerberosImpersonationCredentialCacheEnvironment
        extends HiveKerberosImpersonationEnvironment
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
}

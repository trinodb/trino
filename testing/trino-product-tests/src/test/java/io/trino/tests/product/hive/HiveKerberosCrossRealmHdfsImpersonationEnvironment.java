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

import io.trino.testing.containers.KerberosContainer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class HiveKerberosCrossRealmHdfsImpersonationEnvironment
        extends HiveKerberosHdfsImpersonationEnvironment
{
    private static final String OTHER_HDFS_KEYTAB = "/etc/trino/hdfs-other.keytab";
    private static final String OTHER_HIVE_KEYTAB = "/etc/trino/hive-other.keytab";
    private static final String KDC_OTHER_HDFS_KEYTAB = "/keytabs/hdfs-other.keytab";
    private static final String KDC_OTHER_HIVE_KEYTAB = "/keytabs/hive-other.keytab";
    private static final String OTHER_REALM_AUTH_TO_LOCAL = "RULE:[2:$1@$0](.*@OTHER.TRINO.TEST)s/@.*// DEFAULT";

    @Override
    protected List<PrincipalSpec> getAdditionalPrincipals()
    {
        return List.of(
                new PrincipalSpec(HDFS_PRINCIPAL, KDC_OTHER_HDFS_KEYTAB, true),
                new PrincipalSpec(HIVE_PRINCIPAL, KDC_OTHER_HIVE_KEYTAB, true));
    }

    @Override
    protected void writeAdditionalKeytabs(Path keytabDir)
            throws IOException
    {
        writeKeytab(keytabDir, "hdfs-other.keytab", KDC_OTHER_HDFS_KEYTAB);
        writeKeytab(keytabDir, "hive-other.keytab", KDC_OTHER_HIVE_KEYTAB);
    }

    @Override
    protected String getMetastoreClientPrincipal()
    {
        return HIVE_PRINCIPAL + "@" + KerberosContainer.OTHER_REALM;
    }

    @Override
    protected String getHdfsClientPrincipal()
    {
        return HDFS_PRINCIPAL + "@" + KerberosContainer.OTHER_REALM;
    }

    @Override
    protected Map<String, String> getMetastoreAuthenticationProperties()
    {
        return Map.of("hive.metastore.client.keytab", OTHER_HIVE_KEYTAB);
    }

    @Override
    protected Map<String, String> getHdfsAuthenticationProperties()
    {
        return Map.of("hive.hdfs.trino.keytab", OTHER_HDFS_KEYTAB);
    }

    @Override
    protected Map<String, String> getAdditionalTrinoKeytabs()
    {
        return Map.of(
                "hdfs-other.keytab", OTHER_HDFS_KEYTAB,
                "hive-other.keytab", OTHER_HIVE_KEYTAB);
    }

    @Override
    protected Map<String, String> getAdditionalCatalogProperties()
    {
        return Map.of("hive.non-managed-table-writes-enabled", "true");
    }

    @Override
    protected Map<String, String> getCoreSiteProperties()
    {
        return Map.of(
                "hadoop.proxyuser.hdfs.hosts", "*",
                "hadoop.proxyuser.hdfs.groups", "*",
                "hadoop.proxyuser.hdfs.users", "*",
                "hadoop.security.auth_to_local", OTHER_REALM_AUTH_TO_LOCAL);
    }

    @Override
    protected Map<String, String> getHdfsClientSiteProperties()
    {
        return Map.of("hadoop.security.auth_to_local", OTHER_REALM_AUTH_TO_LOCAL);
    }
}

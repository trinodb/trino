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
package io.trino.plugin.ranger;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class RangerConfig
{
    private String keytab;
    private String principal;
    private boolean useUgi;
    private String hadoopConfigPath;
    private String serviceName;
    private String securityConfigPath;
    private String auditConfigPath;
    private String policyMgrSslConfigPath;

    public String getKeytab()
    {
        return keytab;
    }

    @Config("ranger.keytab")
    @ConfigDescription("Keytab for authentication against Ranger")
    @SuppressWarnings("unused")
    public RangerConfig setKeytab(String keytab)
    {
        this.keytab = keytab;
        return this;
    }

    public String getPrincipal()
    {
        return principal;
    }

    @Config("ranger.principal")
    @ConfigDescription("Principal for authentication against Ranger with keytab")
    @SuppressWarnings("unused")
    public RangerConfig setPrincipal(String principal)
    {
        this.principal = principal;
        return this;
    }

    public boolean isUseUgi()
    {
        return useUgi;
    }

    @Config("ranger.use_ugi")
    @ConfigDescription("Use Hadoop User Group Information instead of Trino groups")
    @SuppressWarnings("unused")
    public RangerConfig setUseUgi(boolean useUgi)
    {
        this.useUgi = useUgi;
        return this;
    }

    @Config("ranger.hadoop_config")
    @ConfigDescription("Path to hadoop configuration. Defaults to trino-ranger-site.xml in classpath")
    @SuppressWarnings("unused")
    public RangerConfig setHadoopConfigPath(String hadoopConfigPath)
    {
        this.hadoopConfigPath = hadoopConfigPath;
        return this;
    }

    public String getHadoopConfigPath()
    {
        return hadoopConfigPath;
    }

    public String getServiceName()
    {
        return serviceName;
    }

    @Config("ranger.service_name")
    @ConfigDescription("Name of Ranger service containing policies to enforce. Defaults to dev_trino")
    public RangerConfig setServiceName(String serviceName)
    {
        this.serviceName = serviceName;
        return this;
    }

    public String getSecurityConfigPath()
    {
        return securityConfigPath;
    }

    @Config("ranger.security_config")
    @ConfigDescription("Path to Ranger plugin security configuration. Defaults to ranger-trino-security.xml in classpath")
    public RangerConfig setSecurityConfigPath(String securityConfigPath)
    {
        this.securityConfigPath = securityConfigPath;
        return this;
    }

    public String getAuditConfigPath()
    {
        return auditConfigPath;
    }

    @Config("ranger.audit_config")
    @ConfigDescription("Path to Ranger plugin audit configuration. Defaults to ranger-trino-audit.xml in classpath")
    public RangerConfig setAuditConfigPath(String auditConfigPath)
    {
        this.auditConfigPath = auditConfigPath;
        return this;
    }

    public String getPolicyMgrSslConfigPath()
    {
        return policyMgrSslConfigPath;
    }

    @Config("ranger.policy_mgr_ssl_config")
    @ConfigDescription("Path to Ranger admin ssl configuration. Defaults to ranger-policymgr-ssl.xml in classpath")
    public RangerConfig setPolicyMgrSslConfigPath(String policyMgrSslConfigPath)
    {
        this.policyMgrSslConfigPath = policyMgrSslConfigPath;
        return this;
    }
}

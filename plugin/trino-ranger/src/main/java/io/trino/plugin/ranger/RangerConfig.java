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

import java.util.Arrays;
import java.util.List;

public class RangerConfig
{
    public static final String RANGER_TRINO_DEFAULT_SERVICE_NAME = "dev_trino";
    public static final String RANGER_TRINO_DEFAULT_SECURITY_CONF = "ranger-trino-security.xml";
    public static final String RANGER_TRINO_DEFAULT_AUDIT_CONF = "ranger-trino-audit.xml";
    public static final String RANGER_TRINO_DEFAULT_POLICYMGR_SSL_CONF = "ranger-policymgr-ssl.xml";
    public static final String RANGER_TRINO_DEFAULT_HADOOP_CONF = "trino-ranger-site.xml";

    private String serviceName = RANGER_TRINO_DEFAULT_SERVICE_NAME;
    private List<String> pluginConfigResource = Arrays.asList(RANGER_TRINO_DEFAULT_SECURITY_CONF, RANGER_TRINO_DEFAULT_AUDIT_CONF, RANGER_TRINO_DEFAULT_POLICYMGR_SSL_CONF);
    private List<String> hadoopConfigResource = Arrays.asList(RANGER_TRINO_DEFAULT_HADOOP_CONF);

    public String getServiceName()
    {
        return serviceName;
    }

    @Config("ranger.service.name")
    @ConfigDescription("Name of Ranger service containing policies to enforce. Defaults to dev_trino")
    public RangerConfig setServiceName(String serviceName)
    {
        this.serviceName = serviceName;
        return this;
    }

    public List<String> getPluginConfigResource()
    {
        return pluginConfigResource;
    }

    @Config("ranger.plugin.config.resource")
    @ConfigDescription("List of paths to Ranger plugin configuration files. Defaults to ranger-trino-security.xml,ranger-trino-audit.xml,ranger-policymgr-ssl.xml in classpath")
    public RangerConfig setPluginConfigResource(List<String> pluginConfigResource)
    {
        this.pluginConfigResource = pluginConfigResource;
        return this;
    }

    @Config("ranger.hadoop.config.resource")
    @ConfigDescription("List of paths to hadoop configuration files. Defaults to trino-ranger-site.xml in classpath")
    @SuppressWarnings("unused")
    public RangerConfig setHadoopConfigResource(List<String> hadoopConfigResource)
    {
        this.hadoopConfigResource = hadoopConfigResource;
        return this;
    }

    public List<String> getHadoopConfigResource()
    {
        return hadoopConfigResource;
    }
}

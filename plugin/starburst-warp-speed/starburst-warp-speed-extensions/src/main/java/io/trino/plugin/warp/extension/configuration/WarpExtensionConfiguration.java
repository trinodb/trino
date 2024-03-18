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
package io.trino.plugin.warp.extension.configuration;

import io.airlift.configuration.Config;
import io.airlift.configuration.LegacyConfig;

import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.LOCAL_DATA_STORAGE_PREFIX;
import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.WARP_SPEED_PREFIX;

public class WarpExtensionConfiguration
{
    public static final String CLUSTER_UUID = "cluster-uuid";
    public static final String HTTP_REST_PORT = "http-rest-port";
    // true in case we leverage the trino http port
    public static final String USE_HTTP_SERVER_PORT = "use-http-server-port";
    public static final String HTTP_REST_PORT_ENABLED = "config.http-rest-port-enabled";
    public static final String INTERNAL_COMMUNICATION_SHARED_SECRET = "config.internal-communication.shared-secret";
    public static final String ENABLED = "config.extensions.enabled";
    public static final int HTTP_REST_DEFAULT_PORT = 8088;

    private String clusterUUID;
    private int restHttpPort = HTTP_REST_DEFAULT_PORT;
    private boolean useHttpServerPort;
    private boolean restHttpDefaultPortEnabled;
    private String internalCommunicationSharedSecret;
    private boolean enabled;

    public String getClusterUUID()
    {
        return clusterUUID;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + CLUSTER_UUID)
    @Config(WARP_SPEED_PREFIX + CLUSTER_UUID)
    public void setClusterUUID(String clusterUUID)
    {
        this.clusterUUID = clusterUUID;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + USE_HTTP_SERVER_PORT)
    @Config(WARP_SPEED_PREFIX + USE_HTTP_SERVER_PORT)
    public void setUseHttpServerPort(boolean useHttpServerPort)
    {
        this.useHttpServerPort = useHttpServerPort;
    }

    public int getRestHttpPort()
    {
        return restHttpPort;
    }

    @Config(HTTP_REST_PORT)
    public void setRestHttpPort(int restHttpPort)
    {
        this.restHttpPort = restHttpPort;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + HTTP_REST_PORT_ENABLED)
    @Config(WARP_SPEED_PREFIX + HTTP_REST_PORT_ENABLED)
    public void setRestHttpDefaultPortEnabled(boolean restHttpDefaultPortEnabled)
    {
        this.restHttpDefaultPortEnabled = restHttpDefaultPortEnabled;
    }

    public String getInternalCommunicationSharedSecret()
    {
        return internalCommunicationSharedSecret;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + INTERNAL_COMMUNICATION_SHARED_SECRET)
    @Config(WARP_SPEED_PREFIX + INTERNAL_COMMUNICATION_SHARED_SECRET)
    public void setInternalCommunicationSharedSecret(String internalCommunicationSharedSecret)
    {
        this.internalCommunicationSharedSecret = internalCommunicationSharedSecret;
    }

    public boolean isEnabled()
    {
        return enabled;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + ENABLED)
    @Config(WARP_SPEED_PREFIX + ENABLED)
    public void setEnabled(boolean enabled)
    {
        this.enabled = enabled;
    }

    public boolean isUseHttpServerPort()
    {
        return useHttpServerPort;
    }

    public boolean isRestHttpDefaultPortEnabled()
    {
        return restHttpDefaultPortEnabled;
    }
}

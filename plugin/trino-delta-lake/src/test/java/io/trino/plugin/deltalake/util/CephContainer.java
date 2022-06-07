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
package io.trino.plugin.deltalake.util;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.containers.wait.strategy.SelectedPortWaitStrategy;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import java.io.Closeable;
import java.time.Duration;

public final class CephContainer
        implements Closeable
{
    public static final String CEPH_ACCESS_KEY = "ceph-access-key";
    public static final String CEPH_SECRET_KEY = "ceph-secret-key";
    public static final int CEPH_PORT = 8000;

    private final DockerContainer ceph;

    public CephContainer(Network network)
    {
        ceph = new DockerContainer("ceph/daemon:master-dba849b-pacific-centos-8")
                .withEnv(ImmutableMap.<String, String>builder()
                        .put("MON_IP", "127.0.0.1")
                        .put("OSD_COUNT", "1")
                        .put("CLUSTER", "ceph")
                        .put("CEPH_PUBLIC_NETWORK", "0.0.0.0/0")
                        .put("CEPH_DEMO_UID", "ceph")
                        .put("CEPH_DEMO_ACCESS_KEY", CEPH_ACCESS_KEY)
                        .put("CEPH_DEMO_SECRET_KEY", CEPH_SECRET_KEY)
                        .put("RGW_NAME", "ceph")
                        .put("RGW_FRONTEND_PORT", String.valueOf(CEPH_PORT))
                        .buildOrThrow())
                .withNetwork(network)
                .withNetworkAliases("ceph")
                .withCommand("demo")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(new SelectedPortWaitStrategy(CEPH_PORT))
                .withStartupTimeout(Duration.ofMinutes(1));

        ceph.addExposedPort(CEPH_PORT);
    }

    public void start()
    {
        ceph.start();
    }

    @Override
    public void close()
    {
        ceph.close();
    }

    public String getCephEndpoint()
    {
        // Ceph demo container tries to resolve given hostname into a bucket name.
        // That results in a NoSuchBucket error. Workaround is to use service IP address.
        return "http://0.0.0.0:" + ceph.getMappedPort(CEPH_PORT);
    }
}

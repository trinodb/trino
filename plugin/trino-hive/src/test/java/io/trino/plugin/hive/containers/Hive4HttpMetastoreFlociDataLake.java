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
package io.trino.plugin.hive.containers;

import com.google.common.collect.ImmutableMap;

import java.net.URI;
import java.util.Map;
import java.util.Set;

import static io.trino.plugin.hive.containers.HiveFlociDataLake.State.STARTED;
import static io.trino.testing.containers.TestContainers.getPathFromClassPathResource;
import static java.util.Objects.requireNonNull;

/**
 * Floci S3 and a Hive 4 metastore that serves Thrift over HTTP or HTTPS.
 * HTTPS uses the metastore's own TLS with {@code hive_floci_datalake/hms-keystore.jks};
 * clients trust it with {@link #TRUSTSTORE_RESOURCE}.
 */
public class Hive4HttpMetastoreFlociDataLake
        extends HiveFlociDataLake
{
    // must match hive.metastore.warehouse.dir in hive4-hive-site-http(s).xml
    public static final String BUCKET_NAME = "test-hive-http-metastore";
    public static final String TRUSTSTORE_RESOURCE = "hive_floci_datalake/hms-truststore.jks";
    public static final String TRUSTSTORE_PASSWORD = "changeit";

    public enum Transport
    {
        HTTP,
        HTTPS,
    }

    private final Transport transport;
    private final Hive4Metastore hiveMetastore;

    public Hive4HttpMetastoreFlociDataLake(Transport transport)
    {
        super(BUCKET_NAME);
        this.transport = requireNonNull(transport, "transport is null");
        ImmutableMap.Builder<String, String> filesToMount = ImmutableMap.builder();
        switch (transport) {
            case HTTP -> filesToMount.put("/opt/hive/conf/hive-site.xml", getPathFromClassPathResource("hive_floci_datalake/hive4-hive-site-http.xml"));
            case HTTPS -> filesToMount
                    .put("/opt/hive/conf/hive-site.xml", getPathFromClassPathResource("hive_floci_datalake/hive4-hive-site-https.xml"))
                    .put("/opt/hive/conf/hms-keystore.jks", getPathFromClassPathResource("hive_floci_datalake/hms-keystore.jks"));
        }
        this.hiveMetastore = closer.register(Hive4Metastore.builder()
                .withEnvVars(Map.of("SERVICE_NAME", "metastore"))
                .withNetwork(network)
                .withExposePorts(Set.of(Hive4Metastore.HIVE_METASTORE_PORT))
                .withFilesToMount(filesToMount.buildOrThrow())
                .build());
    }

    @Override
    public void start()
    {
        super.start();
        hiveMetastore.start();
        state = STARTED;
    }

    @Override
    public String runOnHive(String sql)
    {
        throw new UnsupportedOperationException("HiveServer2 is not started");
    }

    @Override
    public HiveHadoop getHiveHadoop()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public URI getHiveMetastoreEndpoint()
    {
        return hiveMetastore.getHiveMetastoreHttpEndpoint(transport == Transport.HTTPS);
    }
}

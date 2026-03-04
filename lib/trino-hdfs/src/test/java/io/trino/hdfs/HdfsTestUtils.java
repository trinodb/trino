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
package io.trino.hdfs;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.hdfs.azure.HiveAzureConfig;
import io.trino.hdfs.azure.TrinoAzureConfigurationInitializer;
import io.trino.hdfs.gcs.GoogleGcsConfigurationInitializer;
import io.trino.hdfs.gcs.HiveGcsConfig;
import io.trino.hdfs.s3.HiveS3Config;
import io.trino.hdfs.s3.TrinoS3ConfigurationInitializer;

import java.util.Optional;

public final class HdfsTestUtils
{
    public static final Optional<HostAndPort> SOCKS_PROXY = Optional.ofNullable(System.getProperty("hive.metastore.thrift.client.socks-proxy"))
            .map(HostAndPort::fromString);

    public static final DynamicHdfsConfiguration HDFS_CONFIGURATION = new DynamicHdfsConfiguration(
            new HdfsConfigurationInitializer(
                    new HdfsConfig()
                            .setSocksProxy(SOCKS_PROXY.orElse(null)),
                    ImmutableSet.of(
                            new TrinoS3ConfigurationInitializer(new HiveS3Config()),
                            new GoogleGcsConfigurationInitializer(new HiveGcsConfig()),
                            new TrinoAzureConfigurationInitializer(new HiveAzureConfig()))),
            ImmutableSet.of());

    public static final TrinoHdfsFileSystemStats HDFS_FILE_SYSTEM_STATS = new TrinoHdfsFileSystemStats();

    public static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment(
            HDFS_CONFIGURATION,
            new HdfsConfig(),
            new NoHdfsAuthentication());

    public static final HdfsFileSystemFactory HDFS_FILE_SYSTEM_FACTORY = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS);

    private HdfsTestUtils() {}
}

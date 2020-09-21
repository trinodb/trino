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
package io.prestosql.tests.product.launcher.env.configs;

import com.google.common.collect.ImmutableList;
import io.prestosql.tests.product.launcher.docker.DockerFiles;
import io.prestosql.tests.product.launcher.env.Environment;

import javax.inject.Inject;

import java.util.List;

import static io.prestosql.tests.product.launcher.env.EnvironmentContainers.PRESTO;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

public class ConfigHdp3
        extends ConfigDefault
{
    private final DockerFiles dockerFiles;

    @Inject
    public ConfigHdp3(DockerFiles dockerFiles)
    {
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
    }

    /**
     * export HADOOP_BASE_IMAGE="prestodev/hdp3.1-hive"
     * export TEMPTO_ENVIRONMENT_CONFIG_FILE="/docker/presto-product-tests/conf/tempto/tempto-configuration-for-hive3.yaml"
     * export DISTRO_SKIP_GROUP=iceberg
     */
    @Override
    public String getHadoopBaseImage()
    {
        return "prestodev/hdp3.1-hive";
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainers(container -> {
            if (container.getLogicalName().startsWith(PRESTO)) {
                container.withCopyFileToContainer(forHostPath(
                        // HDP3's handling of timestamps is incompatible with previous versions of Hive (see https://issues.apache.org/jira/browse/HIVE-21002);
                        // in order for Presto to deal with the differences, we must set catalog properties for Parquet and RCFile
                        dockerFiles.getDockerFilesHostPath("common/standard/presto-init-hdp3.sh")),
                        "/docker/presto-init.d/presto-init-hdp3.sh");
            }
        });
    }

    @Override
    public List<String> getExcludedGroups()
    {
        return ImmutableList.of("iceberg");
    }

    @Override
    public String getTemptoEnvironmentConfigFile()
    {
        return "/docker/presto-product-tests/conf/tempto/tempto-configuration-for-hive3.yaml";
    }
}

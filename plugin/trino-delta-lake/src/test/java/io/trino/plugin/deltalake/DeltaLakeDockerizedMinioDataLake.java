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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.deltalake.util.DockerizedMinioDataLake;

import java.util.Optional;

public final class DeltaLakeDockerizedMinioDataLake
{
    private DeltaLakeDockerizedMinioDataLake()
    {
    }

    public static DockerizedMinioDataLake createDockerizedMinioDataLakeForDeltaLake(String bucketName)
    {
        return createDockerizedMinioDataLakeForDeltaLake(bucketName, Optional.empty());
    }

    public static DockerizedMinioDataLake createDockerizedMinioDataLakeForDeltaLake(String bucketName, Optional<String> hadoopBaseImage)
    {
        return new DockerizedMinioDataLake(
                bucketName,
                hadoopBaseImage,
                ImmutableMap.of(
                        "io/trino/plugin/deltalake/core-site.xml", "/etc/hadoop/conf/core-site.xml",
                        "io/trino/plugin/deltalake/hdp3.1-core-site.xml.abfs-template", "/etc/hadoop/conf/core-site.xml.abfs-template"),
                ImmutableMap.of());
    }
}

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

import io.trino.testing.TestingProperties;

final class DockerImages
{
    private DockerImages() {}

    public static final String DEFAULT_HADOOP_BASE_IMAGE = System.getenv().getOrDefault("HADOOP_BASE_IMAGE", "ghcr.io/trinodb/testing/hdp2.6-hive");
    public static final String DOCKER_IMAGES_VERSION = System.getenv().getOrDefault("DOCKER_IMAGES_VERSION", TestingProperties.getDockerImagesVersion());
}

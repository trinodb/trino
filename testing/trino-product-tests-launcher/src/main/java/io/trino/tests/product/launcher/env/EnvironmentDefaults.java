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
package io.trino.tests.product.launcher.env;

public final class EnvironmentDefaults
{
    public static final String DOCKER_IMAGES_VERSION = "46";
    public static final String HADOOP_BASE_IMAGE = "ghcr.io/trinodb/testing/hdp2.6-hive";
    public static final String HADOOP_IMAGES_VERSION = DOCKER_IMAGES_VERSION;
    public static final String TEMPTO_ENVIRONMENT_CONFIG = "/dev/null";

    private EnvironmentDefaults() {}
}

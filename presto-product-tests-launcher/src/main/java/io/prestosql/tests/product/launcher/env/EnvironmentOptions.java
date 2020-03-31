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
package io.prestosql.tests.product.launcher.env;

import com.google.inject.Module;
import io.airlift.airline.Option;

import java.io.File;

public final class EnvironmentOptions
{
    @Option(name = "--hadoop-base-image", title = "image", description = "Hadoop base image")
    public String hadoopBaseImage = System.getenv().getOrDefault("HADOOP_BASE_IMAGE", "prestodev/hdp2.6-hive");

    @Option(name = "--image-version", title = "version", description = "docker images version")
    public String imagesVersion = System.getenv().getOrDefault("DOCKER_IMAGES_VERSION", "27");

    @Option(name = "--server-package", title = "server-package", description = "path to Presto server package")
    public File serverPackage = new File("presto-server/target/presto-server-${project.version}.tar.gz");

    @Option(name = "--without-presto", title = "without Presto", description = "do not start presto-master")
    public boolean withoutPrestoMaster;

    @Option(name = "--debug", description = "open Java debug ports")
    public boolean debug;

    public Module toModule()
    {
        return binder -> {
            binder.bind(EnvironmentOptions.class).toInstance(this);
        };
    }
}

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
package io.trino.tests.product.launcher.env.environment;

import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

import static java.nio.file.attribute.PosixFilePermissions.fromString;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvMultinodeSnowflake
        extends EnvironmentProvider
{
    private final DockerFiles.ResourceProvider configDir;

    @Inject
    public EnvMultinodeSnowflake(DockerFiles dockerFiles, Standard standard)
    {
        super(standard);
        configDir = requireNonNull(dockerFiles, "dockerFiles is null").getDockerFilesHostDirectory("conf/environment/multinode-snowflake");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addConnector("snowflake", forHostPath(getEnvProperties()));
    }

    private Path getEnvProperties()
    {
        try {
            String properties = Files.readString(configDir.getPath("snowflake.properties"))
                    .replace("${ENV:SNOWFLAKE_URL}", requireEnv("SNOWFLAKE_URL"))
                    .replace("${ENV:SNOWFLAKE_USER}", requireEnv("SNOWFLAKE_USER"))
                    .replace("${ENV:SNOWFLAKE_PASSWORD}", requireEnv("SNOWFLAKE_PASSWORD"))
                    .replace("${ENV:SNOWFLAKE_DATABASE}", requireEnv("SNOWFLAKE_DATABASE"))
                    .replace("${ENV:SNOWFLAKE_ROLE}", requireEnv("SNOWFLAKE_ROLE"))
                    .replace("${ENV:SNOWFLAKE_WAREHOUSE}", requireEnv("SNOWFLAKE_WAREHOUSE"));
            File newProperties = Files.createTempFile("snowflake-replaced", ".properties", PosixFilePermissions.asFileAttribute(fromString("rwxrwxrwx"))).toFile();
            newProperties.deleteOnExit();
            Files.writeString(newProperties.toPath(), properties);
            return newProperties.toPath();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String requireEnv(String variable)
    {
        return requireNonNull(System.getenv(variable), () -> "environment variable not set: " + variable);
    }
}

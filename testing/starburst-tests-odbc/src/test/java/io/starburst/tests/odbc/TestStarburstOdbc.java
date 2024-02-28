/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.tests.odbc;

import com.github.dockerjava.api.DockerClient;
import io.airlift.log.Logger;
import io.starburst.tests.odbc.protocol.StarburstProtocolModule;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.containers.junit.ReportLeakedContainers;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

public class TestStarburstOdbc
{
    private final Logger log = Logger.get(TestStarburstOdbc.class);

    @Test
    public void testOdbcDriver()
            throws Exception
    {
        DistributedQueryRunner runner = TpchQueryRunnerBuilder.builder()
                .setAdditionalModule(new StarburstProtocolModule())
                .build();

        try (runner; GenericContainer<?> container = new GenericContainer<>(buildTestsContainer())) {
            container.withAccessToHost(true); // Access locally running query runner
            container.withNetworkMode("host");
            container.withStartupCheckStrategy(new OneShotStartupCheckStrategy()
                            .withTimeout(Duration.ofMinutes(5)))
                    .withEnv("TRINO_HOST", runner.getCoordinator().getAddress().getHost())
                    .withEnv("TRINO_PORT", String.valueOf(runner.getCoordinator().getAddress().getPort()))
                    .start();

            String results = container.getLogs();
            log.info("ODBC test results:\n%s", results.strip());
            assertThat(results).contains("22 passed");

            ignoreSshContainer();
        }
    }

    private static String buildTestsContainer()
    {
        return new ImageFromDockerfile()
                .withFileFromClasspath("tests", "tests")
                .withFileFromClasspath("Dockerfile", "Dockerfile")
                .withBuildImageCmdModifier(buildImageCmd -> buildImageCmd.withPlatform("linux/amd64")) // Only x86 is supported for ODBC
                .get();
    }

    private static void ignoreSshContainer()
    {
        DockerClient client = DockerClientFactory.lazyClient();
        client.listContainersCmd()
                .exec()
                .stream()
                .filter(container -> container.getImage().contains("testcontainers/sshd"))
                .forEach(container -> ReportLeakedContainers.ignoreContainerId(container.getId()));
    }
}

/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.testing.testcontainers;

import com.github.dockerjava.api.model.Ulimit;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.Map;
import java.util.function.BiConsumer;

import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.wait.strategy.Wait.forHealthcheck;
import static org.testcontainers.utility.MountableFile.forClasspathResource;

public final class SapHanaDockerInitializer
{
    public static final String SAP_HANA_DOCKER_IMAGE = "843985043183.dkr.ecr.us-east-2.amazonaws.com/testing/hanaexpress:2.00.045.00.20200121.1";
    public static final int SYSTEM_PORT = 39013;
    public static final int SYSTEMDB_PORT = 39017;

    BiConsumer<GenericContainer<?>, Integer> portBinder;

    private static Map<String, String> sysctlMap()
    {
        // https://help.sap.com/viewer/6b94445c94ae495c83a19646e7c3fd56/2.0.04/en-US/82e4575eec664846a9918e9ed1d90d41.html
        return ImmutableMap
                .of("kernel.shmmax", "1073741824", // Maximum size of shared memory segment
                        "kernel.shmall", "8388608", // Total amount of shared memory pages
                        "net.ipv4.ip_local_port_range", "40000 60999");
    }

    public SapHanaDockerInitializer(BiConsumer<GenericContainer<?>, Integer> portBinder)
    {
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    private void exposePort(GenericContainer<?> container, int port)
    {
        portBinder.accept(container, port);
    }

    private void exposePortsRange(GenericContainer<?> container, int from, int to)
    {
        for (int port = from; port <= to; port++) {
            exposePort(container, port);
        }
    }

    public void apply(GenericContainer<?> container)
    {
        // https://blogs.sap.com/2017/12/04/hey-sap-hana-express-edition-any-idea-whats-your-sql-port-number/
        exposePort(container, SYSTEM_PORT); // SYSTEM tenant database port
        exposePort(container, SYSTEMDB_PORT); // SYSTEMDB tenant database port
        exposePortsRange(container, 39041, 39045); // Additional tenant ports
        exposePortsRange(container, 1128, 1129); // SAP host agent
        exposePortsRange(container, 59013, 59014); // Instance agent

        container.withCreateContainerCmdModifier(command -> command
                .getHostConfig()
                .withSysctls(sysctlMap())
                .withUlimits(ImmutableList.of(new Ulimit("nofile", 1048576L, 1048576L))))
                .withCommand("--passwords-url", "file:///hana/mounts/passwords.json", "--agree-to-sap-license");

        container.withCopyFileToContainer(
                forClasspathResource("com/starburstdata/presto/testing/testcontainers/passwords.json"),
                "/hana/mounts/passwords.json");

        container.waitingFor(forHealthcheck());
        container.withStartupTimeout(Duration.ofMinutes(15));
        container.withStartupAttempts(3);
    }
}

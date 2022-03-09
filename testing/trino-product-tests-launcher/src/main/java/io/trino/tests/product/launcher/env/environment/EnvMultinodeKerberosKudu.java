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
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Kerberos;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;

import javax.inject.Inject;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.isPrestoContainer;
import static io.trino.tests.product.launcher.env.common.Kerberos.DEFAULT_WAIT_STRATEGY;
import static io.trino.tests.product.launcher.env.common.Kerberos.KERBEROS;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_ETC;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.BindMode.READ_ONLY;
import static org.testcontainers.containers.BindMode.READ_WRITE;
import static org.testcontainers.containers.wait.strategy.Wait.forLogMessage;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvMultinodeKerberosKudu
        extends EnvironmentProvider
{
    private static final String KUDU_IMAGE = "apache/kudu:1.15.0";
    private static final Integer KUDU_MASTER_PORT = 7051;
    private static final Integer NUMBER_OF_REPLICA = 3;
    private static final String KUDU_MASTER = "kudu-master";
    private static final String KUDU_TABLET_TEMPLATE = "kudu-tserver-%s";

    private static Integer initialKuduTserverPort = 7060;

    private final PortBinder portBinder;
    private final DockerFiles.ResourceProvider configDir;

    @Inject
    public EnvMultinodeKerberosKudu(PortBinder portBinder, DockerFiles dockerFiles, StandardMultinode standardMultinode, Kerberos kerberos)
    {
        super(standardMultinode, kerberos);
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
        configDir = requireNonNull(dockerFiles, "dockerFiles is null").getDockerFilesHostDirectory("conf/environment/multinode-kerberos-kudu/");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        Path kerberosCredentialsDirectory = DockerFiles.createTemporaryDirectoryForDocker();

        builder.configureContainer(KERBEROS, container -> container
                .withFileSystemBind(kerberosCredentialsDirectory.toString(), "/kerberos", READ_WRITE)
                .withCopyFileToContainer(forHostPath(configDir.getPath("kerberos_init.sh"), 0777), "/docker/kerberos-init.d/kerberos_init.sh")
                .waitingForAll(
                        DEFAULT_WAIT_STRATEGY,
                        forLogMessage(".*Kerberos init script completed successfully.*", 1)));

        DockerContainer kuduMaster = createKuduMaster(kerberosCredentialsDirectory);
        List<DockerContainer> kuduTablets = createKuduTablets(kerberosCredentialsDirectory, kuduMaster);

        builder.addContainer(kuduMaster);
        builder.containerDependsOn(kuduMaster.getLogicalName(), KERBEROS);

        kuduTablets.forEach(tablet -> {
            builder.addContainer(tablet);
            builder.containerDependsOn(tablet.getLogicalName(), KERBEROS);
        });

        builder.configureContainers(container -> {
            if (isPrestoContainer(container.getLogicalName())) {
                configurePrestoContainer(container, kerberosCredentialsDirectory);
                addKrb5(container);
            }
        });
    }

    private DockerContainer createKuduMaster(Path kerberosCredentialsDirectory)
    {
        DockerContainer container = new DockerContainer(KUDU_IMAGE, KUDU_MASTER)
                .withCommand("master")
                .withEnv("MASTER_ARGS", format("--fs_wal_dir=/var/lib/kudu/master --logtostderr --use_hybrid_clock=false --rpc_authentication=required --rpc_bind_addresses=%s:%s --rpc_authentication=required --principal=kuduservice/kudu-master@STARBURSTDATA.COM --keytab_file=/kerberos/kudu-master.keytab", KUDU_MASTER, KUDU_MASTER_PORT))
                .withFileSystemBind(kerberosCredentialsDirectory.toString(), "/kerberos", READ_ONLY)
                .waitingFor(forSelectedPorts(KUDU_MASTER_PORT));

        addKrb5(container);

        portBinder.exposePort(container, KUDU_MASTER_PORT);

        return container;
    }

    private List<DockerContainer> createKuduTablets(Path kerberosCredentialsDirectory, DockerContainer kuduMaster)
    {
        List<DockerContainer> tabletContainers = new ArrayList<>();

        for (int i = 0; i < NUMBER_OF_REPLICA; i++) {
            String instanceName = format(KUDU_TABLET_TEMPLATE, i);
            DockerContainer kuduTablet = new DockerContainer(KUDU_IMAGE, instanceName)
                    .withCommand("tserver")
                    .withEnv("KUDU_MASTERS", format("%s:%s", KUDU_MASTER, KUDU_MASTER_PORT))
                    .withEnv("TSERVER_ARGS", format("--fs_wal_dir=/var/lib/kudu/tserver --logtostderr --use_hybrid_clock=false --rpc_authentication=required --rpc_bind_addresses=%s:%s --rpc_authentication=required --principal=kuduservice/kudu-tserver-%s@STARBURSTDATA.COM --keytab_file=/kerberos/kudu-tserver-%s.keytab", instanceName, initialKuduTserverPort, i, i))
                    .withFileSystemBind(kerberosCredentialsDirectory.toString(), "/kerberos", READ_ONLY)
                    .waitingFor(forSelectedPorts(initialKuduTserverPort))
                    .dependsOn(kuduMaster);

            addKrb5(kuduTablet);

            portBinder.exposePort(kuduTablet, initialKuduTserverPort);
            tabletContainers.add(kuduTablet);
            initialKuduTserverPort += 1;
        }

        return tabletContainers;
    }

    private void addKrb5(DockerContainer container)
    {
        container
                .withCopyFileToContainer(
                        forHostPath(configDir.getPath("krb5.conf")),
                        "/etc/krb5.conf");
    }

    private void configurePrestoContainer(DockerContainer container, Path kerberosCredentialsDirectory)
    {
        container
                .withCopyFileToContainer(
                        forHostPath(configDir.getPath("kudu.properties")),
                        CONTAINER_PRESTO_ETC + "/catalog/kudu.properties")
                .withFileSystemBind(kerberosCredentialsDirectory.toString(), "/kerberos", READ_ONLY);
    }
}

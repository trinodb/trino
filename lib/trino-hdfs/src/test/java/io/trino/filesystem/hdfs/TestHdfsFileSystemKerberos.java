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
package io.trino.filesystem.hdfs;

import com.google.common.io.Closer;
import com.google.common.net.HostAndPort;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.hdfs.authentication.CachingKerberosHadoopAuthentication;
import io.trino.hdfs.authentication.DirectHdfsAuthentication;
import io.trino.hdfs.authentication.HadoopAuthentication;
import io.trino.hdfs.authentication.KerberosHadoopAuthentication;
import io.trino.plugin.base.authentication.KerberosAuthentication;
import io.trino.plugin.base.authentication.KerberosConfiguration;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.testcontainers.containers.Network;

import javax.security.auth.kerberos.KerberosTicket;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Throwables.getCausalChain;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.base.authentication.KerberosTicketUtils.getRefreshTime;
import static io.trino.plugin.base.authentication.KerberosTicketUtils.getTicketGrantingTicket;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;

@Isolated
public class TestHdfsFileSystemKerberos
{
    private static final String PRINCIPAL = "hdfs/hadoop-master@TRINO.TEST";

    private static Network network;
    private static KerberosKdc kdc;
    private static KerberosHadoop hadoop;
    private static Path keytabDirectory;

    @RegisterExtension
    final AfterTestExecutionCallback diagnostics = context -> context.getExecutionException()
            .filter(failure -> !isIncompleteExecution(failure))
            .ifPresent(_ -> {
                if (kdc != null) {
                    kdc.printDiagnostics();
                }
                if (hadoop != null) {
                    hadoop.printDiagnostics();
                }
            });

    @BeforeAll
    public static void beforeAll()
            throws Exception
    {
        network = Network.newNetwork();
        keytabDirectory = Files.createTempDirectory("test-hdfs-kerberos-keytabs");

        kdc = new KerberosKdc(network);
        kdc.start();
        kdc.createKeytabs(keytabDirectory);

        hadoop = new KerberosHadoop(keytabDirectory, network);
        hadoop.start();
    }

    @AfterAll
    public static void afterAll()
            throws Exception
    {
        try (Closer closer = Closer.create()) {
            if (keytabDirectory != null) {
                closer.register(() -> deleteRecursively(keytabDirectory, ALLOW_INSECURE));
            }
            if (network != null) {
                closer.register(network::close);
            }
            if (kdc != null) {
                closer.register(kdc::close);
            }
            if (hadoop != null) {
                closer.register(hadoop::close);
            }
        }
    }

    @Test
    public void testOpenOutputStreamRefreshesKerberosTicket(@TempDir Path tempDirectory)
            throws Exception
    {
        Path keytab = tempDirectory.resolve("hdfs.keytab");
        Files.write(keytab, hadoop.getHdfsKeytab());
        Path krb5 = tempDirectory.resolve("krb5.conf");
        Files.writeString(krb5, krb5Conf(kdc.getKdcAddress()));
        System.setProperty("java.security.krb5.conf", krb5.toString());

        Path hdfsSite = tempDirectory.resolve("hdfs-site.xml");
        Files.writeString(hdfsSite, hdfsSite(hadoop.getHdfsUri()));

        HdfsConfig hdfsConfig = new HdfsConfig()
                .setResourceConfigFiles(List.of(hdfsSite.toString()))
                .setDfsReplication(1);
        HdfsConfigurationInitializer initializer = new HdfsConfigurationInitializer(hdfsConfig);
        HdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(initializer, emptySet());

        KerberosConfiguration kerberosConfiguration = new KerberosConfiguration.Builder()
                .withKerberosPrincipal(PRINCIPAL)
                .withKeytabLocation(keytab.toString())
                .build();
        KerberosHadoopAuthentication kerberosHadoopAuthentication = KerberosHadoopAuthentication.createKerberosHadoopAuthentication(
                new KerberosAuthentication(kerberosConfiguration),
                initializer);
        CountingHadoopAuthentication countingAuthentication = new CountingHadoopAuthentication(kerberosHadoopAuthentication);
        CachingKerberosHadoopAuthentication cachingAuthentication = new CachingKerberosHadoopAuthentication(countingAuthentication);

        HdfsEnvironment environment = new HdfsEnvironment(
                hdfsConfiguration,
                hdfsConfig,
                new DirectHdfsAuthentication(cachingAuthentication));
        TrinoFileSystem fileSystem = new HdfsFileSystem(
                environment,
                new HdfsContext(ConnectorIdentity.ofUser("test")),
                new TrinoHdfsFileSystemStats());

        Location file = Location.of(hadoop.getHdfsUri()).appendPath("ticket-refresh-" + UUID.randomUUID());
        try (OutputStream outputStream = fileSystem.newOutputFile(file).create()) {
            outputStream.write("before-refresh\n".getBytes(UTF_8));
            outputStream.flush();
            UserGroupInformation cachedUserGroupInformation = cachingAuthentication.getUserGroupInformation();
            KerberosTicket originalTicket = getTicketGrantingTicket(cachedUserGroupInformation.getSubject());
            sleepUntilRefreshTime(originalTicket);
            int invocationsBeforeRefresh = countingAuthentication.getInvocations();
            outputStream.write("after-refresh\n".getBytes(UTF_8));

            KerberosTicket refreshedTicket = getTicketGrantingTicket(cachedUserGroupInformation.getSubject());
            assertThat(refreshedTicket.getEndTime()).isAfter(originalTicket.getEndTime());
            assertThat(countingAuthentication.getInvocations()).isGreaterThan(invocationsBeforeRefresh);
        }

        assertThat(new String(fileSystem.newInputFile(file).newStream().readAllBytes(), UTF_8))
                .isEqualTo("before-refresh\nafter-refresh\n");
    }

    private static void sleepUntilRefreshTime(KerberosTicket ticket)
            throws InterruptedException
    {
        long millisUntilRefresh = getRefreshTime(ticket) - System.currentTimeMillis();
        assertThat(millisUntilRefresh).isPositive();
        long waitMillis = millisUntilRefresh + Duration.ofSeconds(2).toMillis();
        assertThat(waitMillis).isLessThan(Duration.ofMinutes(1).toMillis());
        Thread.sleep(waitMillis);
    }

    private static String krb5Conf(HostAndPort kdcAddress)
    {
        return """
               [libdefaults]
                 default_realm = TRINO.TEST
                 dns_lookup_realm = false
                 dns_lookup_kdc = false
                 forwardable = true
                 udp_preference_limit = 1
                 ticket_lifetime = 30s

                [realms]
                 TRINO.TEST = {
                  kdc = %s
                 }
               """.formatted(kdcAddress);
    }

    private static String hdfsSite(String hdfsUri)
            throws IOException
    {
        Configuration configuration = new Configuration(false);
        configuration.set("fs.defaultFS", hdfsUri);
        configuration.set("hadoop.security.authentication", "kerberos");
        configuration.set("dfs.namenode.kerberos.principal", PRINCIPAL);
        configuration.set("dfs.datanode.kerberos.principal", PRINCIPAL);

        StringWriter writer = new StringWriter();
        configuration.writeXml(writer);
        return writer.toString();
    }

    private static class CountingHadoopAuthentication
            implements HadoopAuthentication
    {
        private final HadoopAuthentication delegate;
        private final AtomicInteger invocations = new AtomicInteger();

        private CountingHadoopAuthentication(HadoopAuthentication delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public UserGroupInformation getUserGroupInformation()
        {
            invocations.incrementAndGet();
            return delegate.getUserGroupInformation();
        }

        public int getInvocations()
        {
            return invocations.get();
        }
    }

    private static boolean isIncompleteExecution(Throwable failure)
    {
        return getCausalChain(failure).stream()
                .map(Throwable::getClass)
                .anyMatch(failureClass -> failureClass.getName().equals("org.opentest4j.IncompleteExecutionException"));
    }
}

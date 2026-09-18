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
package io.trino.tests.product.deltalake;

import io.trino.testing.containers.HadoopContainer;
import io.trino.tests.product.hive.HiveKerberosEnvironment;
import org.testcontainers.images.builder.Transferable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Delta Lake environment backed by a Kerberized Hive Metastore and HDFS.
 */
public class DeltaLakeKerberosHdfsEnvironment
        extends HiveKerberosEnvironment
{
    @Override
    protected Map<String, Map<String, String>> getAdditionalCatalogs()
    {
        String realm = getKerberosRealm();
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("connector.name", "delta_lake");
        properties.put("hive.metastore.uri", "thrift://" + HadoopContainer.HOST_NAME + ":" + HadoopContainer.HIVE_METASTORE_PORT);
        properties.put("fs.hadoop.enabled", "true");
        properties.put("hive.config.resources", "/etc/trino/hdfs-site.xml");
        properties.put("hive.metastore.authentication.type", "KERBEROS");
        properties.put("hive.metastore.service.principal", HIVE_PRINCIPAL + "@" + realm);
        properties.put("hive.metastore.client.principal", getMetastoreClientPrincipal());
        properties.putAll(getMetastoreAuthenticationProperties());
        properties.put("hive.hdfs.authentication.type", "KERBEROS");
        properties.put("hive.hdfs.impersonation.enabled", "false");
        properties.put("hive.hdfs.trino.principal", getHdfsClientPrincipal());
        properties.putAll(getHdfsAuthenticationProperties());
        properties.put("delta.register-table-procedure.enabled", "true");
        return Map.of("delta", Map.copyOf(properties));
    }

    public void deleteHdfsPath(String path)
    {
        executeHdfsCommand("hdfs dfs -rm -r -f \"$1\"", path);
    }

    public void createHdfsDirectory(String path)
    {
        executeHdfsCommand("hdfs dfs -mkdir -p \"$1\"", path);
    }

    public void saveHdfsFile(String path, byte[] content)
    {
        String source = "/tmp/delta-lake-fixture-" + UUID.randomUUID();
        getHadoop().copyFileToContainer(Transferable.of(content), source);
        executeHdfsCommand("hdfs dfs -mkdir -p \"$(dirname \"$2\")\" && hdfs dfs -put -f \"$1\" \"$2\" && rm -f \"$1\"", source, path);
    }

    private void executeHdfsCommand(String hdfsCommand, String... arguments)
    {
        String authenticatedCommand = "kinit -kt %s %s@%s && %s".formatted(
                HADOOP_HDFS_KEYTAB,
                HDFS_PRINCIPAL,
                getKerberosRealm(),
                hdfsCommand);
        String[] command = new String[4 + arguments.length];
        command[0] = "bash";
        command[1] = "-lc";
        command[2] = authenticatedCommand;
        command[3] = "hdfs-command";
        System.arraycopy(arguments, 0, command, 4, arguments.length);

        try {
            var result = getHadoop().execInContainer(command);
            if (result.getExitCode() != 0) {
                throw new IllegalStateException("HDFS command failed: " + result.getStderr());
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to execute HDFS command", e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while executing HDFS command", e);
        }
    }

    static void main(String[] args)
            throws Exception
    {
        try (DeltaLakeKerberosHdfsEnvironment environment = new DeltaLakeKerberosHdfsEnvironment()) {
            environment.start();
            System.out.println("DeltaLakeKerberosHdfsEnvironment started. Press Ctrl+C to stop.");
            Thread.sleep(Long.MAX_VALUE);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

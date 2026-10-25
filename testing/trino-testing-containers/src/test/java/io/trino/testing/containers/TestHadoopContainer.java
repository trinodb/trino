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
package io.trino.testing.containers;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container.ExecResult;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class TestHadoopContainer
{
    @Test
    void testAzureConfig()
            throws IOException, InterruptedException
    {
        try (HadoopContainer hadoop = new HadoopContainer().withAzureConfig("account", "access-key")) {
            hadoop.start();

            assertThat(coreSiteProperty(hadoop, "fs.azure.account.key.account.dfs.core.windows.net")).isEqualTo("access-key");
            assertThat(coreSiteProperty(hadoop, "fs.abfs.impl")).isEqualTo("org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem");
            assertThat(coreSiteProperty(hadoop, "fs.AbstractFileSystem.abfs.impl")).isEqualTo("org.apache.hadoop.fs.azurebfs.Abfs");
        }
    }

    private static String coreSiteProperty(HadoopContainer hadoop, String name)
            throws IOException, InterruptedException
    {
        ExecResult result = hadoop.execInContainer("hdfs", "getconf", "-confKey", name);
        assertThat(result.getExitCode()).as(result.getStderr()).isZero();
        return result.getStdout().strip();
    }
}

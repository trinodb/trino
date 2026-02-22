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

import com.google.common.reflect.ClassPath;
import io.trino.testing.containers.HdfsClient;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.util.regex.Matcher.quoteReplacement;

@ProductTest
abstract class BaseTestDeltaLakeHdfsReadsJunit
{
    private static final String TARGET_PATH = "/tmp/region";

    private final String regionResourcePath;

    protected BaseTestDeltaLakeHdfsReadsJunit(String regionResourcePath)
    {
        this.regionResourcePath = regionResourcePath;
    }

    @BeforeEach
    void setUp(DeltaLakeOssEnvironment env)
            throws IOException
    {
        HdfsClient hdfsClient = env.createHdfsClient();
        try {
            hdfsClient.delete(TARGET_PATH);
        }
        catch (RuntimeException ignored) {
        }
        hdfsClient.createDirectory(TARGET_PATH);

        List<ClassPath.ResourceInfo> resources = ClassPath.from(getClass().getClassLoader())
                .getResources().stream()
                .filter(resourceInfo -> resourceInfo.getResourceName().startsWith(regionResourcePath))
                .collect(toImmutableList());

        for (ClassPath.ResourceInfo resourceInfo : resources) {
            try (InputStream resourceInputStream = resourceInfo.asByteSource().openBufferedStream()) {
                String fileName = resourceInfo.getResourceName().replaceFirst("^" + Pattern.quote(regionResourcePath), quoteReplacement(TARGET_PATH));
                hdfsClient.saveFile(fileName, resourceInputStream.readAllBytes());
            }
        }
    }

    @Test
    @TestGroup.DeltaLakeHdfs
    @TestGroup.ProfileSpecificTests
    void testReads(DeltaLakeOssEnvironment env)
    {
        try {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS delta_lake.default.region");
            env.executeTrinoUpdate("CALL delta_lake.system.register_table('default', 'region', 'hdfs://hadoop-master:9000/tmp/region')");
            assertThat(env.executeTrino("SELECT count(*) FROM delta_lake.default.region")).containsOnly(row(5L));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS delta_lake.default.region");
        }
    }
}

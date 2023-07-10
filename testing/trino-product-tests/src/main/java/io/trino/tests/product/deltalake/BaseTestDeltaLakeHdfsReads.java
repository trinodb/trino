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
import com.google.inject.Inject;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_HDFS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.util.Objects.requireNonNull;
import static java.util.regex.Matcher.quoteReplacement;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseTestDeltaLakeHdfsReads
        extends ProductTest
{
    private String regionResourcePath;
    @Inject
    private HdfsClient hdfsClient;

    public BaseTestDeltaLakeHdfsReads(String regionResourcePath)
    {
        this.regionResourcePath = requireNonNull(regionResourcePath, "regionResourcePath is null");
    }

    @BeforeMethodWithContext
    public void setUp()
            throws IOException
    {
        String dstPath = "/tmp/region";
        hdfsClient.createDirectory(dstPath);

        List<ClassPath.ResourceInfo> resources = ClassPath.from(getClass().getClassLoader())
                .getResources().stream()
                .filter(resourceInfo -> resourceInfo.getResourceName().startsWith(regionResourcePath))
                .collect(toImmutableList());

        for (ClassPath.ResourceInfo resourceInfo : resources) {
            try (InputStream resourceInputStream = resourceInfo.asByteSource().openBufferedStream()) {
                String fileName = resourceInfo.getResourceName().replaceFirst("^" + Pattern.quote(regionResourcePath), quoteReplacement(dstPath));
                hdfsClient.saveFile(fileName, resourceInputStream);
            }
        }
    }

    @Test(groups = {DELTA_LAKE_HDFS, PROFILE_SPECIFIC_TESTS})
    public void testReads()
    {
        onTrino().executeQuery("CALL delta.system.register_table('default', 'region', 'hdfs://hadoop-master:9000/tmp/region')");

        assertThat(onTrino().executeQuery("SELECT count(*) FROM delta.default.region")).containsOnly(row(5L));
        onTrino().executeQuery("DROP TABLE delta.default.region");
    }
}

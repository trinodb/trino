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
package io.trino.tests.product;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

import static io.trino.tests.product.TestGroups.CONFIGURED_FEATURES;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.sql.JDBCType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests in this class verify that the test environments have been defined correctly,
 * that is that the Trino features they declare to be using
 * matches what's enabled in the running Trino server.
 *
 * They don't test that these features work correctly in the Trino server itself.
 */
public class TestConfiguredFeatures
        extends ProductTest
{
    @Inject
    @Named("databases.presto.configured_connectors")
    private List<String> configuredConnectors;

    @Test(groups = CONFIGURED_FEATURES)
    public void selectConfiguredConnectors()
    {
        if (configuredConnectors.size() == 0) {
            throw new SkipException("Skip checking configured connectors since none were set in Tempto configuration");
        }
        String sql = "SELECT DISTINCT connector_name FROM system.metadata.catalogs";
        assertThat(onTrino().executeQuery(sql))
                .hasColumns(VARCHAR)
                .containsOnly(configuredConnectors.stream()
                        .map(QueryAssert.Row::row)
                        .collect(Collectors.toList()));
    }
}

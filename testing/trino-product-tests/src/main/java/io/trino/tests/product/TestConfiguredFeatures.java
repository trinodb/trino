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
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tests.product.TestGroups.CONFIGURED_FEATURES;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
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
    @Named("databases.trino.configured_connectors")
    private List<String> configuredConnectors;

    @Test(groups = CONFIGURED_FEATURES)
    public void selectConfiguredConnectors()
    {
        if (configuredConnectors.isEmpty()) {
            throw new SkipException("Skip checking configured connectors since none were set in Tempto configuration");
        }
        String sql = "SELECT DISTINCT connector_name FROM system.metadata.catalogs";
        List<String> loadedCatalogs = onTrino().executeQuery(sql).column(1).stream()
                .map(Object::toString)
                .collect(toImmutableList());
        // TODO https://github.com/trinodb/trino/issues/26500
        // Loki connector is not loading properly. Once this is fixed, test will fail.
        List<String> filteredCatalogs = configuredConnectors.stream()
                .filter(connector -> !connector.equals("loki"))
                .collect(toImmutableList());

        assertThat(filteredCatalogs)
                .containsExactlyInAnyOrder(loadedCatalogs.toArray(new String[0]));
    }
}

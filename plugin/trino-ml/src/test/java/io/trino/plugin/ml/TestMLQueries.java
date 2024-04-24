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
package io.trino.plugin.ml;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.tpch.TpchConnectorFactory.TPCH_SPLITS_PER_NODE;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestMLQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        QueryRunner queryRunner = new StandaloneQueryRunner(defaultSession);
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog(defaultSession.getCatalog().get(), "tpch", ImmutableMap.of(TPCH_SPLITS_PER_NODE, "1"));

        queryRunner.installPlugin(new MLPlugin());
        return queryRunner;
    }

    @Test
    public void testPrediction()
    {
        assertQuery("SELECT classify(features(1, 2), model) " +
                "FROM (SELECT learn_classifier(labels, features) AS model FROM (VALUES (1, features(1, 2))) t(labels, features)) t2", "SELECT 1");
    }

    @Test
    public void testVarcharPrediction()
    {
        assertQuery("SELECT classify(features(1, 2), model) " +
                "FROM (SELECT learn_classifier(labels, features) AS model FROM (VALUES ('cat', features(1, 2))) t(labels, features)) t2", "SELECT 'cat'");
    }
}

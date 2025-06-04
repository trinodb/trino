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
package io.trino.plugin.pinot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.kafka.TestingKafka;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.pinot.TestingPinotCluster.PINOT_LATEST_IMAGE_NAME;
import static io.trino.tpch.TpchTable.REGION;
import static org.assertj.core.api.Assertions.assertThat;

final class TestPinotPasswordAuthentication
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingKafka kafka = closeAfterClass(TestingKafka.createWithSchemaRegistry());
        kafka.start();
        TestingPinotCluster pinot = closeAfterClass(new TestingPinotCluster(PINOT_LATEST_IMAGE_NAME, kafka.getNetwork(), true));
        pinot.start();

        return PinotQueryRunner.builder()
                .setKafka(kafka)
                .setPinot(pinot)
                .addPinotProperties(ImmutableMap.<String, String>builder()
                        .put("pinot.controller.authentication.type", "PASSWORD")
                        .put("pinot.controller.authentication.user", "admin")
                        .put("pinot.controller.authentication.password", "verysecret")
                        .put("pinot.broker.authentication.type", "PASSWORD")
                        .put("pinot.broker.authentication.user", "query")
                        .put("pinot.broker.authentication.password", "secret")
                        .buildOrThrow())
                .setInitialTables(ImmutableList.of(REGION))
                .build();
    }

    @Test
    void testSelect()
    {
        assertThat(query("SELECT regionkey, name, comment FROM pinot.default.region"))
                .skippingTypesCheck()
                .matches("SELECT regionkey, name, comment FROM tpch.tiny.region");

        // The following query goes to broker
        assertThat(computeScalar("SELECT count(*) FROM pinot.default.region"))
                .isEqualTo(5L);
    }
}

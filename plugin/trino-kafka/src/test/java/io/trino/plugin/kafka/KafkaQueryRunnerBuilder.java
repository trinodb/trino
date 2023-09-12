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
package io.trino.plugin.kafka;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.airlift.log.Level;
import io.airlift.log.Logging;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.kafka.TestingKafka;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.kafka.KafkaPlugin.DEFAULT_EXTENSION;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public abstract class KafkaQueryRunnerBuilder
        extends DistributedQueryRunner.Builder<KafkaQueryRunnerBuilder>
{
    protected final TestingKafka testingKafka;
    protected Map<String, String> extraKafkaProperties = ImmutableMap.of();
    protected Module extension = DEFAULT_EXTENSION;
    private final String catalogName;

    public KafkaQueryRunnerBuilder(TestingKafka testingKafka, String defaultSessionName)
    {
        this(testingKafka, "kafka", defaultSessionName);
    }

    public KafkaQueryRunnerBuilder(TestingKafka testingKafka, String catalogName, String defaultSessionSchema)
    {
        super(testSessionBuilder()
                .setCatalog(catalogName)
                .setSchema(defaultSessionSchema)
                .build());
        this.testingKafka = requireNonNull(testingKafka, "testingKafka is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    public KafkaQueryRunnerBuilder setExtraKafkaProperties(Map<String, String> extraKafkaProperties)
    {
        this.extraKafkaProperties = ImmutableMap.copyOf(requireNonNull(extraKafkaProperties, "extraKafkaProperties is null"));
        return this;
    }

    public KafkaQueryRunnerBuilder setExtension(Module extension)
    {
        this.extension = requireNonNull(extension, "extension is null");
        return this;
    }

    @Override
    public final DistributedQueryRunner build()
            throws Exception
    {
        Logging logging = Logging.initialize();
        logging.setLevel("org.apache.kafka", Level.WARN);

        DistributedQueryRunner queryRunner = super.build();
        try {
            testingKafka.start();
            preInit(queryRunner);
            queryRunner.installPlugin(new KafkaPlugin(extension));
            // note: additional copy via ImmutableList so that if fails on nulls
            Map<String, String> kafkaProperties = new HashMap<>(ImmutableMap.copyOf(extraKafkaProperties));
            kafkaProperties.putIfAbsent("kafka.nodes", testingKafka.getConnectString());
            kafkaProperties.putIfAbsent("kafka.messages-per-split", "1000");
            queryRunner.createCatalog(catalogName, "kafka", kafkaProperties);
            postInit(queryRunner);
            return queryRunner;
        }
        catch (RuntimeException e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    protected void preInit(DistributedQueryRunner queryRunner)
            throws Exception
    {}

    protected void postInit(DistributedQueryRunner queryRunner) {}
}

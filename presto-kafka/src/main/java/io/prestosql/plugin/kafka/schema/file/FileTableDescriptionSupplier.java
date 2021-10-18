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
package io.prestosql.plugin.kafka.schema.file;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.prestosql.plugin.kafka.ECPKafkaTopicDescription;
import io.prestosql.plugin.kafka.KafkaConfig;
import io.prestosql.plugin.kafka.KafkaConsumerFactory;
import io.prestosql.plugin.kafka.KafkaTopicDescription;
import io.prestosql.plugin.kafka.schema.MapBasedTableDescriptionSupplier;
import io.prestosql.plugin.kafka.schema.TableDescriptionSupplier;
import io.prestosql.spi.connector.SchemaTableName;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.inject.Inject;
import javax.inject.Provider;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

import static java.nio.file.Files.readAllBytes;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class FileTableDescriptionSupplier
        implements Provider<TableDescriptionSupplier>
{
    public static final String NAME = "file";

    private static final Logger log = Logger.get(FileTableDescriptionSupplier.class);

    private final KafkaConsumerFactory consumerFactory;
    private final JsonCodec<ECPKafkaTopicDescription> topicDescriptionCodec;
    private final File tableDescriptionDir;

    @Inject
    FileTableDescriptionSupplier(
            FileTableDescriptionSupplierConfig config,
            KafkaConfig kafkaConfig,
            KafkaConsumerFactory consumerFactory,
            JsonCodec<ECPKafkaTopicDescription> topicDescriptionCodec) {
        requireNonNull(kafkaConfig, "kafkaConfig is null");
        this.consumerFactory = consumerFactory;
        this.tableDescriptionDir = config.getTableDescriptionDir();
        this.topicDescriptionCodec = requireNonNull(topicDescriptionCodec, "topicDescriptionCodec is null");
    }

    @Override
    public TableDescriptionSupplier get()
    {
        Map<SchemaTableName, KafkaTopicDescription> tables = populateTables();
        return new MapBasedTableDescriptionSupplier(tables);
    }

    private Set<String> existingTopics() {
        try (KafkaConsumer<byte[], byte[]> kafkaConsumer = consumerFactory.create()) {
            return kafkaConsumer.listTopics().keySet();
        } catch (Exception e) {
            return Set.of();
        }
    }

    private Map<SchemaTableName, KafkaTopicDescription> populateTables() {
        ImmutableMap.Builder<String, ECPKafkaTopicDescription> ecpTopicBuilder = ImmutableMap.builder();
        log.debug("Loading kafka table definitions from %s", tableDescriptionDir.getAbsolutePath());

        try {
            for (File file : listFiles(tableDescriptionDir)) {
                if (file.isFile() && file.getName().endsWith(".json")) {
                    ECPKafkaTopicDescription ecpTopic = topicDescriptionCodec.fromJson(readAllBytes(file.toPath()));
                    log.debug("Loading kafka table %s", ecpTopic.getTableName());
                    ecpTopicBuilder.put(ecpTopic.getTopicPrefix(), ecpTopic);
                }
            }

            ImmutableMap<String, ECPKafkaTopicDescription> ecpTopicPrefixes = ecpTopicBuilder.build();

            ImmutableMap.Builder<SchemaTableName, KafkaTopicDescription> builder = ImmutableMap.builder();

            Set<String> topics = existingTopics();

            ecpTopicPrefixes.forEach((prefix, ecpTopic) ->
                    topics.forEach(topic -> {
                                if (topic.startsWith(prefix)) {
                                    String sub = topic.substring(prefix.length() + 1);
                                    String schemaName = sub.toLowerCase(Locale.getDefault());
                                    KafkaTopicDescription table = new KafkaTopicDescription(
                                            ecpTopic.getTableName(),
                                            Optional.of(schemaName),
                                            topic,
                                            ecpTopic.getKey(),
                                            ecpTopic.getMessage()
                                    );
                                    builder.put(new SchemaTableName(schemaName, ecpTopic.getTableName()), table);
                                }
                            }
                    )
            );

            return builder.build();

        } catch (IOException e) {
            log.warn(e, "Error: ");
            throw new UncheckedIOException(e);
        }
    }

    private static List<File> listFiles(File dir)
    {
        if ((dir != null) && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                log.debug("Considering files: %s", asList(files));
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }
}

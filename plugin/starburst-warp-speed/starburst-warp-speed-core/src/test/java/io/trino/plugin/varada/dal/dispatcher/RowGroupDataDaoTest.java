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
package io.trino.plugin.varada.dal.dispatcher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.MoreCollectors;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.dal.RowGroupDataDao;
import io.trino.plugin.varada.dispatcher.model.DictionaryInfo;
import io.trino.plugin.varada.dispatcher.model.DictionaryKey;
import io.trino.plugin.varada.dispatcher.model.DictionaryState;
import io.trino.plugin.varada.dispatcher.model.ExportState;
import io.trino.plugin.varada.dispatcher.model.FastWarmingState;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupDataValidation;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmState;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.model.WarmUpElementState;
import io.trino.plugin.varada.dispatcher.model.WildcardColumn;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.varada.util.json.VaradaColumnJsonKeyDeserializer;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.connector.SchemaTableName;
import io.varada.tools.util.StringUtils;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RowGroupDataDaoTest
{
    private static Path localStorePath;

    private GlobalConfiguration globalConfiguration;
    private StorageEngineConstants storageEngineConstants;
    private RowGroupDataDao rowGroupDataDao;

    @BeforeAll
    static void beforeAll()
            throws IOException
    {
        localStorePath = Files.createTempDirectory("RowGroupDataDaoTest");
    }

    @AfterAll
    static void afterAll()
    {
        if (Objects.nonNull(localStorePath)) {
            try (Stream<Path> stream = Files.walk(localStorePath)) {
                stream.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
            catch (IOException e) {
                System.out.printf("failed to delete localStorePath '%s'%n", localStorePath);
            }
        }
    }

    @BeforeEach
    public void before()
    {
        globalConfiguration = new GlobalConfiguration();
        globalConfiguration.setLocalStorePath(localStorePath.toFile().getAbsolutePath());

        storageEngineConstants = mock(StorageEngineConstants.class);
        when(storageEngineConstants.getPageSize()).thenReturn(8192);

        rowGroupDataDao = new RowGroupDataDao(globalConfiguration, new ObjectMapperProvider(), storageEngineConstants);
    }

    private void createFileIfNeeded(String filePath)
    {
        File rowGroupDataFile = new File(filePath);

        if (!rowGroupDataFile.exists()) {
            try {
                FileUtils.createParentDirectories(rowGroupDataFile);
                boolean newFile = rowGroupDataFile.createNewFile();
                if (!newFile) {
                    throw new RuntimeException("failed creating file " + rowGroupDataFile.getAbsolutePath());
                }
                Writer writer = Files.newBufferedWriter(rowGroupDataFile.toPath(), UTF_8);
                writer.write(StringUtils.randomAlphanumeric(8192));
                writer.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void testSimpleCRUD()
    {
        WarmUpElement warmUpElement = WarmUpElement.builder()
                .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                .recTypeLength(4)
                .varadaColumn(new RegularColumn("aaa"))
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .build();

        List<WarmUpElement> warmUpElements = List.of(warmUpElement);
        RowGroupData rowGroupData = RowGroupData.builder()
                .rowGroupKey(new RowGroupKey(
                        "schema",
                        "table",
                        "file_path",
                        0,
                        1L,
                        0,
                        "",
                        ""))
                .warmUpElements(warmUpElements)
                .build();

        assertThat(rowGroupDataDao.get(rowGroupData.getRowGroupKey())).isNull();

        rowGroupDataDao.save(List.of(rowGroupData));

        assertThat(rowGroupDataDao.get(rowGroupData.getRowGroupKey())).isEqualTo(rowGroupData);

        assertThat(rowGroupDataDao.getAll()).hasSize(1).containsExactlyElementsOf(List.of(rowGroupData));

        //test merge
        Collection<RowGroupData> updateRowGroupDataList = rowGroupDataDao.save(List.of(RowGroupData.builder(rowGroupData)
                .partitionKeys(Map.of(new RegularColumn("key"), "val"))
                .build()));

        assertThat(updateRowGroupDataList.stream().collect(MoreCollectors.onlyElement())).isNotEqualTo(rowGroupData);
        assertThat(updateRowGroupDataList.stream().collect(MoreCollectors.onlyElement()).getLock()).isEqualTo(rowGroupData.getLock());
        assertThat(updateRowGroupDataList.stream().collect(MoreCollectors.onlyElement())).isEqualTo(rowGroupDataDao.get(rowGroupData.getRowGroupKey()));
        assertThat(updateRowGroupDataList.stream().collect(MoreCollectors.onlyElement()).getLock()).isEqualTo(rowGroupDataDao.get(rowGroupData.getRowGroupKey()).getLock());

        rowGroupDataDao.delete(rowGroupData.getRowGroupKey());

        assertThat(rowGroupDataDao.getAll()).isEmpty();
    }

    @Test
    public void testLongFilePath()
    {
        WarmUpElement warmUpElement = WarmUpElement.builder()
                .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                .recTypeLength(4)
                .varadaColumn(new RegularColumn("aaa"))
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .build();

        List<WarmUpElement> warmUpElements = List.of(warmUpElement);

        String validFilePath = StringUtils.randomAlphanumeric(246);

        RowGroupData rowGroupData = RowGroupData.builder()
                .rowGroupKey(new RowGroupKey(
                        "schema",
                        "table",
                        validFilePath,
                        0,
                        1L,
                        0,
                        "",
                        ""))
                .warmUpElements(warmUpElements)
                .nextOffset(1)
                .build();

        createFileIfNeeded(rowGroupData.getRowGroupKey().stringFileNameRepresentation(globalConfiguration.getLocalStorePath()));

        rowGroupDataDao.save(List.of(rowGroupData));
        rowGroupDataDao.flush(rowGroupData.getRowGroupKey());

        assertThat(rowGroupDataDao.get(rowGroupData.getRowGroupKey())).isEqualTo(rowGroupData);

        String nonValidFilePath = StringUtils.randomAlphanumeric(260);

        RowGroupKey nonValidRowGroupKey = new RowGroupKey(
                "schema",
                "table",
                nonValidFilePath,
                0,
                1L,
                0,
                "",
                "");

        assertThatThrownBy(() -> createFileIfNeeded(nonValidRowGroupKey.stringFileNameRepresentation(globalConfiguration.getLocalStorePath())))
                .hasCauseInstanceOf(IOException.class)
                .hasMessageContaining("Cannot create directory");
    }

    @Test
    public void testSerialization()
    {
        VaradaColumn varadaColumn = new RegularColumn("aaa");
        String nodeIdentifier = "node1";
        WarmUpElement warmUpElement = WarmUpElement.builder()
                .varadaColumn(varadaColumn)
                .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                .recTypeLength(4)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .usedDictionarySize(7)
                .dictionaryInfo(new DictionaryInfo(
                        new DictionaryKey(
                                new SchemaTableColumn(
                                        new SchemaTableName("schema", "table"), varadaColumn),
                                nodeIdentifier,
                                54321),
                        DictionaryState.DICTIONARY_IMPORTED,
                        3,
                        6))
                .startOffset(1)
                .queryOffset(2)
                .queryReadSize(3)
                .totalRecords(7)
                .warmEvents(4)
                .endOffset(5)
                .state(WarmUpElementState.FAILED_PERMANENTLY)
//                .lastUsedTimestamp()      <== transient
                .exportState(ExportState.FAILED_PERMANENTLY)
                .isImported(true)
                .warmState(WarmState.WARM)
                .build();
        List<WarmUpElement> warmUpElements = List.of(warmUpElement);
        String validFilePath = StringUtils.randomAlphanumeric(246);
        RowGroupData rowGroupData = RowGroupData.builder()
                .rowGroupKey(new RowGroupKey(
                        "schema",
                        "table",
                        validFilePath,
                        0,
                        1L,
                        123456789,
                        "",
                        ""))
                .warmUpElements(warmUpElements)
                .partitionKeys(Map.of(new RegularColumn("key1"), "val1"))
                .nodeIdentifier(nodeIdentifier)
                .nextOffset(2)
                .nextExportOffset(3)
                .sparseFile(true)
                .fastWarmingState(FastWarmingState.NOT_EXPORTED)
                .dataValidation(new RowGroupDataValidation(987654321, 918273645))
                .build();

        createFileIfNeeded(rowGroupData.getRowGroupKey().stringFileNameRepresentation(globalConfiguration.getLocalStorePath()));

        rowGroupDataDao.save(List.of(rowGroupData));
        rowGroupDataDao.flush(rowGroupData.getRowGroupKey());

        assertThat(rowGroupDataDao.get(rowGroupData.getRowGroupKey())).isEqualTo(rowGroupData);

        //read from another dao
        RowGroupDataDao rowGroupDataDao2 = new RowGroupDataDao(globalConfiguration, new ObjectMapperProvider(), storageEngineConstants);
        assertThat(rowGroupDataDao2.get(rowGroupData.getRowGroupKey())).isEqualTo(rowGroupData);
    }

    @Test
    public void testSerializesAndDeserialize()
            throws JsonProcessingException
    {
        ObjectMapper objectMapper = new ObjectMapperProvider().get();
        SimpleModule simpleModule = new SimpleModule();
        simpleModule.addKeyDeserializer(VaradaColumn.class, new VaradaColumnJsonKeyDeserializer());
        objectMapper.registerModules(simpleModule);

        //DictionaryKey
        DictionaryKey dictionaryKey = new DictionaryKey(
                new SchemaTableColumn(
                        new SchemaTableName("schema1", "table1"),
                        "column1"),
                "nodeIdentifier111",
                DictionaryKey.CREATED_TIMESTAMP_UNKNOWN);
        String str = objectMapper.writerFor(dictionaryKey.getClass()).writeValueAsString(dictionaryKey);
        assertThat(dictionaryKey)
                .isEqualTo(objectMapper.readValue(str, dictionaryKey.getClass()));

        //VaradaColumn
        List<VaradaColumn> varadaColumns = List.of(
                new RegularColumn("aaa"),
                new WildcardColumn());
        str = objectMapper.writerFor(new TypeReference<List<VaradaColumn>>() {}).writeValueAsString(varadaColumns);
        assertThat(varadaColumns).isEqualTo(objectMapper.readValue(str, new TypeReference<List<VaradaColumn>>() {}));

        // WarmUpElement
        WarmUpElement warmUpElement = WarmUpElement.builder()
                .varadaColumn(new RegularColumn("aaa"))
                .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                .recTypeLength(4)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .usedDictionarySize(7)
                .dictionaryInfo(new DictionaryInfo(
                        new DictionaryKey(
                                new SchemaTableColumn(
                                        new SchemaTableName("schema1", "table1"),
                                        "column1"),
                                "nodeIdentifier111",
                                DictionaryKey.CREATED_TIMESTAMP_UNKNOWN),
                        DictionaryState.DICTIONARY_IMPORTED,
                        4,
                        DictionaryInfo.NO_OFFSET))
                .startOffset(1)
                .queryOffset(2)
                .totalRecords(7)
                .queryReadSize(3)
                .warmEvents(4)
                .endOffset(5)
                .state(WarmUpElementState.VALID)
//                .lastUsedTimestamp()      <== transient
                .exportState(ExportState.EXPORTED)
                .isImported(true)
                .warmState(WarmState.WARM)
                .build();

        str = objectMapper.writeValueAsString(warmUpElement);
        assertThat(warmUpElement).isEqualTo(objectMapper.readValue(str, WarmUpElement.class));

        //RowGroupData
        RowGroupData rowGroupData = RowGroupData.builder()
                .rowGroupKey(new RowGroupKey(
                        "schema",
                        "table",
                        StringUtils.randomAlphanumeric(10),
                        0,
                        1L,
                        0,
                        "",
                        ""))
                .warmUpElements(List.of(warmUpElement))
                .partitionKeys(Map.of(new RegularColumn("key1"), "val1"))
                .nodeIdentifier("node1")
                .nextOffset(2)
                .nextExportOffset(3)
                .sparseFile(true)
                .fastWarmingState(FastWarmingState.NOT_EXPORTED)
                .dataValidation(new RowGroupDataValidation(987654321, 918273645))
                .build();

        str = objectMapper.writeValueAsString(rowGroupData);
        assertThat(rowGroupData).isEqualTo(objectMapper.readValue(str, RowGroupData.class));
    }

    @Disabled
    @Test
    public void testMultiThread()
            throws InterruptedException
    {
        int numberOfThreads = 10;
        ExecutorService service = Executors.newFixedThreadPool(numberOfThreads);
        CountDownLatch latch = new CountDownLatch(numberOfThreads);
        List<WarmUpElement> warmUpElements = List.of(WarmUpElement.builder()
                .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                .recTypeLength(4)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .varadaColumn(new RegularColumn("aaa"))
                .build());

        IntStream.range(0, 10).forEach(i -> service.execute(() -> {
            RowGroupData rowGroupData = RowGroupData.builder()
                    .rowGroupKey(new RowGroupKey(
                            "schema",
                            "table",
                            StringUtils.randomAlphanumeric(i),
                            0,
                            1L,
                            0,
                            "",
                            ""))
                    .warmUpElements(warmUpElements)
                    .build();
            int waitTime = Math.abs((new Random(i).nextInt(Integer.MAX_VALUE) % 10) + 1) * 100;
            IntStream.range(0, 3).forEach(j -> {
                wait(waitTime, () -> rowGroupDataDao.get(rowGroupData.getRowGroupKey()));
                wait(waitTime, () -> rowGroupDataDao.getAll());
                wait(waitTime, () -> rowGroupDataDao.save(rowGroupData));
                wait(waitTime, () -> rowGroupDataDao.get(rowGroupData.getRowGroupKey()));
                wait(waitTime, () -> rowGroupDataDao.getAll());
                wait(waitTime, () -> rowGroupDataDao.delete(rowGroupData));
                wait(waitTime, () -> rowGroupDataDao.get(rowGroupData.getRowGroupKey()));
                wait(waitTime, () -> rowGroupDataDao.getAll());
            });
            latch.countDown();
        }));
        latch.await();
    }

    private void wait(int i, Runnable runnable)
    {
        try {
            Thread.sleep(i);
        }
        catch (InterruptedException e) {
            //do nothing
        }
        runnable.run();
    }
}

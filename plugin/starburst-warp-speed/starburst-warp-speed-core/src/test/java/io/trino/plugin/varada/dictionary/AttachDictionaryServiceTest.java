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
package io.trino.plugin.varada.dictionary;

import io.airlift.slice.Slices;
import io.trino.plugin.varada.TestingTxService;
import io.trino.plugin.varada.configuration.DictionaryConfiguration;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.model.DictionaryKey;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.write.dictionary.DictionaryWriterFactory;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static io.trino.plugin.varada.dictionary.DictionariesCacheTest.buildDictionaryKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AttachDictionaryServiceTest
{
    private AttachDictionaryService attachDictionaryService;
    private final GlobalConfiguration globalConfiguration = new GlobalConfiguration();

    @BeforeEach
    public void before()
    {
        globalConfiguration.setLocalStorePath("/tmp/test/");

        StorageEngineConstants storageEngineConstants = mock(StorageEngineConstants.class);
        when(storageEngineConstants.getPageSize()).thenReturn(8192);
        when(storageEngineConstants.getPageOffsetMask()).thenReturn(8192 - 1);

        MetricsManager metricsManager = TestingTxService.createMetricsManager();
        DictionaryWriterFactory dictionaryWriterFactory = new DictionaryWriterFactory();

        attachDictionaryService = new AttachDictionaryService(mock(WorkerCapacityManager.class),
                storageEngineConstants,
                new DictionaryConfiguration(),
                metricsManager,
                dictionaryWriterFactory);
    }

    @Test
    public void testReadWriteDictionaryInt()
            throws IOException
    {
        Integer[] intDictionary = {1, 2};
        DictionaryToWrite dictionaryToWrite = new DictionaryToWrite(intDictionary, 2, 4, 8);
        DictionaryKey dictionaryKey = buildDictionaryKey("int1");
        String rowGroupFilePath = globalConfiguration.getLocalStorePath() + dictionaryKey.stringFileNameRepresentation();
        FileUtils.createParentDirectories(new File(rowGroupFilePath));

        attachDictionaryService.save(dictionaryToWrite, RecTypeCode.REC_TYPE_INTEGER, 0, rowGroupFilePath);
        DataValueDictionary res = attachDictionaryService.load(dictionaryKey, RecTypeCode.REC_TYPE_INTEGER, 4, 0, rowGroupFilePath);

        assertTrue(res.isImmutable());
        assertEquals(res.getWriteSize(), 2);
        assertEquals(res.getDictionaryWeight(), 8);
        assertEquals(res.get(0), 1);
        assertEquals(res.get(1), 2);

        FileUtils.deleteQuietly(Path.of(rowGroupFilePath).toFile());
    }

    @Test
    public void testReadWriteDictionaryFixedLengthChar()
            throws IOException
    {
        io.airlift.slice.Slice[] values = {Slices.utf8Slice("aaa"),
                Slices.utf8Slice("bbbb"),
                Slices.utf8Slice("cccc"),
                Slices.utf8Slice("dddd"),
                Slices.utf8Slice("eeee"),
        };
        DictionaryToWrite dictionaryToWrite = new DictionaryToWrite(values, 5, 4, 19);
        DictionaryKey dictionaryKey = buildDictionaryKey("char4");
        String rowGroupFilePath = globalConfiguration.getLocalStorePath() + dictionaryKey.stringFileNameRepresentation();
        FileUtils.createParentDirectories(new File(rowGroupFilePath));

        attachDictionaryService.save(dictionaryToWrite, RecTypeCode.REC_TYPE_CHAR, 0, rowGroupFilePath);
        DataValueDictionary res = attachDictionaryService.load(dictionaryKey, RecTypeCode.REC_TYPE_CHAR, 4, 0, rowGroupFilePath);

        assertTrue(res.isImmutable());
        assertEquals(res.getWriteSize(), 5);
        assertEquals(res.getDictionaryWeight(), 20);
        assertEquals(((io.airlift.slice.Slice) res.get(0)).toStringUtf8(), "aaa");
        assertEquals(((io.airlift.slice.Slice) res.get(1)).toStringUtf8(), "bbbb");
        assertEquals(((io.airlift.slice.Slice) res.get(2)).toStringUtf8(), "cccc");
        assertEquals(((io.airlift.slice.Slice) res.get(3)).toStringUtf8(), "dddd");
        assertEquals(((io.airlift.slice.Slice) res.get(4)).toStringUtf8(), "eeee");

        FileUtils.deleteQuietly(Path.of(rowGroupFilePath).toFile());
    }

    @Test
    public void testReadWriteDictionaryChangedLengthChar()
            throws IOException
    {
        io.airlift.slice.Slice[] values = {Slices.utf8Slice("1"),
                Slices.utf8Slice("88888888"),
                Slices.utf8Slice("333"),
                Slices.utf8Slice("4444"),
                Slices.utf8Slice("00"),
        };
        DictionaryToWrite dictionaryToWrite = new DictionaryToWrite(values, 5, 256, 18);
        DictionaryKey dictionaryKey = buildDictionaryKey("varchar");
        String rowGroupFilePath = globalConfiguration.getLocalStorePath() + dictionaryKey.stringFileNameRepresentation();
        FileUtils.createParentDirectories(new File(rowGroupFilePath));

        attachDictionaryService.save(dictionaryToWrite, RecTypeCode.REC_TYPE_VARCHAR, 0, rowGroupFilePath);
        DataValueDictionary res = attachDictionaryService.load(dictionaryKey, RecTypeCode.REC_TYPE_VARCHAR, 256, 0, rowGroupFilePath);

        assertTrue(res.isImmutable());
        assertEquals(res.getWriteSize(), 5);
        assertEquals(res.getDictionaryWeight(), 18);
        assertEquals(((io.airlift.slice.Slice) res.get(0)).toStringUtf8(), "1");
        assertEquals(((io.airlift.slice.Slice) res.get(1)).toStringUtf8(), "88888888");
        assertEquals(((io.airlift.slice.Slice) res.get(2)).toStringUtf8(), "333");
        assertEquals(((io.airlift.slice.Slice) res.get(3)).toStringUtf8(), "4444");
        assertEquals(((io.airlift.slice.Slice) res.get(4)).toStringUtf8(), "00");

        FileUtils.deleteQuietly(Path.of(rowGroupFilePath).toFile());
    }

    @Test
    void test_save_IOException()
    {
        Integer[] intDictionary = {1, 2};
        DictionaryToWrite dictionaryToWrite = new DictionaryToWrite(intDictionary, 2, 4, 8);
        int dictionaryOffset = Integer.MAX_VALUE;
        DictionaryKey dictionaryKey = buildDictionaryKey("int1");
        String rowGroupFilePath = globalConfiguration.getLocalStorePath() + dictionaryKey.stringFileNameRepresentation() + "/// ...";

        assertThrows(RuntimeException.class, () ->
                attachDictionaryService.save(dictionaryToWrite, RecTypeCode.REC_TYPE_INTEGER, dictionaryOffset, rowGroupFilePath));
    }
}

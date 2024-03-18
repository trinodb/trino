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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.plugin.varada.TestingTxService;
import io.trino.plugin.varada.configuration.DictionaryConfiguration;
import io.trino.plugin.varada.dispatcher.model.DictionaryKey;
import io.trino.plugin.varada.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.stats.VaradaStatsDictionary;
import io.trino.spi.connector.SchemaTableName;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.util.stream.IntStream;

import static io.trino.plugin.varada.dictionary.DictionaryCacheService.DICTIONARY_STAT_GROUP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DictionariesCacheTest
{
    private DictionariesCache dictionariesCache;
    private VaradaStatsDictionary varadaStatsDictionary;
    private AttachDictionaryService attachDictionaryService;

    @BeforeEach
    public void before()
    {
        DictionaryConfiguration dictionaryConfiguration = new DictionaryConfiguration();
        dictionaryConfiguration.setMaxDictionaryTotalCacheWeight(DataSize.of(20, DataSize.Unit.BYTE));
//        dictionaryConfiguration.setMaxDictionaryCacheWeight(DataSize.of(20, DataSize.Unit.BYTE));
        dictionaryConfiguration.setDictionaryCacheConcurrencyLevel(1);
        MetricsManager metricsManager = TestingTxService.createMetricsManager();
        attachDictionaryService = mock(AttachDictionaryService.class);
        dictionariesCache = new DictionariesCache(
                dictionaryConfiguration,
                metricsManager,
                attachDictionaryService);
        varadaStatsDictionary = (VaradaStatsDictionary) metricsManager.get(VaradaStatsDictionary.createKey(DICTIONARY_STAT_GROUP));
    }

    @Test
    public void testLRU()
    {
        WriteDictionary writeDictionary1 = dictionariesCache.getWriteDictionary(
                buildDictionaryKey("1"),
                RecTypeCode.REC_TYPE_VARCHAR);
        writeDictionary1.get(Slices.wrappedBuffer(("number=" + 1).getBytes(Charset.defaultCharset())));

        assertThat(varadaStatsDictionary.getwrite_dictionaries_count()).isEqualTo(1);
        assertThat(varadaStatsDictionary.getdictionary_active_size()).isEqualTo(1);

        WriteDictionary writeDictionary2 = dictionariesCache.getWriteDictionary(
                buildDictionaryKey("2"),
                RecTypeCode.REC_TYPE_VARCHAR);
        IntStream.range(0, 6).forEach(c -> {
            Slice dictionaryObject = Slices.wrappedBuffer(("number=" + c).getBytes(Charset.defaultCharset()));
            writeDictionary2.get(dictionaryObject);
        });
        writeDictionary2.get(Slices.wrappedBuffer(("number=" + 1).getBytes(Charset.defaultCharset())));

        assertThat(varadaStatsDictionary.getwrite_dictionaries_count()).isEqualTo(2);
        assertThat(varadaStatsDictionary.getdictionary_active_size()).isEqualTo(2);

        WriteDictionary writeDictionary3 = dictionariesCache.getWriteDictionary(
                buildDictionaryKey("3"),
                RecTypeCode.REC_TYPE_VARCHAR);
        writeDictionary3.get(Slices.wrappedBuffer(("number=" + 1).getBytes(Charset.defaultCharset())));

        assertThat(varadaStatsDictionary.getwrite_dictionaries_count()).isEqualTo(3);
        assertThat(varadaStatsDictionary.getdictionary_active_size()).isEqualTo(3);
        assertThat(varadaStatsDictionary.getdictionary_entries()).isEqualTo(3);

        dictionariesCache.releaseDictionary(writeDictionary1.getDictionaryKey());
        dictionariesCache.releaseDictionary(writeDictionary2.getDictionaryKey());
        dictionariesCache.releaseDictionary(writeDictionary3.getDictionaryKey());

        assertThat(varadaStatsDictionary.getdictionary_entries()).isEqualTo(2);
        assertThat(varadaStatsDictionary.getdictionary_active_size()).isEqualTo(0);
        assertThat(varadaStatsDictionary.getdictionary_evicted_entries()).isEqualTo(1);

        assertThatThrownBy(() -> dictionariesCache.getActiveDataValueDictionary(writeDictionary1.getDictionaryKey()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("was not found in active list");

        assertThat(dictionariesCache.getReadDictionary(
                writeDictionary1.getDictionaryKey(),
                0,
                RecTypeCode.REC_TYPE_VARCHAR,
                0,
                0,
                "rowGroupFilePath1")).isEqualTo(writeDictionary1);

        assertThat(dictionariesCache.getReadDictionary(
                writeDictionary3.getDictionaryKey(),
                0,
                RecTypeCode.REC_TYPE_VARCHAR,
                0,
                0,
                "rowGroupFilePath3")).isEqualTo(writeDictionary3);

        when(attachDictionaryService.load(
                eq(writeDictionary2.getDictionaryKey()),
                eq(RecTypeCode.REC_TYPE_VARCHAR),
                eq(writeDictionary2.getRecTypeLength()),
                anyInt(),
                anyString())).thenReturn((DataValueDictionary) writeDictionary2);

        long loadedToCache = varadaStatsDictionary.getdictionary_loaded_elements_count();

        assertThat(loadedToCache).isEqualTo(varadaStatsDictionary.getdictionary_loaded_elements_count());

        dictionariesCache.getReadDictionary(
                writeDictionary2.getDictionaryKey(),
                0,
                RecTypeCode.REC_TYPE_VARCHAR,
                writeDictionary2.getRecTypeLength(),
                0,
                "rowGroupFilePath2");

        assertThat(loadedToCache + 1).isEqualTo(varadaStatsDictionary.getdictionary_loaded_elements_count());
    }

    @Test
    public void testActive()
    {
        WriteDictionary writeDictionary1 = dictionariesCache.getWriteDictionary(
                buildDictionaryKey("1"),
                RecTypeCode.REC_TYPE_VARCHAR);
        writeDictionary1.get(Slices.wrappedBuffer(("number=" + 1).getBytes(Charset.defaultCharset())));

        assertThat(varadaStatsDictionary.getwrite_dictionaries_count()).isEqualTo(1);
        assertThat(varadaStatsDictionary.getdictionary_active_size()).isEqualTo(1);

        WriteDictionary writeDictionary2 = dictionariesCache.getWriteDictionary(
                buildDictionaryKey("2"),
                RecTypeCode.REC_TYPE_VARCHAR);

        //increase number of transactions so this dictionary will not be evicted from active cache regardless of it's size
        assertThat(dictionariesCache.getWriteDictionary(writeDictionary2.getDictionaryKey(), RecTypeCode.REC_TYPE_VARCHAR))
                .isEqualTo(writeDictionary2);

        IntStream.range(0, 6)
                .forEach(i -> writeDictionary2.get(Slices.wrappedBuffer(("number=" + i).getBytes(Charset.defaultCharset()))));
        writeDictionary2.get(Slices.wrappedBuffer(("number=" + 1).getBytes(Charset.defaultCharset())));

        assertThat(varadaStatsDictionary.getwrite_dictionaries_count()).isEqualTo(2);
        assertThat(varadaStatsDictionary.getdictionary_active_size()).isEqualTo(2);

        WriteDictionary writeDictionary3 = dictionariesCache.getWriteDictionary(
                buildDictionaryKey("3"),
                RecTypeCode.REC_TYPE_VARCHAR);
        writeDictionary3.get(Slices.wrappedBuffer(("number=" + 1).getBytes(Charset.defaultCharset())));

        // for recalculation of weights
        dictionariesCache.releaseDictionary(writeDictionary1.getDictionaryKey());
        dictionariesCache.releaseDictionary(writeDictionary2.getDictionaryKey());
        dictionariesCache.releaseDictionary(writeDictionary3.getDictionaryKey());

        assertThat(varadaStatsDictionary.getwrite_dictionaries_count()).isEqualTo(3);
        assertThat(varadaStatsDictionary.getdictionary_active_size()).isEqualTo(1);
        assertThat(varadaStatsDictionary.getdictionary_entries()).isEqualTo(3);

        assertThat(dictionariesCache.getActiveDataValueDictionary(writeDictionary2.getDictionaryKey()))
                .isEqualTo(writeDictionary2);

        assertThatThrownBy(() -> dictionariesCache.releaseDictionary(writeDictionary1.getDictionaryKey()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("dictionaryKey was not found in cache");
        dictionariesCache.releaseDictionary(writeDictionary2.getDictionaryKey());
        assertThatThrownBy(() -> dictionariesCache.releaseDictionary(writeDictionary3.getDictionaryKey()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("dictionaryKey was not found in cache");

        //retry due to race condition of removal notification
        Failsafe.with(new RetryPolicy<>()
                        .handle(AssertionError.class)
                        .withMaxRetries(100))
                .run(() -> {
                    assertThat(varadaStatsDictionary.getdictionary_entries()).isEqualTo(1);
                    assertThat(varadaStatsDictionary.getdictionary_evicted_entries()).isEqualTo(2);
                    assertThat(varadaStatsDictionary.getdictionary_active_size()).isEqualTo(0);
                });

        assertThat(dictionariesCache.getReadDictionary(
                writeDictionary1.getDictionaryKey(),
                0,
                RecTypeCode.REC_TYPE_VARCHAR,
                0,
                0,
                "rowGroupFilePath")).isEqualTo(writeDictionary1);
        assertThat(dictionariesCache.getReadDictionary(
                writeDictionary3.getDictionaryKey(),
                0,
                RecTypeCode.REC_TYPE_VARCHAR,
                0,
                0,
                "rowGroupFilePath")).isEqualTo(writeDictionary3);
    }

    public static DictionaryKey buildDictionaryKey(String columnName)
    {
        return new DictionaryKey(
                new SchemaTableColumn(new SchemaTableName("s1", "t1"), columnName),
                "nodeIdentifier",
                DictionaryKey.CREATED_TIMESTAMP_UNKNOWN);
    }
}

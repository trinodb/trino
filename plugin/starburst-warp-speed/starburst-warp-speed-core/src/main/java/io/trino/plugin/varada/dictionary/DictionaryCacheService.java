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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.VaradaErrorCode;
import io.trino.plugin.varada.configuration.DictionaryConfiguration;
import io.trino.plugin.varada.dispatcher.model.DictionaryInfo;
import io.trino.plugin.varada.dispatcher.model.DictionaryKey;
import io.trino.plugin.varada.dispatcher.model.DictionaryState;
import io.trino.plugin.varada.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.TrinoException;

import java.util.Map;

import static java.util.Objects.requireNonNull;

@Singleton
public class DictionaryCacheService
{
    public static final String DICTIONARY_STAT_GROUP = "dictionary";
    public static final int MINIMUM_LEN_FOR_DICTIONARY = 4;
    public static final RecTypeCode DICTIONARY_REC_TYPE_CODE = RecTypeCode.REC_TYPE_SMALLINT;
    public static final int DICTIONARY_REC_TYPE_CODE_NUM = DICTIONARY_REC_TYPE_CODE.ordinal();
    public static final int DICTIONARY_REC_TYPE_LENGTH = Short.BYTES;

    private static final Logger logger = Logger.get(DictionaryCacheService.class);

    private final DictionariesCache dictionariesCache;
    private final AttachDictionaryService attachDictionaryService;
    private final DictionaryConfiguration dictionaryConfiguration;

    @Inject
    public DictionaryCacheService(DictionaryConfiguration dictionaryConfiguration,
            MetricsManager metricsManager,
            AttachDictionaryService attachDictionaryService)
    {
        this.attachDictionaryService = requireNonNull(attachDictionaryService);
        this.dictionaryConfiguration = dictionaryConfiguration;
        this.dictionariesCache = new DictionariesCache(dictionaryConfiguration, metricsManager, attachDictionaryService);
    }

    public WriteDictionary computeWriteIfAbsent(DictionaryKey dictionaryKey, RecTypeCode recTypeCode)
    {
        return dictionariesCache.getWriteDictionary(dictionaryKey, recTypeCode);
    }

    public ReadDictionary computeReadIfAbsent(DictionaryKey dictionaryKey,
            int usedDictionarySize,
            RecTypeCode recTypeCode,
            int recTypeLength,
            int dictionaryOffset,
            String rowGroupFilePath)
    {
        return dictionariesCache.getReadDictionary(dictionaryKey, usedDictionarySize, recTypeCode, recTypeLength, dictionaryOffset, rowGroupFilePath);
    }

    public long getLastCreatedTimestamp(SchemaTableColumn schemaTableColumn, String nodeIdentifier)
    {
        return dictionariesCache.getLastCreatedTimestamp(schemaTableColumn, nodeIdentifier);
    }

    public void updateOnFailedWrite(DictionaryKey dictionaryKey)
    {
        dictionariesCache.incFailedWriteCount(dictionaryKey);
    }

    public DictionaryState calculateDictionaryStateForWrite(WarmUpElement warmUpElement, Boolean dictionaryEnabled)
    {
        if (!isDictionaryValidForColumn(warmUpElement, dictionaryEnabled)) {
            return DictionaryState.DICTIONARY_NOT_EXIST;
        }
        DictionaryState res;
        DictionaryInfo dictionaryInfo = warmUpElement.getDictionaryInfo();
        if (dictionaryInfo == null) {
            res = DictionaryState.DICTIONARY_VALID;
        }
        else if (dictionaryInfo.dictionaryState() == DictionaryState.DICTIONARY_MAX_EXCEPTION) {
            res = DictionaryState.DICTIONARY_MAX_EXCEPTION;
        }
        else {
            res = dictionariesCache.hasExceededFailedWriteLimit(dictionaryInfo.dictionaryKey()) ? DictionaryState.DICTIONARY_REJECTED : DictionaryState.DICTIONARY_VALID;
        }
        return res;
    }

    public int writeDictionary(DictionaryKey dictionaryKey,
            RecTypeCode recTypeCode,
            int dictionaryOffset,
            String rowGroupFilePath)
    {
        DataValueDictionary dataValueDictionary = dictionariesCache.getActiveDataValueDictionary(dictionaryKey);
        if (dataValueDictionary.canSkipWrite()) {
            return 0;
        }

        synchronized (dataValueDictionary) {
            if (dataValueDictionary.canSkipWrite()) {
                return 0;
            }

            try {
                // add keys lock is different from synchronized lock, so we must get a copy of the dictionary before we write it
                DictionaryToWrite dictionaryToWrite = dataValueDictionary.createDictionaryToWrite();
                int dictionarySize = attachDictionaryService.save(dictionaryToWrite, recTypeCode, dictionaryOffset, rowGroupFilePath);
                try {
                    dictionariesCache.loadPreBlock(recTypeCode, dataValueDictionary);
                }
                catch (Exception e) {
                    throw new TrinoException(VaradaErrorCode.VARADA_DICTIONARY_ERROR, e);
                }
                dataValueDictionary.dictionaryAttached();
                return dictionarySize;
            }
            catch (Exception e) {
                throw new TrinoException(VaradaErrorCode.VARADA_DICTIONARY_ERROR, e);
            }
        }
    }

    public ReadDictionary getReadDictionaryIfPresent(DictionaryKey dictionaryKey)
    {
        return dictionariesCache.getReadDictionaryIfPresent(dictionaryKey);
    }

    public Map<DebugDictionaryKey, DebugDictionaryMetadata> getWorkerDictionaryMetadata()
    {
        return dictionariesCache.getWriteDictionaryMetadata();
    }

    public void reset()
    {
        dictionariesCache.resetAllDictionaries();
    }

    public int resetMemoryDictionaries(long dictionaryCacheTotalWight, int concurrency)
    {
        return dictionariesCache.resetMemoryDictionaries(dictionaryCacheTotalWight, concurrency);
    }

    private boolean isDictionaryEnabledForRecType(RecTypeCode recTypeCode, Boolean sessionPropertyEnableDictionary)
    {
        if (!recTypeCode.isSupportedDictionary()) {
            return false;
        }
        // if we have a session property than we go by its value
        if (sessionPropertyEnableDictionary != null) {
            return sessionPropertyEnableDictionary;
        }
        // no session property we use the global configuration list and flag
        boolean inExceptionalList = dictionaryConfiguration.getExceptionalListDictionary().contains(recTypeCode);
        if (dictionaryConfiguration.getEnableDictionary()) {
            // dictionary is enabled - the list contains disabled record types
            return !inExceptionalList;
        }
        // dictionary is disabled - the list contains enabled record types
        return inExceptionalList;
    }

    private boolean isDictionaryValidForColumn(WarmUpElement warmUpElement, Boolean sessionPropertyEnableDictionary)
    {
        return warmUpElement.getWarmUpType() == WarmUpType.WARM_UP_TYPE_DATA &&
                isDictionaryEnabledForRecType(warmUpElement.getRecTypeCode(), sessionPropertyEnableDictionary) &&
                warmUpElement.getRecTypeLength() >= MINIMUM_LEN_FOR_DICTIONARY;
    }

    public void releaseActiveDictionary(DictionaryKey dictionaryKey)
    {
        try {
            dictionariesCache.releaseDictionary(dictionaryKey);
        }
        catch (Exception e) {
            logger.error(e, "failed to release dictionary %s", dictionaryKey);
        }
    }

    public DictionaryCacheConfiguration getDictionaryConfiguration()
    {
        return new DictionaryCacheConfiguration(
                dictionariesCache.getDictionaryCacheTotalSize(),
                dictionariesCache.getCacheConcurrency(),
                dictionaryConfiguration.getDictionaryMaxSize());
    }

    public Map<String, Integer> getDictionaryCachedKeys()
    {
        return dictionariesCache.getDictionaryCachedKeys();
    }

    public record DictionaryCacheConfiguration(
            long maxDictionaryTotalCacheWeight,
            int concurrency,
            long dictionaryMaxSize) {}
}

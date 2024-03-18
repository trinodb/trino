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
package io.trino.plugin.varada.configuration;

import io.airlift.configuration.Config;
import io.airlift.configuration.LegacyConfig;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.plugin.warp.gen.constants.RecTypeCode;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.LOCAL_DATA_STORAGE_PREFIX;
import static io.varada.tools.configuration.MultiPrefixConfigurationWrapper.WARP_SPEED_PREFIX;

public class DictionaryConfiguration
{
    public static final String EXCEPTIONAL_LIST_DICTIONARY = "enable.dictionary.exceptional-list";
    private int dictionaryMaxSize = 64 * 1024;
    private DataSize maxDictionaryTotalCacheWeight = DataSize.of(1024, DataSize.Unit.MEGABYTE);
    private DataSize maxDictionaryCacheWeight = DataSize.of(16, DataSize.Unit.MEGABYTE);
    private int dictionaryCacheConcurrencyLevel = 4;

    //////////////////////////// Enable flags and lists ///////////////////////////////
    // Exceptional Lists of record types is optional and not used for all features
    // If exists, the exceptional list contains exceptions to the general rule:
    //     In case the general rule is Enable, the exceptional list will contain Disabled record types
    //     In case the general rule is Disable, the exceptional list will contain Enabled record types
    // With this approach field engineers can very easily disable/enable fully or partially any supported feature
    ///////////////////////////////////////////////////////////////////////////////////
    private boolean enableDictionary = true;
    private Set<RecTypeCode> exceptionalListDictionary;

    private static final Logger logger = Logger.get(DictionaryConfiguration.class);

    public int getDictionaryMaxSize()
    {
        return dictionaryMaxSize;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.dictionary.max-size")
    @Config(WARP_SPEED_PREFIX + "config.dictionary.max-size")
    public void setDictionaryMaxSize(int dictionaryMaxSize)
    {
        this.dictionaryMaxSize = dictionaryMaxSize;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.max-dictionary-total-cache-weight-mb")
    @Config(WARP_SPEED_PREFIX + "config.max-dictionary-total-cache-weight-mb")
    public void setMaxDictionaryTotalCacheWeight(DataSize maxDictionaryTotalCacheWeight)
    {
        this.maxDictionaryTotalCacheWeight = maxDictionaryTotalCacheWeight;
    }

    public long getMaxDictionaryTotalCacheWeight()
    {
        return maxDictionaryTotalCacheWeight.toBytes();
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.max-dictionary-cache-weight-mb")
    @Config(WARP_SPEED_PREFIX + "config.max-dictionary-cache-weight-mb")
    public void setMaxDictionaryCacheWeight(int maxDictionaryCacheWeight)
    {
        setMaxDictionaryCacheWeight(DataSize.of(maxDictionaryCacheWeight, DataSize.Unit.MEGABYTE));
    }

    public void setMaxDictionaryCacheWeight(DataSize maxDictionaryCacheWeight)
    {
        this.maxDictionaryCacheWeight = maxDictionaryCacheWeight;
    }

    public int getMaxDictionaryCacheWeight()
    {
        return (int) maxDictionaryCacheWeight.toBytes();
    }

    public int getDictionaryCacheConcurrencyLevel()
    {
        return dictionaryCacheConcurrencyLevel;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "config.dictionary-cache-concurrency-level")
    @Config(WARP_SPEED_PREFIX + "config.dictionary-cache-concurrency-level")
    public void setDictionaryCacheConcurrencyLevel(int cacheConcurrencyLevel)
    {
        this.dictionaryCacheConcurrencyLevel = cacheConcurrencyLevel;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + "enable.dictionary")
    @Config(WARP_SPEED_PREFIX + "enable.dictionary")
    public void setEnableDictionary(boolean enableDictionary)
    {
        this.enableDictionary = enableDictionary;
    }

    public boolean getEnableDictionary()
    {
        return enableDictionary;
    }

    @LegacyConfig(LOCAL_DATA_STORAGE_PREFIX + EXCEPTIONAL_LIST_DICTIONARY)
    @Config(WARP_SPEED_PREFIX + EXCEPTIONAL_LIST_DICTIONARY)
    public void setExceptionalListDictionary(String exceptionalListDictionary)
    {
        try {
            this.exceptionalListDictionary = string2RecTypeCodeSet(exceptionalListDictionary);
        }
        catch (Exception e) {
            logger.error("failed to set %s list", EXCEPTIONAL_LIST_DICTIONARY);
        }
    }

    public Set<RecTypeCode> getExceptionalListDictionary()
    {
        return (exceptionalListDictionary != null) ? exceptionalListDictionary : Set.of();
    }

    private Set<RecTypeCode> string2RecTypeCodeSet(String str)
    {
        return Arrays.stream(str.split(",")).map(RecTypeCode::valueOf).collect(Collectors.toSet());
    }
}

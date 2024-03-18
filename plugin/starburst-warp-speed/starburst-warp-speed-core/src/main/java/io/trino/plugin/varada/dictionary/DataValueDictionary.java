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

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.varada.configuration.DictionaryConfiguration;
import io.trino.plugin.varada.dispatcher.model.DictionaryKey;
import io.trino.plugin.varada.dispatcher.model.WarmUpElementState;
import io.trino.plugin.warp.gen.stats.VaradaStatsDictionary;
import io.trino.spi.block.Block;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DataValueDictionary
        implements WriteDictionary, ReadDictionary
{
    static final int MAX_DICTIONARY_SIZE = 1024 * 1024; //Note: the actual dictionary size in double, each element we add to preBlock
    private static final Logger logger = Logger.get(DataValueDictionary.class);

    private final DictionaryKey dictionaryKey;
    private final Lock addKeyLock;
    private final ConcurrentHashMap<Object, Short> writeDictionary;
    private final int fixedRecTypeLength;
    private final VaradaStatsDictionary varadaStatsDictionary;
    private final AtomicInteger usingTransactions = new AtomicInteger();
    private final int maxDictionaryCacheWeight;

    private int dictionaryWeight;
    private int attachedDictionarySize;
    private int maxRecTypeLength;
    private Object[] readDictionary;
    private Block preBlock; //optimization. dictionary values as block. Used by Fillers
    private boolean shouldExport;
    private boolean isImmutable;

    /**
     * dictionary index (value) is increment in serial order from zero. in case it over Short.MAX_VALUE we will cast to unsigned short value.
     * it's done because we need to use ShortBuffer, which gets only short values.
     * in Fillers, we convert back index value to positive number with Short.toUnsignedInt function.
     */
    DataValueDictionary(DictionaryConfiguration dictionaryConfiguration,
            DictionaryKey dictionaryKey,
            int fixedRecTypeLength,
            int maxRecTypeLength,
            VaradaStatsDictionary varadaStatsDictionary)
    {
        this.dictionaryKey = dictionaryKey;
        this.addKeyLock = new ReentrantLock();
        this.writeDictionary = new ConcurrentHashMap<>() {};
        this.dictionaryWeight = 0;
        this.attachedDictionarySize = 0;
        this.readDictionary = new Object[dictionaryConfiguration.getDictionaryMaxSize()];
        this.fixedRecTypeLength = fixedRecTypeLength;
        this.maxDictionaryCacheWeight = dictionaryConfiguration.getMaxDictionaryCacheWeight();
        // maxRecTypeLength
        //   new or loaded fixed length dictionaries: equal to fixedRecTypeLength
        //   new varlen dictionaries: zero (and will be updated on each new key)
        //   loaded varlen dictionaries: the loaded record type length
        this.maxRecTypeLength = maxRecTypeLength;
        this.varadaStatsDictionary = varadaStatsDictionary;
    }

    @Override
    public Short get(Object key)
            throws DictionaryExecutionException
    {
        Short index = writeDictionary.get(key);
        if (index == null) {
            addKeyLock.lock();
            try {
                index = writeDictionary.computeIfAbsent(key, (x) -> {
                    int incDictionaryWeight = addedWeight(key);
                    if (writeDictionary.size() == readDictionary.length || (incDictionaryWeight + dictionaryWeight > maxDictionaryCacheWeight)) {
                        throw new DictionaryMaxException("dictionary get failed", WarmUpElementState.State.FAILED_TEMPORARILY, dictionaryKey);
                    }
                    try {
                        increaseDictionaryWeight(incDictionaryWeight);
                        int newIndex = writeDictionary.size();
                        readDictionary[newIndex] = key;
                        return (short) newIndex;
                    }
                    catch (Exception e) {
                        logger.error(e, "failed to append key %s to dictionary %s", key, this);
                        throw new RuntimeException(e);
                    }
                });
            }
            finally {
                addKeyLock.unlock();
            }
        }
        return index;
    }

    // we assume this API is called under a lock to get a dictionary reference, thus no need to take the key lock
    @Override
    public void loadKey(Object key, int index)
    {
        readDictionary[index] = key;
        attachedDictionarySize++;
        writeDictionary.put(key, (short) index);
        int incDictionaryWeight = addedWeight(key);
        increaseDictionaryWeight(incDictionaryWeight);
    }

    @Override
    public DictionaryKey getDictionaryKey()
    {
        return dictionaryKey;
    }

    @Override
    public Object get(int index)
    {
        return readDictionary[index];
    }

    @Override
    public int getReadSize()
    {
        return attachedDictionarySize;
    }

    @Override
    public int getReadAvailableSize()
    {
        return getWriteSize();
    }

    @Override
    public int getWriteSize()
    {
        addKeyLock.lock();
        try {
            return writeDictionary.size();
        }
        finally {
            addKeyLock.unlock();
        }
    }

    @Override
    public int getRecTypeLength()
    {
        return maxRecTypeLength;
    }

    public void setImmutable()
    {
        isImmutable = true;
    }

    public boolean isImmutable()
    {
        return isImmutable;
    }

    @Override
    public Block getPreBlockDictionaryIfExists(int rowsToFill)
    {
        if (preBlock == null) {
            return null;
        }
        if (rowsToFill < (getReadSize() / 2)) {
            return null;
        }
        return preBlock;
    }

    // called under synchronized
    public void setDictionaryPreBlock(Block preBlock)
    {
        this.preBlock = preBlock;
    }

    DictionaryToWrite createDictionaryToWrite()
    {
        addKeyLock.lock();
        try {
            return new DictionaryToWrite(readDictionary, writeDictionary.size(), maxRecTypeLength, dictionaryWeight);
        }
        finally {
            addKeyLock.unlock();
        }
    }

    // called under synchronized
    void dictionaryAttached()
    {
        shouldExport = true;
    }

    boolean canSkipWrite()
    {
        return getWriteSize() == attachedDictionarySize;
    }

    void reset()
    {
        writeDictionary.clear();
        readDictionary = new Object[readDictionary.length];
        preBlock = null;
        attachedDictionarySize = 0;
    }

    public int getDictionaryWeight()
    {
        return dictionaryWeight;
    }

    public boolean isVarlen()
    {
        return fixedRecTypeLength == 0;
    }

    private void increaseDictionaryWeight(int incDictionaryWeight)
    {
        if (isVarlen()) {
            varadaStatsDictionary.adddictionaries_varlen_str_weight(incDictionaryWeight);
        }
        else {
            incDictionaryWeight = fixedRecTypeLength; //for preDictionary
        }
        dictionaryWeight += incDictionaryWeight;
        varadaStatsDictionary.adddictionaries_weight(incDictionaryWeight);
    }

    private int addedWeight(Object key)
    {
        int incDictionaryWeight;
        if (isVarlen()) {
            maxRecTypeLength = Math.max(((Slice) key).length(), maxRecTypeLength);
            incDictionaryWeight = ((Slice) key).length(); //for preDictionary
        }
        else {
            incDictionaryWeight = fixedRecTypeLength; //for preDictionary
        }
        return incDictionaryWeight;
    }

    public int getUsingTransactions()
    {
        return usingTransactions.get();
    }

    public int incUsingTransactions()
    {
        return this.usingTransactions.incrementAndGet();
    }

    public int decUsingTransactions()
    {
        return this.usingTransactions.decrementAndGet();
    }

    @Override
    public String toString()
    {
        return "DataValueDictionary{" +
                "dictionaryKey=" + dictionaryKey +
                ", writeDictionarySize=" + writeDictionary.size() +
                ", fixedRecTypeLength=" + fixedRecTypeLength +
                ", usingTransactions=" + usingTransactions +
                ", dictionaryWeight=" + dictionaryWeight +
                ", attachedDictionarySize=" + attachedDictionarySize +
                ", recTypeLength=" + maxRecTypeLength +
                ", preBlockExist=" + (preBlock != null ? "true" : "false") +
                ", shouldExport=" + shouldExport +
                ", isImmutable=" + isImmutable +
                '}';
    }
}

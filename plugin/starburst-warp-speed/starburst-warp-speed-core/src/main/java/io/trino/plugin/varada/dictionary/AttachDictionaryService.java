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
import io.trino.plugin.varada.configuration.DictionaryConfiguration;
import io.trino.plugin.varada.dispatcher.model.DictionaryKey;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.write.dictionary.DictionaryWriter;
import io.trino.plugin.varada.storage.write.dictionary.DictionaryWriterFactory;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.stats.VaradaStatsDictionary;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import static io.trino.plugin.varada.dictionary.DictionaryCacheService.DICTIONARY_STAT_GROUP;
import static java.util.Objects.requireNonNull;

@Singleton
public class AttachDictionaryService
{
    private static final Logger logger = Logger.get(AttachDictionaryService.class);

    private static final byte[] padding = new byte[8192];   // PageSize

    private final StorageEngineConstants storageEngineConstants;
    private final WorkerCapacityManager workerCapacityManager;
    private final DictionaryConfiguration dictionaryConfiguration;
    private final VaradaStatsDictionary varadaStatsDictionary;
    private final DictionaryWriterFactory dictionaryWriterFactory;

    @Inject
    public AttachDictionaryService(WorkerCapacityManager workerCapacityManager,
            StorageEngineConstants storageEngineConstants,
            DictionaryConfiguration dictionaryConfiguration,
            MetricsManager metricsManager,
            DictionaryWriterFactory dictionaryWriterFactory)
    {
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.workerCapacityManager = requireNonNull(workerCapacityManager);
        this.dictionaryConfiguration = requireNonNull(dictionaryConfiguration);
        this.varadaStatsDictionary = metricsManager.registerMetric(VaradaStatsDictionary.create(DICTIONARY_STAT_GROUP));
        this.dictionaryWriterFactory = requireNonNull(dictionaryWriterFactory);
    }

    public int save(
            DictionaryToWrite dictionaryToWrite,
            RecTypeCode recTypeCode,
            int dictionaryOffset,
            String rowGroupFilePath)
    {
        DictionaryWriter dictionaryWriter = dictionaryWriterFactory.getDictionaryWriter(recTypeCode.ordinal());
        byte[] dictionaryBytes = dictionaryWriter.getDictionaryWriteData(dictionaryToWrite);
        File rowGroupDataFile = new File(rowGroupFilePath);
        int dictionarySizeInPages;

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(rowGroupDataFile, "rw")) {
            long offset = Integer.toUnsignedLong(dictionaryOffset) * storageEngineConstants.getPageSize();
            randomAccessFile.seek(offset);

            int length = 0;

            // write dictionaryBytes length
            randomAccessFile.writeLong(Integer.toUnsignedLong(dictionaryBytes.length));
            length += Long.BYTES;

            // write dictionaryBytes
            randomAccessFile.write(dictionaryBytes);
            length += dictionaryBytes.length;

            int pageRemainder = length & storageEngineConstants.getPageOffsetMask();
            int paddingSize = (pageRemainder != 0) ? (storageEngineConstants.getPageSize() - pageRemainder) : 0;

            if (paddingSize > 0) {
                // write padding
                randomAccessFile.write(padding, 0, paddingSize);
                length += paddingSize;
            }

            dictionarySizeInPages = length / storageEngineConstants.getPageSize();
            logger.debug("save dictionary rowGroupFilePath %s recTypeCode %s dictionaryOffset %d dictionarySizeInPages %d",
                    rowGroupFilePath, recTypeCode, dictionaryOffset, dictionarySizeInPages);
        }
        catch (IOException e) {
            logger.error("save dictionary failed rowGroupFilePath %s message %s", rowGroupFilePath, e.getMessage());
            throw new RuntimeException(e);
        }
        workerCapacityManager.setCurrentUsage();
        return dictionarySizeInPages;
    }

    public DataValueDictionary load(
            DictionaryKey dictionaryKey,
            RecTypeCode recTypeCode,
            int recTypeLength,
            int dictionaryOffset,
            String rowGroupFilePath)
    {
        int fixedRecTypeLength = TypeUtils.isVarlenStr(recTypeCode) ? 0 : recTypeLength;

        DataValueDictionary dataValuesDictionary = new DataValueDictionary(dictionaryConfiguration,
                dictionaryKey,
                fixedRecTypeLength,
                recTypeLength,
                varadaStatsDictionary);

        File rowGroupDataFile = new File(rowGroupFilePath);

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(rowGroupDataFile, "rw")) {
            long offset = Integer.toUnsignedLong(dictionaryOffset) * storageEngineConstants.getPageSize();
            randomAccessFile.seek(offset);

            // read dictionaryBytes length
            long length = randomAccessFile.readLong();

            if (length == 0) {
                logger.error("load dictionary failed dictionaryKey %s rowGroupFilePath %s dictionaryOffset %d length %d",
                        dictionaryKey, rowGroupFilePath, dictionaryOffset, length);
                throw new RuntimeException("end of file reached");
            }

            // read dictionaryBytes
            byte[] dictionaryBytes = new byte[Long.valueOf(length).intValue()];
            int readBytes = randomAccessFile.read(dictionaryBytes);

            if (readBytes <= 0) {
                logger.error("load dictionary failed dictionaryKey %s rowGroupFilePath %s length %d readBytes %d", dictionaryKey, rowGroupFilePath, length, readBytes);
                throw new RuntimeException("end of file reached");
            }
            logger.debug("load dictionary dictionaryKey %s rowGroupFilePath %s recTypeCode %s", dictionaryKey, rowGroupFilePath, recTypeCode);

            DictionaryWriter dictionaryWriter = dictionaryWriterFactory.getDictionaryWriter(recTypeCode.ordinal());
            dictionaryWriter.loadDataValueDictionary(dataValuesDictionary, dictionaryBytes);
            dataValuesDictionary.setImmutable();
        }
        catch (IOException e) {
            logger.error("load dictionary failed dictionaryKey %s rowGroupFilePath %s message %s", dictionaryKey, rowGroupFilePath, e.getMessage());
            throw new RuntimeException(e);
        }
        return dataValuesDictionary;
    }
}

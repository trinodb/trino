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
package io.trino.plugin.varada.dispatcher.dal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.util.json.SliceSerializer;
import io.trino.plugin.varada.util.json.VaradaColumnJsonKeyDeserializer;
import io.varada.tools.util.CompressionUtil;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static java.util.Objects.requireNonNull;

@Singleton
public class RowGroupDataDao
{
    private static final Logger logger = Logger.get(RowGroupDataDao.class);

    private final GlobalConfiguration globalConfiguration;
    private final ObjectMapper objectMapper;
    private final LoadingCache<RowGroupKey, Optional<RowGroupData>> cache;
    private final StorageEngineConstants storageEngineConstants;

    protected boolean loaded;

    @Inject
    public RowGroupDataDao(
            GlobalConfiguration globalConfiguration,
            ObjectMapperProvider objectMapperProvider,
            StorageEngineConstants storageEngineConstants)
    {
        this.globalConfiguration = requireNonNull(globalConfiguration);

        objectMapper = requireNonNull(objectMapperProvider).get();
        SimpleModule simpleModule = new SimpleModule();
        simpleModule.addKeyDeserializer(VaradaColumn.class, new VaradaColumnJsonKeyDeserializer());
        simpleModule.addSerializer(Slice.class, new SliceSerializer());
        objectMapper.registerModules(simpleModule);

        CacheLoader<RowGroupKey, Optional<RowGroupData>> loader = new CacheLoader<>()
        {
            @SuppressWarnings("NullableProblems")
            @Override
            public Optional<RowGroupData> load(RowGroupKey key)
            {
                File rowGroupDataFile = getRowGroupDataFile(key);
                if (rowGroupDataFile.exists()) {
                    logger.debug("loading row group for key [%s]", key);
                    try (RandomAccessFile randomAccessFile = new RandomAccessFile(rowGroupDataFile, "r")) {
                        //read object size
                        final int offsetSize = Long.BYTES;
                        randomAccessFile.seek(randomAccessFile.length() - offsetSize);
                        long objectSize = randomAccessFile.readLong();

                        if ((objectSize <= 0) || (objectSize >= randomAccessFile.length() - offsetSize)) {
                            logger.error("load file failed %s length %d objectSize %d",
                                    rowGroupDataFile.getAbsolutePath(), randomAccessFile.length(), objectSize);
                            throw new RuntimeException("corrupted RowGroupData file");
                        }

                        // read object
                        long objectOffset = randomAccessFile.length() - (objectSize + offsetSize);
                        logger.debug("read length %d objectSize %d objectOffset %d objectOffsetInPages %d",
                                randomAccessFile.length(), objectSize, objectOffset, objectOffset >> 13);
                        randomAccessFile.seek(objectOffset);
                        byte[] bytes = new byte[Long.valueOf(objectSize).intValue()];
                        int readBytes = randomAccessFile.read(bytes);

                        if (readBytes <= 0) {
                            logger.error("load file failed %s length %d objectSize %d objectOffset %d",
                                    rowGroupDataFile.getAbsolutePath(), randomAccessFile.length(), objectSize, objectOffset);
                            throw new RuntimeException("corrupted RowGroupData file");
                        }
                        String str = CompressionUtil.decompressGzip(bytes);
                        return Optional.of(objectMapper.readerFor(RowGroupData.class).readValue(str));
                    }
                    catch (IOException e) {
                        logger.error(e, "failed reading file [%s]", rowGroupDataFile.getAbsolutePath());
                        throw new RuntimeException("corrupted RowGroupData file");
                    }
                }
                return Optional.empty();
            }
        };
        cache = buildUnsafeCache(CacheBuilder.newBuilder()
                        .maximumSize(Integer.MAX_VALUE), // replace MAX_VALUE with a configuration
                loader);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
    }

    @SuppressModernizer
    private static <K, V> LoadingCache<K, V> buildUnsafeCache(CacheBuilder<? super K, ? super V> cacheBuilder, CacheLoader<? super K, V> cacheLoader)
    {
        return cacheBuilder.build(cacheLoader);
    }

    private File getRowGroupDataFile(RowGroupKey rowGroupKey)
    {
        return new File(rowGroupKey.stringFileNameRepresentation(globalConfiguration.getLocalStorePath()));
    }

    public RowGroupData merge(RowGroupData origRowGroupData, RowGroupData newRowGroupData)
    {
        return RowGroupData.builder(newRowGroupData)
                .lock(origRowGroupData.getLock())
                .build();
    }

    public void save(RowGroupData... entities)
    {
        save(Arrays.asList(entities));
    }

    public Collection<RowGroupData> save(Collection<RowGroupData> entities)
    {
        entities.forEach(rowGroupData -> cache.put(rowGroupData.getRowGroupKey(), Optional.of(rowGroupData)));
        return entities;
    }

    public RowGroupData get(RowGroupKey rowGroupKey)
    {
        try {
            return cache.get(rowGroupKey).orElse(null);
        }
        catch (UncheckedExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                // RowGroupData file is corrupted, delete it and invalidate cache
                File rowGroupDataFile = getRowGroupDataFile(rowGroupKey);

                deleteFile(rowGroupDataFile);
                invalidate(rowGroupKey);
                return null;
            }
            throw new RuntimeException(e);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public RowGroupData getIfPresent(RowGroupKey rowGroupKey)
    {
        Optional<RowGroupData> rowGroupDataOptional = cache.getIfPresent(rowGroupKey);
        return rowGroupDataOptional != null ? rowGroupDataOptional.orElse(null) : null;
    }

    public Collection<RowGroupData> get(Collection<RowGroupKey> rowGroupKeys)
    {
        try {
            return cache.getAll(rowGroupKeys)
                    .values()
                    .stream()
                    .map(rowGroupData -> rowGroupData.orElse(null))
                    .filter(Objects::nonNull)
                    .toList();
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public Collection<RowGroupData> getAll()
    {
        return cache.asMap()
                .values()
                .stream()
                .map(rowGroupData -> rowGroupData.orElse(null))
                .filter(Objects::nonNull)
                .toList();
    }

    public synchronized void delete(RowGroupKey rowGroupKey, boolean deleteFromCache)
    {
        if (get(rowGroupKey) == null) {
            logger.debug("RowGroup for [%s] was deleted already", rowGroupKey);
            return;
        }

        File rowGroupDataFile = getRowGroupDataFile(rowGroupKey);
        if (!rowGroupDataFile.exists()) {
            logger.debug("cant delete file %s since it was deleted already", rowGroupDataFile.getAbsolutePath());
        }
        else {
            deleteFile(rowGroupDataFile);
        }

        if (deleteFromCache) {
            invalidate(rowGroupKey);
        }
    }

    public synchronized void delete(RowGroupKey rowGroupKey)
    {
        delete(rowGroupKey, true);
    }

    public void delete(RowGroupData entity)
    {
        delete(entity.getRowGroupKey());
    }

    public synchronized void refresh(RowGroupKey rowGroupKey)
    {
        cache.refresh(rowGroupKey);
    }

    public void invalidate(RowGroupKey rowGroupKey)
    {
        cache.invalidate(rowGroupKey);
    }

    private void deleteFile(File rowGroupDataFile)
    {
        if (!rowGroupDataFile.delete()) {
            throw new RuntimeException("failed deleting file " + rowGroupDataFile.getAbsolutePath());
        }
        // @ToDo: clear RowGroupData.fileModifiedTime
    }

    private synchronized void flushInternal(RowGroupKey rowGroupKey)
    {
        File rowGroupDataFile = getRowGroupDataFile(rowGroupKey);

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(rowGroupDataFile, "rw")) {
            // flush footer from cache
            RowGroupData rowGroupData = get(rowGroupKey);

            long offset = Integer.toUnsignedLong(rowGroupData.getNextOffset()) * storageEngineConstants.getPageSize();
            logger.debug("flush nextOffsetPages %d nextOffset %d", rowGroupData.getNextOffset(), offset);
            randomAccessFile.seek(offset);

            // write object
            String str = objectMapper.writeValueAsString(rowGroupData);
            byte[] bytes = CompressionUtil.compressGzip(str);

            logger.debug("flushInternal writing %d bytes %s", bytes.length, str);
            randomAccessFile.write(bytes);

            // write object size
            randomAccessFile.writeLong(Integer.valueOf(bytes.length).longValue());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.debug("flushed row group file [%s] for row group key [%s]", rowGroupDataFile.getAbsolutePath(), rowGroupKey);
    }

    public void flush(RowGroupKey rowGroupKey)
    {
        // do not flush footer if there is no data
        File rowGroupDataFile = getRowGroupDataFile(rowGroupKey);
        if (!rowGroupDataFile.exists()) {
            logger.debug("flush rowGroupDataFile %s does not exist", rowGroupDataFile);
            return;
        }
        if (rowGroupDataFile.length() == 0) {
            logger.debug("flush rowGroupDataFile %s length is 0", rowGroupDataFile);
            deleteFile(rowGroupDataFile);
            return;
        }
        flushInternal(rowGroupKey);
    }
}

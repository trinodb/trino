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
package io.trino.plugin.varada.storage.lucene;

import io.airlift.log.Logger;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.juffers.ReadJuffersWarmUpElement;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

public class VaradaInputDirectory
        extends BaseDirectory
{
    private static final Logger logger = Logger.get(VaradaInputDirectory.class);
    private final StorageEngine storageEngine;
    private final StorageEngineConstants storageEngineConstants;
    private final ReadJuffersWarmUpElement juffersWE;
    private final long nativeCookie;
    private final int[] fileLengths;
    private final String filePrefix;

    protected VaradaInputDirectory(StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            ReadJuffersWarmUpElement juffersWE,
            long nativeCookie,
            String filePrefix,
            int[] fileLengths)
    {
        super(NoLockFactory.INSTANCE);
        this.storageEngine = storageEngine;
        this.storageEngineConstants = storageEngineConstants;
        this.juffersWE = juffersWE;
        this.nativeCookie = nativeCookie;
        this.filePrefix = filePrefix;
        this.fileLengths = fileLengths;
    }

    @Override
    public String[] listAll()
    {
        // need to combine temporary with constant files??
        // is this api used only in read?
        // is this api used to get the list of files before the merge begin

        String[] fileNames = new String[LuceneFileType.values().length - 1];
        fileNames[LuceneFileType.CFE.getNativeId()] = filePrefix + ".cfe";
        fileNames[LuceneFileType.CFS.getNativeId()] = filePrefix + ".cfs";
        fileNames[LuceneFileType.SI.getNativeId()] = filePrefix + ".si";
        fileNames[LuceneFileType.SEGMENTS.getNativeId()] = "segments_1";
        logger.debug("nativeCookie=%s, listAll %s", nativeCookie, Arrays.toString(fileNames));
        return fileNames;
    }

    @Override
    public void deleteFile(String fileName)
    {
        logger.debug("nativeCookie=%s, delete file %s while input", nativeCookie, fileName);
    }

    @Override
    public long fileLength(String fileName)
    {
        LuceneFileType type = LuceneFileType.getType(fileName);
        long len = fileLengths[type.getNativeId()];
        logger.debug("nativeCookie=%s, fileLength = %d", nativeCookie, len);
        return len;
    }

    @Override
    public IndexOutput createOutput(String fileName, IOContext ioContext)
    {
        logger.debug("nativeCookie=%s, createTempOutput file %s while input", nativeCookie, fileName);
        return null;
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext ioContext)
    {
        logger.debug("nativeCookie=%s, createTempOutput file %s while input", nativeCookie, prefix);
        return null;
    }

    @Override
    public void sync(Collection<String> collection)
    {
        logger.debug("nativeCookie=%s, sync file while input", nativeCookie);
    }

    @Override
    public void syncMetaData()
    {
        logger.debug("nativeCookie=%s, syncMetadata file while input", nativeCookie);
    }

    @Override
    public void rename(String source, String dest)
    {
        logger.debug("nativeCookie=%s, rename file %s while input", nativeCookie, source);
    }

    @Override
    public IndexInput openInput(String fileName, IOContext ioContext)
    {
        logger.debug("nativeCookie=%s, openInput file %s while input", nativeCookie, fileName);
        LuceneFileType luceneFileType = LuceneFileType.getType(fileName);
        return new VaradaReadIndexInput(storageEngine, storageEngineConstants, juffersWE, luceneFileType, nativeCookie, 0, fileLengths[luceneFileType.getNativeId()], "root");
    }

    @Override
    public ChecksumIndexInput openChecksumInput(String name, IOContext context)
    {
        logger.debug("nativeCookie=%s, openChecksumInput file %s while input", nativeCookie, name);
        LuceneFileType luceneFileType = LuceneFileType.getType(name);
        return new VaradaReadIndexInput(storageEngine, storageEngineConstants, juffersWE, luceneFileType, nativeCookie, 0, fileLengths[luceneFileType.getNativeId()], "root");
    }

    @Override
    public void close()
    {
        logger.debug("nativeCookie=%s, close while input", nativeCookie);
    }

    @Override
    public Set<String> getPendingDeletions()
    {
        logger.debug("nativeCookie=%s, getPendingDeletions", nativeCookie);
        return null;
    }
}

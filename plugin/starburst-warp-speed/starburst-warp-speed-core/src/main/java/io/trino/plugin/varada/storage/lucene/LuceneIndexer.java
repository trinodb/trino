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

import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.varada.VaradaErrorCode;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.warp.gen.stats.VaradaStatsLuceneIndexer;
import io.trino.spi.TrinoException;
import io.varada.tools.util.StopWatch;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.google.common.io.BaseEncoding.base64;
import static io.trino.plugin.varada.VaradaErrorCode.VARADA_LUCENE_FAILURE;
import static io.trino.plugin.varada.VaradaErrorCode.VARADA_LUCENE_WRITER_ERROR;
import static io.trino.plugin.varada.util.SliceUtils.serializeSlice;

public class LuceneIndexer
{
    public static final Slice LUCENE_NULL_STRING = Slices.wrappedBuffer(base64().decode("27d52991a99c455789ed5e39b77226c8"));
    static final String VALUE_FIELD_NAME = "value";
    private static final Logger logger = Logger.get(LuceneIndexer.class);
    private final Analyzer analyzer = new KeywordAnalyzer();
    private final StorageEngine storageEngine;
    private final StorageEngineConstants storageEngineConstants;
    private final WriteJuffersWarmUpElement juffersWE;
    private IndexWriter indexWriter;
    private final VaradaStatsLuceneIndexer stats;
    private final StopWatch stopWatch;
    private final Document doc = new Document();
    private boolean failedCommit;
    private String failedDocumentError;

    public LuceneIndexer(StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            WriteJuffersWarmUpElement juffersWE,
            VaradaStatsLuceneIndexer luceneStats)
    {
        this.storageEngine = storageEngine;
        this.storageEngineConstants = storageEngineConstants;
        this.juffersWE = juffersWE;
        this.stats = luceneStats;
        this.stopWatch = new StopWatch();
    }

    public void addDoc(Slice... values)
            throws IOException
    {
        if (failedCommit) {
            return;
        }

        try {
            stopWatch.reset();
            stopWatch.start();
            doc.clear();
            for (Slice value : values) {
                TextField textField = new TextField(VALUE_FIELD_NAME, serializeSlice(value), Field.Store.NO);
                doc.add(textField);
            }

            indexWriter.addDocument(doc);
            stopWatch.stop();
            stats.addaddDoc(stopWatch.getNanoTime());
        }
        catch (Exception e) {
            logger.error("got exception from addDocument - %s", e);
            failedDocumentError = e.getMessage();
            failedCommit = true;
            stats.incfailedAddDoc();
            throw e;
        }
    }

    public void resetLuceneIndex()
    {
        logger.debug("reset index");
        try {
            LogDocMergePolicy logDocMergePolicy = new LogDocMergePolicy();
            logDocMergePolicy.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);
            logDocMergePolicy.setNoCFSRatio(1.0);
            logDocMergePolicy.setMinMergeDocs(1_000_000);
            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            config.setCodec(new Lucene99Codec(Lucene99Codec.Mode.BEST_SPEED));
            config.setUseCompoundFile(true);
            config.setMergePolicy(logDocMergePolicy);

            indexWriter = new IndexWriter(new ByteBuffersDirectory(), config);
        }
        catch (Exception e) {
            logger.warn("Got exception when creating the indexWriter - %s", e);
            stats.incfailedReset();
            throw new TrinoException(VaradaErrorCode.VARADA_LUCENE_WRITER_ERROR, "failed creating index writer of col", e);
        }
    }

    public void resetLuceneBufferPosition()
    {
        for (ByteBuffer buffer : juffersWE.getLuceneFileBuffers()) {
            if (buffer != null) {
                buffer.position(0);
            }
        }
    }

    @SuppressWarnings("ThrowFromFinallyBlock")
    public void closeLuceneIndex(long weCookie)
    {
        IndexWriter weIndexWriter = indexWriter;
        if (weIndexWriter == null) {
            return;
        }
        logger.debug("weCookie %x, close index", weCookie);
        Directory tempDirectory = weIndexWriter.getDirectory();
        VaradaOutputDirectory finalDirectory = new VaradaOutputDirectory(storageEngine, storageEngineConstants, juffersWE, weCookie);
        int[] filesLength = new int[LuceneFileType.values().length - 1];
        try {
            stopWatch.reset();
            stopWatch.start();
            weIndexWriter.forceMerge(1);
            stopWatch.stop();
            stats.addmerge(stopWatch.getTime());
            indexWriter.close();

            if (failedCommit) {
                if (failedDocumentError != null) {
                    throw new TrinoException(VARADA_LUCENE_FAILURE, "lucene index failed on at least one document " + failedDocumentError);
                }
                else {
                    throw new TrinoException(VARADA_LUCENE_FAILURE, "lucene index failed before closing");
                }
            }

            Directory lastDirectory = weIndexWriter.getDirectory();
            String[] luceneFiles = lastDirectory.listAll();
            for (String luceneFile : luceneFiles) {
                LuceneFileType luceneFileType = LuceneFileType.getType(luceneFile);
                if (luceneFileType != LuceneFileType.UNKNOWN) {
                    stopWatch.reset();
                    stopWatch.start();
                    logger.debug("weCookie %x, start copy file %s", weCookie, luceneFile);
                    String dest = LuceneFileType.getFixedFileName(luceneFileType);
                    int fileLength = copyFromWithLength(finalDirectory, lastDirectory, luceneFile, dest);
                    stopWatch.stop();
                    stats.addcopy(stopWatch.getNanoTime());
                    filesLength[luceneFileType.getNativeId()] = fileLength;
                }
            }
        }
        catch (Exception e) {
            filesLength = new int[0]; //zero size -  in order to mark it as failed in native
            String error = String.format("weCookie %x, Got exception when creating the indexWriter", weCookie);
            throw new TrinoException(VARADA_LUCENE_WRITER_ERROR, error, e);
        }
        finally {
            try {
                if (!failedCommit) {
                    logger.debug("weCookie %x, committing lucene buffer: fileLengths %s", weCookie, Arrays.toString(filesLength));
                    int rc = storageEngine.luceneCommitBuffers(weCookie, juffersWE.getSingleAndResetLuceneWE(), filesLength);
                    if (rc < 0) {
                        throw new TrinoException(VARADA_LUCENE_WRITER_ERROR, "lucene commit failed, file too big");
                    }
                }
            }
            catch (Throwable t) {
                logger.debug(t, "weCookie=%x, failed committing buffer", weCookie);
                failedCommit = true;
                throw t;
            }
            close(tempDirectory);
        }
    }

    private void close(Directory directory)
    {
        stopWatch.reset();
        stopWatch.start();
        try {
            directory.close();
        }
        catch (IOException e) {
            logger.info(e.getMessage());
        }
        stopWatch.stop();
    }

    public int copyFromWithLength(Directory directory, Directory from, String src, String dest)
            throws IOException
    {
        IOContext context = IOContext.READONCE;
        boolean success = false;
        int fileLength;
        try (IndexInput is = from.openInput(src, context);
                IndexOutput os = directory.createOutput(dest, context)) {
            os.copyBytes(is, is.length());
            fileLength = (int) is.length();
            success = true;
        }
        finally {
            if (!success) {
                IOUtils.deleteFilesIgnoringExceptions(directory, dest);
            }
        }
        return fileLength;
    }

    public void abort()
    {
        if (indexWriter != null) {
            close(indexWriter.getDirectory());
        }
    }

    @VisibleForTesting
    public Directory getLuceneDirectory()
    {
        return indexWriter.getDirectory();
    }
}

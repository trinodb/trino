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
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.query.data.match.LuceneQueryMatchData;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.plugin.warp.gen.stats.VaradaStatsDispatcherPageSource;
import io.varada.log.ShapingLogger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;

@SuppressWarnings("deprecation")
public class LuceneMatcher
{
    private static final Logger logger = Logger.get(LuceneMatcher.class);

    private static final String FILE_PREFIX = "_0";
    private static final int JAVA_RC_ERR = -1; // predicate got from Domain. in case of error presto can handle
    private static final int JAVA_RC_STOP_EXECUTION = -2; // predicate got from hack
    private static final int JAVA_NON_HACK_PREDICATE = -3; // predicate got from non-hack
    private final StorageEngine storageEngine;
    private final StorageEngineConstants storageEngineConstants;
    private final VaradaStatsDispatcherPageSource statsDispatcherPageSource;
    private final ReadJuffersWarmUpElement juffersWE;
    private final Optional<Query> query; // when query is empty it means that the predicate is from Domain
    private final ShapingLogger shapingLogger;

    public LuceneMatcher(StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            ReadJuffersWarmUpElement juffersWE,
            LuceneQueryMatchData luceneQueryMatchData,
            VaradaStatsDispatcherPageSource statsDispatcherPageSource,
            GlobalConfiguration globalConfiguration)
    {
        this.storageEngine = storageEngine;
        this.storageEngineConstants = storageEngineConstants;
        this.juffersWE = juffersWE;
        this.query = Optional.of(luceneQueryMatchData.getQuery());
        this.statsDispatcherPageSource = statsDispatcherPageSource;
        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfiguration.getShapingLoggerThreshold(),
                globalConfiguration.getShapingLoggerDuration(),
                globalConfiguration.getShapingLoggerNumberOfSamples());
        logger.debug("lucene matcher for luceneQueryMatchData %s", luceneQueryMatchData);
    }

    /**
     * This method is called from native as part of the match flow.
     * DO NOT CHANGE THE SIGNATURE
     *
     * @return - 0 for success and JAVA_RC_ERR/JAVA_RC_STOP_EXECUTION for error
     */
    @SuppressWarnings("unused")
    int luceneMatch(long nativeCookie, boolean isValidIndex, boolean allOrNothing, int resultBufferOffset,
            int siFileLength, int cfeFileLength, int segmentsFileLength, int cfsFileLength)
    {
        logger.debug("nativeCookie %d isValidIndex %b allOrNothing %b resultBufferOffset %d queryPresent %b",
                nativeCookie, isValidIndex, allOrNothing, resultBufferOffset, query.isPresent());
        if (!isValidIndex) {
            shapingLogger.error("index is invalid returning JAVA_RC_ERR. nativeCookie %d allOrNothing %b resultBufferOffset %d queryPresent %b file lengths [%d,%d,%d,%d]",
                    nativeCookie, allOrNothing, resultBufferOffset, query.isPresent(), siFileLength, cfeFileLength, segmentsFileLength, cfsFileLength);
            return JAVA_RC_ERR;
        }
        if (query.isEmpty()) {
            shapingLogger.error("query is not present returning JAVA_NON_HACK_PREDICATE. nativeCookie %d isValidIndex %b allOrNothing %b resultBufferOffset %d file lengths [%d,%d,%d,%d]",
                    nativeCookie, isValidIndex, allOrNothing, resultBufferOffset, siFileLength, cfeFileLength, segmentsFileLength, cfsFileLength);
            return JAVA_NON_HACK_PREDICATE;
        }

        long startTime = System.currentTimeMillis();
        int maxDocsToFind = allOrNothing ? 1 : (1 << storageEngineConstants.getChunkSizeShift());

        int[] filesLength = new int[4];
        filesLength[LuceneFileType.SI.getNativeId()] = siFileLength;
        filesLength[LuceneFileType.CFE.getNativeId()] = cfeFileLength;
        filesLength[LuceneFileType.SEGMENTS.getNativeId()] = segmentsFileLength;
        filesLength[LuceneFileType.CFS.getNativeId()] = cfsFileLength;

        int result = JAVA_RC_STOP_EXECUTION;
        if (logger.isDebugEnabled()) {
            logger.debug("nativeCookie=%d, filesLength=%s", nativeCookie, Arrays.toString(filesLength));
        }
        try {
            VaradaInputDirectory varadaInputDirectory = new VaradaInputDirectory(storageEngine, storageEngineConstants,
                    juffersWE,
                    nativeCookie, FILE_PREFIX, filesLength);
            IndexReader reader = DirectoryReader.open(varadaInputDirectory);
            IndexSearcher indexSearcher = new IndexSearcher(reader);
            ByteBuffer luceneBMResultBuffer = juffersWE.getLuceneBMResultBuffer();
            VaradaCollector varadaCollector = new VaradaCollector(luceneBMResultBuffer, resultBufferOffset, maxDocsToFind);
            indexSearcher.search(query.get(), varadaCollector);

            result = varadaCollector.getCount();
            logger.debug("nativeCookie=%d - found %d match results", nativeCookie, result);
        }
        catch (IOException e) {
            shapingLogger.error(e, "I/O error returning JAVA_RC_STOP_EXECUTION. nativeCookie %d maxDocsToFind %d resultBufferOffset %d file lengths [%d,%d,%d,%d]",
                    nativeCookie, maxDocsToFind, resultBufferOffset, siFileLength, cfeFileLength, segmentsFileLength, cfsFileLength);
        }
        catch (Exception e) {
            shapingLogger.error(e, "general error returning JAVA_RC_STOP_EXECUTION. nativeCookie %d maxDocsToFind %d resultBufferOffset %d file lengths [%d,%d,%d,%d]",
                    nativeCookie, maxDocsToFind, resultBufferOffset, siFileLength, cfeFileLength, segmentsFileLength, cfsFileLength);
        }
        statsDispatcherPageSource.addlucene_execution_time(System.currentTimeMillis() - startTime);
        return result;
    }

    static {
        IndexSearcher.setDefaultQueryCachingPolicy(new QueryCachingPolicy()
        {
            @Override
            public void onUse(Query query)
            {
            }

            @SuppressWarnings("CheckedExceptionNotThrown")
            @Override
            public boolean shouldCache(Query query)
            {
                return false;
            }
        });
        IndexSearcher.setMaxClauseCount(Integer.MAX_VALUE);
    }
}

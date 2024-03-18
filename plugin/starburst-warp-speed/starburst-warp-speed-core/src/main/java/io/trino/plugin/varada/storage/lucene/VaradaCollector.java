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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

import java.nio.ByteBuffer;

/**
 * A simple collector created to bypass all the heap heavy process
 * of collecting the results in Lucene. Lucene by default will
 * create a {@link org.apache.lucene.search.TopScoreDocCollector}
 * which internally uses a {@link org.apache.lucene.search.TopDocsCollector}
 * and uses a PriorityQueue to maintain the top results. From the heap usage
 * experiments (please see the design doc), we found out that this was
 * substantially contributing to heap whereas we currently don't need any
 * scoring or top doc collecting.
 * Every time Lucene finds a matching document for the text search query,
 * a callback is invoked into this collector that simply collects the
 * matching doc's docID. We store the docID in a bitmap to be traversed later
 * as part of doc id iteration etc.
 */
public class VaradaCollector
        implements Collector
{
    private final ByteBuffer resultBuffer;
    private final int startOffset;
    private final int docsToFind;
    private int count;

    public VaradaCollector(ByteBuffer resultBuffer, int startOffset, int docsToFind)
    {
        this.resultBuffer = resultBuffer;
        this.startOffset = startOffset;
        this.docsToFind = docsToFind;
    }

    @Override
    public ScoreMode scoreMode()
    {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context)
    {
        return new LeafCollector()
        {
            @SuppressWarnings("CheckedExceptionNotThrown")
            @Override
            public void setScorer(Scorable scorer)
            {
                // we don't use scoring, so this is NO-OP
            }

            @SuppressWarnings("CheckedExceptionNotThrown")
            @Override
            public void collect(int doc)
            {
                if (count < docsToFind) {
                    // Setting the recIx in the juffer shared with native. Note since we insert also nulls, we use luceneDocId as recIx
                    // Note - must not change the existing bits as the juffer may be filled with null values.
                    resultBuffer.put(startOffset + (doc >> 3), (byte) (resultBuffer.get(startOffset + (doc >> 3)) | (1 << (doc & 7))));
                    count++;
                }
            }
        };
    }

    public int getCount()
    {
        return count;
    }
}

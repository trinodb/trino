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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

import static org.assertj.core.api.Assertions.assertThat;

public class LuceneTest
{
    public static final String FIELD_NAME = "f";
    private final Analyzer analyzer = new KeywordAnalyzer();
    private IndexWriter indexWriter;

    @BeforeEach
    public void before()
            throws IOException
    {
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        Lucene99Codec lucene84Codec = new Lucene99Codec(Lucene99Codec.Mode.BEST_SPEED);
        config.setCodec(lucene84Codec);
        config.setUseCompoundFile(true);
        LogDocMergePolicy logDocMergePolicy = new LogDocMergePolicy();
        logDocMergePolicy.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);
        logDocMergePolicy.setNoCFSRatio(1.0);
        logDocMergePolicy.setMinMergeDocs(1_000_000);
        config.setMergePolicy(logDocMergePolicy);

        this.indexWriter = new IndexWriter(new ByteBuffersDirectory(), config);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testStringArray()
            throws IOException
    {
        insertValues("v1", "v2");
        insertValues("n1", "n2");

        indexWriter.forceMerge(1);
        indexWriter.close();

        IndexReader reader = DirectoryReader.open(indexWriter.getDirectory());
        IndexSearcher indexSearcher = new IndexSearcher(reader);
        Query query;

        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();

        TermQuery termQuery = new TermQuery(new Term(FIELD_NAME, "v2"));
        queryBuilder.add(termQuery, BooleanClause.Occur.SHOULD);
        query = queryBuilder.build();

        ByteBuffer buffer = ByteBuffer.allocate(10);
        VaradaCollector varadaCollector = new VaradaCollector(buffer, 0, 10);
        indexSearcher.search(query, varadaCollector);
        BitSet bitSet = BitSet.valueOf(buffer);
        assertThat(bitSet.get(0)).isTrue();
        assertThat(bitSet.get(1)).isFalse();
    }

    private void insertValues(String... values)
            throws IOException
    {
        Document doc = new Document();
        Field field = new TextField(FIELD_NAME, "", Field.Store.YES);
        for (String value : values) {
            field.setStringValue(value);
            doc.add(field);
        }
        indexWriter.addDocument(doc);
    }
}

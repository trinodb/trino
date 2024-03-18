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

import io.airlift.slice.Slice;
import io.trino.plugin.varada.util.SliceUtils;
import io.trino.spi.predicate.Range;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;

import java.nio.charset.Charset;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.plugin.varada.storage.lucene.LuceneIndexer.VALUE_FIELD_NAME;
import static java.nio.charset.StandardCharsets.UTF_8;

public class LuceneQueryUtils
{
    // See https://www.elastic.co/guide/en/elasticsearch/reference/current/regexp-syntax.html
    private static final char[] REGEXP_RESERVED_CHARACTERS = new char[] {'.', '?', '+', '*', '|', '{', '}', '[', ']', '(', ')', '"', '#', '@', '&', '<', '>', '~'};

    private LuceneQueryUtils()
    {
    }

    public static Query createRangeQuery(Range range)
    {
        if (range.isSingleValue()) {
            return new TermQuery(new Term(VALUE_FIELD_NAME, new String(((Slice) range.getSingleValue()).getBytes(), Charset.defaultCharset())));
        }
        return new TermRangeQuery(VALUE_FIELD_NAME,
                range.isLowUnbounded() ? null : new BytesRef(((Slice) range.getLowBoundedValue()).getBytes()),
                range.isHighUnbounded() ? null : new BytesRef(((Slice) range.getHighBoundedValue()).getBytes()),
                range.isLowInclusive(),
                range.isHighInclusive());
    }

    public static Query createPrefixQuery(Slice prefix)
    {
        return new PrefixQuery(new Term(VALUE_FIELD_NAME, new String(prefix.getBytes(), UTF_8)));
    }

    public static Query createLikeQuery(Slice like)
    {
        return new RegexpQuery(new Term(VALUE_FIELD_NAME, likeToRegexp(like)));
    }

    public static Query createOrOfLikesQuery(List<Slice> likeValues)
    {
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        for (Slice value : likeValues) {
            Query query = createLikeQuery(value);
            queryBuilder.add(query, BooleanClause.Occur.SHOULD);
        }
        return queryBuilder.build();
    }

    public static Query createContainsQuery(Slice slice)
    {
        return new TermQuery(new Term(VALUE_FIELD_NAME, new String(slice.getBytes(), UTF_8)));
    }

    // This method is copy-pasted from our implementation in Trino
    protected static String likeToRegexp(Slice likeSlice)
    {
        String strLike = SliceUtils.serializeSlice(likeSlice);
        // TODO: This can be done more efficiently by using a state machine and iterating over characters (See io.trino.type.LikeFunctions.likePattern(String, char, boolean))
        String regexp = strLike.replaceAll(Pattern.quote("\\"), Matcher.quoteReplacement("\\\\")); // first, escape regexp's escape character
        for (char c : REGEXP_RESERVED_CHARACTERS) {
            regexp = regexp.replaceAll(Pattern.quote(String.valueOf(c)), Matcher.quoteReplacement("\\" + c));
        }
        return regexp
                .replaceAll("%", ".*")
                .replaceAll("_", ".");
    }
}

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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.bson.Document;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.predicate.Range.equal;
import static io.trino.spi.predicate.Range.greaterThan;
import static io.trino.spi.predicate.Range.greaterThanOrEqual;
import static io.trino.spi.predicate.Range.lessThan;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

public class TestMongoSession
{
    private static final MongoColumnHandle COL1 = new MongoColumnHandle("col1", BIGINT, false, Optional.empty());
    private static final MongoColumnHandle COL2 = new MongoColumnHandle("col2", createUnboundedVarcharType(), false, Optional.empty());
    private static final MongoColumnHandle COL3 = new MongoColumnHandle("col3", createUnboundedVarcharType(), false, Optional.empty());
    private static final MongoColumnHandle COL4 = new MongoColumnHandle("col4", BOOLEAN, false, Optional.empty());

    @Test
    public void testBuildQuery()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL1, Domain.create(ValueSet.ofRanges(range(BIGINT, 100L, false, 200L, true)), false),
                COL2, Domain.singleValue(createUnboundedVarcharType(), utf8Slice("a value"))));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document()
                .append(COL1.getName(), new Document().append("$gt", 100L).append("$lte", 200L))
                .append(COL2.getName(), new Document("$eq", "a value"));
        assertEquals(query, expected);
    }

    @Test
    public void testBuildQueryStringType()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL3, Domain.create(ValueSet.ofRanges(range(createUnboundedVarcharType(), utf8Slice("hello"), false, utf8Slice("world"), true)), false),
                COL2, Domain.create(ValueSet.ofRanges(greaterThanOrEqual(createUnboundedVarcharType(), utf8Slice("a value"))), false)));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document()
                .append(COL3.getName(), new Document().append("$gt", "hello").append("$lte", "world"))
                .append(COL2.getName(), new Document("$gte", "a value"));
        assertEquals(query, expected);
    }

    @Test
    public void testBuildQueryIn()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL2, Domain.create(ValueSet.ofRanges(equal(createUnboundedVarcharType(), utf8Slice("hello")), equal(createUnboundedVarcharType(), utf8Slice("world"))), false)));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document(COL2.getName(), new Document("$in", ImmutableList.of("hello", "world")));
        assertEquals(query, expected);
    }

    @Test
    public void testBuildQueryOr()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL1, Domain.create(ValueSet.ofRanges(lessThan(BIGINT, 100L), greaterThan(BIGINT, 200L)), false)));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document("$or", asList(
                new Document(COL1.getName(), new Document("$lt", 100L)),
                new Document(COL1.getName(), new Document("$gt", 200L))));
        assertEquals(query, expected);
    }

    @Test
    public void testBuildQueryNull()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL1, Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, 200L)), true)));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document("$or", asList(
                new Document(COL1.getName(), new Document("$gt", 200L)),
                new Document(COL1.getName(), new Document("$eq", null))));
        assertEquals(query, expected);
    }

    @Test
    public void testBooleanPredicatePushdown()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(COL4, Domain.singleValue(BOOLEAN, true)));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document().append(COL4.getName(), new Document("$eq", true));
        assertEquals(query, expected);
    }
}

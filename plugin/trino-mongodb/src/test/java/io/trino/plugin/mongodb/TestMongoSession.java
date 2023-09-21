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
import io.trino.spi.type.Type;
import org.bson.Document;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.mongodb.MongoSession.projectSufficientColumns;
import static io.trino.spi.predicate.Range.equal;
import static io.trino.spi.predicate.Range.greaterThan;
import static io.trino.spi.predicate.Range.greaterThanOrEqual;
import static io.trino.spi.predicate.Range.lessThan;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestMongoSession
{
    private static final MongoColumnHandle COL1 = createColumnHandle("col1", BIGINT);
    private static final MongoColumnHandle COL2 = createColumnHandle("col2", createUnboundedVarcharType());
    private static final MongoColumnHandle COL3 = createColumnHandle("col3", createUnboundedVarcharType());
    private static final MongoColumnHandle COL4 = createColumnHandle("col4", BOOLEAN);
    private static final MongoColumnHandle COL5 = createColumnHandle("col5", BIGINT);
    private static final MongoColumnHandle COL6 = createColumnHandle("grandparent", createUnboundedVarcharType(), "parent", "col6");

    private static final MongoColumnHandle ID_COL = new MongoColumnHandle("_id", ImmutableList.of(), ObjectIdType.OBJECT_ID, false, false, Optional.empty());

    @Test
    public void testBuildProjectionWithoutId()
    {
        List<MongoColumnHandle> columns = ImmutableList.of(COL1, COL2);

        Document output = MongoSession.buildProjection(columns);
        Document expected = new Document()
                .append(COL1.getBaseName(), 1)
                .append(COL2.getBaseName(), 1)
                .append(ID_COL.getBaseName(), 0);
        assertEquals(output, expected);
    }

    @Test
    public void testBuildProjectionWithId()
    {
        List<MongoColumnHandle> columns = ImmutableList.of(COL1, COL2, ID_COL);

        Document output = MongoSession.buildProjection(columns);
        Document expected = new Document()
                .append(COL1.getBaseName(), 1)
                .append(COL2.getBaseName(), 1)
                .append(ID_COL.getBaseName(), 1);
        assertEquals(output, expected);
    }

    @Test
    public void testBuildQuery()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL1, Domain.create(ValueSet.ofRanges(range(BIGINT, 100L, false, 200L, true)), false),
                COL2, Domain.singleValue(createUnboundedVarcharType(), utf8Slice("a value"))));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document()
                .append(COL1.getBaseName(), new Document().append("$gt", 100L).append("$lte", 200L))
                .append(COL2.getBaseName(), new Document("$eq", "a value"));
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
                .append(COL3.getBaseName(), new Document().append("$gt", "hello").append("$lte", "world"))
                .append(COL2.getBaseName(), new Document("$gte", "a value"));
        assertEquals(query, expected);
    }

    @Test
    public void testBuildQueryIn()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL2, Domain.create(ValueSet.ofRanges(equal(createUnboundedVarcharType(), utf8Slice("hello")), equal(createUnboundedVarcharType(), utf8Slice("world"))), false)));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document(COL2.getBaseName(), new Document("$in", ImmutableList.of("hello", "world")));
        assertEquals(query, expected);
    }

    @Test
    public void testBuildQueryOr()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL1, Domain.create(ValueSet.ofRanges(lessThan(BIGINT, 100L), greaterThan(BIGINT, 200L)), false)));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document("$or", asList(
                new Document(COL1.getBaseName(), new Document("$lt", 100L)),
                new Document(COL1.getBaseName(), new Document("$gt", 200L))));
        assertEquals(query, expected);
    }

    @Test
    public void testBuildQueryNull()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL1, Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, 200L)), true)));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document("$or", asList(
                new Document(COL1.getBaseName(), new Document("$gt", 200L)),
                new Document(COL1.getBaseName(), new Document("$eq", null))));
        assertEquals(query, expected);
    }

    @Test
    public void testBooleanPredicatePushdown()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(COL4, Domain.singleValue(BOOLEAN, true)));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document().append(COL4.getBaseName(), new Document("$eq", true));
        assertEquals(query, expected);
    }

    @Test
    public void testBuildQueryNestedField()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                COL5, Domain.create(ValueSet.ofRanges(greaterThan(BIGINT, 200L)), true),
                COL6, Domain.singleValue(createUnboundedVarcharType(), utf8Slice("a value"))));

        Document query = MongoSession.buildQuery(tupleDomain);
        Document expected = new Document()
                .append("$or", asList(
                        new Document(COL5.getQualifiedName(), new Document("$gt", 200L)),
                        new Document(COL5.getQualifiedName(), new Document("$eq", null))))
                .append(COL6.getQualifiedName(), new Document("$eq", "a value"));
        assertEquals(query, expected);
    }

    @Test
    public void testProjectSufficientColumns()
    {
        MongoColumnHandle col1 = createColumnHandle("x", BIGINT, "a", "b");
        MongoColumnHandle col2 = createColumnHandle("x", BIGINT, "b");
        MongoColumnHandle col3 = createColumnHandle("x", BIGINT, "c");
        MongoColumnHandle col4 = createColumnHandle("x", BIGINT);

        List<MongoColumnHandle> output = projectSufficientColumns(ImmutableList
                .of(col1, col2, col4));
        assertThat(output)
                .containsExactly(col4)
                .hasSize(1);

        output = projectSufficientColumns(ImmutableList.of(col4, col2, col1));
        assertThat(output)
                .containsExactly(col4)
                .hasSize(1);

        output = projectSufficientColumns(ImmutableList.of(col2, col1, col4));
        assertThat(output)
                .containsExactly(col4)
                .hasSize(1);

        output = projectSufficientColumns(ImmutableList.of(col2, col3));
        assertThat(output)
                .containsExactly(col2, col3)
                .hasSize(2);

        MongoColumnHandle col5 = createColumnHandle("x", BIGINT, "a", "b", "c");
        MongoColumnHandle col6 = createColumnHandle("x", BIGINT, "a", "c", "b");
        MongoColumnHandle col7 = createColumnHandle("x", BIGINT, "c", "a", "b");
        MongoColumnHandle col8 = createColumnHandle("x", BIGINT, "b", "a");
        MongoColumnHandle col9 = createColumnHandle("x", BIGINT);

        output = projectSufficientColumns(ImmutableList
                .of(col5, col6));
        assertThat(output)
                .containsExactly(col5, col6)
                .hasSize(2);

        output = projectSufficientColumns(ImmutableList
                .of(col6, col7));
        assertThat(output)
                .containsExactly(col6, col7)
                .hasSize(2);

        output = projectSufficientColumns(ImmutableList
                .of(col5, col8));
        assertThat(output)
                .containsExactly(col8, col5)
                .hasSize(2);

        output = projectSufficientColumns(ImmutableList
                .of(col5, col6, col7, col8, col9));
        assertThat(output)
                .containsExactly(col9)
                .hasSize(1);
    }

    private static MongoColumnHandle createColumnHandle(String baseName, Type type, String... dereferenceNames)
    {
        return new MongoColumnHandle(
                baseName,
                ImmutableList.copyOf(dereferenceNames),
                type,
                false,
                false,
                Optional.empty());
    }
}

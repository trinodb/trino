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
package io.trino.plugin.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.json.ObjectMapperProvider;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.elasticsearch.client.IndexMetadata;
import io.trino.plugin.elasticsearch.decoders.BigintDecoder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.function.OperatorType;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.BasePushdownPlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.testing.PlanTester;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestHighLevelClient;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Predicates.equalTo;
import static com.google.common.io.Resources.getResource;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.elasticsearch.ElasticsearchServer.ELASTICSEARCH_8_IMAGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.planner.assertions.PlanMatchPattern.any;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestElasticsearchProjectionPushdownPlans
        extends BasePushdownPlanTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();
    private static final String CATALOG = "elasticsearch";
    private static final String SCHEMA = "test";
    public static final String USER = "elastic_user";
    public static final String PASSWORD = "123456";

    private ElasticsearchServer elasticsearch;
    private RestHighLevelClient client;

    @Override
    protected PlanTester createPlanTester()
    {
        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(SCHEMA)
                .build();

        PlanTester planTester = PlanTester.create(session);

        try {
            elasticsearch = new ElasticsearchServer(ELASTICSEARCH_8_IMAGE);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        HostAndPort address = elasticsearch.getAddress();
        client = elasticsearch.getClient();

        try {
            planTester.installPlugin(new ElasticsearchPlugin());
            planTester.createCatalog(
                    CATALOG,
                    "elasticsearch",
                    ImmutableMap.<String, String>builder()
                            .put("elasticsearch.host", address.getHost())
                            .put("elasticsearch.port", Integer.toString(address.getPort()))
                            .put("elasticsearch.ignore-publish-address", "true")
                            .put("elasticsearch.default-schema-name", SCHEMA)
                            .put("elasticsearch.scroll-size", "1000")
                            .put("elasticsearch.scroll-timeout", "1m")
                            .put("elasticsearch.request-timeout", "2m")
                            .put("elasticsearch.tls.enabled", "true")
                            .put("elasticsearch.tls.truststore-path", new File(getResource("truststore.jks").toURI()).getPath())
                            .put("elasticsearch.tls.truststore-password", "123456")
                            .put("elasticsearch.tls.verify-hostnames", "false")
                            .put("elasticsearch.security", "PASSWORD")
                            .put("elasticsearch.auth.user", USER)
                            .put("elasticsearch.auth.password", PASSWORD)
                            .buildOrThrow());
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        catch (Throwable e) {
            closeAllSuppress(e, planTester);
            throw e;
        }
        return planTester;
    }

    @AfterAll
    void destroy()
            throws Exception
    {
        elasticsearch.close();
        elasticsearch = null;
        client.close();
        client = null;
    }

    @Test
    void testDereferencePushdown()
            throws IOException
    {
        String tableName = "test_simple_projection_pushdown" + randomNameSuffix();
        QualifiedObjectName completeTableName = new QualifiedObjectName(CATALOG, SCHEMA, tableName);

        index(tableName, ImmutableMap.<String, Object>builder()
                .put("col0", ImmutableMap.<String, Object>builder()
                        .put("x", 5L)
                        .put("y", 6L)
                        .buildOrThrow())
                .put("col1", 5L)
                .buildOrThrow());

        Session session = getPlanTester().getDefaultSession();

        Optional<TableHandle> tableHandle = getTableHandle(session, completeTableName);
        assertThat(tableHandle).as("expected the table handle to be present").isPresent();

        ElasticsearchTableHandle elasticsearchTableHandle = (ElasticsearchTableHandle) tableHandle.get().connectorHandle();
        Map<String, ColumnHandle> columns = getColumnHandles(session, completeTableName);

        ElasticsearchColumnHandle column0Handle = (ElasticsearchColumnHandle) columns.get("col0");
        ElasticsearchColumnHandle column1Handle = (ElasticsearchColumnHandle) columns.get("col1");

        ElasticsearchColumnHandle columnX = projectColumn(ImmutableList.of(column0Handle.path().getFirst(), "x"), BIGINT, new IndexMetadata.PrimitiveType("long"), new BigintDecoder.Descriptor("col0.x"), true);
        ElasticsearchColumnHandle columnY = projectColumn(ImmutableList.of(column0Handle.path().getFirst(), "y"), BIGINT, new IndexMetadata.PrimitiveType("long"), new BigintDecoder.Descriptor("col0.y"), true);

        // Simple Projection pushdown
        assertPlan(
                "SELECT col0.x expr_x, col0.y expr_y FROM " + tableName,
                any(
                        tableScan(
                                equalTo(elasticsearchTableHandle.withColumns(Set.of(columnX, columnY))),
                                TupleDomain.all(),
                                ImmutableMap.of("col0.x", equalTo(columnX), "col0.y", equalTo(columnY)))));

        // Projection and predicate pushdown
        assertPlan(
                "SELECT col0.x FROM " + tableName + " WHERE col0.x = col1 + 3 and col0.y = 2",
                anyTree(
                        filter(
                                new Comparison(EQUAL, new Reference(BIGINT, "x"), new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "col1"), new Constant(BIGINT, 3L)))),
                                tableScan(
                                        table -> {
                                            ElasticsearchTableHandle actualTableHandle = (ElasticsearchTableHandle) table;
                                            TupleDomain<ColumnHandle> constraint = actualTableHandle.constraint();
                                            return actualTableHandle.columns().equals(ImmutableSet.of(column1Handle, columnX))
                                                    && constraint.equals(TupleDomain.withColumnDomains(ImmutableMap.of(columnY, Domain.singleValue(BIGINT, 2L))));
                                        },
                                        TupleDomain.all(),
                                        ImmutableMap.of("col1", equalTo(column1Handle), "x", equalTo(columnX))))));

        // Projection and predicate pushdown with overlapping columns
        assertPlan(
                "SELECT col0, col0.y expr_y FROM " + tableName + " WHERE col0.x = 5",
                anyTree(
                        tableScan(
                                table -> {
                                    ElasticsearchTableHandle actualTableHandle = (ElasticsearchTableHandle) table;
                                    TupleDomain<ColumnHandle> constraint = actualTableHandle.constraint();
                                    return actualTableHandle.columns().equals(ImmutableSet.of(column0Handle, columnY))
                                            && constraint.equals(TupleDomain.withColumnDomains(ImmutableMap.of(columnX, Domain.singleValue(BIGINT, 5L))));
                                },
                                TupleDomain.all(),
                                ImmutableMap.of("col0", equalTo(column0Handle), "y", equalTo(columnY)))));

        // Projection and predicate pushdown with joins
        assertPlan(
                "SELECT T.col0.x, T.col0, T.col0.y FROM " + tableName + " T join " + tableName + " S on T.col1 = S.col1 WHERE T.col0.x = 2",
                anyTree(
                        project(
                                ImmutableMap.of(
                                        "expr_0_x", expression(new FieldReference(new Reference(RowType.anonymousRow(INTEGER), "expr_0"), 0)),
                                        "expr_0", expression(new Reference(RowType.anonymousRow(INTEGER), "expr_0")),
                                        "expr_0_y", expression(new FieldReference(new Reference(RowType.anonymousRow(INTEGER, INTEGER), "expr_0"), 1))),
                                PlanMatchPattern.join(INNER, builder -> builder
                                        .equiCriteria("t_expr_1", "s_expr_1")
                                        .left(
                                                anyTree(
                                                        tableScan(
                                                                table -> {
                                                                    ElasticsearchTableHandle actualTableHandle = (ElasticsearchTableHandle) table;
                                                                    TupleDomain<ColumnHandle> constraint = actualTableHandle.constraint();
                                                                    Set<ElasticsearchColumnHandle> expectedProjections = ImmutableSet.of(column0Handle, column1Handle);
                                                                    TupleDomain<ElasticsearchColumnHandle> expectedConstraint = TupleDomain.withColumnDomains(
                                                                            ImmutableMap.of(columnX, Domain.singleValue(BIGINT, 2L)));
                                                                    return actualTableHandle.columns().equals(expectedProjections)
                                                                            && constraint.equals(expectedConstraint);
                                                                },
                                                                TupleDomain.all(),
                                                                ImmutableMap.of("expr_0", equalTo(column0Handle), "t_expr_1", equalTo(column1Handle)))))
                                        .right(
                                                anyTree(
                                                        tableScan(
                                                                equalTo(elasticsearchTableHandle.withColumns(Set.of(column1Handle))),
                                                                TupleDomain.all(),
                                                                ImmutableMap.of("s_expr_1", equalTo(column1Handle)))))))));
        deleteIndex(tableName);
    }

    private static ElasticsearchColumnHandle projectColumn(List<String> path, Type projectedColumnType, IndexMetadata.Type elasticsearchType, DecoderDescriptor decoderDescriptor, boolean supportsPredicates)
    {
        return new ElasticsearchColumnHandle(
                path,
                projectedColumnType,
                elasticsearchType,
                decoderDescriptor,
                supportsPredicates);
    }

    private void createIndex(String indexName, @Language("JSON") String properties)
            throws IOException
    {
        String mappings = indexMapping(properties);
        Request request = new Request("PUT", "/" + indexName);
        request.setJsonEntity(mappings);
        client.getLowLevelClient().performRequest(request);
    }

    private static String indexMapping(@Language("JSON") String properties)
    {
        return "{\"mappings\": " + properties + "}";
    }

    private void index(String index, Map<String, Object> document)
            throws IOException
    {
        String json = OBJECT_MAPPER.writeValueAsString(document);
        String endpoint = format("%s?refresh", indexEndpoint(index, String.valueOf(System.nanoTime())));

        Request request = new Request("PUT", endpoint);
        request.setJsonEntity(json);
        client.getLowLevelClient().performRequest(request);
    }

    private static String indexEndpoint(String index, String docId)
    {
        return format("/%s/_doc/%s", index, docId);
    }

    private void deleteIndex(String indexName)
            throws IOException
    {
        Request request = new Request("DELETE", "/" + indexName);
        client.getLowLevelClient().performRequest(request);
    }
}

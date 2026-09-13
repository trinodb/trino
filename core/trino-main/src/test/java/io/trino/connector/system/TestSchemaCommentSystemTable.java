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
package io.trino.connector.system;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestSchemaCommentSystemTable
{
    private static final Map<String, Optional<String>> COMMENTS = ImmutableMap.of(
            "described", Optional.of("Schema's description\nwith a second line"),
            "empty", Optional.of(""),
            "uncommented", Optional.empty());

    private final CommentMetadata comments = new CommentMetadata(ImmutableList.copyOf(COMMENTS.keySet()), COMMENTS::get);
    private final CommentMetadata otherComments = new CommentMetadata(ImmutableList.copyOf(COMMENTS.keySet()), COMMENTS::get);
    private final ListingMetadata unsupported = new ListingMetadata(ImmutableList.of("first", "second"));
    private final CommentMetadata manySchemas = new CommentMetadata(
            IntStream.range(0, 1000).mapToObj("schema_%04d"::formatted).collect(toImmutableList()),
            schema -> Optional.of("Comment for " + schema));
    private final CommentMetadata failures = new CommentMetadata(
            ImmutableList.of("dropped", "dropped_error_code", "dropped_before_lookup", "retained", "backend_failure", "permission_failure", "programming_failure", "generic_not_found", "existence_failure"),
            schema -> switch (schema) {
                case "dropped" -> throw new SchemaNotFoundException(schema);
                case "dropped_error_code" -> throw new TrinoException(SCHEMA_NOT_FOUND, "Schema no longer exists");
                case "backend_failure" -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Cannot read schema comment");
                case "permission_failure" -> throw new TrinoException(PERMISSION_DENIED, "Cannot read schema comment");
                case "programming_failure" -> throw new IllegalStateException("Cannot read schema comment");
                case "generic_not_found" -> throw new TrinoException(NOT_FOUND, "Cannot read schema comment");
                default -> Optional.of("Retained comment");
            })
    {
        @Override
        public boolean schemaExists(ConnectorSession session, String schemaName)
        {
            return switch (schemaName) {
                case "dropped_before_lookup" -> false;
                case "existence_failure" -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Cannot check schema existence");
                default -> super.schemaExists(session, schemaName);
            };
        }
    };

    private QueryRunner queryRunner;

    @BeforeAll
    void setUp()
    {
        queryRunner = new StandaloneQueryRunner(testSessionBuilder().build());
        installCatalog("comments", comments);
        installCatalog("other_comments", otherComments);
        installCatalog("unsupported", unsupported);
        installCatalog("many_schemas", manySchemas);
        installCatalog("failures", failures);
        installCatalog("broken_listing", new ConnectorMetadata()
        {
            @Override
            public List<String> listSchemaNames(ConnectorSession session)
            {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Cannot list schemas");
            }
        });
    }

    private void installCatalog(String catalog, ConnectorMetadata metadata)
    {
        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withName(catalog)
                .withMetadataWrapper(_ -> metadata)
                .build()));
        queryRunner.createCatalog(catalog, catalog, ImmutableMap.of());
    }

    @BeforeEach
    void reset()
    {
        queryRunner.getAccessControl().reset();
        for (ListingMetadata metadata : ImmutableList.of(comments, otherComments, unsupported, manySchemas, failures)) {
            metadata.listings.set(0);
        }
        for (CommentMetadata metadata : ImmutableList.of(comments, otherComments, manySchemas, failures)) {
            metadata.lookups.clear();
        }
    }

    @AfterAll
    void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }

    @Test
    void testComments()
    {
        assertThat(queryRunner.execute("SELECT catalog_name, schema_name, comment FROM system.metadata.schema_comments WHERE catalog_name = 'comments'").getMaterializedRows())
                .containsExactlyInAnyOrderElementsOf(resultBuilder(queryRunner.getDefaultSession(), VARCHAR, VARCHAR, VARCHAR)
                        .row("comments", "described", COMMENTS.get("described").orElseThrow())
                        .row("comments", "empty", "")
                        .row("comments", "uncommented", null)
                        .row("comments", "information_schema", null)
                        .build()
                        .getMaterializedRows());
        assertThat(comments.listings).hasValue(4);
        assertThat(comments.lookups).containsExactlyInAnyOrderElementsOf(COMMENTS.keySet());
        assertThat(otherComments.listings).hasValue(0);
    }

    @Test
    void testExactSchemaFilter()
    {
        assertThat(queryRunner.execute("SELECT comment FROM system.metadata.schema_comments WHERE catalog_name = 'comments' AND schema_name = 'described'").getOnlyValue())
                .isEqualTo(COMMENTS.get("described").orElseThrow());
        assertThat(comments.listings).hasValue(2);
        assertThat(comments.lookups).containsExactly("described");
    }

    @Test
    void testMultipleSchemaFilter()
    {
        assertThat(queryRunner.execute("SELECT count(*) FROM system.metadata.schema_comments WHERE catalog_name = 'comments' AND schema_name IN ('empty', 'uncommented', 'missing')").getOnlyValue())
                .isEqualTo(2L);
        assertThat(comments.listings).hasValue(3);
        assertThat(comments.lookups).containsExactlyInAnyOrder("empty", "uncommented");
    }

    @Test
    void testSchemaRangeFilter()
    {
        assertThat(queryRunner.execute("SELECT schema_name FROM system.metadata.schema_comments WHERE catalog_name = 'comments' AND schema_name > 'described' AND schema_name < 'information_schema'").getOnlyValue())
                .isEqualTo("empty");
        assertThat(comments.listings).hasValue(2);
        assertThat(comments.lookups).containsExactly("empty");
    }

    @Test
    void testMissingSchema()
    {
        assertThat(queryRunner.execute("SELECT comment FROM system.metadata.schema_comments WHERE catalog_name = 'comments' AND schema_name = 'missing'").getRowCount())
                .isZero();
        assertThat(comments.listings).hasValue(1);
        assertThat(comments.lookups).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "catalog_name = ''",
            "catalog_name = 'missing'",
            "schema_name = ''",
            "schema_name = 'UPPERCASE'",
            "schema_name IS NULL",
            "catalog_name = 'comments' AND schema_name = 'empty' AND schema_name = 'described'",
            "false",
    })
    void testEmptyResultsDoNotReadSchemas(String predicate)
    {
        assertThat(queryRunner.execute("SELECT comment FROM system.metadata.schema_comments WHERE " + predicate).getRowCount())
                .isZero();
        assertThat(comments.listings).hasValue(0);
        assertThat(otherComments.listings).hasValue(0);
        assertThat(comments.lookups).isEmpty();
        assertThat(otherComments.lookups).isEmpty();
    }

    @Test
    void testSchemaNamesAreCatalogScoped()
    {
        assertThat(queryRunner.execute("SELECT catalog_name, schema_name FROM system.metadata.schema_comments WHERE catalog_name IN ('comments', 'other_comments') AND schema_name = 'described'").getMaterializedRows())
                .containsExactlyInAnyOrderElementsOf(resultBuilder(queryRunner.getDefaultSession(), VARCHAR, VARCHAR)
                        .row("comments", "described")
                        .row("other_comments", "described")
                        .build()
                        .getMaterializedRows());
        assertThat(comments.listings).hasValue(2);
        assertThat(otherComments.listings).hasValue(2);
        assertThat(comments.lookups).containsExactly("described");
        assertThat(otherComments.lookups).containsExactly("described");
    }

    @Test
    void testHiddenCatalog()
    {
        queryRunner.getAccessControl().denyCatalogs(catalog -> !catalog.equals("other_comments"));
        assertThat(queryRunner.execute("SELECT catalog_name FROM system.metadata.schema_comments WHERE catalog_name IN ('comments', 'other_comments') AND schema_name = 'described'").getOnlyValue())
                .isEqualTo("comments");
        assertThat(otherComments.listings).hasValue(0);
        assertThat(otherComments.lookups).isEmpty();
    }

    @Test
    void testHiddenSchema()
    {
        queryRunner.getAccessControl().denySchemas(schema -> !schema.equals("described"));
        assertThat(queryRunner.execute("SELECT count(*) FROM system.metadata.schema_comments WHERE catalog_name = 'comments' AND schema_name IN ('described', 'empty')").getOnlyValue())
                .isEqualTo(1L);
        assertThat(comments.listings).hasValue(2);
        assertThat(comments.lookups).containsExactly("empty");
    }

    @Test
    void testUnsupportedConnector()
    {
        assertThat(queryRunner.execute("SELECT schema_name, comment FROM system.metadata.schema_comments WHERE catalog_name = 'unsupported'").getMaterializedRows())
                .containsExactlyInAnyOrderElementsOf(resultBuilder(queryRunner.getDefaultSession(), VARCHAR, VARCHAR)
                        .row("first", null)
                        .row("second", null)
                        .row("information_schema", null)
                        .build()
                        .getMaterializedRows());
        assertThat(unsupported.listings).hasValue(3);
    }

    @Test
    void testDroppedSchema()
    {
        assertThat(queryRunner.execute("SELECT schema_name, comment FROM system.metadata.schema_comments WHERE catalog_name = 'failures' AND schema_name IN ('dropped', 'dropped_error_code', 'dropped_before_lookup', 'retained')").getMaterializedRows())
                .containsExactlyElementsOf(resultBuilder(queryRunner.getDefaultSession(), VARCHAR, VARCHAR)
                        .row("retained", "Retained comment")
                        .build()
                        .getMaterializedRows());
        assertThat(failures.lookups).containsExactlyInAnyOrder("dropped", "dropped_error_code", "retained");
    }

    @ParameterizedTest
    @ValueSource(strings = {"backend_failure", "permission_failure", "programming_failure", "generic_not_found"})
    void testCommentFailure(String schema)
    {
        assertThatThrownBy(() -> queryRunner.execute("SELECT comment FROM system.metadata.schema_comments WHERE catalog_name = 'failures' AND schema_name = '%s'".formatted(schema)))
                .hasMessageContaining("Cannot read schema comment");
    }

    @Test
    void testListingFailure()
    {
        assertThatThrownBy(() -> queryRunner.execute("SELECT comment FROM system.metadata.schema_comments WHERE catalog_name = 'broken_listing'"))
                .hasMessageContaining("Cannot list schemas");
    }

    @Test
    void testSchemaExistenceFailure()
    {
        assertThatThrownBy(() -> queryRunner.execute("SELECT comment FROM system.metadata.schema_comments WHERE catalog_name = 'failures' AND schema_name = 'existence_failure'"))
                .hasMessageContaining("Cannot check schema existence");
        assertThat(failures.lookups).isEmpty();
    }

    @Test
    void testManySchemas()
    {
        assertThat(queryRunner.execute("SELECT count(*), count(comment) FROM system.metadata.schema_comments WHERE catalog_name = 'many_schemas'").getMaterializedRows())
                .containsExactlyElementsOf(resultBuilder(queryRunner.getDefaultSession(), BIGINT, BIGINT)
                        .row(1001L, 1000L)
                        .build()
                        .getMaterializedRows());
        // The default schemaExists implementation lists schemas for every comment lookup.
        assertThat(manySchemas.listings).hasValue(1001);
        assertThat(manySchemas.lookups).hasSize(1000).doesNotHaveDuplicates();
    }

    @Test
    void testExactSchemaInLargeCatalog()
    {
        assertThat(queryRunner.execute("SELECT comment FROM system.metadata.schema_comments WHERE catalog_name = 'many_schemas' AND schema_name = 'schema_0500'").getOnlyValue())
                .isEqualTo("Comment for schema_0500");
        assertThat(manySchemas.listings).hasValue(2);
        assertThat(manySchemas.lookups).containsExactly("schema_0500");
    }

    private static class ListingMetadata
            implements ConnectorMetadata
    {
        private final List<String> schemas;
        final AtomicInteger listings = new AtomicInteger();

        public ListingMetadata(List<String> schemas)
        {
            this.schemas = ImmutableList.copyOf(schemas);
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session)
        {
            listings.incrementAndGet();
            return schemas;
        }
    }

    private static class CommentMetadata
            extends ListingMetadata
    {
        private final Function<String, Optional<String>> comments;
        private final ConcurrentLinkedQueue<String> lookups = new ConcurrentLinkedQueue<>();

        public CommentMetadata(List<String> schemas, Function<String, Optional<String>> comments)
        {
            super(schemas);
            this.comments = comments;
        }

        @Override
        public Optional<String> getSchemaComment(ConnectorSession session, String schemaName)
        {
            lookups.add(schemaName);
            return comments.apply(schemaName);
        }
    }
}

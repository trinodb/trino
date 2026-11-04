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
package io.trino.connector.informationschema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SourcePage;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.connector.informationschema.InformationSchemaMetadata.defaultPrefixes;
import static io.trino.connector.informationschema.InformationSchemaTable.SCHEMATA;
import static io.trino.testing.TransactionBuilder.transaction;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestInformationSchemaPageSource
{
    private static final int SCHEMA_COUNT = 2000;
    private static final int SCHEMA_NAME_LENGTH = 4096;
    // the names fill several pages
    private static final List<String> SCHEMA_NAMES = IntStream.range(0, SCHEMA_COUNT)
            .mapToObj(i -> "schema_%04d_%s".formatted(i, "x".repeat(SCHEMA_NAME_LENGTH)))
            .collect(toImmutableList());
    private static final long EMPTY_BUILDER_BYTES = new PageBuilder(SCHEMATA.getTableMetadata().getColumns().stream()
            .map(ColumnMetadata::getType)
            .collect(toImmutableList()))
            .getRetainedSizeInBytes();

    private QueryRunner queryRunner;

    @BeforeAll
    void setUp()
    {
        queryRunner = new StandaloneQueryRunner(TEST_SESSION);
        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withListSchemaNames(_ -> SCHEMA_NAMES)
                .build()));
        queryRunner.createCatalog("test_catalog", "mock", ImmutableMap.of());
    }

    @AfterAll
    void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test
    void testReportsMemoryUsageToMemoryContext()
    {
        transaction(queryRunner.getTransactionManager(), queryRunner.getPlannerContext().getMetadata(), queryRunner.getAccessControl())
                .execute(TEST_SESSION, session -> {
                    AtomicLong reportedBytes = new AtomicLong(-1);
                    AtomicLong maxReportedBytes = new AtomicLong();
                    List<ColumnHandle> columns = ImmutableList.of(new InformationSchemaColumnHandle("schema_name"));
                    InformationSchemaPageSource pageSource = new InformationSchemaPageSource(
                            session,
                            queryRunner.getPlannerContext().getMetadata(),
                            queryRunner.getAccessControl(),
                            new InformationSchemaTableHandle("test_catalog", SCHEMATA, defaultPrefixes("test_catalog"), OptionalLong.empty()),
                            columns,
                            bytes -> {
                                reportedBytes.set(bytes);
                                maxReportedBytes.accumulateAndGet(bytes, Math::max);
                            });
                    assertThat(reportedBytes).hasValue(EMPTY_BUILDER_BYTES);

                    int pages = 0;
                    int positions = 0;
                    while (!pageSource.isFinished()) {
                        SourcePage page = pageSource.getNextSourcePage();
                        if (page != null) {
                            pages++;
                            positions += page.getPositionCount();
                        }
                    }
                    assertThat(positions).isEqualTo(SCHEMA_COUNT + 1); // includes information_schema
                    assertThat(pages).isGreaterThan(2);
                    // all pages of the single prefix are buffered before the first one is returned
                    assertThat(maxReportedBytes.get()).isGreaterThan((long) SCHEMA_COUNT * SCHEMA_NAME_LENGTH);
                    // returned pages are no longer accounted
                    assertThat(reportedBytes).hasValue(EMPTY_BUILDER_BYTES);
                    assertThat(pageSource.getMemoryUsage()).isZero();

                    pageSource.close();
                    // the page builder is still retained; its reservation is released by the context's owner
                    assertThat(reportedBytes).hasValue(EMPTY_BUILDER_BYTES);
                });
    }

    @Test
    void testReleasesBufferedPagesOnClose()
    {
        transaction(queryRunner.getTransactionManager(), queryRunner.getPlannerContext().getMetadata(), queryRunner.getAccessControl())
                .execute(TEST_SESSION, session -> {
                    AtomicLong reportedBytes = new AtomicLong(-1);
                    InformationSchemaPageSource pageSource = new InformationSchemaPageSource(
                            session,
                            queryRunner.getPlannerContext().getMetadata(),
                            queryRunner.getAccessControl(),
                            new InformationSchemaTableHandle("test_catalog", SCHEMATA, defaultPrefixes("test_catalog"), OptionalLong.empty()),
                            ImmutableList.of(new InformationSchemaColumnHandle("schema_name")),
                            reportedBytes::set);

                    assertThat(pageSource.getNextSourcePage()).isNotNull();
                    // the remaining pages are buffered
                    assertThat(reportedBytes.get()).isGreaterThan(EMPTY_BUILDER_BYTES + (long) SCHEMA_COUNT * SCHEMA_NAME_LENGTH / 2);

                    pageSource.close();
                    assertThat(reportedBytes).hasValue(EMPTY_BUILDER_BYTES);
                    assertThat(pageSource.isFinished()).isTrue();
                });
    }
}

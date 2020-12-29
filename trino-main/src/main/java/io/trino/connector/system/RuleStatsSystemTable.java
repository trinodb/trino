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
package io.prestosql.connector.system;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedPageSource;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.sql.planner.RuleStatsRecorder;
import io.prestosql.sql.planner.iterative.RuleStats;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.prestosql.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.TypeSignature.mapType;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class RuleStatsSystemTable
        implements SystemTable
{
    private static final SchemaTableName TABLE_NAME = new SchemaTableName("runtime", "optimizer_rule_stats");
    private final ConnectorTableMetadata ruleStatsTable;
    private final Optional<RuleStatsRecorder> ruleStatsRecorder;

    @Inject
    public RuleStatsSystemTable(Optional<RuleStatsRecorder> ruleStatsRecorder, Metadata metadata)
    {
        this.ruleStatsRecorder = requireNonNull(ruleStatsRecorder, "ruleStatsRecorder is null");
        requireNonNull(metadata, "metadata is null");

        this.ruleStatsTable = tableMetadataBuilder(TABLE_NAME)
                .column("rule_name", VARCHAR)
                .column("invocations", BIGINT)
                .column("matches", BIGINT)
                .column("failures", BIGINT)
                .column("average_time", DOUBLE)
                .column("time_distribution_percentiles", metadata.getType(mapType(DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature())))
                .build();
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return ruleStatsTable;
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        checkState(ruleStatsRecorder.isPresent(), "Rule stats system table can return results only on coordinator");
        Map<Class<?>, RuleStats> ruleStats = ruleStatsRecorder.get().getStats();

        int positionCount = ruleStats.size();
        Map<String, BlockBuilder> blockBuilders = ruleStatsTable.getColumns().stream()
                .collect(toImmutableMap(ColumnMetadata::getName, column -> column.getType().createBlockBuilder(null, positionCount)));

        for (Map.Entry<Class<?>, RuleStats> entry : ruleStats.entrySet()) {
            RuleStats stats = entry.getValue();

            VARCHAR.writeString(blockBuilders.get("rule_name"), entry.getKey().getSimpleName());
            BIGINT.writeLong(blockBuilders.get("invocations"), stats.getInvocations());
            BIGINT.writeLong(blockBuilders.get("matches"), stats.getHits());
            BIGINT.writeLong(blockBuilders.get("failures"), stats.getFailures());
            DOUBLE.writeDouble(blockBuilders.get("average_time"), stats.getTime().getAvg());

            BlockBuilder mapWriter = blockBuilders.get("time_distribution_percentiles").beginBlockEntry();
            for (Map.Entry<Double, Double> percentile : stats.getTime().getPercentiles().entrySet()) {
                DOUBLE.writeDouble(mapWriter, percentile.getKey());
                DOUBLE.writeDouble(mapWriter, percentile.getValue());
            }
            blockBuilders.get("time_distribution_percentiles").closeEntry();
        }

        Block[] blocks = ruleStatsTable.getColumns().stream()
                .map(column -> blockBuilders.get(column.getName()).build())
                .toArray(Block[]::new);

        return new FixedPageSource(ImmutableList.of(new Page(positionCount, blocks)));
    }
}

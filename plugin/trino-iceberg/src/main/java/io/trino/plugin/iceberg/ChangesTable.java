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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.ChangelogUtil;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.UpdateStatistics;
import org.apache.iceberg.encryption.EncryptionManager;

import java.util.HashMap;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

// ChangesTable is for querying changes in between snapshots
// it only supports read-only methods from Table interface, including newIncrementalAppendScan and newIncrementalChangelogScan
// and does NOT support TableScan
public class ChangesTable
        extends BaseTable
{
    public ChangesTable(BaseTable baseTable)
    {
        super(baseTable.operations(), baseTable.name());
    }

    // Use iceberg core Changelog schema
    @Override
    public Schema schema()
    {
        return ChangelogUtil.changelogSchema(super.schema());
    }

    @Override
    public Map<Integer, Schema> schemas()
    {
        Map<Integer, Schema> schemas = new HashMap(super.schemas());
        schemas.replaceAll((key, value) -> ChangelogUtil.changelogSchema(value));
        return ImmutableMap.copyOf(schemas);
    }

    // Not supported methods
    @Override
    public TableScan newScan()
    {
        throw new TrinoException(NOT_SUPPORTED, "Method not supported by ChangesTable");
    }

    @Override
    public BatchScan newBatchScan()
    {
        throw new TrinoException(NOT_SUPPORTED, "Method not supported by ChangesTable");
    }

    @Override
    public UpdateSchema updateSchema()
    {
        throw new TrinoException(NOT_SUPPORTED, "Method not supported by ChangesTable");
    }

    @Override
    public UpdatePartitionSpec updateSpec()
    {
        throw new TrinoException(NOT_SUPPORTED, "Method not supported by ChangesTable");
    }

    @Override
    public UpdateProperties updateProperties()
    {
        throw new TrinoException(NOT_SUPPORTED, "Method not supported by ChangesTable");
    }

    @Override
    public ReplaceSortOrder replaceSortOrder()
    {
        throw new TrinoException(NOT_SUPPORTED, "Method not supported by ChangesTable");
    }

    @Override
    public UpdateLocation updateLocation()
    {
        throw new TrinoException(NOT_SUPPORTED, "Method not supported by ChangesTable");
    }

    @Override
    public AppendFiles newAppend()
    {
        throw new TrinoException(NOT_SUPPORTED, "Method not supported by ChangesTable");
    }

    @Override
    public AppendFiles newFastAppend()
    {
        throw new TrinoException(NOT_SUPPORTED, "Method not supported by ChangesTable");
    }

    @Override
    public RewriteFiles newRewrite()
    {
        throw new TrinoException(NOT_SUPPORTED, "Method not supported by ChangesTable");
    }

    @Override
    public RewriteManifests rewriteManifests()
    {
        throw new TrinoException(NOT_SUPPORTED, "Method not supported by ChangesTable");
    }

    @Override
    public OverwriteFiles newOverwrite()
    {
        throw new TrinoException(NOT_SUPPORTED, "Method not supported by ChangesTable");
    }

    @Override
    public RowDelta newRowDelta()
    {
        throw new TrinoException(NOT_SUPPORTED, "Method not supported by ChangesTable");
    }

    @Override
    public ReplacePartitions newReplacePartitions()
    {
        throw new TrinoException(NOT_SUPPORTED, "Method not supported by ChangesTable");
    }

    @Override
    public DeleteFiles newDelete()
    {
        throw new TrinoException(NOT_SUPPORTED, "Method not supported by ChangesTable");
    }

    @Override
    public UpdateStatistics updateStatistics()
    {
        throw new TrinoException(NOT_SUPPORTED, "Method not supported by ChangesTable");
    }

    @Override
    public ExpireSnapshots expireSnapshots()
    {
        throw new TrinoException(NOT_SUPPORTED, "Method not supported by ChangesTable");
    }

    @Override
    public ManageSnapshots manageSnapshots()
    {
        throw new TrinoException(NOT_SUPPORTED, "Method not supported by ChangesTable");
    }

    @Override
    public Transaction newTransaction()
    {
        throw new TrinoException(NOT_SUPPORTED, "Method not supported by ChangesTable");
    }

    @Override
    public EncryptionManager encryption()
    {
        throw new TrinoException(NOT_SUPPORTED, "Method not supported by ChangesTable");
    }
}

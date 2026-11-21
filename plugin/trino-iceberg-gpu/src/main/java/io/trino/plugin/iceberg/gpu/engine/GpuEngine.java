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
package io.trino.plugin.iceberg.gpu.engine;

import com.google.common.collect.ImmutableList;
import uk.ac.manchester.tornado.cudf.Cudf;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * Grouped aggregations over the Parquet data files of a table, on a GPU. The columns a table's
 * queries use stay resident on the device: one table at a time, its columns the union of those its
 * queries asked for.
 */
public final class GpuEngine
{
    /**
     * The data files of one table snapshot: {@code key} names the snapshot.
     */
    public record Table(String key, List<String> files, long rows, long stringBytesGuess)
    {
        public Table
        {
            requireNonNull(key, "key is null");
            files = ImmutableList.copyOf(files);
        }
    }

    /**
     * A query over a table's columns, by index into the column list given with it.
     *
     * @param predicates (column, operator) pairs; operators as in {@link DeviceTable}
     * @param constants one per predicate
     * @param keys group-key columns, or {@code {-1}} for a single group
     * @param factors four per value: a column, -1 for the constant 1, -2 for none
     */
    public record Query(int[] predicates, double[] constants, int[] keys, int[] factors, double[] a, double[] b) {}

    private static final Pattern STRING_BYTES_NEEDED = Pattern.compile("decodes to (\\d+) bytes, beyond");

    private static final GpuEngine INSTANCE = new GpuEngine();

    private String residentKey;
    private List<DeviceTable.Column> residentColumns = List.of();
    private DeviceTable resident;

    private GpuEngine() {}

    public static GpuEngine instance()
    {
        return INSTANCE;
    }

    /**
     * Whether TornadoVM and its cuDF tasks are available: without them no query is taken.
     */
    public static boolean isAvailable()
    {
        try {
            return Cudf.isParquetAvailable() && Cudf.isStringColumnsAvailable();
        }
        catch (LinkageError | RuntimeException e) {
            return false;
        }
    }

    public synchronized DeviceTable.Result aggregate(Table table, List<DeviceTable.Column> columns, Query query)
    {
        int[] map = ensureResident(table, columns);
        try {
            return run(query, map);
        }
        catch (RuntimeException failure) {
            // A string column decoded to more bytes than guessed: load again at the exact size.
            reloadAtNeededSize(table, failure);
            return run(query, map);
        }
    }

    /**
     * Whether every value of a string column fits in 8 bytes, and so can be a group key.
     */
    public synchronized boolean fits(Table table, DeviceTable.Column column)
    {
        int[] map = ensureResident(table, List.of(column));
        try {
            return resident.fits(map[0]);
        }
        catch (RuntimeException failure) {
            reloadAtNeededSize(table, failure);
            return resident.fits(map[0]);
        }
    }

    /**
     * Makes the table resident with these columns among its own; returns their indices in it.
     */
    private int[] ensureResident(Table table, List<DeviceTable.Column> columns)
    {
        List<DeviceTable.Column> wanted = new ArrayList<>(table.key().equals(residentKey) ? residentColumns : List.of());
        Set<DeviceTable.Column> have = new LinkedHashSet<>(wanted);
        int[] map = new int[columns.size()];
        for (int c = 0; c < columns.size(); c++) {
            if (have.add(columns.get(c))) {
                wanted.add(columns.get(c));
            }
            map[c] = wanted.indexOf(columns.get(c));
        }
        if (resident == null || !table.key().equals(residentKey) || wanted.size() != residentColumns.size()) {
            load(table, wanted, table.stringBytesGuess());
        }
        return map;
    }

    private void load(Table table, List<DeviceTable.Column> columns, long stringBytes)
    {
        if (resident != null) {
            resident.close();
            resident = null;
        }
        // Until the new table exists there is none: a failed load leaves nothing stale behind.
        residentKey = null;
        residentColumns = List.of();
        resident = new DeviceTable(table.files(), table.rows(), columns, stringBytes);
        residentKey = table.key();
        residentColumns = ImmutableList.copyOf(columns);
    }

    private void reloadAtNeededSize(Table table, RuntimeException failure)
    {
        long needed = stringBytesNeeded(failure);
        List<DeviceTable.Column> columns = residentColumns;
        if (resident != null) {
            resident.close();
            resident = null;
        }
        residentKey = null;
        residentColumns = List.of();
        if (needed < 0) {
            throw failure;
        }
        load(table, columns, needed + needed / 8);
    }

    private DeviceTable.Result run(Query query, int[] map)
    {
        return resident.aggregate(
                remap(query.predicates(), map, true),
                query.constants(),
                remap(query.keys(), map, false),
                remap(query.factors(), map, false),
                query.a(),
                query.b());
    }

    private static long stringBytesNeeded(Throwable failure)
    {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            Matcher m = STRING_BYTES_NEEDED.matcher(String.valueOf(t.getMessage()));
            if (m.find()) {
                return Long.parseLong(m.group(1));
            }
        }
        return -1;
    }

    /**
     * Column indices of the query mapped to the resident table's; negatives stay.
     */
    private static int[] remap(int[] values, int[] map, boolean pairs)
    {
        int[] out = values.clone();
        for (int i = 0; i < out.length; i += pairs ? 2 : 1) {
            if (out[i] >= 0) {
                out[i] = map[out[i]];
            }
        }
        return out;
    }
}

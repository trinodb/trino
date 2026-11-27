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

import uk.ac.manchester.tornado.api.GridScheduler;
import uk.ac.manchester.tornado.api.KernelContext;
import uk.ac.manchester.tornado.api.TaskGraph;
import uk.ac.manchester.tornado.api.TornadoExecutionPlan;
import uk.ac.manchester.tornado.api.TornadoExecutionResult;
import uk.ac.manchester.tornado.api.WorkerGrid1D;
import uk.ac.manchester.tornado.api.enums.DataTransferMode;
import uk.ac.manchester.tornado.api.types.arrays.ByteArray;
import uk.ac.manchester.tornado.api.types.arrays.DoubleArray;
import uk.ac.manchester.tornado.api.types.arrays.IntArray;
import uk.ac.manchester.tornado.api.types.arrays.LongArray;
import uk.ac.manchester.tornado.cudf.Cudf;
import uk.ac.manchester.tornado.cudf.enums.CudfAggregation;
import uk.ac.manchester.tornado.cudf.enums.CudfType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Columns of a set of Parquet files, decoded once into device memory, and one plan that filters,
 * projects and aggregates them by group for any query of that shape.
 *
 * <p>The first {@link #aggregate} reads the files, packs string columns into longs and gives each
 * a dictionary when it has few distinct values. Later queries read nothing. A query whose group
 * keys all have dictionaries runs on the dense path, one kernel and no intermediate columns; any
 * other runs on the hashed path, cuDF grouping chunks of rows.
 */
public final class DeviceTable
        implements AutoCloseable
{
    // Predicate operators
    public static final int EQ = Kernels.EQ;
    public static final int LT = Kernels.LT;
    public static final int LE = Kernels.LE;
    public static final int GT = Kernels.GT;
    public static final int GE = Kernels.GE;

    /**
     * A column of the table: how it is held.
     */
    public enum Kind
    {
        /**
         * INT32 or DATE.
         */
        INT,
        /**
         * INT64.
         */
        LONG,
        /**
         * DOUBLE.
         */
        DOUBLE,
        /**
         * A string of up to 8 bytes, packed into an INT64 that orders as the string does.
         */
        PACKED_STRING,
    }

    /**
     * A column to hold: the file's column index and its kind.
     */
    public record Column(int fileIndex, Kind kind) {}

    /**
     * A grouped aggregation's answer: per group, its keys and the sum of each value.
     */
    public record Result(int groups, long[][] keys, double[][] sums) {}

    public static final int MAX_PREDICATES = 16;
    public static final int MAX_KEYS = 4;
    /**
     * Value expressions a query may have; one more is kept for the rows a group counts.
     */
    public static final int MAX_VALUES = 7;
    public static final int MAX_GROUPS = 1 << 16;
    private static final int CHUNK = 1 << 22;

    private final List<String> files;
    private final List<Column> columns;
    private final long rows;
    private final int cap;
    private final int chunk;
    private final int strings;
    private final int firstString;
    private final int[] stringOf;
    private final StringBuilder path = new StringBuilder(1 << 16);
    private final long[] rowsHolder = new long[1];
    private final int[] groupSize = new int[3];
    private final IntArray params = new IntArray(Kernels.P_SIZE);
    private final IntArray pred = new IntArray(2 * MAX_PREDICATES);
    private final DoubleArray predConst = new DoubleArray(MAX_PREDICATES);
    private final IntArray keyCol = new IntArray(MAX_KEYS);
    private final IntArray keyString = new IntArray(MAX_KEYS);
    private final IntArray keyMult = new IntArray(MAX_KEYS);
    private final IntArray factorCol = new IntArray((MAX_VALUES + 1) * Kernels.FACTORS);
    private final DoubleArray factorA = new DoubleArray((MAX_VALUES + 1) * Kernels.FACTORS);
    private final DoubleArray factorB = new DoubleArray((MAX_VALUES + 1) * Kernels.FACTORS);
    private final LongArray dict;
    private final IntArray dictSize;
    private final LongArray longData;
    private final DoubleArray slots = new DoubleArray(Kernels.SLOTS);
    private final IntArray groups = new IntArray(1);
    // Per string column: 1 once the load finds a value longer than 8 bytes, which cannot be a key.
    private final IntArray tooLong;
    private boolean[] fits = new boolean[0];
    private final LongArray groupKeys = new LongArray(MAX_KEYS * MAX_GROUPS);
    private final DoubleArray groupResults = new DoubleArray((MAX_VALUES + 1) * MAX_GROUPS);
    private final TornadoExecutionPlan plan;
    private boolean loaded;
    private boolean encoded;

    public DeviceTable(List<String> files, long rows, List<Column> columns, long charsPerString)
    {
        this.files = List.copyOf(files);
        this.columns = List.copyOf(columns);
        this.rows = rows;
        this.cap = (int) ((rows + Kernels.GROUP - 1) / Kernels.GROUP * Kernels.GROUP);
        this.chunk = Math.min(cap, CHUNK);

        List<Integer> numeric = new ArrayList<>();
        List<CudfType> types = new ArrayList<>();
        List<Integer> stringList = new ArrayList<>();
        IntArray colKind = new IntArray(columns.size());
        IntArray colSlot = new IntArray(columns.size());
        int ints = 0;
        int longs = 0;
        int doubles = 0;
        for (int c = 0; c < columns.size(); c++) {
            Column column = columns.get(c);
            switch (column.kind()) {
                case INT -> {
                    colKind.set(c, Kernels.INT);
                    colSlot.set(c, ints++);
                    numeric.add(column.fileIndex());
                    types.add(CudfType.INT32);
                }
                case LONG -> {
                    colKind.set(c, Kernels.LONG);
                    colSlot.set(c, longs++);
                    numeric.add(column.fileIndex());
                    types.add(CudfType.INT64);
                }
                case DOUBLE -> {
                    colKind.set(c, Kernels.DOUBLE);
                    colSlot.set(c, doubles++);
                    numeric.add(column.fileIndex());
                    types.add(CudfType.FLOAT64);
                }
                case PACKED_STRING -> {
                    colKind.set(c, Kernels.LONG);
                    stringList.add(c);
                }
            }
        }
        // Packed strings take the long slots after the INT64 columns'.
        this.strings = stringList.size();
        this.tooLong = new IntArray(Math.max(1, strings));
        this.firstString = longs;
        this.stringOf = new int[columns.size()];
        Arrays.fill(stringOf, -1);
        int[] stringColumns = new int[strings];
        for (int s = 0; s < strings; s++) {
            colSlot.set(stringList.get(s), longs + s);
            stringOf[stringList.get(s)] = s;
            stringColumns[s] = columns.get(stringList.get(s)).fileIndex();
        }
        int longSlots = longs + strings;
        if (strings > 0 && (long) strings * Math.max(1024, charsPerString) > Integer.MAX_VALUE - 8) {
            // The device's string bytes are indexed with ints: refuse before anything runs.
            throw new UnsupportedOperationException(
                    strings + " string columns of up to " + charsPerString + " bytes each are beyond the device's 2 GB of string bytes");
        }
        int charsStride = (int) Math.max(1024, charsPerString);

        IntArray intData = new IntArray(Math.max(1, ints * cap));
        this.longData = new LongArray(Math.max(1, longSlots * cap));
        DoubleArray doubleData = new DoubleArray(Math.max(1, doubles * cap));
        IntArray offsets = new IntArray(Math.max(1, strings * (cap + 1)));
        ByteArray chars = new ByteArray(Math.max(1, (long) strings * charsStride));
        ByteArray valid = new ByteArray(Math.max(1, (long) (numeric.size() + strings) * cap));
        IntArray codes = new IntArray(Math.max(1, strings * cap));
        this.dict = new LongArray(Math.max(1, strings * Kernels.DICT));
        this.dictSize = new IntArray(Math.max(1, strings));
        DoubleArray partials = new DoubleArray(Kernels.DENSE_BLOCKS * Kernels.SLOTS);
        LongArray keys = new LongArray(MAX_KEYS * chunk);
        DoubleArray values = new DoubleArray((MAX_VALUES + 1) * chunk);
        LongArray outKeys = new LongArray(MAX_KEYS * chunk);
        DoubleArray outResults = new DoubleArray((MAX_VALUES + 1) * chunk);
        KernelContext context = new KernelContext();

        TaskGraph graph =
                new TaskGraph("table")
                        .transferToDevice(
                                DataTransferMode.EVERY_EXECUTION,
                                params,
                                pred,
                                predConst,
                                keyCol,
                                keyString,
                                keyMult,
                                factorCol,
                                factorA,
                                factorB,
                                dict,
                                dictSize)
                        .transferToDevice(
                                DataTransferMode.FIRST_EXECUTION,
                                colKind,
                                colSlot,
                                intData,
                                longData,
                                doubleData,
                                offsets,
                                chars,
                                valid,
                                codes,
                                partials,
                                keys,
                                values,
                                outKeys,
                                outResults,
                                groups,
                                groupKeys,
                                groupResults,
                                slots,
                                tooLong);
        // A table of string columns only (a group-key check) has no numeric read: cuDF refuses an empty one.
        if (!numeric.isEmpty()) {
            graph.libraryTask(
                    "read",
                    Cudf::readParquetColumns,
                    path,
                    0,
                    0,
                    numeric.stream().mapToInt(Integer::intValue).toArray(),
                    types.toArray(new CudfType[0]),
                    rowsHolder,
                    cap,
                    intData,
                    longData,
                    doubleData,
                    valid,
                    false);
        }
        List<String> rowTasks = new ArrayList<>();
        if (strings > 0) {
            graph.libraryTask(
                    "readStrings",
                    Cudf::readParquetStringColumns,
                    path,
                    stringColumns,
                    rowsHolder,
                    cap,
                    offsets,
                    chars,
                    (long) charsStride,
                    valid,
                    numeric.size());
            for (int s = 0; s < strings; s++) {
                graph.task("pack" + s, Kernels::packStrings, context, params, offsets, chars, s, charsStride, longs + s, longData, tooLong);
                rowTasks.add("pack" + s);
            }
            graph.task("encode", Kernels::encode, context, params, longData, dict, dictSize, codes);
            rowTasks.add("encode");
        }
        graph.task(
                        "dense",
                        Kernels::dense,
                        context,
                        params,
                        colKind,
                        colSlot,
                        pred,
                        predConst,
                        keyString,
                        keyMult,
                        factorCol,
                        factorA,
                        factorB,
                        intData,
                        longData,
                        doubleData,
                        codes,
                        partials)
                .task("reduce", Kernels::reduceSlots, context, params, partials, slots)
                .task("evaluate",
                        Kernels::evaluate,
                        context,
                        params,
                        colKind,
                        colSlot,
                        pred,
                        predConst,
                        keyCol,
                        factorCol,
                        factorA,
                        factorB,
                        intData,
                        longData,
                        doubleData,
                        keys,
                        values)
                .libraryTask(
                        "aggregate",
                        Cudf::groupAggregateLongs,
                        groupSize,
                        CudfAggregation.SUM.code(),
                        keys,
                        values,
                        chunk,
                        outKeys,
                        outResults,
                        groups)
                .task("collect", Kernels::collectGroups, context, params, groups, outKeys, outResults, MAX_GROUPS, groupKeys, groupResults)
                .transferToHost(DataTransferMode.EVERY_EXECUTION, slots, groups, groupKeys, groupResults, tooLong)
                .transferToHost(DataTransferMode.UNDER_DEMAND, longData);

        GridScheduler grids = new GridScheduler();
        for (String task : rowTasks) {
            grids.addWorkerGrid("table." + task, grid(cap, Kernels.GROUP));
        }
        grids.addWorkerGrid("table.dense", grid(Kernels.DENSE_BLOCKS * Kernels.DENSE_THREADS, Kernels.DENSE_THREADS));
        grids.addWorkerGrid("table.reduce", grid(Kernels.SLOTS, Kernels.SLOTS));
        grids.addWorkerGrid("table.evaluate", grid(chunk, Kernels.GROUP));
        grids.addWorkerGrid("table.collect", grid(MAX_GROUPS, Kernels.GROUP));
        this.plan = new TornadoExecutionPlan(graph.snapshot()).withGridScheduler(grids);
    }

    private static WorkerGrid1D grid(int global, int local)
    {
        WorkerGrid1D grid = new WorkerGrid1D(global);
        grid.setLocalWork(local, 1, 1);
        return grid;
    }

    /**
     * Sums each value expression per group of the rows that pass every predicate.
     *
     * @param predicates (column, operator) pairs, operators as in {@link Kernels}
     * @param constants one per predicate
     * @param keyColumns the group-key columns, or {@code {-1}} for one group
     * @param factorColumns {@link Kernels#FACTORS} per value: a column, -1 for 1, -2 for none
     */
    public synchronized Result aggregate(
            int[] predicates,
            double[] constants,
            int[] keyColumns,
            int[] factorColumns,
            double[] a,
            double[] b)
    {
        int nPred = constants.length;
        int nValues = factorColumns.length / Kernels.FACTORS;
        int[] keysUsed = Arrays.stream(keyColumns).filter(k -> k >= 0).toArray();
        if (nPred > MAX_PREDICATES || keyColumns.length > MAX_KEYS || nValues > MAX_VALUES || keyColumns.length == 0) {
            throw new IllegalArgumentException("Query beyond the plan's limits");
        }
        if (!loaded) {
            load();
        }
        for (int k : keysUsed) {
            if (stringOf[k] >= 0 && !fits[stringOf[k]]) {
                throw new UnsupportedOperationException("A group-key string column has values longer than 8 bytes");
            }
        }
        // The values, then one more: a count of each group's rows.
        int all = nValues + 1;
        for (int i = 0; i < factorColumns.length; i++) {
            factorCol.set(i, factorColumns[i]);
            factorA.set(i, a[i]);
            factorB.set(i, b[i]);
        }
        for (int f = 0; f < Kernels.FACTORS; f++) {
            int at = nValues * Kernels.FACTORS + f;
            factorCol.set(at, f == 0 ? -1 : -2);
            factorA.set(at, 0.0);
            factorB.set(at, f == 0 ? 1.0 : 0.0);
        }
        for (int i = 0; i < 2 * nPred; i++) {
            pred.set(i, predicates[i]);
        }
        for (int i = 0; i < nPred; i++) {
            predConst.set(i, constants[i]);
        }
        params.set(Kernels.P_ROWS, (int) rows);
        params.set(Kernels.P_CAP, cap);
        params.set(Kernels.P_PREDS, nPred);
        params.set(Kernels.P_VALUES, all);
        params.set(Kernels.P_LOAD, 0);
        params.set(Kernels.P_CHUNK, chunk);
        params.set(Kernels.P_STRINGS, strings);
        params.set(Kernels.P_FIRST_STRING, firstString);
        params.set(Kernels.P_ENCODE, encoded ? 0 : 1);
        encoded = true;
        path.setLength(0);
        rowsHolder[0] = 0;

        int groupsDense = 1;
        boolean dense = true;
        for (int k : keysUsed) {
            int s = stringOf[k];
            if (s < 0 || dictSize.get(s) > Kernels.DICT) {
                dense = false;
                break;
            }
            groupsDense *= Math.max(1, dictSize.get(s));
        }
        dense = dense && groupsDense * all <= Kernels.SLOTS;
        return dense ? runDense(keysUsed, groupsDense, nValues) : runHashed(keyColumns, nValues);
    }

    private Result runDense(int[] keysUsed, int groupsDense, int nValues)
    {
        int all = nValues + 1;
        int mult = 1;
        for (int k = keysUsed.length - 1; k >= 0; k--) {
            keyString.set(k, stringOf[keysUsed[k]]);
            keyMult.set(k, mult);
            mult *= dictSize.get(stringOf[keysUsed[k]]);
        }
        params.set(Kernels.P_KEYS, keysUsed.length);
        params.set(Kernels.P_MODE, Kernels.DENSE);
        params.set(Kernels.P_CHUNK_ROWS, 0);
        groupSize[0] = 0;
        groupSize[1] = 1;
        groupSize[2] = 1;
        plan.execute();

        List<Integer> live = new ArrayList<>();
        for (int g = 0; g < groupsDense; g++) {
            if (slots.get(g * all + nValues) > 0) {
                live.add(g);
            }
        }
        int nKeys = Math.max(1, keysUsed.length);
        long[][] keys = new long[nKeys][live.size()];
        double[][] sums = new double[all][live.size()];
        for (int j = 0; j < live.size(); j++) {
            int g = live.get(j);
            for (int k = 0; k < keysUsed.length; k++) {
                int s = stringOf[keysUsed[k]];
                int code = (g / keyMult.get(k)) % dictSize.get(s);
                keys[k][j] = dict.get(s * Kernels.DICT + code);
            }
            for (int v = 0; v < all; v++) {
                sums[v][j] = slots.get(g * all + v);
            }
        }
        return new Result(live.size(), keys, sums);
    }

    private Result runHashed(int[] keyColumns, int nValues)
    {
        int all = nValues + 1;
        int nKeys = keyColumns.length;
        for (int k = 0; k < nKeys; k++) {
            keyCol.set(k, keyColumns[k]);
        }
        params.set(Kernels.P_KEYS, nKeys);
        params.set(Kernels.P_MODE, Kernels.HASHED);
        Map<List<Long>, double[]> merged = new LinkedHashMap<>();
        for (long offset = 0; offset < rows; offset += chunk) {
            int chunkRows = (int) Math.min(chunk, rows - offset);
            params.set(Kernels.P_OFFSET, (int) offset);
            params.set(Kernels.P_CHUNK_ROWS, chunkRows);
            groupSize[0] = chunkRows;
            groupSize[1] = nKeys;
            groupSize[2] = all;
            plan.execute();
            params.set(Kernels.P_ENCODE, 0);
            int n = groups.get(0);
            if (n > MAX_GROUPS) {
                throw new IllegalStateException(n + " groups in a chunk, beyond " + MAX_GROUPS);
            }
            for (int g = 0; g < n; g++) {
                if (groupKeys.get(g) == Kernels.SENTINEL) {
                    continue;
                }
                List<Long> key = new ArrayList<>(nKeys);
                for (int k = 0; k < nKeys; k++) {
                    key.add(groupKeys.get(k * MAX_GROUPS + g));
                }
                double[] sums = merged.computeIfAbsent(key, _ -> new double[all]);
                for (int v = 0; v < all; v++) {
                    sums[v] += groupResults.get(v * MAX_GROUPS + g);
                }
            }
        }
        long[][] keys = new long[nKeys][merged.size()];
        double[][] sums = new double[all][merged.size()];
        int j = 0;
        for (Map.Entry<List<Long>, double[]> e : merged.entrySet()) {
            for (int k = 0; k < nKeys; k++) {
                keys[k][j] = e.getKey().get(k);
            }
            for (int v = 0; v < all; v++) {
                sums[v][j] = e.getValue()[v];
            }
            j++;
        }
        return new Result(merged.size(), keys, sums);
    }

    /**
     * Reads the files, packs the strings, and builds each string column's dictionary on the host.
     */
    private void load()
    {
        params.set(Kernels.P_ROWS, (int) rows);
        params.set(Kernels.P_CAP, cap);
        params.set(Kernels.P_LOAD, 1);
        params.set(Kernels.P_MODE, 0);
        params.set(Kernels.P_ENCODE, 0);
        params.set(Kernels.P_STRINGS, strings);
        params.set(Kernels.P_FIRST_STRING, firstString);
        params.set(Kernels.P_CHUNK, chunk);
        params.set(Kernels.P_CHUNK_ROWS, 0);
        groupSize[0] = 0;
        groupSize[1] = 1;
        groupSize[2] = 1;
        path.setLength(0);
        path.append(String.join("\n", files));
        rowsHolder[0] = rows;
        TornadoExecutionResult result = plan.execute();
        loaded = true;
        fits = new boolean[strings];
        for (int s = 0; s < strings; s++) {
            fits[s] = tooLong.get(s) == 0;
        }
        if (strings == 0) {
            return;
        }
        result.transferToHost(longData);
        for (int s = 0; s < strings; s++) {
            Set<Long> distinct = new LinkedHashSet<>();
            int base = (firstString + s) * cap;
            for (int i = 0; i < rows && distinct.size() <= Kernels.DICT; i++) {
                distinct.add(longData.get(base + i));
            }
            long[] sorted = distinct.stream().mapToLong(Long::longValue).sorted().toArray();
            dictSize.set(s, sorted.length);
            for (int d = 0; d < Math.min(sorted.length, Kernels.DICT); d++) {
                dict.set(s * Kernels.DICT + d, sorted[d]);
            }
        }
    }

    /**
     * Whether every value of the column (a packed string) fits in 8 bytes; loads the table first.
     */
    public synchronized boolean fits(int column)
    {
        if (!loaded) {
            load();
        }
        return stringOf[column] < 0 || fits[stringOf[column]];
    }

    public long rows()
    {
        return rows;
    }

    public List<Column> columns()
    {
        return columns;
    }

    @Override
    public void close()
    {
        try {
            plan.freeDeviceMemory();
            plan.close();
        }
        catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}

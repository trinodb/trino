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

import uk.ac.manchester.tornado.api.KernelContext;
import uk.ac.manchester.tornado.api.types.arrays.ByteArray;
import uk.ac.manchester.tornado.api.types.arrays.DoubleArray;
import uk.ac.manchester.tornado.api.types.arrays.IntArray;
import uk.ac.manchester.tornado.api.types.arrays.LongArray;

/**
 * Device kernels of a filter, projection and grouped aggregation over resident columns.
 *
 * <p>Two aggregation paths. Dense: when every group key is a string column with a small
 * dictionary, a row's group is a number below {@link #SLOTS} / values, and one kernel filters,
 * evaluates and sums into per-thread accumulators in local memory, with no intermediate column.
 * Hashed: otherwise, a chunk of rows gets keys and values written out and cuDF groups them.
 */
final class Kernels
{
    static final int GROUP = 256;

    /**
     * Factors per value expression: a value is the product of up to this many {@code a*x+b}.
     */
    static final int FACTORS = 4;

    /**
     * A filtered-out row's key on the hashed path: the group the host drops.
     */
    static final long SENTINEL = Long.MIN_VALUE;

    /**
     * Distinct values a string column may have for the dense path.
     */
    static final int DICT = 64;

    /**
     * Accumulators a thread keeps on the dense path: groups times values.
     */
    static final int SLOTS = 64;

    /**
     * Threads a block on the dense path; their accumulators fill the block's local memory.
     */
    static final int DENSE_THREADS = 64;

    /**
     * Blocks on the dense path.
     */
    static final int DENSE_BLOCKS = 1024;

    // column kinds
    static final int INT = 0;
    static final int LONG = 1;
    static final int DOUBLE = 2;

    // predicate operators
    static final int EQ = 0;
    static final int NE = 1;
    static final int LT = 2;
    static final int LE = 3;
    static final int GT = 4;
    static final int GE = 5;

    // modes
    static final int DENSE = 1;
    static final int HASHED = 2;

    // params
    static final int P_ROWS = 0;
    static final int P_CAP = 1;
    static final int P_PREDS = 2;
    static final int P_KEYS = 3;
    static final int P_VALUES = 4;
    static final int P_MODE = 5;
    static final int P_LOAD = 6;
    static final int P_ENCODE = 7;
    static final int P_OFFSET = 8;
    static final int P_CHUNK_ROWS = 9;
    static final int P_CHUNK = 10;
    static final int P_STRINGS = 11;
    static final int P_FIRST_STRING = 12;
    static final int P_SIZE = 13;

    /**
     * The value of column {@code c} at row {@code i}, as a double.
     */
    static double column(int c, int i, int cap, IntArray colKind, IntArray colSlot, IntArray ints, LongArray longs, DoubleArray doubles)
    {
        int kind = colKind.get(c);
        int at = colSlot.get(c) * cap + i;
        double x;
        if (kind == INT) {
            x = ints.get(at);
        }
        else if (kind == LONG) {
            x = longs.get(at);
        }
        else {
            x = doubles.get(at);
        }
        return x;
    }

    /**
     * Whether row {@code i} passes every predicate.
     */
    static int passes(
            int i,
            int cap,
            IntArray params,
            IntArray colKind,
            IntArray colSlot,
            IntArray pred,
            DoubleArray predConst,
            IntArray ints,
            LongArray longs,
            DoubleArray doubles)
    {
        int pass = 1;
        int preds = params.get(P_PREDS);
        for (int p = 0; p < preds && pass == 1; p++) {
            double x = column(pred.get(2 * p), i, cap, colKind, colSlot, ints, longs, doubles);
            int op = pred.get(2 * p + 1);
            double k = predConst.get(p);
            if (op == EQ) {
                pass = x == k ? 1 : 0;
            }
            else if (op == NE) {
                pass = x != k ? 1 : 0;
            }
            else if (op == LT) {
                pass = x < k ? 1 : 0;
            }
            else if (op == LE) {
                pass = x <= k ? 1 : 0;
            }
            else if (op == GT) {
                pass = x > k ? 1 : 0;
            }
            else {
                pass = x >= k ? 1 : 0;
            }
        }
        return pass;
    }

    /**
     * Value expression {@code v} at row {@code i}: a product of affine factors of columns.
     */
    static double value(
            int v,
            int i,
            int cap,
            IntArray colKind,
            IntArray colSlot,
            IntArray factorCol,
            DoubleArray factorA,
            DoubleArray factorB,
            IntArray ints,
            LongArray longs,
            DoubleArray doubles)
    {
        double product = 1.0;
        for (int f = 0; f < FACTORS; f++) {
            int c = factorCol.get(v * FACTORS + f);
            if (c != -2) {
                double x = 1.0;
                if (c >= 0) {
                    x = column(c, i, cap, colKind, colSlot, ints, longs, doubles);
                }
                product = product * (factorA.get(v * FACTORS + f) * x + factorB.get(v * FACTORS + f));
            }
        }
        return product;
    }

    /**
     * On the load: packs each string of a strings column (cuDF layout) into a long that orders as
     * the string does, its first 8 bytes big-endian; flags a string longer than 8 bytes.
     */
    static void packStrings(
            KernelContext context,
            IntArray params,
            IntArray offsets,
            ByteArray chars,
            int column,
            int charsStride,
            int slot,
            LongArray longs,
            IntArray tooLong)
    {
        int i = context.globalIdx;
        int rows = params.get(P_ROWS);
        int cap = params.get(P_CAP);
        if (i < rows && params.get(P_LOAD) == 1) {
            int start = offsets.get(column * (cap + 1) + i);
            int end = offsets.get(column * (cap + 1) + i + 1);
            if (end - start > 8) {
                tooLong.set(column, 1);
            }
            long packed = 0;
            for (int b = 0; b < 8; b++) {
                packed = packed << 8;
                if (start + b < end) {
                    packed = packed | (chars.get(column * charsStride + start + b) & 0xFFL);
                }
            }
            longs.set(slot * cap + i, packed);
        }
    }

    /**
     * Once, after the load: each string column's code, its value's index in the column's dictionary.
     */
    static void encode(KernelContext context, IntArray params, LongArray longs, LongArray dict, IntArray dictSize, IntArray codes)
    {
        int i = context.globalIdx;
        int rows = params.get(P_ROWS);
        int cap = params.get(P_CAP);
        if (i < rows && params.get(P_ENCODE) == 1) {
            int strings = params.get(P_STRINGS);
            int first = params.get(P_FIRST_STRING);
            for (int s = 0; s < strings; s++) {
                int n = dictSize.get(s);
                int code = -1;
                if (n <= DICT) {
                    long x = longs.get((first + s) * cap + i);
                    for (int d = 0; d < n; d++) {
                        if (dict.get(s * DICT + d) == x) {
                            code = d;
                        }
                    }
                }
                codes.set(s * cap + i, code);
            }
        }
    }

    /**
     * The dense path: each thread strides over the rows, and for each row that passes adds every
     * value into its own accumulators for the row's group, in local memory; then the block sums
     * its threads' accumulators into one partial per slot.
     */
    static void dense(
            KernelContext context,
            IntArray params,
            IntArray colKind,
            IntArray colSlot,
            IntArray pred,
            DoubleArray predConst,
            IntArray keyString,
            IntArray keyMult,
            IntArray factorCol,
            DoubleArray factorA,
            DoubleArray factorB,
            IntArray ints,
            LongArray longs,
            DoubleArray doubles,
            IntArray codes,
            DoubleArray partials)
    {
        double[] acc = context.allocateDoubleLocalArray(DENSE_THREADS * SLOTS);
        int t = context.localIdx;
        int mode = params.get(P_MODE);
        int base = t * SLOTS;
        for (int s = 0; s < SLOTS; s++) {
            acc[base + s] = 0.0;
        }
        if (mode == DENSE) {
            int rows = params.get(P_ROWS);
            int cap = params.get(P_CAP);
            int nKeys = params.get(P_KEYS);
            int nValues = params.get(P_VALUES);
            int total = DENSE_BLOCKS * DENSE_THREADS;
            for (int i = context.globalIdx; i < rows; i += total) {
                if (passes(i, cap, params, colKind, colSlot, pred, predConst, ints, longs, doubles) == 1) {
                    int group = 0;
                    for (int k = 0; k < nKeys; k++) {
                        group = group + codes.get(keyString.get(k) * cap + i) * keyMult.get(k);
                    }
                    int slot = base + group * nValues;
                    for (int v = 0; v < nValues; v++) {
                        acc[slot + v] = acc[slot + v] + value(v, i, cap, colKind, colSlot, factorCol, factorA, factorB, ints, longs, doubles);
                    }
                }
            }
        }
        context.localBarrier();
        if (t < SLOTS && mode == DENSE) {
            double sum = 0.0;
            for (int j = 0; j < DENSE_THREADS; j++) {
                sum = sum + acc[j * SLOTS + t];
            }
            partials.set(context.groupIdx * SLOTS + t, sum);
        }
    }

    /**
     * The dense path's last step: each slot summed over the blocks.
     */
    static void reduceSlots(KernelContext context, IntArray params, DoubleArray partials, DoubleArray slots)
    {
        int t = context.globalIdx;
        if (t < SLOTS && params.get(P_MODE) == DENSE) {
            double sum = 0.0;
            for (int b = 0; b < DENSE_BLOCKS; b++) {
                sum = sum + partials.get(b * SLOTS + t);
            }
            slots.set(t, sum);
        }
    }

    /**
     * The hashed path, for the rows {@code [offset, offset + chunkRows)}: each row's group keys
     * (the sentinel when it does not pass) and values, at chunk stride.
     */
    static void evaluate(
            KernelContext context,
            IntArray params,
            IntArray colKind,
            IntArray colSlot,
            IntArray pred,
            DoubleArray predConst,
            IntArray keyCol,
            IntArray factorCol,
            DoubleArray factorA,
            DoubleArray factorB,
            IntArray ints,
            LongArray longs,
            DoubleArray doubles,
            LongArray keys,
            DoubleArray values)
    {
        int j = context.globalIdx;
        int chunk = params.get(P_CHUNK);
        if (j < params.get(P_CHUNK_ROWS) && params.get(P_MODE) == HASHED) {
            int cap = params.get(P_CAP);
            int i = params.get(P_OFFSET) + j;
            int pass = passes(i, cap, params, colKind, colSlot, pred, predConst, ints, longs, doubles);
            int nKeys = params.get(P_KEYS);
            for (int k = 0; k < nKeys; k++) {
                int c = keyCol.get(k);
                long key = 0;
                if (c >= 0) {
                    int at = colSlot.get(c) * cap + i;
                    if (colKind.get(c) == INT) {
                        key = ints.get(at);
                    }
                    else {
                        key = longs.get(at);
                    }
                }
                keys.set(k * chunk + j, pass == 1 ? key : SENTINEL);
            }
            int nValues = params.get(P_VALUES);
            for (int v = 0; v < nValues; v++) {
                values.set(v * chunk + j, value(v, i, cap, colKind, colSlot, factorCol, factorA, factorB, ints, longs, doubles));
            }
        }
    }

    /**
     * The hashed path: copies a chunk's first groups into the small arrays the host reads.
     */
    static void collectGroups(
            KernelContext context,
            IntArray params,
            IntArray groups,
            LongArray outKeys,
            DoubleArray outResults,
            int maxGroups,
            LongArray groupKeys,
            DoubleArray groupResults)
    {
        int g = context.globalIdx;
        if (params.get(P_MODE) == HASHED) {
            int chunk = params.get(P_CHUNK);
            int n = groups.get(0);
            if (g < n && g < maxGroups) {
                int nKeys = params.get(P_KEYS);
                for (int k = 0; k < nKeys; k++) {
                    groupKeys.set(k * maxGroups + g, outKeys.get(k * chunk + g));
                }
                int nValues = params.get(P_VALUES);
                for (int v = 0; v < nValues; v++) {
                    groupResults.set(v * maxGroups + g, outResults.get(v * chunk + g));
                }
            }
        }
    }

    private Kernels() {}
}

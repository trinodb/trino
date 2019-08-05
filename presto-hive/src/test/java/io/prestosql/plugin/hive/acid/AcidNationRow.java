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
package io.prestosql.plugin.hive.acid;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.type.Type;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.prestosql.plugin.hive.HiveTestUtils.SESSION;
import static org.testng.Assert.assertTrue;

public class AcidNationRow
{
    int nationkey;
    String name;
    int regionkey;
    String comment;

    public AcidNationRow(Map<String, Object> row)
    {
        this(
                (Integer) row.getOrDefault("n_nationkey", -1),
                (String) row.getOrDefault("n_name", "INVALID"),
                (Integer) row.getOrDefault("n_regionkey", -1),
                (String) row.getOrDefault("n_comment", "INVALID"));
    }

    public AcidNationRow(int nationkey, String name, int regionkey, String comment)
    {
        this.nationkey = nationkey;
        this.name = name;
        this.regionkey = regionkey;
        this.comment = comment;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, nationkey, regionkey, comment);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof AcidNationRow)) {
            return false;
        }

        AcidNationRow other = (AcidNationRow) obj;
        return (nationkey == other.nationkey
                && name.equals(other.name)
                && regionkey == other.regionkey
                && comment.equals(other.comment));
    }

    public static List<AcidNationRow> readFileCols(ConnectorPageSource pageSource, List<String> columnNames, List<Type> columnTypes, boolean resultsNeeded)
    {
        List<AcidNationRow> rows = new ArrayList(resultsNeeded ? 25000 : 0);

        while (!pageSource.isFinished()) {
            Page page = pageSource.getNextPage();
            if (page != null) {
                assertTrue(page.getChannelCount() == columnNames.size(), "Did not read required number of blocks: " + page.getChannelCount());
                page = page.getLoadedPage();

                if (!resultsNeeded) {
                    continue;
                }

                for (int pos = 0; pos < page.getPositionCount(); pos++) {
                    ImmutableMap.Builder<String, Object> values = ImmutableMap.builder();
                    for (int idx = 0; idx < columnTypes.size(); idx++) {
                        values.put(columnNames.get(idx), columnTypes.get(idx).getObjectValue(SESSION, page.getBlock(idx), pos));
                    }
                    rows.add(new AcidNationRow(values.build()));
                }
            }
        }
        return rows;
    }

    /*
     * Returns rows for expected response, explodes each row from nation.tbl into 1000 rows
     *
     * If onlyForRowId is provided, then only that row from nation.tbls is read and exploded and others are ignored
     */
    public static List<AcidNationRow> getExpectedResult(Optional<Integer> onlyForRowId, Optional<Integer> onlyForColumnId)
            throws IOException
    {
        String nationFilePath = Thread.currentThread().getContextClassLoader().getResource("nation.tbl").getPath();
        final ImmutableList.Builder<AcidNationRow> result = ImmutableList.builder();
        long rowId = 0;
        BufferedReader br = new BufferedReader(new FileReader(nationFilePath));
        try {
            String line;
            int lineNum = -1;
            while ((line = br.readLine()) != null) {
                lineNum++;
                if (onlyForRowId.isPresent() && onlyForRowId.get() != lineNum) {
                    continue;
                }
                rowId += replicateIntoResult(line, result, rowId, onlyForColumnId);
            }
        }
        finally {
            br.close();
        }
        return result.build();
    }

    public static long replicateIntoResult(String line, ImmutableList.Builder<AcidNationRow> resultBuilder, long startRowId, Optional<Integer> onlyForColumnId)
    {
        long replicationFactor = 1000; // same way the nationFile25kRowsSortedOnNationKey.orc is created
        for (int i = 0; i < replicationFactor; i++) {
            String[] cols = line.split("\\|");
            resultBuilder.add(new AcidNationRow(
                    (!onlyForColumnId.isPresent() || onlyForColumnId.get() == 0) ? Integer.parseInt(cols[0]) : -1,
                    (!onlyForColumnId.isPresent() || onlyForColumnId.get() == 1) ? cols[1] : "INVALID",
                    (!onlyForColumnId.isPresent() || onlyForColumnId.get() == 2) ? Integer.parseInt(cols[2]) : -1,
                    (!onlyForColumnId.isPresent() || onlyForColumnId.get() == 3) ? cols[3] : "INVALID"));
        }
        return replicationFactor;
    }
}

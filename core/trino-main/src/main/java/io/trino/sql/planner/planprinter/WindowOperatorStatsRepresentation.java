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
package io.trino.sql.planner.planprinter;

//import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class WindowOperatorStatsRepresentation
{
    private final int activeDrivers;
    private final int totalDrivers;
    private final double indexSizeStdDev;
    private final double indexPositionsStdDev;
    private final double indexCountPerDriverStdDev;
    private final double rowsPerDriverStdDev;
    private final double partitionRowsStdDev;

    public WindowOperatorStatsRepresentation(WindowOperatorStats windowOperatorStats)
    {
        this.activeDrivers = windowOperatorStats.getActiveDrivers();
        this.totalDrivers = windowOperatorStats.getTotalDrivers();
        this.indexSizeStdDev = windowOperatorStats.getIndexSizeStdDev();
        this.indexPositionsStdDev = windowOperatorStats.getIndexPositionsStdDev();
        this.indexCountPerDriverStdDev = windowOperatorStats.getIndexCountPerDriverStdDev();
        this.rowsPerDriverStdDev = windowOperatorStats.getRowsPerDriverStdDev();
        this.partitionRowsStdDev = windowOperatorStats.getPartitionRowsStdDev();
    }

    @JsonProperty
    public int getActiveDrivers()
    {
        return activeDrivers;
    }

    @JsonProperty
    public int getTotalDrivers()
    {
        return totalDrivers;
    }

    @JsonProperty
    public double getIndexSizeStdDev()
    {
        return indexSizeStdDev;
    }

    @JsonProperty
    public double getIndexPositionsStdDev()
    {
        return indexPositionsStdDev;
    }

    @JsonProperty
    public double getIndexCountPerDriverStdDev()
    {
        return indexCountPerDriverStdDev;
    }

    @JsonProperty
    public double getRowsPerDriverStdDev()
    {
        return rowsPerDriverStdDev;
    }

    @JsonProperty
    public double getPartitionRowsStdDev()
    {
        return partitionRowsStdDev;
    }
}

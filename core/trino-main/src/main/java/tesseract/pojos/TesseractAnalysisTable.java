/**
 * Licensed to the Airtel International LLP (AILLP) under one
 * or more contributor license agreements.
 * The AILLP licenses this file to you under the AA License, Version 1.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @author Akash Roy
 * @department Big Data Analytics Airtel Africa
 * @since Thu, 03-02-2022
 */
package tesseract.pojos;

import io.trino.spi.tesseract.TesseractTableInfo;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.tree.NodeLocation;

import java.util.Comparator;

/**
 * Table structure for the tesseract cube detected in Analysis Plan
 */
public class TesseractAnalysisTable
{

    private static final Comparator<NodeLocation> nodeLocationComparator = Comparator.comparingInt(NodeLocation::getLineNumber).thenComparingInt(NodeLocation::getColumnNumber);

    PlanNodeId planNodeId;
    TesseractTableInfo tesseractTableInfo;
    NodeLocation nodeLocation;

    public TesseractAnalysisTable(PlanNodeId planNodeId, TesseractTableInfo tesseractTableInfo, NodeLocation nodeLocation)
    {
        this.planNodeId = planNodeId;
        this.tesseractTableInfo = tesseractTableInfo;
        this.nodeLocation = nodeLocation;
    }

    public PlanNodeId getPlanNodeId()
    {
        return planNodeId;
    }

    public TesseractTableInfo getTesseractTableInfo()
    {
        return tesseractTableInfo;
    }

    public NodeLocation getNodeLocation()
    {
        return nodeLocation;
    }

    public static Comparator<NodeLocation> getNodeLocationComparator()
    {
        return nodeLocationComparator;
    }
}

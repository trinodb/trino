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
 * @since Tue, 01-03-2022
 */
package tesseract.pojos;

import io.trino.metadata.QualifiedObjectName;
import io.trino.sql.tree.NodeLocation;

/**
 * Context to capture required details while traversing the analysis plan
 */
public class TesseractAnalysisContext
{
    NodeLocation nodeLocation;
    QualifiedObjectName tableName;

    public TesseractAnalysisContext()
    {
    }

    public TesseractAnalysisContext(NodeLocation nodeLocation, QualifiedObjectName tableName)
    {
        this.nodeLocation = nodeLocation;
        this.tableName = tableName;
    }

    public NodeLocation getNodeLocation()
    {
        return nodeLocation;
    }

    public void setNodeLocation(NodeLocation nodeLocation)
    {
        this.nodeLocation = nodeLocation;
    }

    public QualifiedObjectName getTableName()
    {
        return tableName;
    }

    public void setTableName(QualifiedObjectName tableName)
    {
        this.tableName = tableName;
    }

}

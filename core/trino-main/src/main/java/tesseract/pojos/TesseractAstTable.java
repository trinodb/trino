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

import io.trino.sql.tree.NodeLocation;

/**
 * Table structure for a tesseract optimized table detected in a AST of original query
 */
public class TesseractAstTable {

    /*
     this captures the exact coordinate [line number & index at said line] where we'll have to insert the optimal predicate if required
     */
    NodeLocation nodeLocationForEdit;
    NodeLocation originalNodeLocation;
    String tableName;
    boolean wherePresent;
    String optimalPredicate;

    public TesseractAstTable(NodeLocation nodeLocationForEdit, String tableName,NodeLocation originalNodeLocation) {
        this.nodeLocationForEdit = nodeLocationForEdit;
        this.tableName = tableName;
        this.originalNodeLocation = originalNodeLocation;
    }

    public NodeLocation getOriginalNodeLocation()
    {
        return originalNodeLocation;
    }

    public NodeLocation getNodeLocationForEdit() {
        return nodeLocationForEdit;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean isWherePresent() {
        return wherePresent;
    }

    public void setWherePresent(boolean wherePresent) {
        this.wherePresent = wherePresent;
    }

    public String getOptimalPredicate()
    {
        return optimalPredicate;
    }

    public void setOptimalPredicate(String optimalPredicate)
    {
        this.optimalPredicate = optimalPredicate;
    }
}

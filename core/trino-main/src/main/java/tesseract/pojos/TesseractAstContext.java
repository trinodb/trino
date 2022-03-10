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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * This is context object we maintain while traversing through the AST of a query
 */
public class TesseractAstContext
{

    /*
    If a table has alias in a query then its location should override the location of table name in query
    as we will need to insert the optimal predicate after the alias
     */
    NodeLocation aliasLocationInfo;

    /*
    While traversing a complex query containing multiple joins etc , this list stores the list of tables in a particular
    select query section .
     */
    List<TesseractAstTable> querySpecificationLocalAstTables = new ArrayList<>();

    /*
    This stores all tesseract optimized tables in the complete AST ordered by their existence in the original query
     */
    Map<NodeLocation, TesseractAstTable> nodeLocationToTesseractAstTables = new TreeMap<>(TesseractAnalysisTable.getNodeLocationComparator());

    public Optional<NodeLocation> getAliasLocationInfo()
    {
        return Optional.ofNullable(aliasLocationInfo);
    }

    public Map<NodeLocation, TesseractAstTable> getNodeLocationToTesseractAstTables()
    {
        return nodeLocationToTesseractAstTables;
    }

    public List<TesseractAstTable> getQuerySpecificationLocalAstTables()
    {
        return querySpecificationLocalAstTables;
    }

    public void setAliasLocationInfo(NodeLocation aliasLocationInfo)
    {
        this.aliasLocationInfo = aliasLocationInfo;
    }
}

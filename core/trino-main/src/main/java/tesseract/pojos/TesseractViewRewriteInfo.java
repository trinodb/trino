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
 * @since Fri, 04-03-2022
 */
package tesseract.pojos;

import io.trino.sql.tree.NodeLocation;

/**
 * Details required to unfold a particular view
 */
public class TesseractViewRewriteInfo
{

    /*
    Say for a query : { select * from hive."default"     ."table_name" groupby some_col }
        - startOfViewIdentifier : will be node location of start of keyword {hive} in the original query
        - endOfViewIdentifier : will be node location of start of keyword {"table_name"} in the original query
        - endOfIdentifierLength : this is the length of the string {"table_name"} (including quotes)
        - viewDefinition : this is the actual view definition of the trino view
     */

    NodeLocation startOfViewIdentifier;
    NodeLocation endOfViewIdentifier;
    Integer endOfIdentifierLength;
    String viewDefinition;

    public TesseractViewRewriteInfo(NodeLocation startOfViewIdentifier, NodeLocation endOfViewIdentifier, Integer endOfIdentifierLength, String viewDefinition)
    {
        this.startOfViewIdentifier = startOfViewIdentifier;
        this.endOfViewIdentifier = endOfViewIdentifier;
        this.endOfIdentifierLength = endOfIdentifierLength;
        this.viewDefinition = viewDefinition;
    }

    public NodeLocation getStartOfViewIdentifier()
    {
        return startOfViewIdentifier;
    }

    public void setStartOfViewIdentifier(NodeLocation startOfViewIdentifier)
    {
        this.startOfViewIdentifier = startOfViewIdentifier;
    }

    public NodeLocation getEndOfViewIdentifier()
    {
        return endOfViewIdentifier;
    }

    public void setEndOfViewIdentifier(NodeLocation endOfViewIdentifier)
    {
        this.endOfViewIdentifier = endOfViewIdentifier;
    }

    public Integer getEndOfIdentifierLength()
    {
        return endOfIdentifierLength;
    }

    public void setEndOfIdentifierLength(Integer endOfIdentifierLength)
    {
        this.endOfIdentifierLength = endOfIdentifierLength;
    }

    public String getViewDefinition()
    {
        return viewDefinition;
    }

    public void setViewDefinition(String viewDefinition)
    {
        this.viewDefinition = viewDefinition;
    }
}

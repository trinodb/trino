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
package tesseract.visitor;

import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.Table;
import tesseract.pojos.TesseractAnalysisContext;

import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;

/**
 * Visitor to traverse the analysis plan and capture node locations of tablescannode in the AST.
 * These node locations are later on required to identify which optimal predicate belongs to what tablescan node in the original AST of query
 */
public class TesseractAnalysisVisitor
        extends DefaultTraversalVisitor<TesseractAnalysisContext>
{

    private final Session session;

    public TesseractAnalysisVisitor(Session session)
    {
        this.session = session;
    }

    @Override
    protected Void visitTable(Table node, TesseractAnalysisContext context)
    {
        QualifiedObjectName name = createQualifiedObjectName(session, node, node.getName());
        context.setNodeLocation(node.getLocation().get());
        context.setTableName(name);

        return super.visitTable(node, context);
    }
}

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

import java.util.Set;
import java.util.TreeSet;

/**
 * Context to capture details related to various views being referred in a query
 */
public class TesseractViewContext
{

    Set<TesseractViewRewriteInfo> viewLocationToRewriteInfo;
    boolean referringTesseractTable;

    public TesseractViewContext()
    {
        this.viewLocationToRewriteInfo = new TreeSet<>((view1, view2) -> {
            int compareLine = Integer.compare(view1.getStartOfViewIdentifier().getLineNumber(), view2.getStartOfViewIdentifier().getLineNumber());
            if (compareLine != 0) {
                return compareLine;
            }
            else {
                return Integer.compare(view1.getStartOfViewIdentifier().getColumnNumber(), view2.getStartOfViewIdentifier().getColumnNumber());
            }
        });
    }

    public Set<TesseractViewRewriteInfo> getViewLocationToRewriteInfo()
    {
        return viewLocationToRewriteInfo;
    }

    public void setViewLocationToRewriteInfo(Set<TesseractViewRewriteInfo> viewLocationToRewriteInfo)
    {
        this.viewLocationToRewriteInfo = viewLocationToRewriteInfo;
    }

    public boolean isReferringTesseractTable()
    {
        return referringTesseractTable;
    }

    public void setReferringTesseractTable(boolean referringTesseractTable)
    {
        this.referringTesseractTable = referringTesseractTable;
    }
}

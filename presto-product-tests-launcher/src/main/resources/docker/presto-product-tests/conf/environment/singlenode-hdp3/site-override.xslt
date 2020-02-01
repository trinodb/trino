<!-- Copied from https://stackoverflow.com/a/31186191/65458 and adapted -->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes" />
    <xsl:strip-space elements="*" />

    <xsl:variable name="override-properties" select="document($override-path)/configuration/property" />

    <xsl:template match="/configuration">
        <xsl:copy>
            <!-- copy local properties not overridden by external properties -->
            <xsl:copy-of select="property[not(name=$override-properties/name)]" />
            <!-- add all overriding properties -->
            <xsl:copy-of select="$override-properties" />
        </xsl:copy>
    </xsl:template>

</xsl:stylesheet>

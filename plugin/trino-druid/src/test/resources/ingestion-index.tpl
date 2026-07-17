 {
     "type": "index",
     "spec": {
         "dataSchema": {
             "dataSource": "${datasource}",
             "timestampSpec": {
                 "column": "${timestampSpec.column}",
                 "format": "${timestampSpec.format}"
             },
             "dimensionsSpec": {
                 "dimensions": [
                    <#list columns as column>
                    {
                        "name": "${column.name}",
                        "type": "${column.type}"
                    }<#sep>,
                    </#list>
                 ]
             },
             "granularitySpec": {
                 "type": "uniform",
                 "intervals": [
                     "1958-01-01/2028-12-01"
                 ],
                 "segmentGranularity": "year",
                 "queryGranularity": "none"
             }
         },
         "ioConfig": {
             "type": "index",
             "inputSource": {
                 "type": "local",
                 "baseDir": "/opt/druid/var/",
                 "filter": "${datasource}.tsv"
             },
             "inputFormat": {
                 "type": "tsv",
                 "findColumnsFromHeader": false,
                 "columns": [
                    "${timestampSpec.column}",
                    <#list columns as column>
                    "${column.name}"<#sep>, </#sep>
                    </#list>
                 ]
             },
             "appendToExisting": false
         },
         "tuningConfig": {
             "type": "index",
             "maxRowsPerSegment": 5000000,
             "maxRowsInMemory": 250000,
             "segmentWriteOutMediumFactory": {
                 "type": "offHeapMemory"
             }
         }
     }
 }

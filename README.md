# sysmwatch

This python tool watches the postgres systemmetadata tables, pulling out entries
with dateModified more recent than a specified value.

Identifiers are examined in the Solr index, and flagged if the indexed dateModified 
does not match that of the systemMetadata.

The process is fairly efficient and may provide a basis for implementing
a replacement for the index-task-generator which currently relies on hazelcast events.

Output is to a JSON file that can be rendered with a simple handsontable implemnetation.
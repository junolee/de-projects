/*

Load CSV files from external S3 stage into a Snowflake table via COPY INTO

args:
    table_name: target table
    path:       subpath under stage URL
    key:        selects copy options via var('copy_into_settings')[key]

Requires var('copy_into_settings')[key]:
    target_schema:    database.schema to load into    
    stage:            external stage name
    file_format:      Snowflake file format name
    force:            force reload for dev/testing; typically FALSE in production

Notes:
- Loads rows into raw table; downstream models should dedupe using loaded_at
- INCLUDE_METADATA injects loaded_at from METADATA$START_SCAN_TIME
- MATCH_BY_COLUMN_NAME is required with INCLUDE_METADATA
- For CSV + INCLUDE_METADATA, file format must set ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE
*/

{% macro load_csv(table_name, path='', key='walmart') %}

    {% set c = var('copy_into_settings').get(key) %}

    COPY INTO {{ c.target_schema }}.{{ table_name }}
    FROM @{{ c.stage }}/{{ path }}
    FILE_FORMAT = {{ c.file_format }}
    FORCE = {{ c.force }}

    INCLUDE_METADATA = (loaded_at=METADATA$START_SCAN_TIME)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

{% endmacro %}
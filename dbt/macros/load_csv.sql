
{% macro load_csv(table_name, path='', key='walmart') %}

    {% set c = var('copy_into_settings').get(key) %}

    COPY INTO {{ c.target_schema }}.{{ table_name }}
    FROM @{{ c.stage }}/{{ path }}
    FILE_FORMAT = {{ c.file_format }}
    FORCE = {{ c.force }}

    INCLUDE_METADATA = (loaded_at=METADATA$START_SCAN_TIME)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

{% endmacro %}
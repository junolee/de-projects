
{% macro load_csv(table_name, path='', config='walmart') %}
    {% set cfg = var('copy_into_config').get(config) %}
    COPY INTO {{ cfg.target_schema }}.{{ table_name }}
    FROM @{{ cfg.from_stage }}/{{ path }}
    FILE_FORMAT = {{ cfg.file_format }}
    FORCE={{ cfg.force }}
    INCLUDE_METADATA = (loaded_at=METADATA$START_SCAN_TIME)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ;
{% endmacro %}
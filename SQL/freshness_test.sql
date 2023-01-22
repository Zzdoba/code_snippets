{% test freshness(model, column_name, interval, datepart, is_partition=False, window_limit=0) %}
    /*
    if your model is partitioned by some date/time column and you want to test this column
    set "is_partition: True" in model's .yml
    */
    {% if is_partition %}
    WITH source AS (
        SELECT DATETIME(MAX(PARSE_DATE("%Y%m%d",partition_id))) max_date,
        FROM {{model.database}}.{{ model.schema }}.INFORMATION_SCHEMA.PARTITIONS
        WHERE table_name = '{{ model.identifier }}'
        AND REGEXP_CONTAINS(partition_id, r'^\d+$')
    )

    {% else %}

    WITH source AS (
        SELECT
            DATETIME(MAX({{ column_name }})) AS max_date
        FROM {{ model }}
        {% if window_limit > 0 %}
        /* 
        if your model isn't partitioned but you want to limit the amount of data
        set  window_limit parameter in model's .yml
        */
        WHERE DATE({{ column_name }}) > DATE_SUB(CURRENT_DATE(), INTERVAL {{ window_limit }} DAY)
        {% endif %}
    )

    {% endif %}

    , mistakes AS (
        SELECT max_date
        FROM source
        WHERE max_date < DATETIME_SUB(CURRENT_DATETIME(), INTERVAL {{ interval }} {{ datepart }})
    )

    SELECT *    
    FROM mistakes

{% endtest %}
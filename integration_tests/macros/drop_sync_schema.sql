{% macro drop_sync_schema() %}
    {% set sync_schema=transform_dbt_sync.get_sync_schema() %}

    {% if adapter.check_schema_exists(target.database, sync_schema) %}
        {% set sync_schema_relation = api.Relation.create(database=target.database, schema=sync_schema).without_identifier() %}
        {% do drop_schema(sync_schema_relation) %}
        {% if adapter.type() != 'bigquery' %}
            {% do run_query("commit;") %}
        {% endif %}
        {{ dbt_utils.log_info("sync schema dropped")}}

    {% else %}
        {{ dbt_utils.log_info("sync schema does not exist so was not dropped") }}
    {% endif %}

{% endmacro %}

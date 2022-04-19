{% macro bigquery__log_sync_event(event_name, schema, relation, user, target_name, is_full_refresh) %}

    insert into {{ transform_dbt_sync.get_sync_relation() }} (
        event_name,
        event_timestamp,
        event_schema,
        event_model,
        event_target,
        event_is_full_refresh,
        invocation_id
    )

    values (
        '{{ event_name }}',
        {{ dbt_utils.current_timestamp_in_utc() }},
        {% if schema != None %}'{{ schema }}'{% else %}null{% endif %},
        {% if relation != None %}'{{ relation }}'{% else %}null{% endif %},
        {% if target_name != None %}'{{ target_name }}'{% else %}null{% endif %},
        {% if is_full_refresh %}TRUE{% else %}FALSE{% endif %},
        '{{ invocation_id }}'
    );

{% endmacro %}


{% macro bigquery__create_sync_log_table() -%}

    {% set required_columns = [
       ["event_name", dbt_utils.type_string()],
       ["event_timestamp", dbt_utils.type_timestamp()],
       ["event_schema", dbt_utils.type_string()],
       ["event_model", dbt_utils.type_string()],
       ["event_target", dbt_utils.type_string()],
       ["event_is_full_refresh", "BOOLEAN"],
       ["invocation_id", dbt_utils.type_string()],
    ] -%}

    {% set sync_table = transform_dbt_sync.get_sync_relation() -%}

    {% set sync_table_exists = adapter.get_relation(sync_table.database, sync_table.schema, sync_table.name) -%}


    {% if sync_table_exists -%}

        {%- set columns_to_create = [] -%}

        {# map to lower to cater for snowflake returning column names as upper case #}
        {%- set existing_columns = adapter.get_columns_in_relation(sync_table)|map(attribute='column')|map('lower')|list -%}

        {%- for required_column in required_columns -%}
            {%- if required_column[0] not in existing_columns -%}
                {%- do columns_to_create.append(required_column) -%}

            {%- endif -%}
        {%- endfor -%}


        {%- for column in columns_to_create -%}
            alter table {{ sync_table }}
            add column {{ column[0] }} {{ column[1] }}
            default null;
        {% endfor -%}

    {%- else -%}
        create table if not exists {{ sync_table }}
        (
        {% for column in required_columns %}
            {{ column[0] }} {{ column[1] }}{% if not loop.last %},{% endif %}
        {% endfor %}
        )
    {%- endif -%}

{%- endmacro %}
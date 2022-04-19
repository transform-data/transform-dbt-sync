{% macro get_sync_schema() %}

    {{ return('transform_dbt_sync')}}

{% endmacro %}

{% macro get_sync_relation() %}

    {%- set sync_schema=transform_dbt_sync.get_sync_schema() -%}

    {%- set sync_table =
        api.Relation.create(
            database=target.database,
            schema=sync_schema,
            identifier='transform_sync_log',
            type='table'
        ) -%}

    {{ return(sync_table) }}

{% endmacro %}


{% macro log_sync_event(event_name, schema, relation, user, target_name, is_full_refresh) -%}

  {{ return(adapter.dispatch('log_sync_event', 'transform_dbt_sync')(event_name, schema, relation, user, target_name, is_full_refresh)) }}

{% endmacro %}

{% macro default__log_sync_event(event_name, schema, relation, user, target_name, is_full_refresh) %}

    insert into {{ transform_dbt_sync.get_sync_relation() }} (
        event_name,
        event_timestamp,
        event_schema,
        event_model,
        event_user,
        event_target,
        event_is_full_refresh,
        invocation_id
    )

    values (
        '{{ event_name }}',
        {{ dbt_utils.current_timestamp_in_utc() }},
        {% if schema != None %}'{{ schema }}'{% else %}null::varchar(512){% endif %},
        {% if relation != None %}'{{ relation }}'{% else %}null::varchar(512){% endif %},
        {% if user != None %}'{{ user }}'{% else %}null::varchar(512){% endif %},
        {% if target_name != None %}'{{ target_name }}'{% else %}null::varchar(512){% endif %},
        {% if is_full_refresh %}TRUE{% else %}FALSE{% endif %},
        '{{ invocation_id }}'
    );

    commit;

{% endmacro %}


{% macro create_sync_schema() %}
    {%- set schema_name = transform_dbt_sync.get_sync_schema() -%}
    {%- set schema_exists = adapter.check_schema_exists(database=target.database, schema=schema_name) -%}
    {% if schema_exists == 0 %}
        {% do create_schema(api.Relation.create(
            database=target.database,
            schema=schema_name)
        ) %}
    {% endif %}
{% endmacro %}


{% macro create_sync_log_table() -%}

    {{ return(adapter.dispatch('create_sync_log_table', 'transform_dbt_sync')()) }}

{% endmacro %}


{% macro default__create_sync_log_table() -%}

    {% set required_columns = [
       ["event_name", dbt_utils.type_string()],
       ["event_timestamp", dbt_utils.type_timestamp()],
       ["event_schema", dbt_utils.type_string()],
       ["event_model", dbt_utils.type_string()],
       ["event_user", dbt_utils.type_string()],
       ["event_target", dbt_utils.type_string()],
       ["event_is_full_refresh", "boolean"],
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

        {%- if columns_to_create|length > 0 %}
            commit;
        {% endif -%}

    {%- else -%}
        create table if not exists {{ sync_table }}
        (
        {% for column in required_columns %}
            {{ column[0] }} {{ column[1] }}{% if not loop.last %},{% endif %}
        {% endfor %}
        )
    {%- endif -%}

{%- endmacro %}


{% macro log_model_end_event() %}
    {{ transform_dbt_sync.log_sync_event(
        'model deployment completed', schema=this.schema, relation=this.name, user=target.user, target_name=target.name, is_full_refresh=flags.FULL_REFRESH
    ) }}
{% endmacro %}

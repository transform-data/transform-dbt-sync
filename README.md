# Transform-dbt Sync
This package powers the Transform-dbt integration and is a detached fork of the
[dbt-labs/dbt-event-logging](https://github.com/dbt-labs/dbt-event-logging)
package. In this package we've hard coded the `schema.table` that the logs are
written to, altered the columns logged to fit our needs, removed logs
unnecessary for powering the Transform-dbt integration, and changed the package
name as to not conflict with the `dbt-labs/dbt-event-logging` in case you are
already using that.

> :note: **ADDING THIS PACKAGE TO YOUR DBT PROJECT CAN SLOW
> DOWN YOUR DBT RUNS**. This is due to the number of insert statements executed by
> this package as a post hook.

Requires dbt >= 1.0.0

This package provides out-of-the-box functionality to log events for all dbt
models successfully deployed. It outputs to the schema `transform_dbt_sync`.

### Setup

1. Include this package in your `packages.yml`

```YAML
# packages.yml
packages:
   ...
  - git: "https://github.com/transform-data/transform-dbt-sync"
    version: 0.1.0
```

2. Include the following in your `dbt_project.yml` directly within your
   `models:` block (making sure to handle indenting appropriately):

```YAML
# dbt_project.yml
...

models:
  ...
  post-hook: "{{ transform_dbt_sync.log_model_end_event() }}"
```

That's it! You're data warehouse will now have a log of when models were last
successfully built by dbt for Transform to integrate with.

### Adapter support

This package is currently compatible with dbt's BigQuery<sup>1</sup>, Snowflake, Redshift, and
Postgres integrations.

<sup>1</sup> BigQuery support may only work when 1 thread is set in your `profiles.yml` file. Anything larger may result in "quota exceeded" errors.  


### Contributing
Additional contributions to this repo are very welcome! Additionally if you are making performance improvements, consider contributing upstream to [dbt-labs/dbt-event-logging](https://github.com/dbt-labs/dbt-event-logging)

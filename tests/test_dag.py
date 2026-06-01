from dags.dag_selic_medallion import dag


def test_dag_structure_and_properties():
    # Assert DAG properties
    assert dag is not None
    assert dag.dag_id == "dag_selic_medallion"
    assert dag.catchup is False
    assert getattr(dag, "schedule", None) is None or getattr(dag, "schedule_interval", None) is None

    # Assert Tasks presence
    assert dag.has_task("ingest_bronze")
    assert dag.has_task("transform_silver")
    assert dag.has_task("aggregate_gold")

    # Assert Task sequence
    ingest_task = dag.get_task("ingest_bronze")
    transform_task = dag.get_task("transform_silver")
    aggregate_task = dag.get_task("aggregate_gold")

    assert transform_task in ingest_task.downstream_list
    assert aggregate_task in transform_task.downstream_list

    # Assert retry and retry delay policies
    assert dag.default_args.get("retries") == 2
    assert dag.default_args.get("retry_delay").total_seconds() == 300

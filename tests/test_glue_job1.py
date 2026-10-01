def test_job1_uses_run_date_and_five_partitions():
    p = open("glue/snowflake_to_s3/snowflake_to_s3.py").read()
    assert '"JOB_NAME", "run_date"' in p
    assert 'raw/{run_date}/' in p
    assert 'repartition(5)' in p

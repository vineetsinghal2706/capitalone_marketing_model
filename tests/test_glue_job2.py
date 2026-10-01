def test_job2_uses_date_paths_and_five_partitions():
    p = open("glue/preprocessing/marketing-model-preprocessing.py").read()
    assert 'raw/{run_date}/' in p
    assert 'processed/marketing_features/{run_date}/' in p
    assert 'repartition(5)' in p

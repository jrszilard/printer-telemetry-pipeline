from telemetry.bench import benchmark_size


def test_benchmark_proves_noop_pruning_and_parser_repair(tmp_path):
    result = benchmark_size(tmp_path / "benchmark", printers=30, days=3, repetitions=2, reprocess=True)
    assert result["noop_verified"]
    assert result["ground_truth_jobs_verified"]
    assert result["clean_duplicate_keys"] == 0
    assert result["query"]["identical_results"]
    assert result["query"]["pruned_files"] < result["query"]["total_files"]
    assert result["parser_repair"]["job_count_unchanged"]
    assert result["parser_repair"]["extra_parsed_events"] > 0

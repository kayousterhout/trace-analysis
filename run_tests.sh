rm test_files/agg_out*
python parse_logs.py test_files/test_job_log info test_files/agg_out

echo "Checking normalized_runtime_hdfs output file (should see no output)"
diff test_files/agg_out_normalized_runtimes_hdfs test_files/expected_agg_out_normalized_runtimes_hdfs

echo "Checking normalized_runtime_hdfs_non_local output file (should see no output)"
diff test_files/agg_out_normalized_runtimes_hdfs_non_local test_files/expected_agg_out_normalized_runtimes_hdfs_non_local

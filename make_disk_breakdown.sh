echo Parsing agg_results file in $1 and writing output with prefix $2
python plot_cdf.py $1/agg_results 4 $2_no_disk
python plot_cdf.py $1/agg_results 13 $2_no_disk_input
python plot_cdf.py $1/agg_results 16 $2_no_disk_shuffle_write
python plot_cdf.py $1/agg_results 17 $2_no_disk_shuffle_read

average_run()
{
	hdfs dfs -copyFromLocal $1 $2
	hadoop jar ZValueTest.jar AverageCalculator $2 $3
}

variance_run()
{
	hadoop jar ZValueTest.jar VarianceCalculator $1 $2 $3
}

z_test_run()
{
	hadoop jar ZValueTest.jar ZValueTest $1 $2 $3 $4
}

average_run '/Afif/input/data0.in' '/Afif/output/average_output'

variance_run '0.0' '/Afif/input/data0.in' '/Afif/output/variance_output'

z_test_run '0.0' '1.0' '/Afif/input/data.in' '/Afif/output/z_output'
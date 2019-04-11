hadoop dfs -rmr /user/root/iotspark.jar
hadoop dfs -put iotspark.jar /user/root/
spark-submit   --class com.dcits.spark.RuleEngine --master spark://dsj2:6066   --deploy-mode cluster   --supervise   --name ruleEngine   --num-executors 1  --executor-memory 5G   --executor-cores 5   --total-executor-cores 5  --driver-memory 2G hdfs:///user/root/iotspark.jar
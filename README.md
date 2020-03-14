# pubanalysis
pub analysis in England 


Prerequisites,
 - Upload the input data to s3 bucket
 - Install AWS cli
 - Upload python script to s3
 

Run below command to run on AWS EMR cluster


aws emr create-cluster --name "EMRCluster" --release-label emr-5.29.0 --applications Name=Hive Name=Spark --use-default-roles  --instance-type m5.xlarge --instance-count 3 --steps '[{"Args":["spark-submit","s3://<path>/pubextract.py","--input","s3://<inputpath>","--output","s3://<outputpath>/"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark Job"}]' 

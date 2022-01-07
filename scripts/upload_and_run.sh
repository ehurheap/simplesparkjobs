#upload_and_run.sh -j local-path-to-jar -i input-path -o output-path
#upload_and_run.sh -j ./target/scala-2.11/simple-spark-jobs_2.11-1.0.jar -i "s3://heap-platform-nextgen-data/3064244106/definedevents/spark/1633737600000/1637077513563/" -o "s3://heap-platform-nextgen-data/3064244106/definedevents/spark/parquet"
#s3://heap-platform-nextgen-data/3064244106/definedevents/spark/1633737600000/1637077513563/ has 94GB of defined events in avro
#
while getopts "j:i:o:" flag
 do
     case "${flag}" in
         j) jarPath=${OPTARG};;
         i) inputPath=${OPTARG};;
         o) outputPath=${OPTARG};;
     esac
 done

if (( $# < 1 ))
then
        echo
        echo "         USAGE: $0 -j <jarPath> -i <inputPath> -o <outputPath>"
        echo "                -j <jarPath> is required, others optional."
        exit
fi
jarBucket="s3://heap-platform-nextgen-data/jars"
jar="$jarBucket/ReadAvroWriteParquet.jar"

cmd="aws s3 cp ${jarPath} ${jar}"
echo "running command ${cmd}"
${cmd}

clusterId="j-3CO2SQEQNH3I"
if [[ -z ${inputPath} ]]; then
   INPUTPATH="";
else
  INPUTPATH=",${inputPath}"
fi
if [[ -z ${outputPath} ]]; then
   OUTPUTPATH="";
else
  OUTPUTPATH=",${outputPath}"
fi
echo "submitting job to cluster ${clusterId}"
echo "     jar: ${jar}"

aws emr add-steps --region us-east-1 --cluster-id ${clusterId} --steps \
Type=Spark,\
Name=ReadAvroWriteParquet,\
ActionOnFailure=CONTINUE,\
Args=[\
--deploy-mode,cluster,\
--master,yarn,\
--conf,spark.yarn.submit.waitAppCompletion=true,\
--num-executors,20,\
--executor-cores,4,\
--executor-memory,8g,\
--driver-memory,8g,\
--packages,org.apache.spark:spark-avro_2.11:2.4.8,\
--class,com.heap.nextgen.ReadAvroWriteParquet,\
${jar}${INPUTPATH}${OUTPUTPATH}\
]

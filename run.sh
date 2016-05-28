rm *.class
HBASE_LIB1="/usr/local/hbase/lib/*"
# HBASE_LIB1 = "/usr/local/hadoop/share/hadoop/mapreduce/*"
HBASE_LIB2="/usr/local/hadoop/share/hadoop/common/lib/*"
if [ $# -lt 1 ]
then
	echo "Argument required"
	exit
fi
filename=$1
classname=${filename%.*}
param1=$2
param2=$3
param3=$4
param4=$5
echo "Start compiling..." $filename &&
javac -Xlint -cp ".:$HBASE_LIB2:$HBASE_LIB1" $filename &&
echo "Compiled, Running........." $ &&

java -cp ".:$HBASE_LIB2:$HBASE_LIB1" $classname $param1 $param2 $param3 $param4 &&
echo "Finished Succesfully!"

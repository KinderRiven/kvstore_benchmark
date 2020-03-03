YCSB_BIN=bin/ycsb
WORKLOADS=workloads
RESULT_PATH=result

mkdir $RESULT_PATH

$YCSB_BIN load mapkeeper -s -P $WORKLOADS/workloada -p "mapkeeper.host=127.0.0.1" -p "mapkeeper.port=9090" > $RESULT_PATH/workloada_load

$YCSB_BIN  run mapkeeper -s -P $WORKLOADS/workloada -p "mapkeeper.host=127.0.0.1" -p "mapkeeper.port=9090" > $RESULT_PATH/workloada_run

$YCSB_BIN  run mapkeeper -s -P $WORKLOADS/workloadb -p "mapkeeper.host=127.0.0.1" -p "mapkeeper.port=9090" > $RESULT_PATH/workloadb

$YCSB_BIN  run mapkeeper -s -P $WORKLOADS/workloadc -p "mapkeeper.host=127.0.0.1" -p "mapkeeper.port=9090" > $RESULT_PATH/workloadc

$YCSB_BIN  run mapkeeper -s -P $WORKLOADS/workloadd -p "mapkeeper.host=127.0.0.1" -p "mapkeeper.port=9090" > $RESULT_PATH/workloadd

$YCSB_BIN  run mapkeeper -s -P $WORKLOADS/workloadf -p "mapkeeper.host=127.0.0.1" -p "mapkeeper.port=9090" > $RESULT_PATH/workloadf

# $YCSB_BIN load mapkeeper -s -P $WORKLOADS/workloade -p "mapkeeper.host=127.0.0.1" -p "mapkeeper.port=9090" > $RESULT_PATH/workloade_load

# $YCSB_BIN  run mapkeeper -s -P $WORKLOADS/workloade -p "mapkeeper.host=127.0.0.1" -p "mapkeeper.port=9090" > $RESULT_PATH/workloade_run

# EXEC=${1}
# KVSTORE=(../leveldb/exec/test)
KVSTORE=(../leveldb/exec/test ../rocksdb/exec/test ../novelsm/exec/test)
NAME=(leveldb rocksdb novelsm)

# DB=${2}
DB=/home/hanshukai/ssd/dbtmp

# NVM=${3}
NVM=/home/pmem0

# VALUE_LENGTH=${4}
ARR_VALUE_LENGTH=(1024)

# NUM_WARM
WARM_SIZE=$((100*1024*1024*1024))

# NUM_PUT
PUT_SIZE=0

# NUM_GET
GET_SIZE=0

# NUM_SCAN
SCAN_COUNT=0

# SCAN_RANGE
SCAN_RANGE=0

# WRITE_BUFFER_SIZE=${10}
WRITE_BUFFER_SIZE=64

# NVM_BUFFER_SIZE=${11}
NVM_BUFFER_SIZE=8192

# MAX_FILE_SIZE=${12}
MAX_FILE_SIZE=64

# BLOOM_BITS=${13}

BLOOM_BITS=10

# RESULT_SAVE=${14}

for ((i=0; i<${#KVSTORE[*]}; i+=1))
do

for ((k=0; k<${#ARR_VALUE_LENGTH[*]}; k++))
do

OUTPUT=${NAME[$i]}_result/${ARR_VALUE_LENGTH[$k]}
mkdir -p $OUTPUT

VALUE_LENGTH=${ARR_VALUE_LENGTH[$k]}
NUM_WARM=$((WARM_SIZE / VALUE_LENGTH))
NUM_PUT=$((PUT_SIZE / VALUE_LENGTH))
NUM_GET=$((GET_SIZE / VALUE_LENGTH))
echo "[DB:${NAME[$i]}][VALUE_LENGTH:${VALUE_LENGTH}][WARM:${NUM_WARM}][PUT:${NUM_PUT}][GET:${NUM_GET}][SCAN/RANGE:${SCAN_COUNT}|${SCAN_RANGE}]"

if (($NUM_PUT > 0 || $NUM_GET > 0));
then
echo "running ${NAME[$i]} warmup..."
./run.sh ${KVSTORE[$i]} $DB_${NAME[$i]} $NVM $VALUE_LENGTH $NUM_WARM $NUM_PUT $NUM_GET 0 0 $WRITE_BUFFER_SIZE $NVM_BUFFER_SIZE $MAX_FILE_SIZE $BLOOM_BITS $OUTPUT/randomwrite
done
done
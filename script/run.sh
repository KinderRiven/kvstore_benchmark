EXEC=${1}
DB=${2}
NVM=${3}
VALUE_LENGTH=${4}
NUM_WARM=${5}
NUM_PUT=${6}
NUM_GET=${7}
NUM_SCAN=${8}
SCAN_RANGE=${9}
WRITE_BUFFER_SIZE=${10}
NVM_BUFFER_SIZE=${11}
MAX_FILE_SIZE=${12}
BLOOM_BITS=${13}
RESULT_SAVE=${14}

./$EXEC --db=$DB --nvm=$NVM --key_length=16 --value_length=$VALUE_LENGTH \
--num_warm=$NUM_WARM --num_put=$NUM_PUT --num_get=$NUM_GET --num_scan=$NUM_SCAN --scan_range=$SCAN_RANGE \
--write_buffer_size=$WRITE_BUFFER_SIZE --nvm_buffer_size=$NVM_BUFFER_SIZE --max_file_size=$MAX_FILE_SIZE \
--bloom_bits=$BLOOM_BITS > $RESULT_SAVE
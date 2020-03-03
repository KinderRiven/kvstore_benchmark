KVSTORE=(../leveldb/exec/test ../rocksdb/exec/test)
NAME=(leveldb rocksdb)

for ((i=0; i<${#KVSTORE[*]}; i+=1))
do
echo "running ${NAME[$i]}..."
./run.sh ${KVSTORE[$i]} dbtmp nvm 1024 100000 0 100000 0 0 64 2 10 ${NAME[$i]}.result
rm -rf dbtmp
done
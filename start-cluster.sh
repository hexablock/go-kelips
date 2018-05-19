#!/bin/bash
#
# start-cluster.sh starts the given number of cluster nodes. The default is 3
#
# Usage:
#   ./scripts/start-cluster.sh 4 -debug
#
set -e

BIN="kelipsd"

COUNT=${1:-3}

ADV_PORT="43210"
HTTP_PORT="7070"

args="$@"
DEFAULT_ARGS="${args[@]:1} -adv-addr 127.0.0.1:"
CMD="${BIN} ${DEFAULT_ARGS}"


peers=""
set_peers() {
    peers=""
    self=$1
    for i in `seq 0 $COUNT`; do
        if [ ${i} -eq ${self} ]; then continue; fi
        port=`expr ${ADV_PORT} \+ $i`
        peers="127.0.0.1:${port},${peers}"
    done
}

[[ ! -e ${BIN} ]] && { echo "$BIN not found!"; exit 1; }

echo -e "\nStarting ${COUNT} nodes ...\n"

count=`expr ${COUNT} \- 1`
for i in `seq 0 $count`; do
    adv_port=`expr ${ADV_PORT} \+ $i`
    join_port=`expr ${adv_port} \- $i`
    http_port=`expr ${HTTP_PORT} \+ $i`

    set_peers $i
    ./${CMD}${adv_port} -join "${peers}" -http-addr 127.0.0.1:${http_port} &

    sleep 1;
done

trap "{ pkill -9 ${BIN}; }" SIGINT SIGTERM
wait

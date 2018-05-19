#!/bin/bash
#
# start-cluster.sh starts the given number of cluster nodes. The default is 3
#
# Usage:
#   ./scripts/start-cluster.sh 4 -debug
#
set -e

COUNT=${1:-2}
BIN="kelipsd"

[[ ! -e ${BIN} ]] && { echo "$BIN not found!"; exit 1; }

sb="43210"
hport="7070"
bport="42100"

args="$@"
DEFAULT_ARGS="${args[@]:1} -adv-addr 127.0.0.1:"
CMD="${BIN} ${DEFAULT_ARGS}"

total=`expr $COUNT \+ 1`
echo -e "\nStarting ${total} nodes ...\n"

for i in `seq 0 $COUNT`; do
    hp=`expr $hport \+ $i`
    jp=`expr $sb \+ $i`
    bp=`expr $bport \+ $i`
    ./${CMD}$jp -join 127.0.0.1:$sb -http-addr 127.0.0.1:$hp &
    sleep 1;
done

trap "{ pkill -9 ${BIN}; }" SIGINT SIGTERM
wait

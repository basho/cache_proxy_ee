#!/bin/sh
DIR=$(dirname $0)
source $DIR/include_common.sh

usage () {
    echo "Usage: $0 COMMAND NODE_NAME1 NODE_NAME2 .. NODE_NAMEn"
    echo "Commands:"
    echo "start - start all nodes"
    echo "stop  - stop all nodes"
    echo "ping  - ping all nodes"
}

RIAK_COMMAND=$1
shift
NODE_NAMES=$@

if [ "$NODE_NAMES" = "" ]; then
    usage && exit 1
fi
for node_name in $NODE_NAMES; do
    case $RIAK_COMMAND in
        start)
            retry_cmd 5 riak_start $node_name
            RET=$?
            ;;
        stop)
            retry_cmd 5 riak_stop $node_name
            RET=$?
            ;;
        ping)
            retry_cmd 5 riak_ping $node_name
            RET=$?
            ;;
    esac
done
for node_name in $NODE_NAMES; do
    case $RIAK_COMMAND in
        start)
            retry_cmd 5 riak_start_postwait $node_name
            RET=$?
            ;;
        stop)
            riak_kill_stragglers $node_name
            RET=$?
            ;;
    esac
done
exit "$RET"

#!/bin/sh
DIR=$(dirname $0)
. "$DIR/include_common.sh"
usage () {
    echo "Usage: $0 NODE_NAME1 NODE_NAME2 .. NODE_NAMEn"
}

NODE_NAMES=$@
if [ -z "$NODE_NAMES" ]; then
    usage && exit 1
fi
for node_name in $NODE_NAMES; do
    retry_cmd 5 riak_start $node_name
done
# HACK: hitting the head node w/ a "cluster status" admin command sporadically
# crashes the node, so sleep :/
sleep 5
HEAD_NODE=""
for node_name in $NODE_NAMES; do
    if [ -z $HEAD_NODE ]; then
        HEAD_NODE=$node_name
    fi
    retry_cmd 5 riak_join_plan $HEAD_NODE $node_name
done
retry_cmd 5 riak_ring_ready $HEAD_NODE "$NODE_NAMES"
retry_cmd 5 riak_join_commit $HEAD_NODE
retry_cmd 100 riak_transfer_complete $HEAD_NODE

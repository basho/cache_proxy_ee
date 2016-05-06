DIR=$(dirname $0)
TMP_BASE=/tmp/r/riak_devrel_

retry_cmd () {
    local retries=$1
    local retry_delay=5
    shift
    local cmd="$*"
    [ $# -le 1 ] && {
        echo "Usage $0 <retry_number> <command>"
    }
    while [ $retries -gt 0 ]; do
        $cmd && break || {
        let retries-=1
        sleep $retry_delay
        }
    done
}

riak_is_deployed () {
    local node=$1
    if [ -e $TMP_BASE$node/etc ]; then ret=0; else ret=1; fi
    return $ret
}

riak_ping () {
    local node=$1
    $TMP_BASE$node/bin/riak ping
    local ret=$?
    return $ret
}

riak_admin_test () {
    local node=$1
    $TMP_BASE$node/bin/riak-admin test
    ret=$?
    return $ret
}

riak_start () {
    local node=$1
    local ret=0
    if ! riak_ping $node; then
        $TMP_BASE$node/bin/riak start
        ret=$?
    fi
    return $ret
}

riak_stop () {
    local node=$1
    local ret=0
    if riak_ping $node; then
        $TMP_BASE$node/bin/riak stop
        ret=$?
    fi
    return $ret
}

riak_join_plan () {
    local head_node=$1
    local joining_node=$2
    if [ "$head_node" = "$joining_node" ]; then
        return 0
    fi
    local cluster_status=$(riak_cluster_status $head_node)
    if echo "$cluster_status" |grep $joining_node >/dev/null 2>&1; then
        #already a member, nop
        return 0
    fi

    local join_result=$($TMP_BASE$joining_node/bin/riak-admin cluster join $head_node)
    # local ret=$? #<< HACK: cluster join exit code is not precisely accurate
    local ret=1
    case "$join_result" in
        *Unable*to*get*ring*)
            exit 513
            ;;
        *not*reachable*)
            exit 520
            ;;
        *staged*join*)
            ret=0
            ;;
        *)
            ret=1
            ;;
    esac
    return $ret
}

riak_leave_plan () {
    local $head_node=$1
    local $leaving_node=$2
    if [ "$head_node" = "$joining_node" ]; then
        return 0
    fi

    local cluster_status=$(riak_cluster_status $head_node)
    if ! echo "$cluster_status" |grep $leaving_node >/dev/null 2>&1; then
        # not already a member, nop
        return 0
    fi

    local leave_result=$($TMP_BASE$leaving_node/bin/riak-admin cluster leave $head_node)
    # local ret=$? #<< HACK: cluster leave exit code is not precisely accurate
    local ret=1
    case "$leave_result" in
        *staged*leave*request*)
            ret=0
            ;;
    esac
    return $ret
}

riak_join_commit () {
    local head_node=$1
    local ret=0
    if $TMP_BASE$head_node/bin/riak-admin cluster plan; then
        ret=$?
        return $ret
    fi

    $TMP_BASE$head_node/bin/riak-admin cluster commit
    ret=$?
}

riak_leave_commit () {
    local head_node=$1
    local ret=0
    if $TMP_BASE$head_node/bin/riak-admin cluster plan; then
        ret=$?
        return $ret
    fi

    $TMP_BASE$head_node/bin/riak-admin cluster commit
    ret=$?
}

riak_cluster_status () {
    local head_node=$1
    $TMP_BASE$head_node/bin/riak-admin cluster status --format=csv
}

riak_transfer_complete () {
    local head_node=$1
    if riak_cluster_status |grep '[-]-' >/dev/null 2>&1; then
        return 1
    fi
    if riak_cluster_status |grep 'joining' >/dev/null 2>&1; then
        return 1
    fi
    if riak_cluster_status |grep 'leaving' >/dev/null 2>&1; then
        return 1
    fi
}

riak_cluster_grep_predicate_all_nodes () {
    local grep_predicate_pattern="$1"
    local rcs="$2" #<< riak cluster status
    local node="$3"
    local ts_event_key=${4:-rcsg}

    local unmatched_nodes=0
    for node in $nodes; do
        local grep_predicate=$(echo $grep_predicate_pattern |sed -e "s/\<node\>/${node}/g")
        if echo "$rcs" |grep $grep_predicate >/dev/null 2>&1; then
            let unmatched_nodes+=1
        fi
    done
    return $unmatched_nodes
}

riak_cluster_contains_all_nodes () {
    riak_cluster_grep_predicate_all_nodes "<node>" "$@" "cluster-contains"
}
riak_cluster_is_up_all_nodes () {
    riak_cluster_grep_predicate_all_nodes "<node>.*up" "$@" "cluster-up"
}
riak_cluster_is_transfer_complete_all_nodes () {
    riak_cluster_grep_predicate_all_nodes "<node>" "$@" "cluster-transfer_complete"
}
riak_cluster_is_joined_all_nodes () {
    riak_cluster_grep_predicate_all_nodes "<node>.*valid" "$@" "cluster-valid"
}
riak_cluster_is_valid () {
    local nodes=$@
    local head_node=""
    for node in $nodes; do head_node=$node && break; done
    local cluster_status=$(retry_cmd 5 riak_cluster_status $head_node)
    riak_cluster_contains_all_nodes "$cluster_status" "$NODE_NAMES" &&
        riak_cluster_is_up_all_nodes "$cluster_status" "$NODE_NAMES" &&
        riak_cluster_is_transfer_complete_all_nodes "$cluster_status" "$NODE_NAMES" &&
        riak_cluster_is_joined_all_nodes "$cluster_status" "$NODE_NAMES"
}

riak_ring_ready () {
    local head_node=$1
    local $nodes="$@"
    local ring_ready_result=$($TMP_BASE$head_node/bin/riak-admin ringready)
    local ret=$?
    if [ $ret != 0 ]; then
        return $ret
    fi
    local not_in_ring=0
    for node in $nodes; do
        if ! echo "$ring_ready_result" |grep $node >/dev/null 2>&1; then
            let not_in_ring+=1
        fi
    done
    return $not_in_ring
}

riak_kill_stragglers () {
    local node=$1
    local pids=$(ps aux |grep [r]iak.*$node |awk '{print $2}')
    if [ ! -z "$pids" ]; then
        # give time to shutdown cleanly
        sleep 1
        pids=$(ps aux |grep "[r]iak.*$node.*beam" |awk '{print $2}')
        for pid in $pids; do
            kill $pid >/dev/null 2>&1
        done
    fi
    # kill epmd for good measure
    pids=$(ps aux |grep "[r]iak.*epmd" |awk '{print $2}')
    for pid in $pids; do
        kill $pid >/dev/null 2>&1
    done
    return 0
}

riak_start_postwait () {
    local node=$1
    riak_admin_test $node
}

riak_cluster_list_all_nodes () {
    local cluster_status="$1"
    echo "$cluster_status" |awk -F ',' \
        '
BEGIN { nodes="" }
/\(C\).*@/{ node=$1; gsub(/\(C\)[ ]+/, "", node); gsub(/@.*/, "", node) t=nodes; nodes=t " " node; }
/!\(C\).*@/{ node=$1;                             gsub(/@.*/, "", node) t=nodes; nodes=t " " node; }
END { print nodes }'
}

node_list_contains_node_list () {
    local expecting_nodes="$1"
    local actual_nodes="$2"
    local match=1
    for node in $expecting_nodes; do
        echo "$actual_nodes" |grep "$node" >/dev/null 2>&1 | {
            match=0
            break
        }
    done
}

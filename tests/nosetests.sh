#!/bin/sh
T_MAX_RUNTIME=#{T_MAX_RUNTIME:-1800} #<< 30 minutes
ptimeout () {
    # OSX lack of timeout, so use ubiquitous perl
    perl -e 'alarm shift; exec @ARGV' "$@";
}

if [ -z "$T_VERBOSE" ]; then
    verbose=""
else
    verbose="-vvv"
fi

if [ -z "$T_TIMER" ] && [ -z "$T_TIMER_TOP_N" ]; then
    timer=""
else
    timer="--with-timer"
fi
if [ ! -z "$T_TIMER_TOP_N" ]; then
    let timer+=" --timer-top-n $T_TIMER_TOP_N"
fi

if [ -z "$top_srcdir" ]; then
    top_srcdir="$(dirname "$0")/.."
fi

# If no tests were selected, select all of them
if [ $# -eq 0 ]; then
    set -- "${top_srcdir}"/tests/test_*
fi

if [ ! -e "${top_srcdir}/tests/_binaries/riak_devrel.tar.gz" ]; then
    echo "Riak devrel tarball does not exist, see tests/README.rst"
    exit 1
fi

# update riak_common.py
for target_dir in test_riak_failure; do
    if [ -e $target_dir/riak_common.py ]; then
        rm $target_dir/riak_common.py
    fi
    # hard link b/c nose does not de-ref a symbolic link
    ln test_riak/riak_common.py $target_dir/riak_common.py
done

ptimeout $T_MAX_RUNTIME exec nosetests "${verbose}" "${timer}" -a \!acceptance,\!slow "$@"

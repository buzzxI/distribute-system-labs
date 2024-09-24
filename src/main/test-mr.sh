#!/usr/bin/env bash

# Print environment variables and current directory
echo "Environment Variables: $(env)" >> debug_log.txt
echo "Current Directory: $(pwd)" >> debug_log.txt

#
# map-reduce tests
#

# un-comment this to run the tests with the Go race detector.
# RACE=-race

if [[ "$OSTYPE" = "darwin"* ]]
then
  if go version | grep 'go1.17.[012345]'
  then
    # -race with plug-ins on x86 MacOS 12 with
    # go1.17 before 1.17.6 sometimes crash.
    RACE=
    echo '*** Turning off -race since it may not work on a Mac'
    echo '    with ' `go version`
  fi
fi

ISQUIET=$1
maybe_quiet() {
    if [ "$ISQUIET" = "quiet" ]; then
      "$@" > /dev/null 2>&1
    else
      "$@"
    fi
}

# Debugging: Print the commands being run by maybe_quiet
# maybe_quiet() {
#     echo "invoke maybe_quiet"
#     if [ "$ISQUIET" = "quiet" ]; then
#       echo "Running (quiet): $@"
#       "$@" > /dev/null 2>&1
#     else
#       echo "Running: $@"
#       "$@"
#     fi
# }


# TIMEOUT=timeout
# TIMEOUT2=""
# if timeout 2s sleep 1 > /dev/null 2>&1
# then
#   :
# else
#   if gtimeout 2s sleep 1 > /dev/null 2>&1
#   then
#     TIMEOUT=gtimeout
#   else
#     # no timeout command
#     TIMEOUT=
#     echo '*** Cannot find timeout command; proceeding without timeouts.'
#   fi
# fi
# if [ "$TIMEOUT" != "" ]
# then
#   TIMEOUT2=$TIMEOUT
#   TIMEOUT2+=" -k 2s 120s "
#   TIMEOUT+=" -k 2s 45s "
# fi


# Check if timeout command is available
TIMEOUT=("timeout" "-k" "2s" "45s")
if ! command -v timeout &> /dev/null
then
    if command -v gtimeout &> /dev/null
    then
        TIMEOUT=("gtimeout" "-k" "2s" "45s")
    else
        echo '*** Cannot find timeout command; proceeding without timeouts.'
        TIMEOUT=("")
    fi
fi

# Debugging: Print the value of TIMEOUT
# echo "TIMEOUT: ${TIMEOUT[@]}"

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

# make sure software is freshly built.
(cd ../../mrapps && go clean)
(cd .. && go clean)
(cd ../../mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin indexer.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin mtiming.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin rtiming.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin jobcount.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin early_exit.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin crash.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin nocrash.go) || exit 1
(cd .. && go build $RACE mrcoordinator.go) || exit 1
(cd .. && go build $RACE mrworker.go) || exit 1
(cd .. && go build $RACE mrsequential.go) || exit 1

failed_any=0

#########################################################
# first word-count

# generate the correct output
../mrsequential ../../mrapps/wc.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out*

echo '***' Starting wc test.

# maybe_quiet $TIMEOUT ../mrcoordinator ../pg*txt &
# Example usage of maybe_quiet with timeout
if [ -n "${TIMEOUT[0]}" ]; then
    maybe_quiet "${TIMEOUT[@]}" ../mrcoordinator ../pg*txt &
else
    maybe_quiet ../mrcoordinator ../pg*txt &
fi

pid=$!

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
# (maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) &
# (maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) &
# (maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) &


# Example usage of maybe_quiet with timeout for mrworker
if [ -n "${TIMEOUT[0]}" ]; then
    maybe_quiet "${TIMEOUT[@]}" ../mrworker ../../mrapps/wc.so &
    maybe_quiet "${TIMEOUT[@]}" ../mrworker ../../mrapps/wc.so &
    maybe_quiet "${TIMEOUT[@]}" ../mrworker ../../mrapps/wc.so &
else
    maybe_quiet ../mrworker ../../mrapps/wc.so &
    maybe_quiet ../mrworker ../../mrapps/wc.so &
    maybe_quiet ../mrworker ../../mrapps/wc.so &
fi

# wait for the coordinator to exit.
wait $pid

# since workers are required to exit when a job is completely finished,
# and not before, that means the job has finished.
sort mr-out* | grep . > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi

# wait for remaining workers and coordinator to exit.
wait

#########################################################
# now indexer

# compare the result
rm -f mr-*

# generate the correct output
../mrsequential ../../mrapps/indexer.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-indexer.txt
rm -f mr-out*

echo '***' Starting indexer test.

maybe_quiet $TIMEOUT ../mrcoordinator ../pg*txt &
sleep 1

# start multiple workers
maybe_quiet $TIMEOUT ../mrworker ../../mrapps/indexer.so &
maybe_quiet $TIMEOUT ../mrworker ../../mrapps/indexer.so

sort mr-out* | grep . > mr-indexer-all
if cmp mr-indexer-all mr-correct-indexer.txt
then
  echo '---' indexer test: PASS
else
  echo '---' indexer output is not the same as mr-correct-indexer.txt
  echo '---' indexer test: FAIL
  failed_any=1
fi

wait

#########################################################
echo '***' Starting map parallelism test.

rm -f mr-*

maybe_quiet $TIMEOUT ../mrcoordinator ../pg*txt &
sleep 1

maybe_quiet $TIMEOUT ../mrworker ../../mrapps/mtiming.so &
maybe_quiet $TIMEOUT ../mrworker ../../mrapps/mtiming.so

NT=`cat mr-out* | grep '^times-' | wc -l | sed 's/ //g'`
if [ "$NT" != "2" ]
then
  echo '---' saw "$NT" workers rather than 2
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

if cat mr-out* | grep '^parallel.* 2' > /dev/null
then
  echo '---' map parallelism test: PASS
else
  echo '---' map workers did not run in parallel
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

wait

#########################################################
echo '***' Starting reduce parallelism test.

rm -f mr-*

maybe_quiet $TIMEOUT ../mrcoordinator ../pg*txt &
sleep 1

maybe_quiet $TIMEOUT ../mrworker ../../mrapps/rtiming.so  &
maybe_quiet $TIMEOUT ../mrworker ../../mrapps/rtiming.so

NT=`cat mr-out* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
if [ "$NT" -lt "2" ]
then
  echo '---' too few parallel reduces.
  echo '---' reduce parallelism test: FAIL
  failed_any=1
else
  echo '---' reduce parallelism test: PASS
fi

wait

#########################################################
echo '***' Starting job count test.

rm -f mr-*

maybe_quiet $TIMEOUT ../mrcoordinator ../pg*txt  &
sleep 1

maybe_quiet $TIMEOUT ../mrworker ../../mrapps/jobcount.so &
maybe_quiet $TIMEOUT ../mrworker ../../mrapps/jobcount.so
maybe_quiet $TIMEOUT ../mrworker ../../mrapps/jobcount.so &
maybe_quiet $TIMEOUT ../mrworker ../../mrapps/jobcount.so

NT=`cat mr-out* | awk '{print $2}'`
if [ "$NT" -eq "8" ]
then
  echo '---' job count test: PASS
else
  echo '---' map jobs ran incorrect number of times "($NT != 8)"
  echo '---' job count test: FAIL
  failed_any=1
fi

wait

#########################################################
# test whether any worker or coordinator exits before the
# task has completed (i.e., all output files have been finalized)
rm -f mr-*

echo '***' Starting early exit test.

DF=anydone$$
rm -f $DF

(maybe_quiet $TIMEOUT ../mrcoordinator ../pg*txt; touch $DF) &
COORD_PID=$!

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/early_exit.so; touch $DF) &
WORKER1_PID=$!
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/early_exit.so; touch $DF) &
WORKER2_PID=$!
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/early_exit.so; touch $DF) &
WORKER3_PID=$!

# wait for any of the coord or workers to exit.
# `jobs` ensures that any completed old processes from other tests
# are not waited upon.
# jobs &> /dev/null
# if [[ "$OSTYPE" = "darwin"* ]]
# then
#   # bash on the Mac doesn't have wait -n
#   while [ ! -e $DF ]
#   do
#     sleep 0.2
#   done
# else
#   # the -n causes wait to wait for just one child process,
#   # rather than waiting for all to finish.
#   wait -n
# fi

# Wait for any of the coordinator or workers to exit (zsh does not support wait -n)
while true; do
  if ! kill -0 $COORD_PID 2>/dev/null; then
    echo "Coordinator exited"
    break
  elif ! kill -0 $WORKER1_PID 2>/dev/null; then
    echo "Worker 1 exited"
    break
  elif ! kill -0 $WORKER2_PID 2>/dev/null; then
    echo "Worker 2 exited"
    break
  elif ! kill -0 $WORKER3_PID 2>/dev/null; then
    echo "Worker 3 exited"
    break
  fi
done

rm -f $DF

# a process has exited. this means that the output should be finalized
# otherwise, either a worker or the coordinator exited early
sort mr-out* | grep . > mr-wc-all-initial

# wait for remaining workers and coordinator to exit.
wait

# compare initial and final outputs
sort mr-out* | grep . > mr-wc-all-final
if cmp mr-wc-all-final mr-wc-all-initial
then
  echo '---' early exit test: PASS
else
  echo '---' output changed after first worker exited
  echo '---' early exit test: FAIL
  failed_any=1
fi
rm -f mr-*

#########################################################
echo '***' Starting crash test.

# generate the correct output
../mrsequential ../../mrapps/nocrash.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-crash.txt
rm -f mr-out*

rm -f mr-done
((maybe_quiet $TIMEOUT2 ../mrcoordinator ../pg*txt); touch mr-done ) &
sleep 1

# start multiple workers
maybe_quiet $TIMEOUT2 ../mrworker ../../mrapps/crash.so &

# mimic rpc.go's coordinatorSock()
SOCKNAME=/var/tmp/5840-mr-`id -u`

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    maybe_quiet $TIMEOUT2 ../mrworker ../../mrapps/crash.so
    sleep 1
  done ) &

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    maybe_quiet $TIMEOUT2 ../mrworker ../../mrapps/crash.so
    sleep 1
  done ) &

while [ -e $SOCKNAME -a ! -f mr-done ]
do
  maybe_quiet $TIMEOUT2 ../mrworker ../../mrapps/crash.so
  sleep 1
done

wait

rm $SOCKNAME
sort mr-out* | grep . > mr-crash-all
if cmp mr-crash-all mr-correct-crash.txt
then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  failed_any=1
fi

#########################################################
if [ $failed_any -eq 0 ]; then
    echo '***' PASSED ALL TESTS
else
    echo '***' FAILED SOME TESTS
    exit 1
fi

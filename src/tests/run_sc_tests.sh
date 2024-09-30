#!/bin/bash

extra_cmd=""
if [[ "$SUPERCOMPUTER" == "perlmutter" ]]; then
    extra_cmd="--cpu_bind=cores --overlap "
fi

if [ $# -lt 1 ]; then echo "missing test argument" && exit -1 ; fi

test_exe="$1"
mpi_cmd="$2"
n_servers="$3"
n_client="$4"

# copy the remaining test input arguments (if any)
test_args="$5 $6 $7"
shift

echo "Input arguments are the followings"
echo $test_args
if [ -x $test_exe ]; then echo "testing: $test_exe"; else echo "test: $test_exe not found or not and executable" && exit -2; fi

# Remove directories from previous executions
rm -rf pdc_tmp pdc_data ${PDC_TMPDIR} ${PDC_DATA_LOC}

if [ -z ${AWS_TEST_BUCKET} ]; then
    # For S3 tests, we need to make sure the bucket is cleared between runs
    # Notice that instead deleting the bucket itself can take up to 2 hours before re-creating
    aws s3 rm s3://${AWS_TEST_BUCKET} --recursive
fi

# START the server (in the background)
echo "$mpi_cmd -n $n_servers $extra_cmd ./bin/pdc_server.exe &"
$mpi_cmd -n $n_servers $extra_cmd ./bin/pdc_server.exe &

# WAIT a bit, for 1 second
sleep 1

# RUN the actual test
echo "$mpi_cmd -n $n_client $extra_cmd $test_exe $test_args"
$mpi_cmd -n $n_client $extra_cmd $test_exe $test_args

# Need to test the return value
ret="$?"

# and shutdown the SERVER before exiting
echo "$mpi_cmd -n $n_servers $extra_cmd ./bin/close_server"
$mpi_cmd -n $n_servers $extra_cmd ./bin/close_server




# ./mpi_test.sh obj_info srun 1 1
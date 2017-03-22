#!/bin/sh
export GOPATH=/Users/KDF5000/Documents/2016/Coding/Go/MIT6.824
count=0
for k in $( seq 1 10 )
do
    echo "Test loop:${k}"
    res=`go test | grep PASS`
    if [ $res='PASS' ];then
       echo 'PASS'
       let count++
    else
       echo 'FAIL'
    fi
done

echo "${count}/10"

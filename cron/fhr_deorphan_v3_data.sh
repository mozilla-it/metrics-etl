#!/bin/bash

JOBID="fhr_deorphan_v3_data"

LFS_LIB_DIR=$HOME/prod/fhrtools/
HDFS_INPUT_BASE=/data/fhr/raw/
HDFS_DEST_DIR_BASE=/data/fhr/nopartitions/
HDFS_TMP=/data/fhr/tmp/


OUTPUT_COMPRESSION_CODEC="org.apache.hadoop.io.compress.SnappyCodec"

DATE=$(date -d today +%Y%m%d)


if [ $# -eq 1 ]; then
    DATE=$(date -d "$1" +%Y%m%d)
    
    if [ $? -ne 0 ]; then
        usage
    fi
fi

usage() {
    echo "Usage: $0 [date(YYYYmmdd)]" >&2
    exit 2
}


TMPDIR=`mktemp -d /tmp/${JOBID}.${DATE}.XXXXXX || exit 1`

HDFS_TMP_OUTPUT_DIR=${HDFS_TMP}/${JOBID}.${DATE}.$$
HDFS_DEST_DIR=${HDFS_DEST_DIR_BASE}/${DATE}/3

cd ${LFS_LIB_DIR}

HADOOP_CLASSPATH=fhrtools-0.1-SNAPSHOT-cdh4.jar hadoop com.mozilla.metrics.fhrtools.tools.findorphans.FindOrphans -libjars fastjson-1.1.35.jar,joda-time-2.3.jar -D mapred.reduce.tasks=180 -Dmapred.job.queue.name=prod ${HDFS_INPUT_BASE}/${DATE}/part-m-\*  ${HDFS_TMP_OUTPUT_DIR} > ${TMPDIR}/job.log 2>&1

if [ $? -eq 0 ]; then

    hadoop dfs -mkdir $(dirname ${HDFS_DEST_DIR}) > /dev/null 2>&1
    hadoop dfs -rmr ${HDFS_DEST_DIR} > /dev/null 2>&1
    hadoop dfs -mv ${HDFS_TMP_OUTPUT_DIR} ${HDFS_DEST_DIR} > ${TMPDIR}/mv.log 2>&1

    if [ $? -eq 0 ]; then
        rm -fr ${TMPDIR}
        
        exit 0
    fi
fi

echo "Error output in ${TMPDIR}" >&2
chmod -R 755 ${TMPDIR}
exit 1

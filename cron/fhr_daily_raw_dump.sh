#!/bin/bash

HDFS_DEST_DIR_BASE=/data/fhr/raw/
HDFS_TMP=/data/fhr/raw/tmp/

HBASE_TBL="metrics"
HBASE_SCANNER_CACHING=1000
HBASE_NUM_VERSIONS=1

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


TMPDIR=`mktemp -d /tmp/fhr_daily_raw_dump.${DATE}.XXXXXX || exit 1`
cd ${TMPDIR}

HDFS_TMP_OUTPUT_DIR=${HDFS_TMP}/exporttable.${HBASE_TBL}.${DATE}.$$

hbase org.apache.hadoop.hbase.mapreduce.Export -Dmapred.job.reuse.jvm.num.tasks=128  -Dmapred.output.compression.codec=${OUTPUT_COMPRESSION_CODEC} -Dhbase.mapreduce.include.deleted.rows=false -Dhbase.client.scanner.caching=${HBASE_SCANNER_CACHING} -Dmapred.job.queue.name=prod -Ddfs.blocksize=$(( 1024 * 1024 * 1024 * 2 ))  ${HBASE_TBL} ${HDFS_TMP_OUTPUT_DIR} ${HBASE_NUM_VERSIONS} > ${TMPDIR}/job.log 2>&1

if [ $? -eq 0 ]; then

    hadoop dfs -mv ${HDFS_TMP_OUTPUT_DIR} ${HDFS_DEST_DIR_BASE}/${DATE} > mv.log 2>&1

    if [ $? -eq 0 ]; then
        cd ..
        rm -fr ${TMPDIR}
        
        exit 0
    fi
fi

echo "Error output in ${TMPDIR}" >&2
chmod -R 755 ${TMPDIR}
exit 1

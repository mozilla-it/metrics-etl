#!/bin/bash

LFS_LIB_DIR=$HOME/prod/fhrtools/
HDFS_DEST_DIR_BASE=/data/fhr/text/
HDFS_TMP=/data/fhr/text/tmp/

OUTPUT_COMPRESSION_CODEC="org.apache.hadoop.io.compress.GzipCodec"

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

hadoop dfs -stat ${HDFS_DEST_DIR_BASE}/${DATE}/_SUCCESS > /dev/null 2>&1

if [ $? -eq 0 ]; then
    echo "Text dump for ${DATE} already exists." >&2
    exit 0
fi

TMPDIR=`mktemp -d /tmp/fhr_daily_text_dump.${DATE}.XXXXXX || exit 1`
cd ${TMPDIR}

HDFS_TMP_OUTPUT_DIR=${HDFS_TMP}/textdump.${DATE}.$$

hadoop jar ${LFS_LIB_DIR}/export_with_ts.jar  -Dmapred.job.reuse.jvm.num.tasks=128  -Dmapred.output.compression.codec=${OUTPUT_COMPRESSION_CODEC} -Dmapred.job.queue.name=prod -Ddfs.blocksize=$(( 1024 * 1024 * 1024 * 2 )) -Dmapred.reduce.tasks=0 /data/fhr/raw/${DATE}/ ${HDFS_TMP_OUTPUT_DIR}  > ${TMPDIR}/job.log 2>&1

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

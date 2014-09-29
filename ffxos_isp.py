import os

from datetime import timedelta
import datetime
import sys
import pyodbc


cnxn = pyodbc.connect("DSN=vertica_dsn")
cursor = cnxn.cursor()
AUX_JAR_PATH = "ParseDateForQtr.jar"


def get_max_date():
    print "+ Calculating most recent date that has data and adding one day"
    max_date_sql = """
        select (max(date)::Date+1)::Varchar from ffos_dimensional_by_date
    """
    print "SQL: " + max_date_sql
    cursor.execute(max_date_sql)
    try:
        date = cursor.fetchall()[0][0]
    except IndexError:
        print "Couldn't fetch max date data"
        return None
    return date


def main():
    if (len(sys.argv) == 2):
        start_date = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d')
        end_date = (start_date + timedelta(days=1)).strftime("%Y-%m-%d")
        start_date = start_date.strftime("%Y-%m-%d")
    else:
        # Start collecting data at the date that is one greater than the most
        # recently processed day (i.e. data >= start_date)
        start_date = get_max_date()

        # Collect data up to today (i.e. data < today)
        end_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        if not start_date:
            return

    print (
        "#### GENERATING REPORT FOR DATA BETWEEN %s (Inclusive) and %s "
        "(Exclusive)"
    ) % (
        start_date, end_date
    )

    # setup environment
    # TODO, how do we want to set this? getcwd() is a bit hacky
    working_dir = os.getcwd()

    table_name = "ffos_dimensional_by_date"
    local_tab_delimited = os.path.join(
        working_dir, "%s.txt" % table_name
    )

    data_file = 'data.txt'
    data_file_path = os.path.join(local_tab_delimited, data_file)

    exceptions_file = os.path.join(
        working_dir, "%s_exception.txt" % table_name
    )

    bad_data_file = os.path.join(
        working_dir, "%s_bad_data.txt" % table_name
    )

    # Do actual computation
    do_hive_dance(
        working_dir, table_name, start_date, end_date, local_tab_delimited
    )
    build_single_datafile(data_file_path, data_file, local_tab_delimited)
    check_vertica_table(table_name)
    copy_data_into_vertica(
        table_name, data_file_path, exceptions_file, bad_data_file
    )
    generate_report(table_name, start_date, end_date)


def do_hive_dance(working_dir, table_name, start_date, end_date,
                  local_tab_delimited):
    print "+ Connecting to HIVE and requesting data for %s" % start_date
    query_file = os.path.join(working_dir, "%s.hql" % table_name)

    hql_file = open(query_file, "w")
    hql_file.write("add jar %s;\n" % AUX_JAR_PATH)
    hql_file.write(
        "create temporary function parseforqtr as 'com.mozilla.udf.ParseDateForQtr';\n"  # noqa
    )
    query = """
    INSERT OVERWRITE LOCAL DIRECTORY '{data_dir}'
    SELECT
        parseforqtr(logs.utc_time),
        logs.ds AS `date`,
        logs.ua_family as product,
        'unknown',
        'FFOS',
        'unknown',
        coalesce( cc.continent_code, '??'),
        logs.country_code,
        logs.isp_name,
        SPLIT(
            SPLIT(logs.custom_field_2,"X-MOZ-B2G-MCC:-")[0],
            "X-MOZ-B2G-DEVICE:"
        )[1] AS device_type,
        count(1) as tot_request_on_date
    FROM v2_anonymous_logs logs
    LEFT OUTER JOIN lookup_continent_country cc
    ON
        cc.country_name = logs.country_name
    WHERE
        (
            logs.domain='marketplace.firefox.com' OR
            logs.domain='marketplace.mozilla.org'
        )
        AND logs.request_type = 'GET'
        AND logs.http_status_code = 200
        AND (
            logs.request_url = '/packaged.webapp' OR
            logs.request_url LIKE '/minifest.webapp%'
        )
        AND logs.custom_field_2 like 'X-MOZ-B2G-DEVICE:%'
        AND logs.ua_family = 'Firefox Mobile'
        AND logs.ds >= '{start_date}' AND logs.ds < '{end_date}'
    GROUP BY
        parseforqtr(logs.utc_time),
        logs.ds,
        logs.isp_name,
        SPLIT(
            SPLIT(logs.custom_field_2, "X-MOZ-B2G-MCC:-")[0],
            "X-MOZ-B2G-DEVICE:"
        )[1],
        coalesce( cc.continent_code, '??'),
        logs.country_code,
        logs.ua_family,
        logs.ua_major,
        logs.ua_minor
    """.format(
        data_dir=local_tab_delimited, start_date=start_date, end_date=end_date,
    )

    hql_file.write(query)
    hql_file.close()
    sys_cmd = "hive -f " + query_file
    ret = os.system(sys_cmd)
    if ret:
        raise Exception("hive shell returned non-zero exit code %s" % ret)


def build_single_datafile(data_file_path, data_file, local_tab_delimited):
    print "+ Building single data file '%s'" % data_file_path
    with open(data_file_path, "wb") as all_data:
        files = filter(
            lambda fn: not fn.startswith('.') and fn != data_file,
            os.listdir(local_tab_delimited)
        )
        row_count = 0
        for filename in files:
            with open(os.path.join(local_tab_delimited, filename), "r") as fr:
                print "writing file %s" % os.path.join(
                    local_tab_delimited, filename
                )
                for line in fr:
                    row_count = row_count + 1
                    all_data.write(line)


def vertica_table_exists(table_name):
    print "+ Checking to see if table '%s' exists" % table_name
    sql = """
    SELECT table_name
    FROM all_tables
    WHERE table_name='copy_adi_dimensional_by_date'
    """
    res = cursor.execute(sql)
    return True if res.fetchall() else False


def check_vertica_table(table_name):
    if not vertica_table_exists(table_name):
        raise Exception("%s doesn't exist!" % table_name)


def copy_data_into_vertica(table_name, data_file_path, exceptions_file,
                           bad_data_file):
    print "+ Coping data found in '%s' into vertica table %s" % (
        data_file_path, table_name
    )
    copy_sql = """
        COPY %s (
            _year_quarter,
            date,
            product,
            v_prod_major,
            prod_os,
            v_prod_os,
            continent_code,
            cntry_code,
            isp_name,
            device_type,
            tot_request_on_date
        )
        FROM LOCAL '%s'
        DELIMITER '\x01'
        EXCEPTIONS '%s'
        REJECTED DATA '%s'
    """ % (table_name, data_file_path, exceptions_file, bad_data_file)

    print "SQL: " + copy_sql
    cursor.execute(copy_sql)


def generate_report(table_name, start_date, end_date):
    print "+ Generating report"
    message_text = ""
    success = False

    insert_row_count = 0
    sql = """
    SELECT count(1) AS c FROM %s WHERE date >= '%s' AND date < '%s'
    """ % (table_name, start_date, end_date)
    print "SQL: " + sql

    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        insert_row_count = int(row.c) + insert_row_count

    if (insert_row_count <= 0):
        success = False
        message_text = "POSSIBLE ERROR: 0 rows were inserted"
    else:
        success = True
        message_text = "Inserted rows: " + str(insert_row_count)

    #lets send email
    if success:
        message_subject = "SUCCESS: ffxos isp count report"
    else:
        message_subject = "FAILURE: ffxos isp count report"
    print message_subject
    print message_text


if __name__ == '__main__':
    main()

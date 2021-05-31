# coding: UTF-8

import cx_Oracle
import os
import csv
import yaml
import datetime as dt
import codecs


def prepare_config(yaml_file):
    with open(yaml_file, 'r') as f:
        y = yaml.safe_load(f)
        return y


def prepare_sql(sql_file):
    with open(sql_file) as f:
        return f.read()


def prepare_output_folder():
    output = 'output/' + dt.datetime.now().strftime('%Y%m%d_%H%M%S')
    os.mkdir(output)
    return output


def prepare_target_tables(cursor, sql, cnf, schema):
    sql_replacement = "'" + schema + "'"
    sql = sql.replace(cnf.get('replacements').get('schema'), sql_replacement)
    cursor.execute(sql)
    target_tables = []
    fetch_rows = cnf.get('db_connection').get('fetch_rows')
    while True:
        data_result = cursor.fetchmany(fetch_rows)
        if len(data_result) == 0:
            break
        for row in data_result:
            target_tables.append(row[1])
    return target_tables


def prepare_columns(target_tables, cursor, sql, cnf, schema):
    target_columns = dict((table, []) for table in target_tables)
    fetch_rows = int(cnf.get('db_connection').get('fetch_rows'))
    sql_replacement = "'" + schema + "'"
    sql = sql.replace(cnf.get('replacements').get('schema'), sql_replacement)
    cursor.execute(sql)
    while True:
        data_result = cursor.fetchmany(fetch_rows)
        if len(data_result) == 0:
            break
        for row in data_result:
            target_table = target_columns.get(row[0])
            if target_table is not None:
                target_table.append((row[1], row[2], row[3]))
    return target_columns


def write_csv(sql, cursor, output, fetch_rows, schema, table, has_special_column):
    cursor.execute(sql)
    csv_header = [s[0] for s in cursor.description]
    out_file_name = output + '/' + schema + '_' + table + '.csv'
    with codecs.open(out_file_name, 'w', 'utf-8') as f:
        writer = csv.writer(f, lineterminator='\r\n', quoting=csv.QUOTE_ALL)
        writer.writerow(csv_header)
        while 1:
            csv_detail = cursor.fetchmany(fetch_rows)
            if len(csv_detail) == 0:
                break
            if has_special_column:
                csv_detail = process_special_column(csv_detail)
            writer.writerows(csv_detail)


def process_special_column(csv_detail):
    processed_csv_detail = list()
    for row in csv_detail:
        list_row = list(row)
        for index, value in enumerate(row):
            if type(value) == cx_Oracle.LOB:
                list_row[index] = value.read()
        processed_csv_detail.append(tuple(list_row))
    return processed_csv_detail


def get_key_columns(target_table_columns):
    key_columns = list()
    if target_table_columns is not None and len(target_table_columns) > 0:
        for column in target_table_columns:
            if column[1] == 'P':
                key_columns.append(column[0])
    return key_columns


def get_fetch_target_columns(target_table_columns, target_process_column_types):
    has_special_column = False
    fetch_target_columns = []
    if target_table_columns is not None and len(target_table_columns) > 0:
        for column in target_table_columns:
            if any(column[2] in s for s in target_process_column_types):
                has_special_column = True
            else:
                if column[1] == 'C':
                    fetch_target_columns.append(column[0])
    return '*', has_special_column


def prepare_single_table_fetch_sql(schema, table, target_columns, special_column_types):
    target_table_columns = target_columns.get(table)
    key_columns = get_key_columns(target_table_columns)
    sort = ''
    if len(key_columns) > 0:
        sort = 'ORDER BY ' + ','.join(key_columns)
    fetch_target_information = get_fetch_target_columns(target_table_columns, special_column_types)
    sql = "SELECT {} FROM {}.{} {}".format(fetch_target_information[0], schema, table, sort)
    return sql, fetch_target_information[1]


def process_schema(cursor, cnf, schema, output, fetch_rows):
    sql = prepare_sql('sql/fetch_target_tables.sql')
    target_tables = prepare_target_tables(cursor, sql, cnf, schema)
    sql = prepare_sql('sql/fetch_columns.sql')
    target_columns = prepare_columns(target_tables, cursor, sql, cnf, schema)
    special_column_types = cnf.get('special_column_types')
    for table in target_tables:
        if table.startswith(tuple(cnf.get('skips').get('tables'))):
            continue
        ret = prepare_single_table_fetch_sql(schema, table, target_columns, special_column_types)
        print(ret[0])
        write_csv(ret[0], cursor, output, fetch_rows, schema, table, ret[1])


def export(cnf):
    output = prepare_output_folder()
    conn = cnf.get('db_connection')
    fetch_rows = int(conn.get('fetch_rows'))
    with cx_Oracle.connect(conn.get('user'),
                           conn.get('pass'),
                           conn.get('host') + ':' + conn.get('port') + '/' + conn.get('service_name'),
                           encoding="UTF-8",
                           nencoding="UTF-8") as conn:
        with conn.cursor() as cursor:
            schemas = cnf.get('schemas')
            for schema in schemas:
                process_schema(cursor, cnf, schema, output, fetch_rows)


if __name__ == '__main__':
    config = prepare_config('config/setting.yaml')
    export(config)

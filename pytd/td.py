def query(sql, connection):
    cur = connection.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]
    return [column_names] + rows


def query_iterrows(sql, connection):
    cur = connection.cursor()
    cur.execute(sql)
    index = 0
    column_names = None
    while True:
        row = cur.fetchone()
        if row is None:
            break
        if index == 0:
            column_names = [desc[0] for desc in cur.description]
        yield index, dict(zip(column_names, row))
        index += 1


def write(df, table, connection, if_exists='error'):
    if connection.td_spark is None:
        try:
            connection.setup_td_spark()
        except Exception as e:
            raise e

    if if_exists not in ('error', 'overwrite', 'append', 'ignore'):
        raise ValueError('invalid valud for if_exists: %s' % if_exists)

    destination = table
    if '.' not in table:
        destination = connection.database + '.' + table

    sdf = connection.td_spark.createDataFrame(df)
    sdf.write.mode(if_exists).format('com.treasuredata.spark').option('table', destination).save()

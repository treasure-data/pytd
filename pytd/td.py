def query(sql, connection):
    cur = connection.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    return columns, rows


def write(df, table, connection, if_exists='error'):
    if if_exists not in ('error', 'overwrite', 'append', 'ignore'):
        raise ValueError('invalid valud for if_exists: %s' % if_exists)

    connection.write_dataframe(df, table, if_exists)

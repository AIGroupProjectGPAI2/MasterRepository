import thebestsql as bsql

connection = bsql.get_connection("niels16", "postgres")
cursor = bsql.get_cursor(connection)
bsql.reset_db(connection, cursor)
files = ['doelgroepen', 'brands', 'categories', 'products', 'profiles', 'sessions', 'profiles_previously_viewed', 'orders']
bsql.copy_files_to_tables(connection, cursor, files)

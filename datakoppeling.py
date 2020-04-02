# #  dit stukje code is aangeboden door Nick Roumimper
#
# import psycopg2
#
# c = psycopg2.connect("dbname=huwebshop user=postgres password=Floris09")  #TODO: eigen wachtwoord instellen
# cur = c.cursor()
#
# def data_koppeling(filenames):
#     for filename in filenames:
#         with open(filename+'.csv') as csvfile:
#             print("copying {}....".format(filename))
#             cur.copy_expert("COPY "+filename+" FROM STDIN DELIMITER ',' CSV HEADER", csvfile)
#             c.commit()
#     return
# filenames = ['orders']
# data_koppeling(filenames)
#
#
# c.commit()
# cur.close()
# c.close()

import thebestsql as bsql

connection = bsql.get_connection("Floris09", "huwebshop")
cursor = bsql.get_cursor(connection)
bsql.reset_db(connection, cursor)
files = ['doelgroepen', 'brands', 'categories', 'products', 'profiles', 'sessions', 'profiles_previously_viewed', 'orders']
bsql.copy_files_to_tables(connection, cursor, files)

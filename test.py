import psycopg2
def SQL_fetch_data(SQL):
    connection = psycopg2.connect(user="postgres",
                                  password="niels16",
                                    host="localhost",
                                  port="5432",
                                  database="postgres")
    cursor = connection.cursor()
    cursor.execute(SQL)
    fetched_data = cursor.fetchall()
    cursor.close()
    connection.close()
    return fetched_data


data = SQL_fetch_data("SELECT * FROM most_popular_products LIMIT 5;")
print(data)
prodlist = []
sql = "SELECT * FROM most_popular_products LIMIT" + "4" + ";"
data = SQL_fetch_data(sql)
for products in data:
    prodlist.append(products[0])
print(prodlist)

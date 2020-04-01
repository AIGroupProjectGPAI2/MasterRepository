import thebestsql as SQL

order_data = SQL.SQL_fetch_data("""SELECT profiles.id, orders.productid, orders.count
FROM orders
INNER JOIN sessions ON orders.sessionsid = sessions.id
INNER JOIN profiles ON sessions.profilesid = profiles.id;""")

viewed_data = SQL.SQL_fetch_data("""select * from profiles_previously_viewed;""")

data = order_data + viewed_data
print(data)
def profile_products(data):
    pass


import thebestsql as SQL

order_data = SQL.SQL_fetch_data("""SELECT profiles.id, orders.productid, orders.count
FROM orders
INNER JOIN sessions ON orders.sessionsid = sessions.id
INNER JOIN profiles ON sessions.profilesid = profiles.id;""")

viewed_data = SQL.SQL_fetch_data("""select * from profiles_previously_viewed;""")

data = order_data + viewed_data


def profile_products(data):
    profile_products_list = dict({})
    for record in data:
        record_in_dic = False
        for profiles in profile_products_list:
            if record[0] == profiles:
                products = [record[1]] * record[2]
                lst = list(profile_products_list.get(profiles)) + products
                profile_products_list.update({profiles: lst})
                record_in_dic = True
                break
        if not record_in_dic:
            products = [record[1]] * record[2]
            profile_products_list.update({record[0]: products})
    return profile_products_list

print(profile_products(data[:300]))





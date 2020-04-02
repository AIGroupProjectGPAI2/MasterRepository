import thebestsql as SQL

order_data = SQL.SQL_fetch_data("""SELECT profiles.id, orders.productid, orders.count
FROM orders
INNER JOIN sessions ON orders.sessionsid = sessions.id
INNER JOIN profiles ON sessions.profilesid = profiles.id;""")

viewed_data = SQL.SQL_fetch_data("""select * from profiles_previously_viewed;""")

data = order_data + viewed_data


def most_frequent_item(List):
    """Count most frequent item in list"""
    counter = 0
    freq_item = List[0]
    for item in List:
        current_freq = List.count(item)
        if current_freq > counter:
            counter = current_freq
            freq_item = item
    return freq_item


def top_frequent_items(List, top_number):
    """Makes a list of the most frequent items in list"""
    if len(List) == top_number:
        return List
    list_frequent_items = []
    for top in range(top_number):
        most_frequent = most_frequent_item(List)
        list_frequent_items.append(most_frequent)
        for remove_item in range(List.count(most_frequent)):
            if most_frequent in List:
                List.remove(most_frequent)
    return list_frequent_items


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
def profile_popular_products(profile_products):
    popular_products = dict({})
    for keys in profile_products:
        products = profile_products.get(keys)
        if len(products) > 4:
            top_products = top_frequent_items(products, 5)
            popular_products.update({keys: top_products})
    return popular_products


print(profile_popular_products(profile_products(data[:300])))





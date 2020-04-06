import thebestsql as SQL
import csv

product_data = SQL.SQL_fetch_data("""select id, herhaalaankopen from products;""")
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
    print("Making profile_products dictionary....")
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
    print("Making profile_popular_products dictionary....")
    popular_products = dict({})
    for keys in profile_products:
        try:
            products = profile_products.get(keys)
            if len(products) > 4:
                top_products = top_frequent_items(products, len(products))
                popular_products.update({keys: top_products})
        except Exception as ERROR:
            print(ERROR)
    return popular_products


data_dic = profile_popular_products(profile_products(data[:1000]))
print(data_dic)

def profile_product_herhaalaankopen_check(popular_products, product_data):
    for profiles in popular_products:
        products = popular_products.get(profiles)
        products2_0 = []
        products2_0 += products
        product_count = len(products) - 5
        for count in range(len(products)):
            count += 1
            if len(products2_0) > 4:
                product = products[-count]
                for products_data in product_data:
                    if product_data[0] == product:
                        if product_data[1] == False:
                            products2_0.remove(product_data[0])
                            popular_products.update({profiles: products2_0})
    return popular_products
data_dic_herhaalaankopen = profile_product_herhaalaankopen_check(data_dic, product_data)
print(data_dic_herhaalaankopen)

def generate_CSV(file_name_string, dictionary, fieldnames):
    print("Creating the CSV file...")
    with open(file_name_string + ".csv", 'w', newline='', encoding='utf-8') as csvout:
        writer = csv.DictWriter(csvout, fieldnames=fieldnames)
        writer.writeheader()
        written_records_counter = 0
        for profile in dictionary:
            writeDict = {}
            writeDict.update({fieldnames[0]: profile})
            count = 0
            for x in (dictionary.get(profile))[:5]:
                count += 1
                writeDict.update({fieldnames[count]: x})
            writer.writerow(writeDict)
            written_records_counter += 1
            if written_records_counter % 10000 == 0:
                print("{} product records written...".format(written_records_counter))
    print("Finished creating the product database contents.")


generate_CSV("persoonlijk_aangeboden", data_dic_herhaalaankopen, ["profileid", "productid1", "productid2", "productid3", "productid4", "productid5"])


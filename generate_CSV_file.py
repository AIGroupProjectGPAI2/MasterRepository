import csv, pymongo
import time
start_time = time.time()
client = pymongo.MongoClient("mongodb://localhost:27017/")
mongoDB = client["huwebshop"]

def generate_CSV(file_name_string, category, fieldnames, values):
    """file_name_string = the name of the csv file"""
    """category = the mongoDB category: products, sessions......"""
    """fieldnames = the header of the csv file (index[0] is not nullable)"""
    """values = the value name of the filedname(must be in the same order as fieldnames)"""
    """fieldname[0] is the name of row 1 in the csv file, value[0] is the value for fieldname[0] to search for in the 
    mongoDB """
    print("Creating the CSV file...")
    with open(file_name_string, 'w', newline='', encoding='utf-8') as csvout:
        writer = csv.DictWriter(csvout, fieldnames=fieldnames)
        writer.writeheader()
        written_records_counter = 0
        for record in category:
            writeDict = {}
            for x in range(len(fieldnames)):
                if x == 0:
                    writeDict.update({fieldnames[x]: record[values[0]]})
                else:
                    writeDict.update({fieldnames[x]: record.get(values[1], None)})
            writer.writerow(writeDict)
            written_records_counter += 1
            if written_records_counter % 10000 == 0:
                print("{} product records written...".format(written_records_counter))
    print("Finished creating the product database contents.")

def gen_csv(filename, fieldnames):
    with open(filename, 'w', newline='') as csvout:
        writer = csv.DictWriter(csvout, fieldnames=fieldnames)
        writer.writeheader()

collections = ["products", "profiles", "sessions"]
buid_profs_dict = {}
all_products = []
for collection in collections:
    col = mongoDB.get_collection(collection)
    if collection == "products":
        with open('products.csv', 'w', newline='', encoding='utf-8') as products_file, open('categories.csv', 'w', newline='', encoding='utf-8') as categories_file, open('doelgroepen.csv', 'w', newline='', encoding='utf-8') as doelgroepen_file, open('brands.csv', 'w', newline='', encoding='utf-8') as brands_file:
            product_fieldnames = ["ID", "doelgroepenID", "categoriesID", "brandID", "name", "description", "herhaalaankopen", "price", "inhoud", "discount"]
            categories_fieldnames = ["ID", "category", "subcategory", "subsubcategory"]
            doelgroepen_fieldnames = ["ID", "target_audience"]
            brands_fieldnames = ["ID", "brand"]
            producten_writer = csv.DictWriter(products_file, fieldnames=product_fieldnames)
            categories_writer = csv.DictWriter(categories_file, fieldnames=categories_fieldnames)
            doelgroepen_writer = csv.DictWriter(doelgroepen_file, fieldnames=doelgroepen_fieldnames)
            brands_writer = csv.DictWriter(brands_file, fieldnames=brands_fieldnames)
            producten_writer.writeheader()
            categories_writer.writeheader()
            doelgroepen_writer.writeheader()
            brands_writer.writeheader()
            c = 0
            categories_dict = {}
            doelgroepen_dict = {}
            brands_dict = {}
            for product in col.find():
                try:
                    cat = product.get("category", None)
                    subcat = product.get("sub_category", None)
                    subsubcat = product.get("sub_sub_category", None)
                    doelgroep = product["properties"].get("doelgroep", None)
                    if doelgroep is None:
                        doelgroep = product["properties"].get("gender", None)
                    brand = product.get("brand", None)
                    cat_search = str(cat) + str(subcat)
                    if cat_search not in categories_dict:
                        if len(categories_dict) > 0:
                            categories_dict[cat_search] = max(categories_dict.values()) + 1
                        else:
                            categories_dict[cat_search] = 1
                        categories_writer.writerow(
                            {
                                'ID': categories_dict[cat_search],
                                'category': cat,
                                'subcategory': subcat,
                                'subsubcategory': subsubcat
                            }
                        )
                    cat_id = categories_dict[cat_search]

                    if brand not in brands_dict:
                        if len(brands_dict) > 0:
                            brands_dict[brand] = max(brands_dict.values()) + 1
                        else:
                            brands_dict[brand] = 1
                        brands_writer.writerow(
                            {
                                'ID': brands_dict[brand],
                                'brand': brand
                            }
                        )
                    brand_id = brands_dict[brand]

                    if doelgroep not in doelgroepen_dict:
                        if len(doelgroepen_dict) > 0:
                            doelgroepen_dict[doelgroep] = max(doelgroepen_dict.values()) + 1
                        else:
                            doelgroepen_dict[doelgroep] = 1
                        doelgroepen_writer.writerow(
                            {
                                'ID': doelgroepen_dict[doelgroep],
                                'target_audience': doelgroep
                            }
                        )
                    doel_id = doelgroepen_dict[doelgroep]
                    price = product["price"]["selling_price"]
                    if "." in str(price):
                        price = int(price * 100)
                    producten_writer.writerow(
                        {
                            # ["ID", "doelgroepenID", "categoriesID", "brandID", "name", "description", "herhaalaankopen", "price", "inhoud", "discount"]
                            'ID': product.get("_id", None),
                            'doelgroepenID': doel_id,
                            'categoriesID': cat_id,
                            'brandID': brand_id,
                            'name': product.get("name", None),
                            'description': product.get("description", None),
                            'herhaalaankopen': product.get("herhaalaankopen", None),
                            'price': price,
                            'inhoud': product["properties"].get("inhoud", None),
                            'discount': product["price"].get("discount", None)
                        }
                    )
                    try:
                        all_products.append(product["_id"])
                    except:
                        continue
                except KeyError:
                    print(product.get("_id", None), "Heeft geen properties")
        print(all_products)
    elif collection == "profiles":
        query = {"buids": {"$ne": None}}
        c = 0
        profiles_pids = {}
        with open('profiles.csv', 'w', newline='', encoding='utf-8') as profiles_file, open(
                'profiles_previously_viewed.csv', 'w', newline='', encoding='utf-8') as ppv_file:
            profiles_fieldnames = ["ID", "segment"]
            ppv_fieldnames = ["profileID", "productID", "count"]
            profiles_writer = csv.DictWriter(profiles_file, fieldnames=profiles_fieldnames)
            ppv_writer = csv.DictWriter(ppv_file, fieldnames=ppv_fieldnames)
            profiles_writer.writeheader()
            ppv_writer.writeheader()
            for profile in col.find(query):
                buids = profile.get("buids", None)
                id = profile["_id"]
                rec = profile.get("recommendations", None)
                if rec is not None:
                    viewed_before = rec.get("viewed_before", [])
                    if len(viewed_before) > 0:
                        rows = []
                        for pid in viewed_before:
                            if pid in all_products:
                                pcnt = 1
                                if pid in profiles_pids:
                                    profiles_pids[pid]["count"] += 1
                                else:
                                    profiles_pids[pid] = {
                                        "profileID": id,
                                        "productID": pid,
                                        "count": pcnt
                                    }
                    if len(buids) > 0:
                        for buid in buids:
                            buid_profs_dict[buid] = str(id)
                    profiles_writer.writerow(
                        {
                            "ID": id,
                            "segment": rec.get("segment", None)
                        }
                    )

                c += 1
                if c % 10000 == 0:
                    print("{} profiles records written...".format(c))
            for i in profiles_pids.keys():
                ppv_writer.writerow(
                    {
                        "profileID": profiles_pids[i]["profileID"],
                        "productID": profiles_pids[i]["productID"],
                        "count": profiles_pids[i]["count"]
                    }
                )
        #print(buid_profs_dict)
    elif collection == "sessions":
        c = 0
        with open('sessions.csv', 'w', newline='', encoding='utf-8') as sessions_file, open('orders.csv', 'w', newline='', encoding='utf-8') as orders_file:
            sessions_fieldnames = ["ID", "profilesID", "device_type"]
            orders_fieldnames = ["sessionsID", "productID", "count"]
            sessions_writer = csv.DictWriter(sessions_file, fieldnames=sessions_fieldnames)
            orders_writer = csv.DictWriter(orders_file, fieldnames=orders_fieldnames)
            sessions_writer.writeheader()
            orders_writer.writeheader()
            query = {"buid": {"$ne": None}}
            orders_pids = {}
            orders_list = []
            for session in col.find(query):
                buid = session.get("buid", None)[0]
                try:
                    profileid = buid_profs_dict[buid]
                except:
                    continue
                id = session.get("_id", None)
                ua = session.get("user_agent", None)
                device_type = "Other"
                if ua is not None:
                    device = ua.get("device", None)
                    if device is not None:
                        family = device.get("family", None)
                        device_type = family
                order = session.get("order", None)
                if order is not None:
                    products = order["products"]
                    for product in products:
                        if product["id"] in all_products:
                            pcnt = 1
                            if product["id"] in orders_pids:
                                orders_pids[product["id"]]["count"] += 1
                            else:
                                orders_pids[product["id"]] = {
                                    'sessionsID': id,
                                    'productID': product["id"],
                                    'count': pcnt
                                }
                            # orders_list.append(
                            #     {
                            #         'sessionsID': id,
                            #         'productID': product["id"],
                            #         'count': pcnt
                            #     }
                            # )
                            # orders_writer.writerow(
                            #     {
                            #         'sessionsID': id,
                            #         'productID': product["id"],
                            #         'count': pcnt
                            #     }
                            # )
                sessions_writer.writerow(
                    {
                        "ID": id,
                        "profilesID": profileid,
                        "device_type": device_type
                    }
                )

                c += 1
                if c % 10000 == 0:
                    print("{} sessions records written...".format(c))
            for i in orders_pids.keys():
                orders_writer.writerow(
                    {
                        'sessionsID': orders_pids[i]["sessionsID"],
                        'productID': orders_pids[i]["productID"],
                        'count': orders_pids[i]["count"]
                    }
                )


# for collection in collections: with open(collection + ".csv", "w", newline="") as csvout: col =
# mongoDB.get_collection(collection) if collection == "products": fieldnames = ["ID", "doelgroepenID",
# "categoriesID", "brandID", "name", "description", "herhaalaankopen", "price", "mcrp", "inhoud", "discount"]


print("--- %s seconds ---" % (time.time() - start_time))

import thebestsql as bsql
import csv

connection = bsql.get_connection("Floris09", "huwebshop")
cursor = bsql.get_cursor(connection)

def profile_doelgroep():
    profiles_orders_query = """SELECT pf.ID,
                        d.ID,
                        o.count
                        FROM profiles AS pf, 
                             sessions AS s, 
                             products AS pd, 
                             orders AS o,
                             doelgroepen AS d
                        WHERE pf.ID = s.profilesID
                          AND s.ID = o.sessionsID
                          AND o.productID = pd.ID
                          AND d.ID = pd.doelgroepenID
                        ORDER BY o.count DESC"""
    profiles_ppv_query = """SELECT ppv.profileid, d.ID, ppv.count
                            FROM profiles_previously_viewed as ppv, doelgroepen as d, products
                            WHERE ppv.productid = products.id
                            AND d.id = products.doelgroepenID"""

    orders_data = bsql.select_data(cursor, profiles_orders_query)
    ppv_data = bsql.select_data(cursor, profiles_ppv_query)
    all_data = orders_data + ppv_data
    print(len(orders_data), len(ppv_data), len(orders_data) + len(ppv_data), len(all_data))
    profiles = {}
    c = 0
    for row in all_data:
        c+=1
        profile_id = row[0]
        doelgroep = row[1]
        doelgroep_count = row[2]
        if profile_id in profiles:
            if doelgroep in profiles[profile_id]:
                profiles[profile_id][doelgroep] += doelgroep_count
            else:
                profiles[profile_id][doelgroep] = doelgroep_count
        else:
            profiles[profile_id] = {
                doelgroep: doelgroep_count
            }
        #print(profile_id)
    with open('profiles_doelgroepen.csv', 'w', newline='', encoding='utf-8') as pd_file:
        pd_fieldnames = ["profileID", "doelgroepID"]
        pd_writer = csv.DictWriter(pd_file, fieldnames=pd_fieldnames)
        pd_writer.writeheader()
        for profile in profiles:
            pd_writer.writerow(
                {
                    "profileID": profile,
                    "doelgroepID": max(profiles[profile], key=lambda key: profiles[profile][key])
                }
            )
            # profiles[profile] = max(profiles[profile], key=lambda key: profiles[profile][key])
            # print(max(profiles[profile], key=lambda key: profiles[profile][key]), profiles[profile])
            # for doel in profiles[profile]:
            #     if profiles[profile][doel] >= highest_doelgroep:
            #         highest_doelgroep = profiles[profile][doel]
            #         dg = doel
        # with open("datatest.json", 'w') as dt:
        #     dt.write( json.dumps(profiles))

    print(len(profiles))


profile_doelgroep()
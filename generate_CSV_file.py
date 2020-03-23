import csv



def generate_CSV(file_name_string, category, fieldnames, values):
    """file_name_string = the name of the csv file"""
    """category = the mongoDB category: products, sessions......"""
    """fieldnames = the header of the csv file (index[0] is not nullable)"""
    """values = the value name of the filedname(must be in the same order as fieldnames)"""
    """fieldname[0] is the name of row 1 in the csv file, value[0] is the value for fieldname[0] to search for in the mongoDB"""
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

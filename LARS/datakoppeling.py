#  dit stukje code is aangeboden door Nick Roumimper

import psycopg2

c = psycopg2.connect("dbname=postgres user=postgres password=niels16")  #TODO: eigen wachtwoord instellen
cur = c.cursor()

def data_koppeling(filenames):
    for filename in filenames:
        with open(filename+'.csv') as csvfile:
            print("copying {}....".format(filename))
            cur.copy_expert("COPY "+filename+" FROM STDIN DELIMITER ',' CSV HEADER", csvfile)
            c.commit()
    return
filenames = ['brands, categories, doelgroepen']
data_koppeling(filenames)


c.commit()
cur.close()
c.close()

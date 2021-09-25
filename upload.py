import psycopg2
import pandas as pd

dbname = 'test'
user = 'postgres'
password = '1234'
host = 'localhost'


def create_temp_dict(data_frame):
    res_dict = {}

    for i in range(data_frame.index.stop):
        res_dict[data_frame['endpoint_id'][i]] = data_frame['endpoint_name'][i]

    return res_dict


def update_db(query_temp_dict, dataframe):
    temp_dict = create_temp_dict(dataframe)

    for el in temp_dict:
        if query_temp_dict[el] != temp_dict[el]:
            cursor.execute(""" UPDATE endpoint
                               SET endpoint_name = (%s)
                               WHERE endpoint_id = (%s) """, (str(temp_dict[el]), str(el)))
    connection.commit()

    cursor.execute(""" SELECT * FROM endpoint """)
    return cursor.fetchall()


connection = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)

cursor = connection.cursor()

cursor.execute("""CREATE TABLE IF NOT EXISTS endpoint 
                 (endpoint_id integer PRIMARY KEY NOT NULL,
                  endpoint_name varchar(100))""")

connection.commit()

cursor.execute(""" SELECT * FROM endpoint """)
try:
    db_query_dict = dict(cursor.fetchall())

    endpoint_table = pd.read_excel('endpoint_names.xlsm')

    print(update_db(db_query_dict, endpoint_table))

    print('Обновление данных завершено')

    exit_input = input('Вы хотите выйти? ')

except:
    print("Лалала")

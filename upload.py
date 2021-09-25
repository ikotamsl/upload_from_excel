import psycopg2
import pandas as pd

dbname = 'test'
user = 'postgres'
password = '1234'
host = 'localhost'


# Функция создания временного слованя с данными из файла Excel
def create_temp_dict(data_frame):
    res_dict = {}

    for i in range(data_frame.index.stop):
        res_dict[data_frame['endpoint_id'][i]] = data_frame['endpoint_name'][i]

    return res_dict


# Функция проверки и обновления данных
def update_db(query_temp_dict, dataframe):
    temp_dict = create_temp_dict(dataframe)

    print(temp_dict)
    print(query_temp_dict)

    for el in temp_dict:
        if not query_temp_dict.get(el):
            cursor.execute(""" INSERT INTO endpoint (endpoint_id, endpoint_name)
                               VALUES (%s, %s) """, (str(el), str(temp_dict[el])))
        elif query_temp_dict[el] != temp_dict[el]:
            cursor.execute(""" UPDATE endpoint
                               SET endpoint_name = (%s)
                               WHERE endpoint_id = (%s) """, (str(temp_dict[el]), str(el)))

    connection.commit()

    cursor.execute(""" SELECT * FROM endpoint """)
    return cursor.fetchall()


# Устанавливаем соединение и создаём таблицу, если она не была создана

connection = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)

cursor = connection.cursor()

cursor.execute("""CREATE TABLE IF NOT EXISTS endpoint 
                 (endpoint_id integer PRIMARY KEY NOT NULL,
                  endpoint_name varchar(100))""")

connection.commit()

# Делаем запрос на БД и получаем текущие данные по endpoint_id/endpoint_name

cursor.execute(""" SELECT * FROM endpoint """)
db_query_dict = dict(cursor.fetchall())

# Обновляем данные в БД

endpoint_table = pd.read_excel(r'c:\\upload_names\\endpoint_names.xlsm')
print(update_db(db_query_dict, endpoint_table))

print('Обновление данных завершено')

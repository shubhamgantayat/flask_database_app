import mysql.connector as connection
import csv
import pandas as pd
from logger.log_db import Logger


class Operations:

    def __init__(self, db_name, password):
        """

        :param db_name: Name of the database
        :param password: <password>
        """
        try:
            self.lg = Logger("my_sql")
            self.my_db = connection.connect(host='localhost', database=db_name, user='root', passwd=password, use_pure=True)
            self.cursor = self.my_db.cursor()
            self.result = "connection with database successful"
            self.lg.log("info", self.result)
        except Exception as e:
            self.result = str(e)
            self.lg.log("error", self.result)

    def create_table(self, table_name, cols):
        """

        :param table_name: Name of the table
        :param cols: [
            {"col_name": "Date", "data_type": "varchar", "size": "100", "constraint": ""},
            {"col_name": "Open", "data_type": "float", "size": "5,2", "constraint": ""},
            {"col_name": "High", "data_type": "float", "size": "5,2", "constraint": ""}
        ]
        :return: Returns the result of the operation.
        """
        try:
            schema = []
            for i in cols:
                d_type = i['data_type']
                if i['size'] != '':
                    d_type = i['data_type'] + "(" + i['size'] + ")"
                schema.append(i['col_name'] + ' ' + d_type + ' ' + i['constraint'])
            query = "create table " + table_name + " (" + ','.join(schema) + ")"
            print(query)
            self.cursor.execute(query)
            self.my_db.commit()
            self.result = "created table " + table_name
            self.lg.log("info", self.result)
        except Exception as e:
            self.result = str(e)
            self.lg.log("error", self.result)

    def insert_one(self, table_name, record):
        """

        :param table_name: Name of the table.
        :param record: {
            entity_1: val_1,
            entity_2: val_2,
            entity_3: val_3,
            ...
         }
        :return: Result of the operation.
        """
        try:
            col_name = ','.join(record.keys())
            values = ','.join(list(map(lambda x: "'" + x + "'" if type(x) == str else str(x), record.values())))
            query = "insert into " + table_name + " (" + col_name + ")" + " values (" + values + ")"
            print(query)
            self.cursor.execute(query)
            self.my_db.commit()
            self.result = "inserted a record"
            self.lg.log("info", self.result)
        except Exception as e:
            self.result = str(e)
            self.lg.log("error", self.result)

    def insert_many(self, table_name, filepath, cols):
        """

        :param table_name: Name of the table.
        :param filepath: path\\to\\the\\csv\\file
        :param cols: [
            {"col_name": "id", "data_type": "int"},
            {"col_name": "name", "data_type": "varchar"},
            {"col_name": "count", "data_type": "int"}
        ]
        :return: Result of the operation.
        """
        try:
            int_types = ["bigint", "int", "smallint", "tinyint"]
            float_types = ["float", "double"]
            str_types = ["char", "varchar", "date", "time", "datetime", "timestamp", "year"]
            bool_types = ["boolean", "bool"]

            map_data_type = {}
            map_data_type.update(dict().fromkeys(int_types, "int"))
            map_data_type.update(dict().fromkeys(float_types, "float"))
            map_data_type.update(dict().fromkeys(str_types, "str"))
            map_data_type.update(dict().fromkeys(bool_types, "bool"))

            with open(filepath, "r") as f:
                records = list(csv.reader(f))
                for i in records[1:]:
                    new_record = []
                    for j in range(len(i)):
                        d_type = map_data_type[cols[j]['data_type']]
                        if d_type != 'str':
                            string = d_type + "(" + i[j] + ")"
                            new_record.append(eval(string))
                        else:
                            new_record.append(i[j])
                    self.insert_one(table_name, dict(zip(records[0], new_record)))
            self.result = "inserted " + str(len(records) - 1) + " records"
            self.lg.log("info", self.result)
        except Exception as e:
            self.result = str(e)
            self.lg.log("error", self.result)

    def update(self, table_name, new_val, condition):

        """

        :param table_name: Name of the table.
        :param new_val: {
            col_name_1: new_val_1,
            col_name_2: new_val_2,
            ...
        }
        :param condition: col_name_3 = val_3 AND/OR col_name_4 = val_4 ...
        :return: Result of the operation
        """
        try:
            val_str = []
            for k, v in new_val.items():
                if type(v) == str:
                    new_v = "'" + v + "'"
                else:
                    new_v = str(v)
                val_str.append(k + "=" + new_v)
            val_str = ",".join(val_str)
            query = "update " + table_name + " set " + val_str + " where " + condition
            print(query)
            self.cursor.execute(query)
            self.result = "updated records successfully"
            self.lg.log("info", self.result)
        except Exception as e:
            self.result = str(e)
            self.lg.log("error", self.result)

    def delete(self, table_name, condition):
        """

        :param table_name: Name of the table
        :param condition: col_name_3 = val_3 AND/OR col_name_4 = val_4 ...
        :return:
        """
        try:
            query = "delete from " + table_name + " where " + condition
            self.cursor.execute(query)
            self.result = "deleted records successfully"
            self.lg.log("info", self.result)
        except Exception as e:
            self.result = str(e)
            self.lg.log("error", self.result)

    def download(self, table_name, filepath):

        """

        :param table_name: Name of the table.
        :param filepath: "path\\to\\new_file.csv"
        :return: Result of the operation.
        """
        try:
            df = pd.read_sql(f"select * from {table_name}".format(table_name=table_name), self.my_db)
            df.to_csv(filepath, index=False)
            self.result = "downloaded file successfully"
            self.lg.log("info", self.result)
        except Exception as e:
            self.result = str(e)
            self.lg.log("error", self.result)

    def display(self, query):
        self.cursor.execute(query)
        row = self.cursor.fetchall()
        for i in row:
            print(row)

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import csv
import pandas as pd
from logger.log_db import Logger


class Operations:

    def __init__(self):
        """
        Establishes connection to a cassandra database.
        """
        try:
            self.lg = Logger("cassandra")
            cloud_config = {'secure_connect_bundle': r'C:\Users\ASUS\Downloads\iNeuron\secure-connect-test.zip'}
            auth_provider = PlainTextAuthProvider('DSgpjBMbeAnjZIXUsrpPEuaK', 'SO9EBm5T1iZd_tEahBB8MmTz6TZG_Xu9ZjNWIpbH7HMsqylz8.7XfxknpQ2bMMJ6.7-heplORnSKc1zuRGFc-m,mEi3cB+wTZZRC.LpNQZZx1e5bntf--qu,vZ9JBJe-')
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            self.session = cluster.connect()
            self.result = "connection with database successful"
            self.lg.log("info", self.result)
        except Exception as e:
            self.result = str(e)
            self.lg.log("error", self.result)

    def create_table(self, table_name, cols):

        """

        :param table_name: keyspace_name.name
            examples : "shubham.hello"
        :param cols:[
            {"col_name": "id", "data_type": "int", "constraint": "PRIMARY KEY"},
            {"col_name": "name", "data_type": "text", "constraint": ""},
            {"col_name": "count", "data_type": "int", "constraint": ""}
        ]
        :return: result of the operation.
        """

        try:
            schema = []
            for i in cols:
                schema.append(i['col_name'] + ' ' + i['data_type'] + ' ' + i['constraint'])
            query = "create table " + table_name + " (" + ','.join(schema) + ")"
            self.session.execute(query)
            self.result = "created table " + table_name
            self.lg.log("info", self.result)
        except Exception as e:
            self.result = str(e)
            self.lg.log("error", self.result)

    def insert_one(self, table_name, record):
        """

        :param table_name: keyspace_name.name
            examples : "shubham.hello"
        :param record: {
            entity_1: val_1,
            entity_2: val_2,
            entity_3: val_3,
            ...
         }
        :return: Result of the operation
        """
        try:
            col_name = ','.join(record.keys())
            values = ','.join(list(map(lambda x: "'" + x + "'" if type(x) == str else str(x), record.values())))
            query = "insert into " + table_name + " (" + col_name + ")" + " values (" + values + ")"
            self.session.execute(query)
            self.result = "inserted a record"
            self.lg.log("info", self.result)
        except Exception as e:
            self.result = str(e)
            self.lg.log("error", self.result)

    def insert_many(self, table_name, filepath, cols):
        """

        :param table_name: keyspace_name.name
            examples : "shubham.hello"
        :param filepath: "path\\to\\the\\csv\\file"
        :param cols: [
            {"col_name": "id", "data_type": "int"},
            {"col_name": "name", "data_type": "text"},
            {"col_name": "count", "data_type": "int"}
        ]
        NOTE :- The col_name in the list should be in order as it is in the csv file.
        :return: Result of the operation.
        """
        try:
            int_types = ["bigint", "int", "smallint", "tinyint", "counter"]
            float_types = ["float", "double"]
            str_types = ["text", "date", "time"]
            bool_types = ["boolean"]

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

        :param table_name: keyspace_name.name
            examples : "shubham.hello"
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
            self.session.execute(query)
            self.result = "updated records successfully"
            self.lg.log("info", self.result)
        except Exception as e:
            self.result = str(e)
            self.lg.log("error", self.result)

    def delete(self, table_name, condition, col_names=[]):

        """

        :param table_name: keyspace_name.name
            examples : "shubham.hello"
        :param condition: col_name_3 = val_3 AND/OR col_name_4 = val_4 ...
        :param col_names: Optional
            [col_name_1, col_name_2, ...]
        :return: Result of the operation.
        """
        try:
            col_str = ','.join(col_names)
            query = "delete " + col_str + " from " + table_name + " where " + condition
            self.session.execute(query)
            self.result = "deleted records successfully"
            self.lg.log("info", self.result)
        except Exception as e:
            self.result = str(e)
            self.lg.log("error", self.result)

    def download(self, table_name, filepath):

        """

        :param table_name: table_name: keyspace_name.name
            examples : "shubham.hello"
        :param filepath: path\\to\\new_file.csv
        :return: Result of the operation.
        """
        try:
            df = pd.DataFrame(self.session.execute(f"select * from {table_name}".format(table_name=table_name)).all())
            df.to_csv(filepath, index=False)
            self.result = "downloaded file successfully"
            self.lg.log("info", self.result)
        except Exception as e:
            self.result = str(e)
            self.lg.log("error", self.result)

    def display(self, query):
        row = self.session.execute(query).all()
        for i in row:
            print(i)

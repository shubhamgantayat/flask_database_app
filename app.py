from flask import Flask, request, jsonify
from cassandra_db.crud import Operations as cassandra_operations
from mongo_db.crud import Operations as mongo_operations
from my_sql.crud import Operations as sql_operations
from logger.log_db import Logger

app = Flask(__name__)
lg = Logger('app')


# CASSANDRA 


@app.route('/cassandra/create_table', methods=['GET', 'POST'])
def cassandra_create_table():
    result = ''
    try:
        if request.method == 'POST':
            table_name = request.json['table_name']
            cols = request.json['cols']
            my_db = cassandra_operations()
            my_db.create_table(table_name, cols)
            result = my_db.result
    except Exception as e:
        result = str(e)
        lg.log("error", result)
    finally:
        return jsonify(result)


@app.route('/cassandra/insert_one', methods=['GET', 'POST'])
def cassandra_insert_one():
    result = ''
    try:
        if request.method == "POST":
            table_name = request.json['table_name']
            record = request.json['record']
            my_db = cassandra_operations()
            my_db.insert_one(table_name, record)
            result = my_db.result
    except Exception as e:
        result = str(e)
        lg.log("error", result)
    finally:
        return jsonify(result)


@app.route('/cassandra/insert_many', methods=['GET', 'POST'])
def cassandra_insert_many():
    result = ''
    try:
        if request.method == 'POST':
            table_name = request.json['table_name']
            filepath = request.json['filepath']
            cols = request.json['cols']
            my_db = cassandra_operations()
            my_db.insert_many(table_name, filepath, cols)
            result = my_db.result
    except Exception as e:
        result = str(e)
        lg.log("error", result)
    finally:
        return jsonify(result)


@app.route('/cassandra/update', methods=['GET', 'POST'])
def cassandra_update():
    result = ''
    try:
        if request.method == 'POST':
            table_name = request.json['table_name']
            new_val = request.json['new_val']
            condition = request.json['condition']
            my_db = cassandra_operations()
            my_db.update(table_name, new_val, condition)
            result = my_db.result
    except Exception as e:
        result = str(e)
        lg.log("error", result)
    finally:
        return jsonify(result)


@app.route('/cassandra/delete', methods=['GET', 'POST'])
def cassandra_delete():
    result = ''
    try:
        if request.method == 'POST':
            table_name = request.json['table_name']
            condition = request.json['condition']
            col_names = request.json['col_names']
            my_db = cassandra_operations()
            my_db.delete(table_name, condition, col_names)
            result = my_db.result
    except Exception as e:
        result = str(e)
        lg.log("error", result)
    finally:
        return jsonify(result)


@app.route('/cassandra/download', methods=['GET', 'POST'])
def cassandra_download():
    result = ''
    try:
        if request.method == 'POST':
            table_name = request.json['table_name']
            filepath = request.json['filepath']
            my_db = cassandra_operations()
            my_db.download(table_name, filepath)
            result = my_db.result
    except Exception as e:
        result = str(e)
        lg.log("error", result)
    finally:
        return jsonify(result)


# MY SQL


@app.route('/sql/create_table', methods=['GET', 'POST'])
def sql_create_table():
    result = ''
    try:
        if request.method == 'POST':
            table_name = request.json['table_name']
            cols = request.json['cols']
            my_db = sql_operations("test", "password")
            my_db.create_table(table_name, cols)
            result = my_db.result
    except Exception as e:
        result = str(e)
        lg.log("error", result)
    finally:
        return jsonify(result)


@app.route('/sql/insert_one', methods=['GET', 'POST'])
def sql_insert_one():
    result = ''
    try:
        if request.method == "POST":
            table_name = request.json['table_name']
            record = request.json['record']
            my_db = sql_operations("test", "password")
            my_db.insert_one(table_name, record)
            result = my_db.result
    except Exception as e:
        result = str(e)
        lg.log("error", result)
    finally:
        return jsonify(result)


@app.route('/sql/insert_many', methods=['GET', 'POST'])
def sql_insert_many():
    result = ''
    try:
        if request.method == 'POST':
            table_name = request.json['table_name']
            filepath = request.json['filepath']
            cols = request.json['cols']
            my_db = sql_operations("test", "password")
            my_db.insert_many(table_name, filepath, cols)
            result = my_db.result
    except Exception as e:
        result = str(e)
        lg.log("error", result)
    finally:
        return jsonify(result)


@app.route('/sql/update', methods=['GET', 'POST'])
def sql_update():
    result = ''
    try:
        if request.method == 'POST':
            table_name = request.json['table_name']
            new_val = request.json['new_val']
            condition = request.json['condition']
            my_db = sql_operations("test", "password")
            my_db.update(table_name, new_val, condition)
            result = my_db.result
    except Exception as e:
        result = str(e)
        lg.log("error", result)
    finally:
        return jsonify(result)


@app.route('/sql/delete', methods=['GET', 'POST'])
def sql_delete():
    result = ''
    try:
        if request.method == 'POST':
            table_name = request.json['table_name']
            condition = request.json['condition']
            my_db = sql_operations("test", "password")
            my_db.delete(table_name, condition)
            result = my_db.result
    except Exception as e:
        result = str(e)
        lg.log("error", result)
    finally:
        return jsonify(result)


@app.route('/sql/download', methods=['GET', 'POST'])
def sql_download():
    result = ''
    try:
        if request.method == 'POST':
            table_name = request.json['table_name']
            filepath = request.json['filepath']
            my_db = sql_operations("test", "password")
            my_db.download(table_name, filepath)
            result = my_db.result
    except Exception as e:
        result = str(e)
        lg.log("error", result)
    finally:
        return jsonify(result)


# MONGO DB

@app.route('/mongo/create_table', methods=['GET', 'POST'])
def mongo_create_table():
    result = ''
    try:
        if request.method == 'POST':
            table_name = request.json['table_name']
            my_db = mongo_operations("shubham")
            my_db.create_table(table_name)
            result = my_db.result
    except Exception as e:
        result = str(e)
        lg.log("error", result)
    finally:
        return jsonify(result)


@app.route('/mongo/insert_one', methods=['GET', 'POST'])
def mongo_insert_one():
    result = ''
    try:
        if request.method == "POST":
            table_name = request.json['table_name']
            record = request.json['record']
            my_db = mongo_operations("shubham")
            my_db.insert_one(table_name, record)
            result = my_db.result
    except Exception as e:
        result = str(e)
        lg.log("error", result)
    finally:
        return jsonify(result)


@app.route('/mongo/insert_many', methods=['GET', 'POST'])
def mongo_insert_many():
    result = ''
    try:
        if request.method == 'POST':
            table_name = request.json['table_name']
            filepath = request.json['filepath']
            cols = request.json['cols']
            my_db = mongo_operations("shubham")
            my_db.insert_many(table_name, filepath, cols)
            result = my_db.result
    except Exception as e:
        result = str(e)
        lg.log("error", result)
    finally:
        return jsonify(result)


@app.route('/mongo/update', methods=['GET', 'POST'])
def mongo_update():
    result = ''
    try:
        if request.method == 'POST':
            table_name = request.json['table_name']
            new_val = request.json['new_val']
            condition = request.json['condition']
            my_db = mongo_operations("shubham")
            my_db.update(table_name, condition, new_val)
            result = my_db.result
    except Exception as e:
        result = str(e)
        lg.log("error", result)
    finally:
        return jsonify(result)


@app.route('/mongo/delete', methods=['GET', 'POST'])
def mongo_delete():
    result = ''
    try:
        if request.method == 'POST':
            table_name = request.json['table_name']
            condition = request.json['condition']
            my_db = mongo_operations("shubham")
            my_db.delete(table_name, condition)
            result = my_db.result
    except Exception as e:
        result = str(e)
        lg.log("error", result)
    finally:
        return jsonify(result)


@app.route('/mongo/download', methods=['GET', 'POST'])
def mongo_download():
    result = ''
    try:
        if request.method == 'POST':
            table_name = request.json['table_name']
            filepath = request.json['filepath']
            my_db = mongo_operations("shubham")
            my_db.download(table_name, filepath)
            result = my_db.result
    except Exception as e:
        result = str(e)
        lg.log("error", result)
    finally:
        return jsonify(result)


if __name__ == '__main__':
    app.run()

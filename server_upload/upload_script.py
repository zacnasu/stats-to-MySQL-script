import sys
import mysql.connector
from mysql.connector import Error
import json


files = ["Dec05","Dec07","Jan02","Jan08","Jan10","Jan22","Feb02","Feb05","Feb07","Feb09","Feb12","Feb14","Feb19","Feb21","Feb22","Feb25","Feb26","Feb28","Mar04"]

months = {"Dec":"2019-12-","Jan":"2020-01-","Feb":"2020-02-","Mar":"2020-03-"}

def main():
    json_file = open('personalInfo.json')
    info = json.load(json_file)

    check_data(files,"ES")
    check_data(files,"PP")
    check_data(files,"PK")
    table_rows = []
    for file in files:
        table_rows.extend(read_file(file,"ES"))
        table_rows.extend(read_file(file,"PP"))
        table_rows.extend(read_file(file,"PK"))
    #repurposed from PYnative article written by Vishal
    try:
        connection = mysql.connector.connect(host=info['host'],
                                            database=info['database'],
                                            auth_plugin=info['auth_plugin'],
                                            user=info['user'],
                                            password=info['password']
                                            )

        mySql_insert_query = """INSERT INTO data (shift_num, game_date, player1, player2, player3, player4, player5, SOGF, MSF, SOF, SOGA, MSA, SOA, shift_type, period)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

        cursor = connection.cursor()
        cursor.executemany(mySql_insert_query, table_rows)
        connection.commit()
        print(cursor.rowcount, "Record inserted successfully into Laptop table")
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


    except mysql.connector.Error as error:
        print("Failed to insert record into MySQL table {}".format(error))

def read_file(file, type):
    try:
        fileptr = open("files/"+ file + type + ".txt", 'r')
    except FileNotFoundError:
        print("Invalid Input File " + file + type)
        sys.exit(1)
    list_of_rows = []
    period = 1
    for index, line in enumerate(fileptr):
        arr = line.split()
        if len(arr) != 0:
            if arr[0] == arr[1] == arr[2] == arr[3] == arr[4] == "0":
                period += 1
            else:
                list_of_rows.append((index+1,get_date(file),arr[0],arr[1],arr[2],arr[3],arr[4],arr[5],arr[6],arr[7],arr[8],arr[9],arr[10],type,period))

    return list_of_rows

def get_date(file_name):
    year_month = months[file_name[:-2]]
    if year_month == None:
        print("error in" + file_name)

    return year_month + file_name[3:]

def check_data(file_list,type):
    for file in file_list:
        try:
            fileptr = open("files/"+ file + type + ".txt", 'r')
        except FileNotFoundError:
            print("Invalid Input File " + file + type)
            sys.exit(1)
        for index, line in enumerate(fileptr):
            to_out = line.split()
            if len(to_out) != 11 and 0:
                print(to_out)
                print(file + type + " " + str(index))
                sys.exit(1)


if __name__ == "__main__":
    main()

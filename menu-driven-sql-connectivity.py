import pymysql
from decimal import Decimal

# Establish a connection to the database
conn = pymysql.connect(host="127.0.0.1", user="root", password="")
cur = conn.cursor()

# Global variable to store the selected database
dbSelected = "No"

def createDatabase():
    try:
        # Prompt user for a new database name
        dbname = input("\nEnter database name: ")
        query = f"CREATE DATABASE IF NOT EXISTS {dbname}"
        cur.execute(query)
        conn.commit()
        print(f"\n>>> {dbname} database created! <<<")
        print("\n---------------------------------------")
    except:
        print("!!!!!!! SOME ERROR OCCURRED !!!!!!!")
        conn.rollback()

def showDatabases():
    try:
        # Show a list of existing databases
        query = f"SHOW DATABASES;"
        cur.execute(query)
        rows = cur.fetchall()
        rows = [row[0] for row in rows]
        print(f"\n >>> {len(rows)} Databases <<< \n")
        for row in rows:
            print(row)
        print("\n---------------------------------------")
    except:
        print("\n!!!!!!! SOME ERROR OCCURRED !!!!!!!")
        print("\n---------------------------------------")
        conn.rollback()

def useDatabase():
    try:
        global dbSelected
        # Prompt user to select a database
        dbname = input("\nEnter database name: ")
        query = f"USE {dbname};"
        cur.execute(query)
        conn.commit()
        dbSelected = dbname
        print(f"\n>>> {dbname} database is selected! <<<")
        print("\n---------------------------------------")
    except:
        print("\n!!!!!!! SOME ERROR OCCURRED !!!!!!!")
        print("\n---------------------------------------")
        conn.rollback()

def createTable():
    try:
        # Prompt user to create a new table with attributes
        tname = input("\nEnter table name: ")
        query = f"CREATE TABLE IF NOT EXISTS {tname} ("
        ans = "yes"
        while ans == "yes":
            ans = input("Do you want to add attributes?(yes/no): ")
            if ans == "yes":
                attributeName = input("Enter attribute name with its constraints: ")
                query += attributeName + ", "

        query = query[0:len(query)-2]
        query += ");"
        cur.execute(f'{query}')
        conn.commit()
        print(f"\n>>> Table {tname} is created successfully! <<<")
        print("\n---------------------------------------")
    except:
        print("\n!!!!!!! SOME ERROR OCCURRED !!!!!!!")
        print("\n---------------------------------------")
        conn.rollback()

def showTables():
    try:
        # Show a list of tables in the selected database
        cur.execute(f'SHOW TABLES')
        rows = cur.fetchall()
        rows = [row[0] for row in rows]
        print(f"\n >>> {len(rows)} Tables <<< \n")
        for row in rows:
            print(row)
        print("\n---------------------------------------")
    except:
        print("\n!!!!!!! SOME ERROR OCCURRED !!!!!!!")
        print("\n---------------------------------------")
        conn.rollback()

def DescribeTable():
    try:
        # Describe the structure of a specific table
        tname = input("\nEnter table name: ")
        cur.execute(f'DESC {tname}')
        rows = cur.fetchall()
        print("\n >>> Rows <<< \n")
        for row in rows:
            print(row)
        print("\n---------------------------------------")
    except:
        print("\n!!!!!!! SOME ERROR OCCURRED !!!!!!!")
        print("\n---------------------------------------")
        conn.rollback()

def dropTable():
    try:
        # Drop a table
        tname = input("\nEnter table name: ")
        query = f"DROP TABLE {tname}"
        cur.execute(query)
        conn.commit()
        print("\n >>> Table deleted successfully! <<<")
        print("\n---------------------------------------")
    except:
        print("\n!!!!!!! SOME ERROR OCCURRED !!!!!!!")
        print("\n---------------------------------------")
        conn.rollback()

def insertRecords():
    try:
        # Insert records into a table
        tname = input("\nEnter table name: ")
        query = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tname}';"
        cur.execute(query)
        rows = cur.fetchall()
        attributeNames = [attributeName[0] for attributeName in rows]
        attributeTypes = [attributeType[1] for attributeType in rows]
        query = f"INSERT INTO {tname} VALUES("

        for i in range(0, len(rows)):
            if(attributeTypes[i] == "int"):
                query += f'{input(f"{attributeNames[i]} = ")}' + ", "
                
            elif(attributeTypes[i] == "decimal"):
                query += f'{Decimal(input(f"{attributeNames[i]} = "))}' + ", "

            else:
                query += f"'{input(f"{attributeNames[i]} = ")}'" + ", "

        query = query[0:len(query)-2]
        query += ");"
        cur.execute(f'{query}')
        conn.commit()
        print("\n >>> Record pupulated successfully! <<<")
        print("\n---------------------------------------")
    except:
        print("\n!!!!!!! SOME ERROR OCCURRED !!!!!!!")
        print("\n---------------------------------------")
        conn.rollback()

def selectRecords():
    try:
        # Select and display records from a table
        tname = input("\nEnter table name: ")
        query = f"SELECT * FROM {tname};"
        cur.execute(query)
        rows = cur.fetchall()
        print("\n")
        for row in rows:
            print(row)
        print("\n---------------------------------------")
    except:
        print("\n!!!!!!! SOME ERROR OCCURRED !!!!!!!")
        print("\n---------------------------------------")
        conn.rollback()

def updateRecords():
    try:
        # Update records in a table
        tname = input("\nEnter table name: ")        
        query = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_NAME = '{tname}' AND CONSTRAINT_NAME = 'PRIMARY'
        """
        cur.execute(query)
        colName = input("Enter column name: ")
        colValue = input("Enter column value: ")
        pkCol = cur.fetchone()[0]
        pkColValue = input(f"Enter the value of {pkCol} where you want to update {colName}: ")
        selectQuery = f"SELECT {pkCol} FROM {tname} WHERE {pkCol} = {pkColValue};"
        cur.execute(selectQuery)
        result = cur.fetchone()

        if result:
            query = f"UPDATE {tname} SET {colName} = '{colValue}' WHERE {pkCol} = {pkColValue};"
            cur.execute(query)
            conn.commit()
            print("\n >>> Record updated successfully! <<<")
        else:
            print(f"\n >>> Record with {pkCol} = {pkColValue} is not present!")
        print("\n---------------------------------------")
    except:
        print("\n!!!!!!! SOME ERROR OCCURRED !!!!!!!")
        print("\n---------------------------------------")
        conn.rollback()

def deleteRecords():
    try:
        # Delete records from a table
        tname = input("\nEnter table name: ")        
        query = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_NAME = '{tname}' AND CONSTRAINT_NAME = 'PRIMARY'
        """
        cur.execute(query)
        pkCol = cur.fetchone()[0]
        pkColValue = input(f"{pkCol} = ")
        selectQuery = f"SELECT {pkCol} FROM {tname} WHERE {pkCol} = {pkColValue}"
        cur.execute(selectQuery)
        result = cur.fetchone()

        if result:
            query = f"DELETE FROM {tname} WHERE {pkCol} = {pkColValue}"
            cur.execute(query)
            conn.commit()
            print("\n >>> Record deleted successfully! <<<")
        else:
            print(f"\n >>> Record with {pkCol} = {pkColValue} is not present!")
        print("\n---------------------------------------")
    except:
        print("\n!!!!!!! SOME ERROR OCCURRED !!!!!!!")
        print("\n---------------------------------------")
        conn.rollback()

# Main program loop
isContinue = True
while isContinue:
    print("\n================================================")
    print(f">>>>> {dbSelected} database is selected! <<<<<<")
    print("================================================\n")

    # Display menu options
    print("1. Create Database")
    print("2. Show Databases")
    print("3. Use Database")
    print("4. Create Table")
    print("5. Show Tables")
    print("6. Describe Table")
    print("7. Drop Table")
    print("8. Insert Records")
    print("9. Select Records")
    print("10. Update Records")
    print("11. Delete Records")
    print("12. Exit Program")

    # Prompt user for choice
    choice = int(input("\nEnter your choice: "))
    print("\n---------------------------------------")
    
    # Perform the selected action based on user choice
    if choice == 1:
        createDatabase()
    elif choice == 2:
        showDatabases()
    elif choice == 3:
        useDatabase()
    elif choice == 4:
        createTable()
    elif choice == 5:
        showTables()
    elif choice == 6:
        DescribeTable()
    elif choice == 7:
        dropTable()
    elif choice == 8:
        insertRecords()
    elif choice == 9:
        selectRecords()
    elif choice == 10:
        updateRecords()
    elif choice == 11:
        deleteRecords()
    elif choice == 12:
        isContinue = False
    else:
        print("Invalid Choice!")

# Close the database connection
conn.close()
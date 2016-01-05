#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import operator
import threading
from decimal import Decimal

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'customer'
SECOND_TABLE_NAME = 'orders'
SORT_COLUMN_NAME_FIRST_TABLE = 'customerid'
SORT_COLUMN_NAME_SECOND_TABLE = 'customerid'
JOIN_COLUMN_NAME_FIRST_TABLE = 'customerid'
JOIN_COLUMN_NAME_SECOND_TABLE = 'customerid'
##########################################################################################################


# Do not close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    records=[]
    
    try:
        cursor=openconnection.cursor()
        records=Partition(InputTable, SortingColumnName, openconnection)
        
        # get column details and  column number  
        columns=GetColumnDetails(InputTable,cursor)
        columnno=GetColumnNo(columns,SortingColumnName)
        
        # distributing each chuck to a thread    
        print records
        for r in records:
            thread1=threading.Thread(target=SortTheList,args=(r,columnno))
            thread1.start()
            thread1.join()
                       
        #Create the output table
        CreateTable(OutputTable,columns,openconnection)
        
        # insert the sorted records into the output table
        InsertResult(OutputTable,records,openconnection)
    except Exception as details:
        print details 
         

    # Sort the tables
    # partition the tables into five chunks - Range partition
    # Merge the table 
    # Sort the output in a new table
def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    ParallelSort(InputTable1, Table1JoinColumn,'jointable1', openconnection)
    ParallelSort(InputTable2, Table2JoinColumn, 'jointable2', openconnection)
    
    table1=Partition('jointable1',Table1JoinColumn, openconnection)
    
    cursor=openconnection.cursor()
    
    #get all the column details and the column no for Table1JoinColumn and Table2JoinColumn 
    join1columns=GetColumnDetails('jointable1',cursor)
    join1columnno=GetColumnNo(join1columns,Table1JoinColumn)
        
    join2columns=GetColumnDetails('jointable2',cursor)
    join2columnno=GetColumnNo(join2columns,Table2JoinColumn)
    for tmp in join2columns:
        if tmp[0] == Table2JoinColumn:
            join2columns[join2columnno]=(tmp[0] + "_2", tmp[1], tmp[2])

    # 5 threads working on the parition
    cursor.execute("select * from jointable2")
    table2=cursor.fetchall()
    joined=[]
    for frag in table1:
        thread=threading.Thread(target=JoinPartition,args=(frag,table2,join1columnno,join2columnno,joined))
        thread.start()
        thread.join()
    # Create the Output Table
    CreateTable(OutputTable,join1columns+join2columns,openconnection)
    #Insert the rows into the Output Table
    InsertResult(OutputTable,joined,openconnection)
    # Deleting the intermediate tables
    deleteTables('jointable1', openconnection)
    deleteTables('jointable2', openconnection)
    
def JoinPartition(fragments, table2,table1colnno,table2colnno,joined):
    
    i=0
    j=0
    
    while i< len(fragments) and j< len(table2):
        t1=fragments[i]
        t2=table2[j]
        if t1[table1colnno] < t2[table2colnno]:
            i=i+1
        elif t1[table1colnno] > t2[table2colnno]:
            j=j+1
        else:
            while t1[table1colnno] == t2[table2colnno] and i < len(fragments):
                t1=fragments[i]
                k=j
                
                while k < len(table2):
                    t2=table2[k]
                    if t1[table1colnno] == t2[table2colnno]: 
                        joined.append(t1+t2)
                        print str(t1+t2)
                    k=k+1                    
                i=i+1
                    

def GetColumnDetails(InputTable,cursor):
    query="SELECT column_name,data_type,character_maximum_length FROM information_schema.columns WHERE table_name = '"+InputTable+"'"
    cursor.execute(query)
    return cursor.fetchall()

def GetColumnNo(columns,SortingColumnName):
    column=[]
    for a in columns:
            column.append(a[0])
    return column.index(SortingColumnName)
        
def SortTheList(partitions,columnno):
    partitions.sort(key=operator.itemgetter(columnno))

def Partition(InputTable,SortingColumnName,openconnection):
            # get the minimum and maximum value in the column

    records=[]
    cursor=openconnection.cursor()
    query="select min("+SortingColumnName+"), max("+SortingColumnName+") from "+InputTable
    cursor.execute(query)
    values=cursor.fetchall()
    values=values[0]

    #Partition the table in 5 chunks
    step=(Decimal(values[1])-Decimal(values[0]))/Decimal(5)
    high=Decimal(values[0])+Decimal(step)

    query2="select * from "+InputTable+" where "+SortingColumnName+" between "+str(values[0])+" and "+str(high)
    cursor.execute(query2)
    a=cursor.fetchall()
    records.append(a)
 
    low=high
    while  high < values[1]:
        high=Decimal(low)+Decimal(step) 
        query3="select * from "+InputTable+" where "+SortingColumnName+" > "+str(low)+" and "+SortingColumnName+" <= "+str(high)
        cursor.execute(query3)
        a=cursor.fetchall()
        #print a
        records.append(a)
        low=high 


    return records
        
def CreateTable(OutputTable,columns,openconnection):
    datafields=" "
    for a in columns:
        datafields=datafields+ str(a[0]) +" "+str(a[1])+" "
        if a[2]!=None:
            datafields=datafields + "(" + str(a[2]) + ")"
        datafields = datafields+","    
      
    cursor=openconnection.cursor()        
    create="create table " + OutputTable + "(" + datafields[:-1] + " )"
    cursor.execute(create)
    openconnection.commit()

def InsertResult(OutputTable,records,openconnection):
    cursor=openconnection.cursor()

    for row in records:
        if type(row) is list:
            for t in row:
                insert="insert into "+OutputTable+" values "+str(t)
                cursor.execute(insert)
                openconnection.commit()
        elif type(row) is tuple:
                insert="insert into "+OutputTable+" values "+str(row)
                cursor.execute(insert)
                openconnection.commit()

################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Do not change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
        # Creating Database ddsassignment2
        print "Creating Database named as ddsassignment2"
        createDB();
        
        # Getting connection to the database
        print "Getting connection from the ddsassignment2 database"
        con = getOpenConnection();

        # Calling ParallelSort
        print "Performing Parallel Sort"
        #''
        ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE,'parallelSortOutputTable', con);
        #ParallelSort('ratings', 'rating', 'parallelSortOutputTable', con)
        #ParallelSort('company', 'id', 'sortedcompany', con)
        # Calling ParallelJoin
        print "Performing Parallel Join"
        ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);

        # Saving parallelSortOutputTable and parallelJoinOutputTable on two files
        saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
        saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

        # Deleting parallelSortOutputTable and parallelJoinOutputTable
        #deleteTables('parallelSortOutputTable', con);
        #deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail

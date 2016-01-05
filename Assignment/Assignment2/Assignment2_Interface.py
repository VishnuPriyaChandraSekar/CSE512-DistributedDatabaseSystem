#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import sys



# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    #Implement RangeQuery Here.
    roundrobin=RoundRobinSelectQuery(TableName=ratingsTableName,MinValue=ratingMinValue,MaxValue=ratingMaxValue,openconnection=openconnection)
    rangepartitions=RangeSelectQuery(TableName=ratingsTableName,MinValue=ratingMinValue,MaxValue=ratingMaxValue,openconnection=openconnection)
    if bool(roundrobin) and bool(rangepartitions):
        result=dict(roundrobin.items()+rangepartitions.items())
        output=open("RangeQueryOut.txt","wb")
        for key in result.keys():
            for row in result.get(key):
                output.write(str(key))
                print row
                for value in row:
                    output.write(", "+str(value))
                output.write("\n")
        output.close()
    else: 
        print "Querying failed"  
    return    
    
def RoundRobinSelectQuery(TableName,MinValue,MaxValue,openconnection):
    partition={}
    tableprefix="roundrobinratingspart"
    try:
        cursor=openconnection.cursor()
        query="select * from information_schema.tables where table_name like '"+tableprefix+ "%'"
        cursor.execute(query)
        noofpartition=cursor.rowcount
        for i in range(0,noofpartition):
            table=tableprefix+str(i)
            partition.setdefault(table,[])
            query="select * from "+table+" where rating between "+str(MinValue)+" and "+str(MaxValue)
            cursor.execute(query)
            rows=cursor.fetchall()
            for row in rows:
                partition[table].append(row)
        return partition    
    except Exception as details:
        print details

def RangeSelectQuery(TableName,MinValue,MaxValue,openconnection):
    rangepartition={}
    try:
        cursor=openconnection.cursor()
        query="select partitionnum from RangeRatingsMetadata where minrating between "+str(MinValue)+" and "+str(MaxValue)+" or maxrating between "+str(MinValue)+" and "+str(MaxValue)
        cursor.execute(query)
        tables=cursor.fetchall();
        for table in tables:
            tableprefix="rangeratingspart" +str(table[0])
            rangepartition.setdefault(tableprefix,[])
            query="select * from "+tableprefix+" where rating between "+str(MinValue)+" and "+str(MaxValue)
            cursor.execute(query)
            rows=cursor.fetchall()
            for row in rows:
                rangepartition[tableprefix].append(row)
        return rangepartition
    except Exception as details:
        print details
        
def PointQuery(ratingsTableName, ratingValue, openconnection):
    #Implement PointQuery Here.
    roundrobin=RoundRobinPointQuery(TableName=ratingsTableName, rating=ratingValue, openconnection=openconnection)
    rangepartition=RangePointQuery(TableName=ratingsTableName,rating=ratingValue,openconnection=openconnection)
    if bool(roundrobin) and bool(rangepartition):
        result=dict(roundrobin.items()+rangepartition.items())
        output=open("PointQueryOut.txt","wb")
        for key in result.keys():
            for row in result.get(key):
                output.write(str(key))
                print row
                for value in row:
                    output.write(", "+str(value))
                output.write("\n")
        output.close()
    else:
        print "Querying failed"   
    return    
    
def RoundRobinPointQuery(TableName,rating,openconnection):
    partition={}
    tableprefix="roundrobinratingspart"
    try:
        cursor=openconnection.cursor()
        query="select * from information_schema.tables where table_name like '"+tableprefix+ "%'"
        cursor.execute(query)
        noofpartition=cursor.rowcount
        print 'before for loop : %d '%noofpartition
        for i in range(0,noofpartition):
            table=tableprefix+str(i)
            partition.setdefault(table,[])
            query="select * from "+table+" where rating="+str(rating)
            cursor.execute(query)
            rows=cursor.fetchall()
            for row in rows:
                partition[table].append(row)
        return partition    
    except Exception as details:
        print details

def RangePointQuery(TableName,rating,openconnection):
    rangepartition={}
    try:
        cursor=openconnection.cursor()
        query="select partitionnum from RangeRatingsMetadata where minrating < "+str(rating)+" or maxrating >= "+str(rating)
        cursor.execute(query)
        tables=cursor.fetchall();
        for table in tables:
            tableprefix="rangeratingspart" +str(table[0])
            rangepartition.setdefault(tableprefix,[])
            query="select * from "+tableprefix+" where rating="+str(rating)
            cursor.execute(query)
            rows=cursor.fetchall()
            for row in rows:
                rangepartition[tableprefix].append(row)
        return rangepartition
    except Exception as details:
        print details

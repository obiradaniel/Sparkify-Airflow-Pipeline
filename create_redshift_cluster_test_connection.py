# -*- coding: utf-8 -*-
"""
Created on Sun Aug  7 22:34:02 2022

@author: ObiraDaniel
This is modified from the Udacity IaC Solution Exercise for Cloud Data Warehouses
Exercise 2: Creating Redshift Cluster using the AWS python SDK (Boto3)
Course 2: Data Engineering

1. Reads Configuration parameters
2. Creates Cluster
3. Checks status every 20 seconds till cluster is active/available
4. Prints Cluster Details
5. Can pause or delete Cluster a fixed amount of time. (Script can be threaded)
"""
import pandas as pd, datetime
import boto3
import json
import configparser
import psycopg2

import time

startTime = datetime.datetime.now()

# Class of different styles
class style():
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    UNDERLINE = '\033[4m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

# 1.0 Reading Config Parameters
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
             })

# 1.1 AWS Clients for EC2, S3, IAM. Redshift
ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )

s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )

iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name="us-west-2"
                  )

redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

# 2 Create RedShift Cluster that is publically accessible
#IAM Role
startProvCluster = datetime.datetime.now()
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
try:
    response = redshift.create_cluster(        
        #HW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        #Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        
        #Public Access
        PubliclyAccessible=True,
        
        #Load Sample Data, (Tickit Database)
        #LoadSampleData='Yes',
        
        #Roles (for s3 access)
        IamRoles=[roleArn]  
    )
except Exception as e:
    print(e)
print(style.GREEN + style.BOLD+'\n\n',datetime.datetime.now() - startProvCluster, \
      "Time to Iniatiate provisiong\n\n")
startCluster = datetime.datetime.now()
# 3 Keep Checking Cluster status every 20 seconds

def prettyRedshiftProps(props):
    #pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
ClusterStatusQuery = prettyRedshiftProps(myClusterProps)
clusterstate = ClusterStatusQuery[ClusterStatusQuery["Key"]=="ClusterStatus"]["Value"].values[0]
print(ClusterStatusQuery)
trys = 0

while ('available' not in clusterstate.lower()):
    print("\n  Trying again in 20 seconds, for {} time(s), {} seconds passed, status {}.\n ".format(trys, trys*20, clusterstate))
    time.sleep(20)
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    ClusterStatusQuery = prettyRedshiftProps(myClusterProps)
    clusterstate = ClusterStatusQuery[ClusterStatusQuery["Key"]=="ClusterStatus"]["Value"].values[0]
    #print(clusterstate)
    trys += 1
    
print(style.GREEN + style.BOLD+'\n\n',datetime.datetime.now() - startCluster, \
      "Time to Create and Avail Cluster\n\n")
    
DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

# 4 Allowing TCP Port for RedShift Traffic
try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    print(e)
    
# 5 Connect to Cluster
#Wait for 2 minutes
"""
print("Waiting for 2 minutes")
count = 0
while count < 25:
    print("\r{} Seconds".format(count*5), end='')
    time.sleep(5)
    count += 1

print("\n")
"""

redshift_query = """
select tab.table_schema,
       tab.table_name,
       tinf.tbl_rows as rows
from svv_tables tab
join svv_table_info tinf
          on tab.table_schema = tinf.schema
          and tab.table_name = tinf.table
where tab.table_type = 'BASE TABLE'
      and tab.table_schema not in('pg_catalog','information_schema')
      and tinf.tbl_rows > 1
order by tinf.tbl_rows desc;
"""

try:
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
    cursor = conn.cursor()
    print("Connection Successful")
    print(conn)
    cursor.execute(redshift_query)
    row = cursor.fetchone()
    while row:
        print(row)
        #print (str(row[0]) + " " + str(row[1]))
        row = cursor.fetchone()
    print("\nQuery Completed\n")
except Exception as e:
    print(e, "Connection Failed")
    
# 6 Delete or Pause Cluster after a fixed amount of time
"""
print("Waiting for 10 minutes then delete Cluster")
count = 0
while count < 121:
    print("\r{} Seconds, {} Minutes.".format(count*5, count*5/60.0), end='')
    time.sleep(5)
    count += 1

redshift.pause_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    
redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)


myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshiftProps(myClusterProps)
"""
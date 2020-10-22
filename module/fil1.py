import boto3
import botocore
from botocore.client import Config
import csv
import pymysql


def sn_1(info):
    file_name = info['file_path']

    ACCESS_KEY_ID = 'AKIAXB3Y4CS3GX34M5OS'
    ACCESS_SECRET_KEY = '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'
    AWS_DEFAULT_REGION = 'ap-south-1'
    BUCKET_NAME = 'kumar776'

    data = open(file_name, 'rb')

    s3_res = boto3.resource('s3',
                            aws_access_key_id=ACCESS_KEY_ID,
                            aws_secret_access_key=ACCESS_SECRET_KEY,
                            region_name=AWS_DEFAULT_REGION,
                            config=Config(signature_version='s3v4')
                            )
    # for uploading data
    s3_res.Bucket(BUCKET_NAME).put_object(Key=file_name, Body=data)

    # for downloading data from url
    s3_cli = boto3.client('s3',
                          config=Config(signature_version=botocore.UNSIGNED)
                          )
    params = {'Bucket': BUCKET_NAME, 'Key': file_name}
    url = s3_cli.generate_presigned_url('get_object', params)

    ##############################################

    edit_url = url.split('/')[-1]

    with open(edit_url, 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        next(csv_reader)  # removes first line

        list_data = []
        for i in csv_reader:
            # print(i)
            dict_data = {'id': i[0], 'period': i[1], 'short_descriptions': i[2], 'temperatures': i[3]}
            list_data.append(dict_data)
        # print(list_data)

        mydb = pymysql.connect(host='database.c42ojr1a1cpj.ap-south-1.rds.amazonaws.com',
                               user='root',
                               password='yskumar775',
                               db='aws3'
                               )

        cur = mydb.cursor()

        for i in list_data:
            a = i['id']
            b = i['period']
            c = i['short_descriptions']
            d = i['temperatures']

            query_1 = "insert into aws3_table values('" + str(a) + "', '" + str(b) + "', '" + str(c) + "', '" + str(
                d) + "')"
            cur.execute(query_1)

        mydb.commit()

        query_2 = "select * from aws3_table"
        cur.execute(query_2)
        s = cur.fetchall()

        total_list = []
        for i in s:
            all_dict = {'id': i[0], 'period': i[1], 'short_descriptions': i[2], 'temperatures': i[3]}
            total_list.append(all_dict)

        print(total_list)

        return total_list

    # return {'my_url': url}


def sn_2():

    mydb = pymysql.connect(host='database.c42ojr1a1cpj.ap-south-1.rds.amazonaws.com',
                           user='root',
                           password='yskumar775',
                           db='aws3'
                           )
    cur = mydb.cursor()

    query = "select * from aws3_table"

    cur.execute(query)

    s = cur.fetchall()

    total_list = []
    for i in s:
        all_dict = {'id': i[0], 'period': i[1], 'short_descriptions': i[2], 'temperatures': i[3]}
        total_list.append(all_dict)

    # print(total_list)

    return total_list

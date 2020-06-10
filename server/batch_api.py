import boto3
from flask import Blueprint

batch_route = Blueprint("batch", __name__)

s3 = boto3.resource("s3")
bucket = s3.Bucket("andrii-prysiazhnyk-bucket")


query1_obj = s3.Object("andrii-prysiazhnyk-bucket", "project/query1.json")
query2_obj = s3.Object("andrii-prysiazhnyk-bucket", "project/query2.json")
query3_obj = s3.Object("andrii-prysiazhnyk-bucket", "project/query3.json")


def get_content(obj):
    stream = obj.get()["Body"]
    byte_array = stream.read()
    return byte_array.decode("utf-8")


@batch_route.route("/query1")
def query1():
    return get_content(query1_obj)


@batch_route.route("/query2")
def query2():
    return get_content(query2_obj)


@batch_route.route("/query3")
def query3():
    return get_content(query3_obj)

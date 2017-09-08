from sklearn.externals import joblib
import boto3
import os, sys
import hashlib
import botocore

def analyzer_func(x): # used as token separation function for most models
    return x.split()

def get_local_md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest() #magic

def s3_file_exists(s3_name, bucket_name='models-analytics'):
    s3 = boto3.resource('s3')
    exists = False
    try:
        s3.Object(bucket_name, s3_name).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            exists = False
        else:
            raise e
    else:
        exists = True
    return exists

def save_model(local_obj, local_filename, s3_filename, overwrite=True, save_locally=False):
    model_exists = s3_file_exists(s3_filename)
    if not (overwrite==False and model_exists):
        upload_object_to_s3(s3_filename, local_obj, save_locally)

def update_local_model(local_filename, s3_filename):
    current_version = is_local_same_as_s3(local_filename, s3_filename)
    if not current_version:
        download_object_from_s3(s3_filename, local_filename)

def upload_object_to_s3(s3_name, py_obj, save_locally = False, bucket_name='models-analytics'):
    s3 = boto3.resource('s3')
    file_shortname = s3_name.split('/')[-1]
    try:
        joblib.dump(py_obj, file_shortname)
        s3.meta.client.upload_file(file_shortname, bucket_name, s3_name)
        if not save_locally:
            os.remove(file_shortname)
    except:
        raise Exception("Could not upload {} to s3 with error\n\n{}".format(file_shortname, sys.exc_info()))

def download_object_from_s3(s3_name, destination, bucket_name='models-analytics'):
    s3 = boto3.resource('s3')
    try:
        s3.meta.client.download_file(bucket_name, s3_name, destination)
    except:
        raise Exception("Could not download s3 file with error\n\n{}".format(sys.exc_info))

def remove_object_from_s3(s3_name, bucket_name='models-analytics'):
    client = boto3.client('s3')
    try:
        client.delete_object(Bucket=bucket_name, Key = s3_name)
    except:
        raise Exception("Could not delete s3 object with error\n\n{}".format(sys.exc_info))

def get_s3_file_md5(s3_name, bucket_name='models-analytics'):
    s3 = boto3.resource('s3')
    try:
        bucket = s3.Bucket(bucket_name)
        return bucket.Object(s3_name).e_tag
    except:
        raise Exception("Could not fetch s3 file md5 with error\n\n{}".format(sys.exc_info()))

def is_local_same_as_s3(local_filename, s3_filename):
    local_md5 = get_local_md5(local_filename)
    s3_md5 = get_s3_file_md5(s3_filename)
    if local_md5 == s3_md5:
        return True
    else:
        return False

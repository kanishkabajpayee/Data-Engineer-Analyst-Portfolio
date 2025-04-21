import s3fs
from utils.constants import AWS_ACCESS_KEY, AWS_ACCESS_KEY_ID, OUTPUT_PATH

def connect_to_s3_bucket():
    try:
        s3 = s3fs.S3FileSystem(anon=False,
                               key=AWS_ACCESS_KEY_ID,
                               secret=AWS_ACCESS_KEY)
        
        print("Connected to s3")
        return s3
    except Exception as e:
        print(e)

def create_bucket_if_not_exists(s3:s3fs.S3FileSystem,bucket_name:str):
    try:
        if not s3.exists(bucket_name):
            print("Bucket Doesn't Exits. Creating Bucket.....")
            s3.mkdir(bucket_name)
            print("Bucket Created")
        else:
            print("Bucket exists")
    except Exception as e:
        print("Error while creating bucket :",e)
    


def  upload_to_s3_bucket(s3:s3fs.S3FileSystem,file_path:str,bucket_name:str,file_name:str):
    try:
        file_path = OUTPUT_PATH+'/'+file_name
        s3.put(file_path,bucket_name+'/raw/'+file_name)
        print("File Uploaded to S3 Bucket")
    except Exception as e:
        print("Error while uploading:",e)
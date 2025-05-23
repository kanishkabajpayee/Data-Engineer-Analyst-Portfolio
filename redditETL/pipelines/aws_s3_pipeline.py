
from etls.aws_etl import connect_to_s3_bucket, create_bucket_if_not_exists, upload_to_s3_bucket
from utils.constants import AWS_BUCKET_NAME

#upload csv to s3 bucket
def upload_to_s3_pipeline(ti):
    file_path = ti.xcom_pull(task_ids='reddit_extraction',key='return_value')
    s3 = connect_to_s3_bucket()
    create_bucket_if_not_exists(s3,AWS_BUCKET_NAME)
    upload_to_s3_bucket(s3,file_path,AWS_BUCKET_NAME,file_path.split('/')[-1])
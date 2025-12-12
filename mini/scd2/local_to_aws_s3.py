import boto3

# Connecting to AWS S3
s3_resource  = boto3.resource('s3', 'us-east-1') # remember to add your own region of aws s3

# Function to upload file in S3
def s3_upload(file_name,fold,bkt):
    
    s3_bucket = s3_resource.Bucket(name=bkt)

    if True:
        s3_bucket.upload_file(
            Filename = file_name,
            Key = fold + '/' + file_name
            )
        return True


if __name__ == '__main__':

    # FILE_NAME = 'Product_Dim.csv'
    FILE_NAME = 'Product_Dim_1.csv'
    S3_FOLDER = 'raw_data'
    BUCKET_NAME = 'jl-scd2-data-bucket'

    status = s3_upload(FILE_NAME, S3_FOLDER, BUCKET_NAME)

    if(status):
        print('Data is saved')
    else:
        print('Error while loading data...')
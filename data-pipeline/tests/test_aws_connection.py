import os
import boto3 #boto3: AWS SDK for Python, AWS 서비스(S3 등)를 조작하는 라이브러리
from dotenv import load_dotenv

def test_aws_connection():

    load_dotenv()

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_DEFAULT_REGION")
    bucket_name = os.getenv("AWS_S3_BUCKET_NAME")

    print("Testing AWS Connection")
    print(f"Region: {region}")
    print(f"Bucket: {bucket_name}")

    try:
        # boto3로 S3에 접근할 수 있는 객체(Client)를 생성.
        s3 = boto3.client(
            "s3",
            aws_access_key_id = access_key,
            aws_secret_access_key = secret_key,
            region_name=region
        )

        # List buckets
        # list_buckets()를 호출할 수 있다는 건 S3 연결 성공을 의미
        buckets =  s3.list_buckets()
        print(f"Connected! You have {len(buckets['Buckets'])} S3 buckets")


        return True
    
    except Exception as e:
        print(f"Connection Failed: {e}")
        print("Check you .env file and aws credentials")
        return False
    
if __name__ == "__main__":
    test_aws_connection()


     


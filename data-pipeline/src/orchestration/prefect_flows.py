"""
TUTORIAL: Data Processing Pipeline
Download data from S3, clean it, transform it, and upload results back to S3.
"""

import os
import boto3
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
import tempfile


from prefect import flow, task, get_run_logger


#1. S3에서 데이터 추출(download/extract) 기능
@task(name="download_data_from_s3",retries=2,retry_delay_seconds=30,cache_policy=None)
def download_data_from_s3(s3,bucket_name):


    logger = get_run_logger()
    logger.info("Starting data download from s3")

    # CSV 파일을 담아둘 딕셔너리를 초기화
    datasets ={}

    data_files = ['customers.csv','products.csv','orders.csv','order_items.csv','reviews.csv']

    # 각 파일마다 S3 key(버킷 내의 경로)를 구성하고, 
    # 운영체제에 맞는 임시 저장 경로를 만들기 위해 tempfile.gettempdir()와 os.path.join을 사용해 로컬 임시 파일 경로를 만든다.
    for file_name in data_files:
        try:
            print(f'Downloading {file_name}...')

            s3_key = f"raw-data/{file_name}"
            local_path = os.path.join(tempfile.gettempdir(),file_name)

            # 다운로드 수행
            s3.download_file(bucket_name,s3_key,local_path)

            # pandas를 사용해 CSV를 데이터프레임으로 읽어들이고, 파일명에서 .csv 확장자를 제거한 문자열을 키로 하여 딕셔너리에 저장
            df = pd.read_csv(local_path)
            dataset_name = file_name.replace(".csv","")

            datasets[dataset_name] = df

            logger.info(f'Loaded {dataset_name}: {len(df)} records')
            # 메모리에 데이터프레임이 저장된 후에는 임시 파일이 더 이상 필요하지 않기 때문에 os.remove(local_path)로 파일을 삭제해 로컬 디스크를 정리
            os.remove(local_path)
        
        except Exception as e:
            logger.error(f"Failed to download {file_name}: {e}")
    # 모든 데이터프레임을 담고 있는 딕셔너리 객체를 반환
    return datasets


#2. 데이터 정제와 변환
@task(name="transform_data",retries=1)
def transform_data(datasets):

    logger = get_run_logger()

    # 가공된 데이터를 저장할 딕셔너리
    processed = {}

    # Process customer data
    if 'customers' in datasets:

        #원본 보존을 위해 복사본으로 진행
        customers = datasets['customers'].copy()

        # 이메일 컬럼 정제(pandas의 문자열 처리 기능을 이용해 모두 소문자로 변환하고, 앞뒤 공백을 제거하여 일관된 포맷)
        customers['email'] = customers['email'].str.lower().str.strip()

        # 날짜 컬럼을 표준화(어떤 형식이든 자동으로 파싱해 통일된 datetime 타입으로 변환)
        customers['date_of_birth'] = pd.to_datetime(customers['date_of_birth'])
        customers['registration_date'] = pd.to_datetime(customers['registration_date'])

        # 없는 데이터 추가 생성(생년월일 -> 나이 뽑아내기)
        customers['age'] = (datetime.now() - customers['date_of_birth']).dt.days // 365

        # 연령대
        customers['age_group'] = pd.cut(customers['age'], bins = [0, 25, 35, 50, 65, 100],
                                        labels = ['18-25','26-35','36-50','51-65','65+'])

        # 'customers_clean'라는 키에 정제된 customers DataFrame을 저장
        processed['customers_clean'] = customers

        logger.info(f'Processed customers: {len(customers)} records')


    # Process products data
    if 'products' in datasets:
        products = datasets['products'].copy()

        # 이름 컬럼 앞뒤 공백 제거
        products['product_name'] = products['product_name'].str.strip()

        # 가격 컬럼( ->숫자: 문자열, 공백을 포함해도 숫자로 변환할 수 있으면 변환)
        # 숫자로 바꿀 수 있는 값 → 숫자로 변환
        # 숫자로 바꿀 수 없는 값 → 오류 내지 않고 NaN으로 바꿔버림
        products['price'] = pd.to_numeric(products['price'], errors = 'coerce')

        # 가격 카테고리 생성
        products['price_category'] = pd.cut(products['price'], bins = [0, 50, 150, 500, float('inf')],
                                        labels = ['Budget','Mid-range','Premium','Luxury'])

        processed['products_clean'] = products

        logger.info(f'Processed products: {len(products)} records')

    # Process orders data
    if 'orders' in datasets:
        orders = datasets['orders'].copy()

        orders['order_date'] = pd.to_datetime(orders['order_date'])

        orders['total_amount'] = pd.to_numeric(orders['total_amount'], errors = 'coerce')

        # Extract month and year for seasonal analysis

        orders['order_month'] = orders['order_date'].dt.month
        orders['order_year'] = orders['order_date'].dt.year

        processed['orders_clean'] = orders

        logger.info(f'Processed orders: {len(orders)} records')


    # Process order items data
    if 'order_items' in datasets:
        order_items = datasets['order_items'].copy()

        order_items['quantity'] = pd.to_numeric(order_items['quantity'], errors = 'coerce')
        order_items['unit_price'] = pd.to_numeric(order_items['unit_price'], errors = 'coerce')

        order_items['total_price'] = order_items['quantity']*order_items['unit_price']

        processed['order_items_clean'] = order_items

        logger.info(f'Processed order_items: {len(order_items)} records')



    # Process review data
    if 'reviews' in datasets:
        reviews = datasets['reviews'].copy()

        reviews['review_date'] = pd.to_datetime(reviews['review_date'])

        reviews['rating'] = pd.to_numeric(reviews['rating'], errors = 'coerce')

        # 평점을 범주형 값으로 나누는 피처 엔지니어링 작업
        reviews['rating_category'] = reviews['rating'].apply(
            lambda x: 'Excellent' if x >=4.5 else
                        'Good' if x>=3.5 else
                        'Average' if x >=2.5 else 'Poor'
        )

        processed['reviews_clean'] = reviews

        logger.info(f'Processed reviews: {len(reviews)} records')
    
    return processed

#  비즈니스 메트릭 생성
@task(name="create_business_metrics",retries=1)
def create_business_metrics(processed_datasets):

    logger = get_run_logger()
    # 새로운 메트릭들을 저장할 빈 딕셔너리
    metrics = {}

    # Customer metric(고객 기반 메트릭)
    if 'customers_clean' in processed_datasets and 'orders_clean' in processed_datasets:
        customers = processed_datasets['customers_clean']
        orders = processed_datasets['orders_clean']

        # Customer lifetime value
        # 주문 데이터를 고객 ID로 그룹화한 다음 각 고객이 지금까지 구매한 총 금액, 총 주문 횟수, 평균 주문 금액을 계산, 첫 구매일과 마지막 구매일도 함께 집계
        customer_metrics = orders.groupby('customer_id').agg({
            'total_amount':['sum','count','mean'],
            'order_date':['min','max']
        }).round(2)

        customer_metrics.columns = ['total_spent','order_count','ave_order_value','first_order','last_order']

        customer_metrics = customer_metrics.reset_index()

        # 고객별 지표를 고객 정보 데이터와 결합

        customer_metrics = customer_metrics.merge(customers[['customer_id','age_group']])

        metrics['customer_metrics'] = customer_metrics

        logger.info(f'Created customer metrics: {len(customer_metrics)} customers')

    # Product metric
    if 'products_clean' in processed_datasets and 'order_items_clean' in processed_datasets:
        products = processed_datasets['products_clean']
        order_items = processed_datasets['order_items_clean']
        
        # Product sales metrics
        product_metrics = order_items.groupby('product_id').agg({
            'quantity': 'sum',
            'total_price': 'sum',
            'order_id': 'count'
        }).round(2)
        
        product_metrics.columns = ['total_quantity_sold', 'total_revenue', 'number_of_orders']
        product_metrics = product_metrics.reset_index()
        
        # product 데이터셋과 현재 만든 집계 결과를 merge
        product_metrics = product_metrics.merge(products[['product_id', 'product_name', 'category', 'price']], on='product_id')
        
        metrics['product_metrics'] = product_metrics
        logger.info(f"Created product metrics: {len(product_metrics)} products")

    # Monthly sales metric
    if 'orders_clean' in processed_datasets:
        orders = processed_datasets['orders_clean']
        
        monthly_sales = orders.groupby(['order_year', 'order_month']).agg({
            'total_amount': 'sum',
            'order_id': 'count'
        }).round(2)
        
        monthly_sales.columns = ['total_revenue', 'order_count']
        monthly_sales = monthly_sales.reset_index()
        
        metrics['monthly_sales'] = monthly_sales
        logger.info(f"Created monthly sales trends: {len(monthly_sales)} months")
    
    return metrics

# Load 과정
@task(name="upload_processed_data",retries=2,retry_delay_seconds=45,cache_policy=None)
def upload_processed_data(s3,bucket_name,processed,metrics):

    logger = get_run_logger()
    # 업로드 모니터링을 위한 업로드 개수를 추적하는 변수
    upload_count = 0
    # 총 업로드 파일 수
    total_files = len(processed) + len(metrics)

    # Upload processed datasets
    for dataset_name, df in processed.items():
        try:
            # 1. DataFrame을 로컬 임시 디렉토리에 CSV 파일로 임시 경로에 저장
            local_path = os.path.join(tempfile.gettempdir(), f"{dataset_name}.csv")
            df.to_csv(local_path, index=False)

            # 2. 생성된 CSV 파일을 S3로 업로드 
            s3_key = f"processed/{dataset_name}.csv"
            s3.upload_file(local_path,bucket_name,s3_key)

            logger.info(f"Uploaded {dataset_name}: {len(df)} records")
            upload_count += 1

            # 3. 업로드가 완료되면 로컬에 생성된 임시 파일을 삭제
            os.remove(local_path)

        except Exception as e:
            logger.error(f"Failed to upload {dataset_name}: {e}")

    # Upload business metrics
    for metric_name, df in metrics.items():
        try:
            # 메트릭 데이터 프레임을 CSV로 임시 저장
            local_path = os.path.join(tempfile.gettempdir(), f"{metric_name}.csv")
            df.to_csv(local_path, index=False)

            # S3 버킷의 processed/metrics/ 경로로 업로드
            s3_key = f"processed/metrics/{metric_name}.csv"
            s3.upload_file(local_path,bucket_name,s3_key)

            # 카운팅 통한 전체 진행 상황을 추적
            logger.info(f"Uploaded {metric_name}: {len(df)} records")
            upload_count += 1

            os.remove(local_path)

        except Exception as e:
            logger.error(f"Failed to upload {metric_name}: {e}")

    # 전체 업로드가 정상완료 확인
    return upload_count == total_files

@flow(name="ecommerce_etl_pipeline")
def process_ecommerce_data():
    """Download, process, and upload e-commerce data"""
    
    logger=get_run_logger()
    
    logger.info("Starting data processing with Prefect Orchestration")
        
    # Load environment variables
    load_dotenv()
    bucket_name = os.getenv('AWS_S3_BUCKET_NAME')
    region = os.getenv('AWS_DEFAULT_REGION', 'ap-northeast-2')
    
    if not bucket_name:
        logger.error("ERROR: AWS_S3_BUCKET_NAME not found in .env file!")
        return False
    
    logger.info(f"Processing data from bucket: {bucket_name}")
    
    try:
        # Create S3 client
        s3 = boto3.client('s3',region_name=region)
        
        # Step 1: Download data from S3
        logger.info("Step 1: Downloading data from S3...")
        datasets = download_data_from_s3(s3, bucket_name)
        
        # Step 2: Clean and transform data
        logger.info("Step 2: Cleaning and transforming data...")
        processed_datasets = transform_data(datasets)
        
        # Step 3: Create business metrics
        logger.info("Step 3: Creating business metrics...")
        business_metrics = create_business_metrics(processed_datasets)
        
        # Step 4: Upload processed data back to S3
        logger.info("Step 4: Uploading processed data to S3...")
        upload_success = upload_processed_data(s3, bucket_name, processed_datasets, business_metrics)
        
        if upload_success:
            logger.info("SUCCESS: Data processing pipeline completed!")
            return True
        else:
            logger.error("ERROR: Failed to upload processed data")
            return False
            
    except Exception as e:
        logger.error(f"ERROR: Data processing failed: {e}")
        return False




if __name__ == "__main__":

    success = process_ecommerce_data()

    if success:
        print("\nNext step: Orchestration with Prefect!")

    else:
        print("\nFix the errors and try again")

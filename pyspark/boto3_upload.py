import boto3

# MinIO 설정
minio_endpoint = "http://minio:9000"  # MinIO 서버 URL (http 또는 https)
minio_access_key = "oMGrfbg5iz0zgt1iMT5w"      # MinIO 액세스 키
minio_secret_key = "GQBVemsvQVSnypFw6qQaWj5eCBPjapVMux972Fpg"      # MinIO 비밀 키
minio_bucket_name = "vm-workplace"    # 업로드할 MinIO 버킷 이름
local_file_path = "/opt/airflow/pyspark_data/boto3_source"  # 업로드할 로컬 파일 경로
minio_object_name = "uploaded_data"   # MinIO 버킷 내 저장될 경로

# boto3 S3 클라이언트 생성
s3_client = boto3.client(
    's3',
    endpoint_url=minio_endpoint,  # MinIO 엔드포인트
    aws_access_key_id=minio_access_key,
    aws_secret_access_key=minio_secret_key
)

try:
    # 파일 업로드
    s3_client.upload_file(local_file_path, minio_bucket_name, minio_object_name)
    print(f"Uploaded {local_file_path} to MinIO bucket {minio_bucket_name}/{minio_object_name}")
except Exception as e:
    print(f"Error uploading file: {e}")
import boto3
import os

# MinIO 설정
minio_endpoint = "http://minio:9000"  # MinIO 서버 URL
minio_access_key = "oMGrfbg5iz0zgt1iMT5w"      # MinIO 액세스 키
minio_secret_key = "GQBVemsvQVSnypFw6qQaWj5eCBPjapVMux972Fpg"      # MinIO 비밀 키
minio_bucket_name = "vm-workplace"    # 업로드할 MinIO 버킷 이름
source_folder = "/opt/airflow/pyspark_data/boto3_source"  # 업로드할 로컬 폴더 경로
minio_target_folder = "uploaded_data"   # MinIO 버킷 내 저장될 폴더

# boto3 S3 클라이언트 생성
s3_client = boto3.client(
    's3',
    endpoint_url=minio_endpoint,  # MinIO 엔드포인트
    aws_access_key_id=minio_access_key,
    aws_secret_access_key=minio_secret_key
)

try:
    # 디렉터리 내 파일 업로드
    for root, _, files in os.walk(source_folder):
        for file_name in files:
            local_file_path = os.path.join(root, file_name)
            minio_object_name = os.path.join(minio_target_folder, os.path.relpath(local_file_path, source_folder)).replace("\\", "/")

            # 파일 업로드
            try:
                s3_client.upload_file(local_file_path, minio_bucket_name, minio_object_name)
                print(f"Uploaded {local_file_path} to MinIO bucket {minio_bucket_name}/{minio_object_name}")
            except Exception as e:
                print(f"Error uploading file {local_file_path}: {e}")

    print("All files uploaded successfully.")

except Exception as e:
    print(f"Error: {e}")
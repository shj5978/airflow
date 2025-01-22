import os
from minio import Minio

############################# MIN IO 설정 ################################################
minio_endpoint = "172.18.0.8:9000"  # MinIO 서버 URL
minio_access_key = "oMGrfbg5iz0zgt1iMT5w"  # MinIO 액세스 키
minio_secret_key = "GQBVemsvQVSnypFw6qQaWj5eCBPjapVMux972Fpg"  # MinIO 비밀 키
minio_bucket_name = "vm-workplace"  # MinIO 버킷 이름
source_folder = "/opt/airflow/pyspark_data/noaa_source" # 업로드할 파일들이 있는 로컬 폴더 경로
############################# MIN IO 설정 ################################################

############################# MIN IO 설정 ################################################
# MinIO 클라이언트 설정
minio_client = Minio(
    minio_endpoint,
    access_key=minio_access_key,
    secret_key=minio_secret_key,
    secure=False  # HTTPS 사용 여부
)
print("MinIO 클라이언트 설정 완료.")
############################# MIN IO 설정 ################################################

try:

    # Bucket 존재 여부 확인
    if minio_client.bucket_exists(minio_bucket_name):
        print(f"Bucket '{minio_bucket_name}' exists.")
    else:
        print(f"Bucket '{minio_bucket_name}' does not exist.")

except Exception as e:
    print(f"오류 발생: {e}")


# try:
#     ############################# MIN IO 에 업로드 ################################################
#     for root, _, files in os.walk(source_folder):
#         for file_name in files:
#             local_file_path = os.path.join(root, file_name)
#             minio_target_path = os.path.relpath(local_file_path, source_folder).replace("\\", "/")

#             try:
#                 # fput_object로 파일 업로드
#                 minio_client.fput_object(
#                     bucket_name=minio_bucket_name,
#                     object_name=minio_target_path,
#                     file_path=local_file_path
#                 )
#                 print(f"Uploaded {local_file_path} to MinIO at {minio_target_path}")
#             except Exception as e:
#                 print(f"Failed to upload {local_file_path}: {e}")
#     ############################# MIN IO 에 업로드 ################################################

#     print("처리 완료!!!!!!")

# except Exception as e:
#     print(f"오류 발생: {e}")
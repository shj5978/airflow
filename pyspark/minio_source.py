import fnmatch
import boto3
import botocore
import os
from minio import Minio
from botocore import UNSIGNED
from botocore.client import Config
from botocore.exceptions import NoCredentialsError

# AWS S3 및 MinIO 설정
aws_s3_bucket_name = "noaa-ghcn-pds"  # AWS S3 버킷 이름
aws_s3_folder = "csv.gz/by_station/"  # AWS S3 폴더 경로 (접두어만 지정)
local_target_folder = "/home/minio/storage/minio/vm-workplace/uploaded_data" # 로컬 저장 경로 ( MinIO 의 Bucket 경로로 설정 )

############################# MIN IO 설정 ################################################
# minio_endpoint = "172.19.0.2:9000"  # MinIO 서버 URL
# minio_access_key = "oMGrfbg5iz0zgt1iMT5w"  # MinIO 액세스 키
# minio_secret_key = "GQBVemsvQVSnypFw6qQaWj5eCBPjapVMux972Fpg"  # MinIO 비밀 키
# minio_bucket_name = "vm-workplace"  # MinIO 버킷 이름
# minio_target_folder = "uploaded_data/"  # MinIO 폴더 경로
############################# MIN IO 설정 ################################################

# AWS S3 클라이언트 설정 (액세스 키와 비밀 키 없이 접근)
s3_client = boto3.client(
    's3',
    aws_access_key_id='',  # 자격 증명 없이 설정
    aws_secret_access_key='',  # 자격 증명 없이 설정
    endpoint_url='https://s3.amazonaws.com',
    config=boto3.session.Config(signature_version=botocore.UNSIGNED),
)
print("AWS S3 클라이언트 설정 완료.")

############################# MIN IO 설정 ################################################
# # MinIO 클라이언트 설정
# minio_client = Minio(
#     minio_endpoint,
#     access_key=minio_access_key,
#     secret_key=minio_secret_key,
#     secure=False  # HTTPS 사용 여부
# )
# print("MinIO 클라이언트 설정 완료.")
############################# MIN IO 설정 ################################################

try:
    # 페이지네이션 설정: 다음 페이지가 있는지 확인하기 위해 계속 요청
    continuation_token = None
    while True:
        # S3에서 파일 목록 가져오기
        list_params = {
            "Bucket": aws_s3_bucket_name,
            "Prefix": aws_s3_folder,
        }
        
        if continuation_token:
            list_params["ContinuationToken"] = continuation_token
        
        s3_objects = s3_client.list_objects_v2(**list_params)

        for obj in s3_objects.get("Contents", []):
            file_key = obj["Key"]
            if file_key.endswith("/"):  # 폴더는 스킵
                continue
            
            # 파일 패턴 매칭 (ASN0000509*.csv.gz) 필터링
            if fnmatch.fnmatch(file_key, "csv.gz/by_station/ASN0000509*.csv.gz"):
                # 로컬 파일 경로 생성
                local_file_path = os.path.join(local_target_folder, file_key.split('/')[-1])

                # S3에서 파일 다운로드 및 저장
                with open(local_file_path, "wb") as local_file:
                    s3_object = s3_client.get_object(Bucket=aws_s3_bucket_name, Key=file_key)
                    local_file.write(s3_object['Body'].read())

                print(f"Downloaded {file_key} to {local_file_path}")
                
                ############################# MIN IO 설정 ################################################
                # # MinIO로 업로드 (스트리밍 전송)
                # minio_target_path = f"{minio_target_folder}{file_key.split('/')[-1]}"
                # minio_client.put_object(
                #     bucket_name=minio_bucket_name,
                #     object_name=minio_target_path,
                #     data=s3_client.get_object(Bucket=aws_s3_bucket_name, Key=file_key)["Body"],
                #     length=obj["Size"]
                # )
                # print(f"Uploaded {file_key} to MinIO at {minio_target_path}")
                ############################# MIN IO 설정 ################################################

        # 페이지네이션: 다음 페이지가 있으면 토큰 갱신
        continuation_token = s3_objects.get("NextContinuationToken")
        if not continuation_token:
            break  # 더 이상 페이지가 없으면 종료

except Exception as e:
    print(f"오류 발생: {e}")
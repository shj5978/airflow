import fnmatch
import boto3
import botocore
from minio import Minio
from botocore import UNSIGNED
from botocore.client import Config
from botocore.exceptions import NoCredentialsError

# AWS S3 및 MinIO 설정
aws_s3_bucket_name = "noaa-ghcn-pds"  # AWS S3 버킷 이름
aws_s3_folder = "csv.gz/by_station/"  # AWS S3 폴더 경로 (접두어만 지정)

minio_endpoint = "localhost:9000"  # MinIO 서버 URL
minio_access_key = "oMGrfbg5iz0zgt1iMT5w"  # MinIO 액세스 키
minio_secret_key = "GQBVemsvQVSnypFw6qQaWj5eCBPjapVMux972Fpg"  # MinIO 비밀 키
minio_bucket_name = "vm-workplace"  # MinIO 버킷 이름
minio_target_folder = "uploaded_data/"  # MinIO 폴더 경로

# AWS S3 클라이언트 설정 (액세스 키와 비밀 키 없이 접근)
s3_client = boto3.client(
    's3',
    aws_access_key_id='',  # 자격 증명 없이 설정
    aws_secret_access_key='',  # 자격 증명 없이 설정
    endpoint_url='https://s3.amazonaws.com',
    config=boto3.session.Config(signature_version=botocore.UNSIGNED),
)
print("AWS S3 클라이언트 설정 완료.")

# MinIO 클라이언트 설정
minio_client = Minio(
    minio_endpoint,
    access_key=minio_access_key,
    secret_key=minio_secret_key,
    secure=False  # HTTPS 사용 여부
)
print("MinIO 클라이언트 설정 완료.")

# MinIO 버킷이 없는 경우 생성
# if not minio_client.bucket_exists(minio_bucket_name):
#     minio_client.make_bucket(minio_bucket_name)

try:
    # 페이지네이션 설정: 다음 페이지가 있는지 확인하기 위해 계속 요청
    continuation_token = None
    while True:
        # S3에서 파일 목록 가져오기
        list_params = {
            "Bucket": aws_s3_bucket_name,
            "Prefix": aws_s3_folder,
        }

        print("여기까지는 OK(1)")
        
        if continuation_token:
            list_params["ContinuationToken"] = continuation_token

        print("여기까지는 OK(2)")
        
        s3_objects = s3_client.list_objects_v2(**list_params)

        print("여기까지는 OK(3)")

        for obj in s3_objects.get("Contents", []):
            file_key = obj["Key"]
            if file_key.endswith("/"):  # 폴더는 스킵
                continue

            # 파일 패턴 매칭 (ASN0000509*.csv.gz) 필터링
            if fnmatch.fnmatch(file_key, "csv.gz/by_station/ASN0000509*.csv.gz"):
                # S3 객체 URL 생성
                s3_object_url = f"https://{aws_s3_bucket_name}.s3.amazonaws.com/{file_key}"

                # MinIO로 업로드 (스트리밍 전송)
                minio_target_path = f"{minio_target_folder}{file_key.split('/')[-1]}"
                minio_client.put_object(
                    bucket_name=minio_bucket_name,
                    object_name=minio_target_path,
                    data=s3_client.get_object(Bucket=aws_s3_bucket_name, Key=file_key)["Body"],
                    length=obj["Size"]
                )
                print(f"Uploaded {file_key} to MinIO at {minio_target_path}")

        # 페이지네이션: 다음 페이지가 있으면 토큰 갱신
        continuation_token = s3_objects.get("NextContinuationToken")
        if not continuation_token:
            break  # 더 이상 페이지가 없으면 종료

except Exception as e:
    print(f"오류 발생: {e}")

#############################


# # 인증되지 않은 접근을 위해 signature_version='s3v4' 사용
# s3_client = boto3.client(
#     's3',
#     endpoint_url="https://s3.amazonaws.com",  # https:// 포함
#     aws_access_key_id="",
#     aws_secret_access_key="",
#     config=boto3.session.Config(signature_version=botocore.UNSIGNED),  # s3v4 서명 방식 사용
# )
# print("client 생성 완료")

# # S3 클라이언트로 객체 리스트 가져오기
# bucket_name = "noaa-ghcn-pds"
# prefix = "csv.gz/by_station/ASN0000509"

# try:
#     print("파일 목록 Read 시작")
#     response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
#     print("파일 목록 Read 완료")
#     if 'Contents' in response:
#         print("파일 목록:")
#         for obj in response['Contents']:
#             print(obj['Key'])
#     else:
#         print("파일이 존재하지 않습니다.")
# except NoCredentialsError:
#     print("자격 증명 오류: 자격 증명이 필요합니다.")
# except Exception as e:
#     print(f"오류 발생: {e}")
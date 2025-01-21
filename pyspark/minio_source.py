import boto3

# S3 클라이언트 생성 (Anonymous access 사용)
s3_client = boto3.client('s3', endpoint_url="https://s3.amazonaws.com")
print("client 생성 완료")

# 버킷과 파일 경로 설정
bucket_name = "noaa-ghcn-pds"
prefix = "csv.gz/by_station/ASN0000509"

# 버킷에서 파일 목록 가져오기
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
print("파일 목록 Read 완료")

# 파일 목록 출력
if 'Contents' in response:
    print("파일 목록:")
    for obj in response['Contents']:
        print(obj['Key'])
else:
    print("파일이 존재하지 않습니다.")

# from minio import Minio
# import boto3
# import fnmatch  # 패턴 매칭을 위한 fnmatch 모듈

# # AWS S3 및 MinIO 설정
# aws_s3_bucket_name = "noaa-ghcn-pds"  # AWS S3 버킷 이름
# aws_s3_folder = "csv.gz/by_station/"  # AWS S3 폴더 경로 (접두어만 지정)

# minio_endpoint = "localhost:9000"  # MinIO 서버 URL
# minio_access_key = "oMGrfbg5iz0zgt1iMT5w"  # MinIO 액세스 키
# minio_secret_key = "GQBVemsvQVSnypFw6qQaWj5eCBPjapVMux972Fpg"  # MinIO 비밀 키
# minio_bucket_name = "vm-workplace"  # MinIO 버킷 이름
# minio_target_folder = "uploaded_data/"  # MinIO 폴더 경로

# # AWS S3 클라이언트 설정 (액세스 키와 비밀 키 없이 접근)
# s3_client = boto3.client(
#     's3',
#     aws_access_key_id='',  # 자격 증명 없이 설정
#     aws_secret_access_key='',  # 자격 증명 없이 설정
#     config=boto3.session.Config(signature_version='v4'),
#     endpoint_url="s3.amazonaws.com"
# )
# print("AWS S3 클라이언트 설정 완료.")

# # MinIO 클라이언트 설정
# minio_client = Minio(
#     minio_endpoint,
#     access_key=minio_access_key,
#     secret_key=minio_secret_key,
#     secure=False  # HTTPS 사용 여부
# )
# print("MinIO 클라이언트 설정 완료.")

# # MinIO 버킷이 없는 경우 생성
# # if not minio_client.bucket_exists(minio_bucket_name):
# #     minio_client.make_bucket(minio_bucket_name)

# try:
#     # 페이지네이션 설정: 다음 페이지가 있는지 확인하기 위해 계속 요청
#     continuation_token = None
#     while True:
#         # S3에서 파일 목록 가져오기
#         list_params = {
#             "Bucket": aws_s3_bucket_name,
#             "Prefix": aws_s3_folder,
#         }
        
#         if continuation_token:
#             list_params["ContinuationToken"] = continuation_token
        
#         s3_objects = s3_client.list_objects_v2(**list_params)

#         for obj in s3_objects.get("Contents", []):
#             file_key = obj["Key"]
#             if file_key.endswith("/"):  # 폴더는 스킵
#                 continue

#             # 파일 패턴 매칭 (ASN0000509*.csv.gz) 필터링
#             if fnmatch.fnmatch(file_key, "csv.gz/by_station/ASN0000509*.csv.gz"):
#                 # S3 객체 URL 생성
#                 s3_object_url = f"https://{aws_s3_bucket_name}.s3.amazonaws.com/{file_key}"

#                 # MinIO로 업로드 (스트리밍 전송)
#                 minio_target_path = f"{minio_target_folder}{file_key.split('/')[-1]}"
#                 minio_client.put_object(
#                     bucket_name=minio_bucket_name,
#                     object_name=minio_target_path,
#                     data=s3_client.get_object(Bucket=aws_s3_bucket_name, Key=file_key)["Body"],
#                     length=obj["Size"]
#                 )
#                 print(f"Uploaded {file_key} to MinIO at {minio_target_path}")

#         # 페이지네이션: 다음 페이지가 있으면 토큰 갱신
#         continuation_token = s3_objects.get("NextContinuationToken")
#         if not continuation_token:
#             break  # 더 이상 페이지가 없으면 종료

# except Exception as e:
#     print(f"오류 발생: {e}")

#############################
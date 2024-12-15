def get_sftp():
    print('sftp 작업을 시작합니다.')

def regist(name, sex, *args):
    print('이름 : ' + name)
    print('성별 : ' + sex)
    print('기타옵션들(1) : ')
    for arg in args:
        print(',' + arg)
    print(f'기타옵션들(2) : {args}')
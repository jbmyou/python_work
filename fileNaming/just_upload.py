import os
import shutil
import pandas as pd
from tqdm import tqdm
import re
import traceback
from os.path import join
import file_function as ff
from datetime import datetime


# PATH = r'C:\Users\SL\Desktop\test'
# PATH_HAND = r"C:\Users\SL\Desktop\test\hand"
# PATH_LOG_SUCCESS = r'C:\Users\SL\Desktop\test\log\success'
# PATH_LOG_FAIL = r'C:\Users\SL\Desktop\test\log\fail'
# PATH_SERVER = r'C:\Users\SL\Desktop\test\server'
# PATH_OUT = r'C:\Users\SL\Desktop\test\관리제외'
# PATH_DF = r'./파일/채무자조회.pkl' #PATH_DF = r'./파일/채무자조회.xlsx



PATH = r'/volume1/스캔파일/새 스캔파일(업로드)/검수완료'
PATH_HAND = r"/volume1/스캔파일/새 스캔파일(업로드)/수작업"
PATH_LOG_SUCCESS = r'/volume1/스캔파일/스캔파일log/success'
PATH_LOG_FAIL = r'/volume1/스캔파일/스캔파일log/fail'
PATH_SERVER = r'/volume1/솔림헬프'
PATH_OUT = r'/volume1/삭제예정파일/관리제외'
PATH_DF = r'/volume1/스캔파일/스캔파일log/project/파일/채무자조회.pkl'

# 참조 df


def dict_refer():
    #df_c = pd.read_excel(PATH_DF)
    df_c = pd.read_pickle(PATH_DF)
    return dict(map(lambda x: (str(x[1].채무자키), [x[1].매각사구분, x[1].채무상태]), df_c.iterrows()))


dict_refer = dict_refer()

# file_list


def file_list(path):
    return [f.name for f in os.scandir(path) if f.is_file() and (f.name != 'Thumbs.db')]


file_list = file_list(PATH)


# 경로 정리
# 기본 변수
p_out = re.compile('개인회생\(면책\)|파산\(면책\)|환매|매각|종결')
p_key = re.compile(r"([\d]{8})[\D]")  # match

error = []
log = []
# 1) depth1 문서종류 매칭
# 등초본은 초본에 걸리고 파일명 전체에서 search로 찾기 때문에 ok
docu_folder_dict = {"원인서류": "1.원인서류", "양도통지서": "2.양도통지서", "집행권원": "3.집행권원", "강제집행": "4.강제집행", "등본": "5.등초본",
              "초본": "5.등초본", "등초본":"5.등초본", "외국인증명": "5.등초본", "개인회생": "6.개인회생", "신용회복": "7.신용회복", "파산": "8.파산", "재산조사": "9.재산조사", "기타": "기타"}

# 1) depth1 문서종류 매칭
cnt_total = len(file_list)
cnt_out = 0


for f in tqdm(file_list, total=cnt_total):
    
    depth3 = f[:8]  # 채무자키, 다 확인한 뒤니까 그냥 이렇게 해도 돼
    try:
        depth2 = dict_refer[depth3][0]  # 매각사구분
    except Exception as e:
        error.append([f, e.__class__, e])
        shutil.move(join(PATH, f), join(PATH_HAND, f))
        continue
    depth1 = ""  # 문서종류

    try :
        # 관리제외 파일이라면
        if p_out.match(dict_refer[depth3][1]):
            out_dir = join(PATH_OUT, depth2, depth3)
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)
            shutil.move(join(PATH, f), join(out_dir, f))
            cnt_out += 1
            continue

        # 관리중인 파일이면
        else:

            # depth1
            for key, value in docu_folder_dict.items():
                if re.search(key, f):
                    depth1 = value
                    break

            if depth1 == "":  # docu검사는 이미 했음.
                error.append([f, "docuError"])
                shutil.move(join(PATH, f), join(PATH_HAND, f))
                continue

            # 파일이동을 위한 준비(도착지 디렉토리 및 파일명 작성)
            dst_dir = os.path.join(PATH_SERVER, depth1, depth2, depth3)

            if not os.path.exists(dst_dir):
                os.makedirs(dst_dir)  # 미리 만들어뒀으니 mkdir해도 됨

            # 파일이동
            log.append(ff.re_name(join(PATH, f), join(dst_dir, f)))
            
        
    except Exception as e:
        print("===================================")
        time = datetime.today().strftime("%H:%M:%S")
        print(time)
        print(f, e.__class__, e, sep=" : ")
        print(traceback.format_exc())
        error.append([f, e.__class__, e])
        continue  # 반복문 계속 돌아

if cnt_total == len(log) + len(error) + cnt_out :
    print("처리된 파일에 누수 없음")
else :
    print("처리된 파일에 누수 있음")

print(f'{cnt_total}개의 파일 중 {len(log)}개 서버, {cnt_out}개 관리제외, {len(error)}개 예외')
ff.write_log_csv(log, PATH_LOG_SUCCESS)
ff.write_log_csv(error, PATH_LOG_FAIL)


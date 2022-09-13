from ast import expr_context
import os
import shutil
#import time
import pandas as pd
# tqdm(filelist, total = len(file_list), position=0, leave=True)
from tqdm.notebook import tqdm
import re
# from pathlib import Path
import traceback
from os.path import join
import file_function as ff
from datetime import datetime


PATH = r'C:\Users\SL\Desktop\test'
PATH_HAND = r"C:\Users\SL\Desktop\test\hand"
PATH_LOG_SUCCESS = r'C:\Users\SL\Desktop\test\log\success'
PATH_LOG_FAIL = r'C:\Users\SL\Desktop\test\log\fail'
PATH_LOG_BASIC_EXCEPT = r'C:\Users\SL\Desktop\test\log\basic_except'
PATH_SERVER = r'C:\Users\SL\Desktop\test\server'
PATH_OUT = r'C:\Users\SL\Desktop\test\관리제외'



# PATH = r'\\192.168.0.75\스캔파일\새 스캔파일(업로드)\0906'
# PATH_HAND = r"\\192.168.0.75\스캔파일\새 스캔파일(업로드)\수작업 필요"
# PATH_LOG_SUCCESS = r'\\192.168.0.75\스캔파일\스캔파일 log\success'
# PATH_LOG_FAIL = r'\\\192.168.0.75\스캔파일\스캔파일 log\fail'
# PATH_LOG_BASIC_EXCEPT = r'\\\192.168.0.75\스캔파일\스캔파일 log\basic_except'
# PATH_SERVER = r'\\192.168.0.75\솔림헬프'
# PATH_OUT = r'\\192.168.0.75\삭제예정파일\관리제외'

# 참조 df
def dict_refer():
    df_c = pd.read_excel(r'C:\Users\SL\Desktop\채무자조회.xlsx')
    return dict(map(lambda x : (str(x[1].채무자키),[x[1].매각사구분, x[1].채무상태]), df_c.iterrows()))
dict_refer = dict_refer()

# file_list
def file_list(path) :
    return [f.name for f in os.scandir(path) if f.is_file() and (f.name != 'Thumbs.db')]
file_list = file_list(PATH)



# 기본 변수
docu_folder_dict={"원인서류": "1.원인서류", "양도통지서": "2.양도통지서", "집행권원": "3.집행권원", "강제집행": "4.강제집행", "등본": "5.등초본",
                "초본": "5.등초본", "외국인": "5.등초본", "개인회생": "6.개인회생", "신용회복": "7.신용회복", "파산": "8.파산", "재산조사": "9.재산조사", "기타": "기타"}

# 수정 및 분류를 위한 컴파일 -----------------------------
# 키와 관리제외 확인을 위한 컴파일
p_out = re.compile('개인회생\(면책\)|파산\(면책\)|환매|매각|종결')
p_key = re.compile(r"([\d]{8})[\D]")  # match

docu_list=["", "원인서류", "양도통지서", "집행권원", "강제집행", "등본",
"초본", "외국인증명", "개인회생", "신용회복", "파산", "재산조사", "기타"]
# 문서구분 수정하기 위한 컴파일
p1=re.compile("원인 서류|입회 신청서|입회신청서")
p2=re.compile("양도 통지서|채권\s?양도\s?통지서|\s?양통\s|\s양통\s?")  # ? 는 {0,1}, 세양통신 해결
p3=re.compile("집행 권원|승계\s?집행문|판결문|지급\s?명령|이행\s?권고|화해\s?권고")
p4=re.compile("결정문|결정|[가-힣]+\s?타채|강제 집행") # '결정'이 반복될 수 있으모르 count=1
# p5 등본
p_5except = re.compile("등본초본|초본등본|등초본")
p6=re.compile("(?<![가-힣])원초본|(?<=원)원초본") # 이렇게까지 해야되냐? ㅠㅠ
# p7 외국인증명
# 연도 다음에 나오는 개회가 아닌 경우, 전방탐색을 통해 '개인회생'의 '회생'이 걸리는 거 방지. count도 하자
p8=re.compile(r"(?<!\d{2})개회|(?<!\d{2}\s)개회|개인 회생|(?<!개인)회생") 
p9=re.compile("(?<![가-힣0-9])신복(?![가-힣])|신용\s*회복") #이름에있는 신복,숫자뒤 신복은 제외. 이름다음에 띄어쓰기 없이 나온 신복은.. 
# p10 파산
p11=re.compile("재산 조사")

# 사건번호 수정하기 위한 컴파일
p_event1=re.compile(r"(?<=\D)(\d{4})\s?([ㄱ-ㅎ가-힣]{1,3})\s?([0-9]+)")
p_event2=re.compile(r"(?<=\D)(\d{2})\s?([ㄱ-ㅎ가-힣]{1,3})\s?([0-9]+)")

# 날짜는 굳이 수정할 필요없겠다. 오히려 6자리로 하면 사건번호와 겹칠 확률만 올라가겠네.
# 혹시 2022. 02. 22 이런식으로 공백 들어가 있는 것만 있는지 체크
# 결과적으로 날짜 / 주민번호의 공백만 제거하는 것
p_day=re.compile(r"(\d{2,4})\s(\d{2})\s(\d{2})")

# 항목별 저장을 위한 컴파일----------------------
docu_kind = r'원인서류|양도통지서[\s]?[\d]?[차]?|집행권원[\s]?[재]?[도]?|강제집행|등본|(?<!원)초본|외국인증명|개인회생|신용회복|파산|재산조사|기타'
#p_docu = re.compile(docu_kind)
p_basic = re.compile(r'(\d{8})[_\s]?(\D+.+)[_\s]?('+docu_kind+r')')
p_event = re.compile(r"\d{4}[ㄱ-ㅎ가-힣]{1,3}\d+")

# 서버 업로드 성공 로그와 에러발생 로그
error = []
log = []

# 문서명 수정 사례 모음
basic_except = [] # 수작업 폴더로 이동 나머지는 그냥 파일명 수정
docu_except = []
event_except = []
day_except = []
mark_except = []

# 카운트
cnt_total=len(file_list)
cnt_out = 0



for f in tqdm(file_list, total=cnt_total):
    try:
        n = os.path.splitext(f)[0]
        ext = os.path.splitext(f)[1]
        n = n
        depth1, depth2, depth3,  = "", "", ""
        key, name, docu, event, extra = "", "", "", "", ""

        # key 없는 거 수작업 폴더로
        if not p_key.match(n):  
            basic_except.append([f, "noKey"])
            shutil.move(join(PATH, f), join(PATH_HAND, f))
            continue

        # key 있는 거 파일명 다듬기
        # 1) 연속 공백 및 기타 기호 제외 : [_()]만 허용
        else:
            # 연속 공백 및 기호 제외하기
            n = re.sub("[\s]{2,}", " ", n)

            if re.search("[^a-zA-Zㄱ-ㅎ가-힣0-9\s_()]", n):
                n = re.sub("[^a-zA-Zㄱ-ㅎ가-힣0-9\s_()]", "", n)
                mark_except.append(f)

            
############ basic 미통과 파일명 내부 수정(사실상 문서구분) : 기타정보에 문서구분 단어가 들어간 경우 수정을 하지 않기 위함
            basic = p_basic.match(n)
            if basic == None : # 필수 양식에 맞지 않는다.

                before = n
                # 문서구분 수정하기 -------------------------------
                # 원인서류
                if p1.search(n):
                    n=p1.sub(docu_list[1], n, count=1)
                # 양도통지서
                elif p2.search(n):
                    n=p2.sub(docu_list[2], n, count=1)  # 이름에 양통들어가는 경우가 있을 수 있어서 공백씀..
                # 집행권원
                elif p3.search(n):
                    n=p3.sub(docu_list[3], n, count=1)
                elif p4.search(n):
                    n=p4.sub(docu_list[4], n, count=1)
                elif p6.search(n):
                    n=p6.sub(docu_list[6], n, count=1)
                elif p8.search(n):
                    n=p8.sub(docu_list[8], n, count=1) # 개회 반복될 수 있음
                elif p9.search(n):
                    n=p9.sub(docu_list[9], n, count=1)
                elif p11.search(n):
                    n=p11.sub(docu_list[11], n, count=1)
                
                after_docu = n
                
                # 변경 내역 기록
                if before != after_docu :
                    docu_except.append((f, after_docu))
            
############  basic과 무관 사건번호 수정하기 -------------------------------
            # 1) 사건번호 내부 공백 제거, 사건번호만 있으면 공백이 없어도 체크되므로 예외 추가는 전후 비교 후
            if p_event1.search(n):
                n=p_event1.sub(r'\1\2\3', n)
                # 1) + 사건번호 연도가 yy라면
            elif p_event2.search(n):
                n=p_event2.sub(r'20\1\2\3', n)
            after_event = n
            
            # 사건번호 변경됐을 경우 예외 log 추가
            if after_docu != after_event :
                event_except.append((f, after_event))

            # 날짜, 주민번호 내부 공백 제거 및 예외 log추가-------
            if p_day.search(n) :
                p_day.sub(r'\1\2\3', n)
                day_except.append((f, n))
            

########## 문서구분 변경 후 필수 요소 재확인 후 new_f 완성

            basic = p_basic.match(n)
            if basic == None : # 필수 양식에 맞지 않는다.
                shutil.move(join(PATH, f), join(PATH_HAND, (n+ext)))
                basic_except.append([f, n+ext])
                continue

            elif p_5except.search(n) :
                key = n[:8]
                name = n[8:p_5except.search(n).start()].replace("_", " ").strip()
                docu = '등초본'
                
            else :

                # 필수3요소 변수 저장
                key=basic.group(1)
                name=basic.group(2).strip()  # [/D]가 공백을 포함하므로
                docu=basic.group(3).strip()  # n차, 재도 때문에 공백을 포함한다.

            # (continue덕에 앞에 탭위치 앞으로)사건번호, 기타정보 변수 저장
            if p_event.search(n):
                temp=p_event.search(n)
                event=temp.group() # 이벤트
                extra=n[temp.end():].strip().replace(" ", "_") # 기타 정보, 앞뒤 공백은 제거하고 중간 공백은 _로
            else:
                extra=n[basic.end():].strip().replace(" ", "_") # 기타 정보, 앞뒤 공백은 제거하고 중간 공백은 _로

            # 새 이름 만들기
            name_items = [key, name, docu]
            if event : name_items.append(event)
            if extra : name_items.append(extra)

            new_f = "_".join(name_items)+ext
            # 마지막에 _ 두개 인 경우 꼭 해줘야 해.
            new_f = re.sub("[_]{2,}", "_", new_f)

    ########## 서버 올리기 ---------------------------------------------------------

            # 1) depth1 문서종류 매칭
            

            depth3=f[:8]  # 채무자키, 다 확인한 뒤니까 그냥 이렇게 해도 돼
            try :
                depth2=dict_refer[depth3][0]  # 매각사구분
            except Exception as e:
                error.append([f, e.__class__, e])
                #basic_except.append([f, new_f])
                shutil.move(join(PATH,f), join(PATH_HAND, new_f))
                continue
            depth1=""  # 문서종류

            # 관리제외 파일이라면
            if p_out.match(dict_refer[depth3][1]):
                out_dir=join(PATH_OUT, depth2, depth3)
                if not os.path.exists(out_dir):
                    os.makedirs(out_dir)
                shutil.move(join(PATH, f), join(out_dir, new_f))
                cnt_out += 1
                continue

            # 관리중인 파일이면
            else:

                # depth1
                for key, value in docu_folder_dict.items():
                    if re.search(key, new_f):
                        depth1=value
                        break

                if depth1 == "" : #docu검사는 이미 했음.
                    basic_except.append([new_f, "keyError"])
                    shutil.move(join(PATH,f), join(PATH_HAND, new_f))
                    continue


                # 파일이동을 위한 준비(도착지 디렉토리 및 파일명 작성)
                dst_dir=os.path.join(PATH_SERVER, depth1, depth2, depth3)

                if not os.path.exists(dst_dir):
                    os.makedirs(dst_dir)  # 미리 만들어뒀으니 mkdir해도 됨

                # 파일이동
                log.append(ff.re_name(join(PATH, f), join(dst_dir, new_f)))

    except Exception as e:
        print("===================================")
        time = datetime.today().strftime("%H:%M:%S")
        print(time)
        print(f, e.__class__, e, sep=" : ")
        print(traceback.format_exc())
        error.append([f, e.__class__, e])
        continue  # 반복문 계속 돌아

#### log #################################
try :
    ff.write_log_csv(log, PATH_LOG_SUCCESS)
    ff.write_log_csv(error, PATH_LOG_FAIL)
    ff.write_log_csv(basic_except, PATH_LOG_BASIC_EXCEPT)
except :
    pass
######################

cnt_hand = len(basic_except)
if cnt_total == len(log) + len(error) + cnt_hand + cnt_out :
    print("처리된 파일에 누수 없음")
else :
    print("처리된 파일에 누수 있음")

print(f'{cnt_total}개의 파일 중 {len(log)}개 서버, {cnt_out}개 관리제외, {cnt_hand}개 수작업, {len(error)}개 예외')
        

if docu_except :
    print("docu", *docu_except, sep='\n')
if event_except :
    print("event", *event_except, sep='\n')
if day_except :
    print("day", *day_except, sep='\n')
if mark_except :
    print("mark", *mark_except, sep='\n')
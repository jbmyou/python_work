import os
import shutil
#import time
import pandas as pd
# tqdm(filelist, total = len(file_list), position=0, leave=True)
from tqdm import tqdm
import re
# from pathlib import Path
import traceback
from os.path import join
import file_function as ff
from datetime import datetime

##differ########################################################################################
PATH = r'C:\Users\SL\Desktop\test\log\success'
PATH_RESULT = r'C:\Users\SL\Desktop\test\log\success\종합테스트'
PATH_DF = r'./파일/채무자조회.pkl' #PATH_DF = r'./파일/채무자조회.xlsx'
PATH_SERVER = r'/volume1/솔림헬프'
PATH_OUT = r'/volume1/삭제예정파일/관리제외'
##########################################################################################

# 참조 df
def dict_refer():
    #df_c = pd.read_excel(PATH_DF)
    df_c = pd.read_pickle(PATH_DF)
    return dict(map(lambda x : (str(x[1].채무자키),[x[1].매각사구분, x[1].채무상태]), df_c.iterrows()))
dict_refer = dict_refer()

###differ#######################################################################################
# file_list
def file_list(path) :
    logs = [f.name for f in os.scandir(path) if all([f.is_file(), re.search("csv$", f.name)])]
    col = ['src_dir', "src_file", "dst_file", "dst_dir"]
    df_c = pd.DataFrame(None, columns=col)
    for result in logs :
        df_temp = pd.read_csv(join(path,result), sep = ",", names=col)
        df_c = pd.concat([df_c, df_temp], axis = 0, ignore_index=True)

    return df_c

df_f = file_list(PATH)
##########################################################################################


# 기본 변수
docu_folder_dict = {"원인서류": "1.원인서류", "양도통지서": "2.양도통지서", "집행권원": "3.집행권원", "강제집행": "4.강제집행", "등본": "5.등초본",
              "초본": "5.등초본", "등초본":"5.등초본", "외국인증명": "5.등초본", "개인회생": "6.개인회생", "신용회복": "7.신용회복", "파산": "8.파산", "재산조사": "9.재산조사", "기타": "기타"}

# 필수 요소에 대한 컴파일
p_key = re.compile(r"([\d]{8})[\D]")  # match
docu_kind = r'원인서류|양도통지서[\s]?\d차|양도통지서|집행권원[\s]?[재]?[도]?|강제집행|등초본|(?<!등기부)등본|(?<!원)초본|외국인증명|개인회생|신용회복|파산|재산조사|기타'
p_docu = re.compile(docu_kind)
p_event = re.compile(r"\d{4}[ㄱ-ㅎ가-힣]{1,3}\d+")

p_basic = re.compile(r'(\d{8})[_\s]?(\D+.+)[_\s]?(' + docu_kind + r')') #event의 스타트 번호 >= basic의 엔드번호
p_out = re.compile('개인회생\(면책\)|파산\(면책\)|환매|매각|종결')

# 파일명 수정을 위한 컴파일--------------------------------
# 날짜
# 년월일 사이에 기호나 공백이 들어가 있는 경우로 찾아서 6자리로 맞춰준다.
# 괄호와 공백을 포함시켜서 모두 제거 대상으로하고 대체 단어에 앞공백만 추가
# 기호를 기준으로 쓰기 때문에 반드시 기호제거보다 나와야
p_day4 = re.compile(r'\s?\(?\s?(20|19)?(?P<y>\d{2})[.-/\s](?P<m>\d{1})[.-/\s](?P<d>\d{1})(?!\d)\s?\)?')
p_day5 = re.compile(r'\s?\(?\s?(20|19)?(?P<y>\d{2})[.-/\s](?P<m>\d{1})[.-/\s](?P<d>\d{2})\s?\)?')
p_day6 = re.compile(r'\s?\(?\s?(20|19)?(?P<y>\d{2})[.-/\s](?P<m>\d{2})[.-/\s](?P<d>\d{2})\s?\)?')
#중간기호는 없고 괄호로 감싸진 경우
p_day_4 = re.compile(r'\s?\(+\s?(20|19)?(?P<y>\d{2})(?P<m>\d{1})(?P<d>\d{1})(?!\d)\s?\)+')#괄호로 감싼거
p_day_5 = re.compile(r'\s?\(+\s?(20|19)?(?P<y>\d{2})(?P<m>\d{1})(?P<d>\d{2})\s?\)+')#괄호로 감싼거
p_day_6 = re.compile(r'\s?\(+\s?(20|19)?(?P<y>\d{2})(?P<m>\d{1,2})(?P<d>\d{1,2})\s?\)+')#괄호로 감싼거


# 사건번호
p_event1=re.compile(r"(?<=\D)(\d{4})\s?([ㄱ-ㅎ가-힣]{1,3})\s?([0-9]+)")
p_event2=re.compile(r"(?<=\D)(\d{2})\s?([ㄱ-ㅎ가-힣]{1,3})\s?([0-9]+)")
# 문서구분(사건번호 앞만 search하므로 더 간단하게 줄일 수 있는데 혹시 몰라 그냥 둔다.)
docu_list=["", "원인서류", "양도통지서", "집행권원", "강제집행", "등본",
"초본", "외국인증명", "개인회생", "신용회복", "파산", "재산조사", "기타"]
p1=re.compile(r"원인\s?서류|입회\s?신청서")
p2=re.compile(r"양도\s?통지서|채권\s?양도\s?통지서|\s?양통\s|\s양통\s?")  # ? 는 {0,1}, 세양통신 해결pass
p3=re.compile(r"집행\s?권원|승계\s?집행문|판결문|지급\s?명령|이행\s?권고|화해\s?권고") # count=1이 의미있게 하기 위해 올바른 표현도 넣는다.
p4=re.compile(r"강제\s?집행|결정문|(?<!\d)\s?타채|(?<!개시)\s?결정") # 결정이라는 말이 여러곳에서 나올 수 있다.ex개인회생 회생결정
# p5 등본 pass
p5except = re.compile(r"등본초본|초본등본|등초본")
p6=re.compile(r"(?<![가-힣])원초본|(?<=원)원초본") # 이렇게까지 해야되냐? ㅠㅠ
# p7 외국인증명. 컴파일 구문에서 외국인은 젤 끝에 와야 하는 거 유의
p7 = re.compile(r"외국인\s?증명서?|외국인\s?등록\s?사실\s?증명서?|외국인\s?등록증?|외국인")
# 연도 다음에 나오는 개회가 아닌 경우, 전방탐색을 통해 '개인회생'의 '회생'이 걸리는 거 방지. count도 하자
p8=re.compile(r"(?<!\d)개회|개인\s?회생|(?<!개인)회생")
p9=re.compile(r"(?<=[가-힣]{3})신복|[\s_]신복|신용\s?회복") #이름에있는 신복,숫자뒤 신복은 제외. 이름다음에 띄어쓰기 없이 나온 신복은.. 
# p10 파산 pass
p11=re.compile(r"재산\s?조사|재산\s?조회")
# -----------------------------------------------------------


##differ########################################################################################
result = []
dir_result = []
cnt_total=len(df_f.index)
##########################################################################################


# 로직 흐름 -----------------------------------------------------
# 파일명에러 : nobasic(f,k or d), 관리제외 : out(f,n), 솔림헬프 : success(f,n), 예외발생 : fail(f)
# 1) 키 없으면 >> 파일명에러, continue
#    키 있으면 key 값 저장
# 2) 문서명 수정
# 3) 사건번호 유 >> docu 없으면 | (있지만 !(docu.end<=event.start)) >> 파일명에러, continue
#       docu, name : key 와 docu 사이, event저장, extra 저장
#    사건번호 무 >> docu 없으면 >> 파일명에러, continue
#      docu, name : key 와 docu 사이, extra 저장
# 4) 리스트 담기, _ 연결하여 new_f완성
# 5) 파일업로드 : 관리제외라면 out, 폴더설정 및 이동
# 6) 예외처리 및 로그 작성
#----------------------------------------------------------------


for f in tqdm(df_f.src_file, total=cnt_total):
    
    n = os.path.splitext(f)[0]
    ext = os.path.splitext(f)[1]

    depth1, depth2, depth3,  = "", "", ""
    key, name, docu, event, extra = "", "", "", "", ""

    # key 없는 거 파일명에러 폴더로
    if not p_key.match(n):  
        result.append(n+ext)
        dir_result.append("127, nokey")
        continue

# key 파일명 다듬기__________________________________________________________
    else:
        key = p_key.match(n).group(1)##############################  key

        # 연속 공백 및 기호 제외하기
        n = re.sub("[\s]{2,}", " ", n)

        # 날짜, 주민번호 내부 공백 제거 및 예외 log추가-------
        if p_day4.search(n):
            n = p_day4.sub(r" \g<y>0\g<m>0\g<d>", n)
        elif p_day5.search(n) :
            n = p_day5.sub(r" \g<y>0\g<m>\g<d>", n)
        elif p_day6.search(n) :
            n = p_day6.sub(r" \g<y>\g<m>\g<d>", n)
        elif p_day_4.search(n) :
            n = p_day_4.sub(r" \g<y>0\g<m>0\g<d>", n)
        elif p_day_5.search(n) :
            n = p_day_5.sub(r" \g<y>0\g<m>\g<d>", n)
        elif p_day_6.search(n) :
            n = p_day_6.sub(r" \g<y>\g<m>\g<d>", n)
        else : pass

        # 기호정리
        if re.search("[^a-zA-Zㄱ-ㅎ가-힣0-9\s_()]", n):
            n = re.sub("[^a-zA-Zㄱ-ㅎ가-힣0-9\s_()]", "", n)

        # 사건번호 : 내부 공백 제거 및 연도에 20 없으면 추가
        if p_event1.search(n):
            n=p_event1.sub(r'\1\2\3', n)
            # 1) + 사건번호 연도가 yy라면
        elif p_event2.search(n):
            n=p_event2.sub(r'20\1\2\3', n)
        else : pass 

        # 법원문서 중 문서구분 서치 범위 설정
        e_s = None
        if p_event.search(n) :
            e_s = p_event.search(n).start()

        # 문서구분 수정하기 -------------------------------
        # 원인서류
        if p1.search(n):
            n=p1.sub(docu_list[1], n, count=1)
        # 양도통지서
        elif p2.search(n):
            n=p2.sub(docu_list[2], n, count=1)  # 이름에 양통들어가는 경우가 있을 수 있어서 공백씀..
        # 집행권원
        elif p3.search(n[:e_s]):
            n=p3.sub(docu_list[3], n, count=1)
        # 강제집행
        elif p4.search(n[:e_s]):
            n=p4.sub(docu_list[4], n, count=1)
        # 등초본
        elif p5except.search(n) :
            n=p5except.sub("등초본", n, count=1)
        # 초본
        elif p6.search(n):
            n=p6.sub(docu_list[6], n, count=1)
        # 외국인 증명
        elif p7.search(n):
            n=p7.sub(docu_list[7], n, count=1)
        #개인회생
        elif p8.search(n[:e_s]):
            n=p8.sub(docu_list[8], n, count=1) # 개회 반복될 수 있음
        #신용회복
        elif p9.search(n[:e_s]):
            n=p9.sub(docu_list[9], n, count=1)
        #재산조사
        elif p11.search(n):
            n=p11.sub(docu_list[11], n, count=1)     
        

# 새 파일명(new_f) 만들기__________________________________________________________
        if not p_docu.search(n) : #docu없으면
            result.append(n+ext)
            dir_result.append("193, nodocu")
            continue

        else : #docu 있으면
            d_obj = p_docu.search(n)##############################  docu, name
            name = n[8:d_obj.start()].replace("_", " ").strip() #replace가 먼저나와야
            docu = d_obj.group().strip() #양통, 집행권원 때문에 strip 필요

            if len(name) < 2 : 
                result.append(n+ext)
                dir_result.append("203, noname")
                continue
            else : pass
            
            
            if not p_event.search(n) : #사건번호 없으면
                ##############################  extra
                extra=n[d_obj.end():].strip().replace(" ", "_") #strip이 먼저 나와야
            else :  # 사건번호 있으면
                e_obj = p_event.search(n)
                event = e_obj.group()##############################  event
                extra = n[e_obj.end():].strip().replace(" ", "_")##############################  extra
                if d_obj.end() > e_obj.start() : # 사건번호앞에 문서구분 없는 경우
                    result.append(n+ext)
                    dir_result.append("217, nodocu")
                    continue
                else : pass #문서구분 - 사건번호 

            # new_f
            # continue 모두 해줬기 때문에 try 안에만 있으면 된다(현재는 key있는 경우)
            name_items = [key, name, docu]
            if event : name_items.append(event)
            if extra : name_items.append(extra) #일련번호나 _로 끝나는 경우는 re_name에서 제거해줌

            new_f = "_".join(name_items)+ext
            # 마지막에 _ 두개 인 경우 꼭 해줘야 해.
            new_f = re.sub("[_]{2,}", "_", new_f)

########## 서버 올리기 ---------------------------------------------------------

        # 1) depth1 문서종류 매칭
        #######################################################################################################################
        depth1 = ""
        depth3 = key  # 채무자키, 다 확인한 뒤니까 그냥 이렇게 해도 돼
        
        try :
            depth2=dict_refer[depth3][0]  # 매각사구분
            for k, v in docu_folder_dict.items() :
                if re.search(k, docu) :
                    depth1 = v
                    break
        
        except Exception as e:
            result.append(new_f)
            dir_result.append("247")
            continue
        
        # 관리제외 파일이라면
        if p_out.match(dict_refer[depth3][1]):
            out_dir=join(PATH_OUT, depth2, depth3)
            result.append(new_f)
            dir_result(out_dir)
            continue

        # 파일이동을 위한 준비(도착지 디렉토리 및 파일명 작성)
        dst_dir=os.path.join(PATH_SERVER, depth1, depth2, depth3)

        # 파일이동
        result.append(new_f)
        dir_result.append(dst_dir)

#### 반복문 끝########################################

df_f['result'] = result
df_f['dir_result'] = dir_result

print(f'{cnt_total} >>> {len(result)}개 서버')

df_f.to_csv(join(PATH_RESULT, "result.csv"), mode='w', encoding='utf-8-sig')
        
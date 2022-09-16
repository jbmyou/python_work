###################### basic 없이 그냥 key, docu로 해야겠다. 첫번째꺼를 알아서 찾으니까. 거기에 다큐end <= 이벤트start 조건만##################################################
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


PATH = r'C:\Users\SL\Desktop\test\log\success'
PATH_DF = r'./파일/채무자조회.xlsx'
PATH_SERVER = r'/volume1/솔림헬프'
key, name, docu = "", "", ""##########################################################################################

# 참조 df
def dict_refer():
    df_c = pd.read_excel(PATH_DF)
    return dict(map(lambda x : (str(x[1].채무자키),[x[1].매각사구분, x[1].채무상태]), df_c.iterrows()))
dict_refer = dict_refer()

# file_list
def file_list(path) :
    logs = [f.name for f in os.scandir(path) if all([f.is_file(), re.search("csv$", f.name)])]
    col = ['src_dir', "src_file", "dst_file", "dst_dir"]
    df_c = pd.DataFrame(None, columns=col)
    for result in logs :
        df_temp = pd.read_csv(join(path,result), sep = ",", names=col)
        df_c = pd.concat([df_c, df_temp], axis = 0)

    return df_c

df_f = file_list(PATH)



# 기본 변수
docu_folder_dict = {"원인서류": "1.원인서류", "양도통지서": "2.양도통지서", "집행권원": "3.집행권원", "강제집행": "4.강제집행", "등본": "5.등초본",
              "초본": "5.등초본", "등초본":"5.등초본", "외국인증명": "5.등초본", "개인회생": "6.개인회생", "신용회복": "7.신용회복", "파산": "8.파산", "재산조사": "9.재산조사", "기타": "기타"}

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
# p7 외국인증명. 컴파일 구문에서 외국인은 젤 끝에 와야 하는 거 유의
p7 = re.compile("외국인\s?증명서?|외국인\s?등록\s?사실\s?증명서?|외국인\s?등록증?|외국인")
# 연도 다음에 나오는 개회가 아닌 경우, 전방탐색을 통해 '개인회생'의 '회생'이 걸리는 거 방지. count도 하자
p8=re.compile(r"(?<!\d{2})개회|(?<!\d{2}\s)개회|개인 회생|(?<!개인)회생") 
p9=re.compile("(?<![가-힣0-9])신복(?![가-힣])|신용\s*회복") #이름에있는 신복,숫자뒤 신복은 제외. 이름다음에 띄어쓰기 없이 나온 신복은.. 
# p10 파산
p11=re.compile("재산 조사|재산\s?조회")#############################################################################################################################################################

# 사건번호 수정하기 위한 컴파일
p_event1=re.compile(r"(?<=\D)(\d{4})\s?([ㄱ-ㅎ가-힣]{1,3})\s?([0-9]+)")
p_event2=re.compile(r"(?<=\D)(\d{2})\s?([ㄱ-ㅎ가-힣]{1,3})\s?([0-9]+)")

# 년월일 사이에 기호나 공백이 들어가 있는 경우로 찾아서 6자리로 맞춰준다.
# 기호를 기준으로 쓰기 때문에 반드시 기호제거보다 나와야
p_day4 = re.compile(r'\s?\(?\d{0,2}(?P<y>\d{2})[.-/\s](?P<m>\d{1})[.-/\s](?P<d>\d{1})(?!\d)\)?')
p_day5 = re.compile(r'\s?\(?\d{0,2}(?P<y>\d{2})[.-/\s](?P<m>\d{1})[.-/\s](?P<d>\d{2})\)?')
p_day6 = re.compile(r'\s?\(?\d{0,2}(?P<y>\d{2})[.-/\s](?P<m>\d{2})[.-/\s](?P<d>\d{2})\)?')

# 항목별 저장을 위한 컴파일----------------------
docu_kind = r'원인서류|양도통지서[\s]?\d차|양도통지서|집행권원[\s]?[재]?[도]?|강제집행|(?<!등기부)등본|(?<!원)초본|외국인증명|개인회생|신용회복|파산|재산조사|기타'
#p_docu = re.compile(docu_kind)
p_basic = re.compile(r'(\d{8})[_\s]?(\D+.+)[_\s]?(' + docu_kind + r')') #event의 스타트 번호 >= basic의 엔드번호
p_event = re.compile(r"\d{4}[ㄱ-ㅎ가-힣]{1,3}\d+")

result = []
dir_result = []
cnt_total=len(df_f.index)
cnt_out = 0


for f in tqdm(df_f.src_file, total=cnt_total):
    n = os.path.splitext(f)[0]
    ext = os.path.splitext(f)[1]
    
    depth1, depth2, depth3,  = "", "", ""
    key, name, docu, event, extra = "", "", "", "", ""

    # key 없는 거 수작업 폴더로
    if not p_key.match(n):  
        result.append(n)
        dir_result.append("100")
        continue

    # key 있는 거 파일명 다듬기
    # 1) 연속 공백 및 기타 기호 제외 : [_()]만 허용
    else:
        
        # 날짜, 주민번호 내부 공백 제거 및 예외 log추가-------
        if p_day4.search(n):
            n = p_day4.sub(r" \g<y>0\g<m>0\g<d>", n)
        elif p_day5.search(n) :
            n = p_day5.sub(r" \g<y>0\g<m>\g<d>", n)
        elif p_day6.search(n) :
            n = p_day6.sub(r" \g<y>\g<m>\g<d>", n)
        else : pass

        # 연속 공백 및 기호 제외하기
        n = re.sub("[\s]{2,}", " ", n)

        if re.search("[^a-zA-Zㄱ-ㅎ가-힣0-9\s_()]", n):
            n = re.sub("[^a-zA-Zㄱ-ㅎ가-힣0-9\s_()]", "", n)

############ basic 미통과 파일명 내부 수정(사실상 문서구분) : 기타정보에 문서구분 단어가 들어간 경우 수정을 하지 않기 위함
        basic = p_basic.match(n)
        if basic == None : # 필수 양식에 맞지 않는다.

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
            elif p7.search(n):
                n=p7.sub(docu_list[7], n, count=1)
            elif p8.search(n):
                n=p8.sub(docu_list[8], n, count=1) # 개회 반복될 수 있음
            elif p9.search(n):
                n=p9.sub(docu_list[9], n, count=1)
            elif p11.search(n):
                n=p11.sub(docu_list[11], n, count=1)
        else : pass
        
        
############  basic과 무관 사건번호 수정하기 -------------------------------
        # 1) 사건번호 내부 공백 제거, 사건번호만 있으면 공백이 없어도 체크되므로 예외 추가는 전후 비교 후
        if p_event1.search(n):
            n=p_event1.sub(r'\1\2\3', n)
            # 1) + 사건번호 연도가 yy라면
        elif p_event2.search(n):
            n=p_event2.sub(r'20\1\2\3', n)
        else : pass
        

########## 문서구분 변경 후 필수 요소 재확인 후 new_f 완성

        basic = p_basic.match(n)
        if basic == None   : # 필수 양식에 맞지 않는다.
            result.append(n)
            dir_result.append("166")
            continue
#################################################################            
        elif (p_event.search(n)!= None) and (basic.end() > p_event.search(n).start()) : # &는 안됨. 선후관계가 없음.
            result.append(n)
            dir_result.append("171")
            continue
                
#################################################################

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
        #######################################################################################################################

        depth3 = key  # 채무자키, 다 확인한 뒤니까 그냥 이렇게 해도 돼
        
        try :
            depth2=dict_refer[depth3][0]  # 매각사구분
            for k, v in docu_folder_dict.items() :
                if re.search(k, docu) :
                    depth1 = v
                    break
        
        except Exception as e:
            result.append(n)
            dir_result.append("211")
            continue
        

        # 파일이동을 위한 준비(도착지 디렉토리 및 파일명 작성)
        dst_dir=os.path.join(PATH_SERVER, depth1, depth2, depth3)

        # 파일이동
        result.append(new_f)
        dir_result.append(dst_dir)

#### 반복문 끝########################################

df_f['result'] = result
df_f['dir_result'] = dir_result

print(f'{cnt_total} > {len(result)}개 서버')

df_f.to_csv(join(PATH, "result.csv"), mode='w', encoding='utf-8-sig')
        
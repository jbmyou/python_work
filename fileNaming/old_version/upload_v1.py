import os, sys
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


PATH = r'C:\Users\SL\Desktop\test'
PATH_HAND = r"C:\Users\SL\Desktop\test\hand"
PATH_LOG_SUCCESS = r'C:\Users\SL\Desktop\test\log\success'
PATH_LOG_FAIL = r'C:\Users\SL\Desktop\test\log\fail'
PATH_LOG_NOBASIC = r'C:\Users\SL\Desktop\test\log\nobasic'
PATH_LOG_OUT = r'C:\Users\SL\Desktop\test\log\out'
PATH_SERVER = r'C:\Users\SL\Desktop\test\server'
PATH_OUT = r'C:\Users\SL\Desktop\test\관리제외'
PATH_DF = r'./파일/채무자조회.pkl'


# PATH = r'/volume1/스캔파일/새 스캔파일(업로드)' ####################
# PATH_HAND = r"/volume1/스캔파일/새 스캔파일(업로드)/파일명에러"
# PATH_LOG_SUCCESS = r'/volume1/스캔파일/스캔파일log/success'
# PATH_LOG_FAIL = r'/volume1/스캔파일/스캔파일log/fail'
# PATH_LOG_NOBASIC = r'/volume1/스캔파일/스캔파일log/nobasic'
# PATH_LOG_OUT = r'/volume1/스캔파일/스캔파일log/out'
# PATH_SERVER = r'/volume1/솔림헬프'
# PATH_OUT = r'/volume1/삭제예정파일/관리제외'
# PATH_DF = r'/volume1/스캔파일/스캔파일log/_project/파일/채무자조회.pkl'

# 참조 df
def dict_refer():
    #df_c = pd.read_excel(PATH_DF)
    df_c = pd.read_pickle(PATH_DF)
    return dict(map(lambda x : (str(x[1].채무자키),[x[1].매각사구분, x[1].채무상태, x[1].성명]), df_c.iterrows()))
dict_refer = dict_refer()

# file_list
def file_list(path) :
    p_extension = re.compile('(jpeg|jpg|bmp|gif|pdf|png|tif|m4a)$', re.I)
    return [f.name for f in os.scandir(path) if f.is_file() and (p_extension.search(f.name))]
    
file_list = file_list(PATH)
# 카운트
cnt_total=len(file_list)

## 파일이 없으면 바로 종료 ##
if not cnt_total :  
    print("파일 없음")
    sys.exit(0)
############################


# 기본 변수
docu_folder_dict = {"원인서류": "1.원인서류", "양도통지서": "2.양도통지서", "집행권원": "3.집행권원", "강제집행": "4.강제집행", "등본": "5.등초본",
              "초본": "5.등초본", "등초본":"5.등초본", "외국인증명": "5.등초본", "개인회생": "6.개인회생", "신용회복": "7.신용회복", "파산": "8.파산", "재산조사": "9.재산조사", "부채증명서" : "10.부채증명서", "기타": "기타"}

# 필수 요소에 대한 컴파일
p_key = re.compile(r"([\d]{8})[\D]")  # match
docu_kind = r'원인서류|양도통지서[\s]?\d차|양도통지서|집행권원[\s]?[재]?[도]?|강제집행|등초본|(?<!등기부)등본|초본|외국인증명|개인회생|신용회복|파산|재산조사|부채증명서|기타'
p_docu = re.compile(docu_kind)
p_event = re.compile(r"(?<=\D)\d{4}[ㄱ-ㅎ가-힣]{1,3}\d+")

p_basic = re.compile(r'(\d{8})[_\s]?(\D+.+)[_\s]?(' + docu_kind + r')')#event의 스타트 번호 >= basic의 엔드번호
p_out = re.compile('개인회생\(면책\)|파산\(면책\)|환매|매각|종결')

# 파일명 수정을 위한 컴파일---------------------------------------------------------------------
# 연속공백은 무조건 처음에 하자(모든 컴파일에 공백은 ?로 처리했으므로)
# 날짜
# 년월일 사이에 기호나 공백이 들어가 있는 경우로 찾아서 6자리로 맞춰준다.
# 괄호와 공백을 포함시켜서 모두 제거 대상으로하고 대체 단어에 앞공백만 추가
# 기호를 기준으로 쓰기 때문에 반드시 기호제거보다 나와야
p_day4 = re.compile(r'\s?\(?\s?(?<=\D)(20|19)?(?P<y>\d{2})[./\-\s](?P<m>\d{1})[./\-\s](?P<d>\d{1})(?!\d)\s?\)?')
p_day5 = re.compile(r'\s?\(?\s?(?<=\D)(20|19)?(?P<y>\d{2})[./\-\s](?P<m>\d{1})[./\-\s](?P<d>\d{2})\s?\)?')
p_day6 = re.compile(r'\s?\(?\s?(?<=\D)(20|19)?(?P<y>\d{2})[./\-\s](?P<m>\d{2})[./\-\s](?P<d>\d{2})\s?\)?')
#중간기호는 없고 괄호로 감싸진 경우
p_day_4 = re.compile(r'\s?\(+\s?(20|19)?(?P<y>\d{2})(?P<m>\d{1})(?P<d>\d{1})(?!\d)\s?\)+')#괄호로 감싼거
p_day_5 = re.compile(r'\s?\(+\s?(20|19)?(?P<y>\d{2})(?P<m>\d{1})(?P<d>\d{2})\s?\)+')#괄호로 감싼거
p_day_6 = re.compile(r'\s?\(+\s?(20|19)?(?P<y>\d{2})(?P<m>\d{1,2})(?P<d>\d{1,2})\s?\)+')#괄호로 감싼거
#괄호,중간기호 없이 숫자만 6/8자리 있는 경우. 발급일을 찾는 것이므로 최대한 보수적으로. 잘못하면 사건번호를 가져옴
p_day_d = re.compile(r'(?<!\d)(20)?(?P<y>[2][012])(?P<m>[0-1][0-9])(?P<d>[0-3][0-9])(?!\d)')
#day변수에 담을 것
p_day = re.compile(r"#(\d{6})")


# 사건번호 - |를 사용하면 그룹이 안 먹어버리네..
p_event1=re.compile(r"(?<=\D)(19\d\d|20\d\d)\s?([ㄱ-ㅎ가-힣]{1,3})\s?([0-9]+)")
# event2의 경우 띄어쓰기 안한 거를 event에서 매치를 못하므로 앞뒤 공백 모두 ? 해줘야 겠네...
p_event2=re.compile(r"(?<=\D)(\d{2})\s?([ㄱ-ㅎ가-힣]{1,3})\s?([0-9]+)")
# 문서구분
docu_list=["", "원인서류", "양도통지서", "집행권원", "강제집행", "등본", #0~5
"초본", "외국인증명", "개인회생", "신용회복", "파산", "재산조사", "부채증명서", "기타"] 
p1=re.compile(r"원인\s?서류|(입회|가입|카드)\s?신청서|(신용)?\s?대출\s?(신청서)?|(분할)?\s?약정서|녹취록?|통화\s?(내용|내역)|(대출)?\s?원장|마이너스\s?대출")
p2=re.compile(r"(채권)?\s?(양도|양수)\s?통지서?|(채권)?\s?양도\s?및?\s?양수\s?(통지)?서?\s?|(?<![^가-힣][가-힣])양통|(?<=\d차)\s?(양통|양도통지서?)") # 세양통신 해결
p3=re.compile(r"집행\s?권원|승계\s?(집행문)?|판결문|지급\s?명령|이행\s?권고|화해\s?권고") # count=1이 의미있게 하기 위해 올바른 표현도 넣는다.
p4=re.compile(r"강제\s?집행|(채권)?\s?(추심|압류|가압류)?\s?결정(문)?|채권\s?가압류\s?(결정)?(문)?|결정문|(?<!\d)\s?타채|(?<!개시)\s?결정|채권압류\s?및\s?추심명령|배당[가-힣]+") # 결정이라는 말이 여러곳에서 나올 수 있다.ex개인회생 회생결정
#p4_1=re.compile(r"(배당[가-힣]+") 차분히 만들자

p5=re.compile(r'\(?\s?법인\s?등기부\s?(등본)?\s?\)?|\(?\s?(법인)?\s?등[기|본]\s?\)?|\(?\s?주민\s?등록\s?등본\s?\)?')
p5except = re.compile(r"\(?\s?등본.{0,3}원?초본\s?\)?|\(?\s?원?초본.{0,3}등본\s?\)?|\(?\s?등\s?초본\s?\)?|\(?\s?주민\s?등록\s?등본\s?및?\s?초본\s?\)?|\(?\s?주민\s?등록\s?초본\s?및?\s?등본\s?\)?") # '등본 및 초본' 때문에 .{0,3}
p6=re.compile(r"(?<![가-힣])원초본|(?<=원)원초본|(?<![가-힣])\(?\s?원\s?초본\s?\)?|(?<=[가-힣]{3})원초본|\(?\s?초본\s?\)?|\(?\s?주민\s?등록\s?초본\s?\)?") # 이렇게까지 해야되냐? ㅠㅠ
# p7 외국인증명. 컴파일 구문에서 외국인은 젤 끝에 와야 하는 거 유의
p7 = re.compile(r"\(?\s?외국인\s?증명서?\s?\)?|\(?\s?외국인\s?등록\s?사실\s?증명서?\s?\)?|\(?\s?외국인\s?등록증?\s?\)?|\(?\s?외국인\s?\)?")

# 연도 다음에 나오는 개회가 아닌 경우, 전방탐색을 통해 '개인회생'의 '회생'이 걸리는 거 방지. count도 하자
p8=re.compile(r"(?<!\d)개회|개인\s?회생|(?<!개인)회생")
p9=re.compile(r"(?<=[가-힣]{3})신복|[\s_]신복|신용\s?회복") #이름에있는 신복,숫자뒤 신복은 제외. 이름다음에 띄어쓰기 없이 나온 신복은.. 
p10=re.compile("파산|면책")
p11=re.compile(r"재산\s?조사|재산\s?조회") # 상세문서를 재산조사가 대체하는 게 아님에 유의
p12=re.compile(r"부채\s?증명\s?[서원]?류?")

# 삭제/대체가 아니라 '기타 '를 삽입하는 것이니 search되게끔만 작성하면 됨
p_etc = re.compile("행자부|재산\s?명시|접수증|집행문\s?부여의\s?소|신용\s?조회|배송[가-힣]+|종[적족]\s?조회|화해\s?\(?(분할)?\)?\s?계약|채무\s?조정|(채무감면|분할\s?상환)")

# 제거할거
#p_rm = re.compile(r"\(?\s?[a-zA-Z\d]{2,}-[a-zA-Z\d]{2,}-[a-zA-Z\d]{2,}\s?\)?|\(?\s?[a-zA-Z0-9]{4,}-[a-zA-Z0-9]{4,}\s?\)?|\(?\s?\d{10,}\s?\)?|(?<![가-힣\d])[3-9]\d{5,}(?!가-힣\d])|(?<![가-힣\d])\d{4}(?![가-힣\d])")
p_rm = re.compile(r"ADMIN.*Conflict|TAA\(회\)|SCSB|(?<![a-zA-Z])[a-zA-Z][\d]?[_\-][a-zA-Z\-\d_]{4,}|\(?\s?[a-zA-Z][\d]{9,}\s?\)?|\(?\s?[a-zA-Z\d]{2,}-[a-zA-Z\d]{2,}-[a-zA-Z\d]{2,}\s?\)?|\(?\s?[a-zA-Z0-9]{4,}-[a-zA-Z0-9]{4,}\s?\)?|\(?\s?\d{10,}\s?\)?|(?<![가-힣\d])\d{6}[\-_](\d{1}|\d{6})(?![가-힣\d])|(?<![가-힣\d])[13-9]\d{5,}(?!가-힣\d])|(?<![가-힣\d])\d{3,4}(?![가-힣\d])|(-)?\d{1,3}(,\d{3})+(\.\d+)?")
#                                                        언더바로 연결된 영어1,숫자(0~1)-_                                            2개의 하이픈으로 연결된 영어 및 숫자                      하이픈으로 연결된 4자리 이상의 영숫자(주민번호 포함)  10자리키      앞과 교집합있지만, 확실한 주민번호(단독숫자앞에있어야)     날짜 아닐 6자리 이상 단독 숫자           3~4자리의 단독 숫자                   금액
p_sign = re.compile("[^㈜a-zA-Zㄱ-ㅎ가-힣0-9\s_()]|\(\s?\)|_ | _|  ")
# ----------------------------------------------------------------------------------------------


# 로그 리스트
nobasic = [] # 파일명에러 폴더로 이동 나머지는 그냥 파일명 수정
out = []
success = []
fail = []

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


for f in tqdm(file_list, total=cnt_total):
    try:
        n = os.path.splitext(f)[0]
        ext = os.path.splitext(f)[1]
        
        depth1, depth2, depth3,  = "", "", ""
        key, name, docu, event, extra, day = "", "", "", "", "", ""

        # key 없는 거 파일명에러 폴더로
        if not p_key.match(n):  
            nobasic.append([f, "noKey"])
            shutil.move(join(PATH, f), join(PATH_HAND, f))
            continue

# key 파일명 다듬기__________________________________________________________
        else:
            key = p_key.match(n).group(1)##############################  key
            
            # 연속공백 처리
            n = re.sub("[\s]{2,}", " ", n)

            # 날짜, 주민번호 내부 공백 제거
            if p_day4.search(n):
                n = p_day4.sub(r"#\g<y>0\g<m>0\g<d>", n)
            elif p_day5.search(n) :
                n = p_day5.sub(r"#\g<y>0\g<m>\g<d>", n)
            elif p_day6.search(n) :
                n = p_day6.sub(r"#\g<y>\g<m>\g<d>", n)
            elif p_day_4.search(n) :
                n = p_day_4.sub(r"#\g<y>0\g<m>0\g<d>", n)
            elif p_day_5.search(n) :
                n = p_day_5.sub(r"#\g<y>0\g<m>\g<d>", n)
            elif p_day_6.search(n) :
                n = p_day_6.sub(r"#\g<y>\g<m>\g<d>", n)
            elif p_day_d.search(n[8:]) :
                n = n[:8] + p_day_d.sub(r"#\g<y>\g<m>\g<d>", n[8:])
            else : pass

            if p_day.search(n) :
                day = p_day.search(n).group(1) #제거
                n = p_day.sub(" ", n)

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

            # 타기관키값 : 날짜, 사건번호 뒤 기호 앞
            n = n[:8] + p_rm.sub(" ", n[8:]) 
            # 기호정리, 연속공백 : set임 순서 주의
            n = p_sign.sub(" ", n)
            n = re.sub("[\s]{2,}", " ", n)


            # 문서구분 수정하기 -------------------------------
            # 기타가 들어가면 if-elif에 따라 elif를 건너뛰게 되고, 기타추가하기만 실행된다.
            # 기타가 들어있되(!none), 부채증명서가 들어있지 않은(none) 경우만 기타 추가하기로~
            if (re.search("기타", n)!=None) and (p12.search(n)==None) : pass
            # 원인서류
            elif p1.search(n):
                n=p1.sub(docu_list[1], n, count=1)
            # 양도통지서
            elif p2.search(n):
                n=p2.sub(docu_list[2], n, count=1)  # 이름에 양통들어가는 경우가 있을 수 있어서 공백씀..
            # 등초본
            elif p5except.search(n) :
                n=p5except.sub("등초본", n, count=1)
            # 등본, 법인의 등기부등본
            elif p5.search(n) :
                n=p5.sub(docu_list[5], n, count=1)
            # 초본
            elif p6.search(n):
                n=p6.sub(docu_list[6], n, count=1)
            # 외국인 증명
            elif p7.search(n):
                n=p7.sub(docu_list[7], n, count=1)
            
            #파산 강제집행의 배당과 구별하기 위해 왔음. 파산은 문서구분에 웬만하면 들어가니 먼저 걸러주기
            elif p10.search(n[:e_s]) : pass
            # 집행권원
            elif p3.search(n[:e_s]):
                n=p3.sub(docu_list[3], n, count=1)
            # 강제집행
            elif p4.search(n[:e_s]):
                n=p4.sub(docu_list[4], n, count=1)
            #개인회생
            elif p8.search(n[:e_s]):
                n=p8.sub(docu_list[8], n, count=1) # 개회 반복될 수 있음
            #신용회복
            elif p9.search(n[:e_s]):
                n=p9.sub(docu_list[9], n, count=1)
            #재산조사
            elif p11.search(n):
                n=p11.sub(docu_list[11], n, count=1)
            #부채증명서
            elif p12.search(n):
                n=p12.sub(docu_list[12], n, count=1)
                n=re.sub("기타", "", n)

            #기타 추가하기
            if (p_docu.search(n)==None) and p_etc.search(n) :
                etc = p_etc.search(n)
                rep = "기타 " + etc.group()
                n = p_etc.sub(rep, n)

            
# 새 파일명(new_f) 만들기__________________________________________________________
            if not p_docu.search(n) : #docu없으면
                nobasic.append([f, "nodocu"])
                shutil.move(join(PATH, f), join(PATH_HAND, f))
                continue

            else : #docu 있으면
                d_obj = p_docu.search(n)##############################  docu, name
                name = n[8:d_obj.start()].replace("_", " ").strip() #replace가 먼저나와야
                docu = d_obj.group().strip() #양통, 집행권원 때문에 strip 필요

                if len(name) < 2 : 
                    try :
                        name = dict_refer[key][2]
                        name = p_sign.sub(" ", name)
                        name = re.sub("[\s]{2,}", " ", name)
                        
                        print(f"275 name = {name}") #임시프린트######################
                        # nobasic.append([f, "noname"])
                        # shutil.move(join(PATH, f), join(PATH_HAND, f))
                        # continue
                    except Exception as e:
                        fail.append([f, e.__class__, e])
                        #nobasic.append([f, new_f])
                        shutil.move(join(PATH,f), join(PATH_HAND, (n+ext)))
                        continue
                else : pass
                
                
                if not p_event.search(n) : #사건번호 없으면
                    ##############################  extra
                    extra=n[d_obj.end():].strip().replace(" ", "_") #strip이 먼저 나와야
                else :  # 사건번호 있으면
                    e_obj = p_event.search(n)
                    event = e_obj.group()##############################  event
                    extra = n[d_obj.end():e_obj.start()].strip().replace(" ", "_") #strip이 먼저 나와야
                    extra = extra + "_" +n[e_obj.end():].strip().replace(" ", "_")##############################  extra
                    if d_obj.end() > e_obj.start() : # 사건번호앞에 문서구분 없는 경우
                        nobasic.append([f, "nodocu"])
                        shutil.move(join(PATH, f), join(PATH_HAND, f))
                        continue
                    else : pass #문서구분 - 사건번호 

            # new_f
            # continue 모두 해줬기 때문에 try 안에만 있으면 된다(현재는 key있는 경우)
            name_items = [key, name, docu]
            if event : name_items.append(event)
            if extra : name_items.append(extra) #일련번호나는 re_name에서 제거해줌
            if day : name_items.append(day)
            print(f'307 항목리스트 : {name_items}')  #임시프린트######################
            new_f = "_".join(name_items)
            # 마지막에 _ 두개 인 경우 꼭 해줘야 해.
            new_f = re.sub("_{2,}", "_", new_f)
            new_f = re.sub("_+$", "", new_f)+ext
            print(f'311 new_f 합치고 2개언더바제거 : {new_f}')  #임시프린트######################

# 서버 올리기__________________________________________________________

            # 폴더 이름
            depth1 = ""
            depth3=key
            print(f"317 최초 depth3 : {depth3}") #임시프린트######################
            try :
                depth2=dict_refer[depth3][0]  # 매각사구분 여기에는 /없으니 생긴대로 그냥 둔다
            except Exception as e:
                fail.append([f, e.__class__, e])
                #nobasic.append([f, new_f])
                shutil.move(join(PATH,f), join(PATH_HAND, new_f))
                continue

            # 관리제외 파일이라면
            if p_out.match(dict_refer[depth3][1]):
                out_dir=join(PATH_OUT, depth2, depth3)
                if not os.path.exists(out_dir):
                    os.makedirs(out_dir)
                shutil.move(join(PATH, f), join(out_dir, new_f))
                out.append([f, new_f, out_dir])
                continue

            # 관리중인 파일이면
            else:

                # depth1 # docu에서 찾아야지.. new_f에서 찾으면 젤 뒤에걸로 되어버리잖아...
                for key, value in docu_folder_dict.items():
                    if re.search(key, docu):
                        depth1=value
                        print(f"342 depth1 : {depth1}") #임시프린트######################
                        break

                if depth1 == "" : #docu검사는 이미 했음.
                    nobasic.append([new_f, "keyError"])
                    shutil.move(join(PATH,f), join(PATH_HAND, new_f))
                    continue


                # 파일이동을 위한 준비(도착지 디렉토리 및 파일명 작성)
                print(f"352 합치기직전 depth1 : {depth1}, depth2:{depth2}, depth3:{depth3}") #임시프린트######################
                dst_dir=join(PATH_SERVER, depth1, depth2, depth3)
                print(f"354 dst_dir : {dst_dir}") #임시프린트######################

                # 경로 만드는 건 re_name에 추가함

                # 파일이동
                success.append(ff.re_name(join(PATH, f), join(dst_dir, new_f)))

    except Exception as e:
        print("===================================")
        time = datetime.today().strftime("%H:%M:%S")
        print(time)
        print(f, n, e.__class__, e, sep=" : ")
        print(traceback.format_exc())
        fail.append([f, e.__class__, e])
        continue  # 반복문 계속 돌아
#### 반복문 끝########################################

# log 작성 및 결과 출력__________________________________________________________
try :
    ff.write_log_csv(success, PATH_LOG_SUCCESS)
    ff.write_log_csv(fail, PATH_LOG_FAIL)
    ff.write_log_csv(nobasic, PATH_LOG_NOBASIC)
    ff.write_log_csv(out, PATH_LOG_OUT)
except Exception as e:
    print(e)

if cnt_total == len(success) + len(fail) + len(nobasic) + len(out) :
    print("처리된 파일에 누수 없음")
else :
    print("처리된 파일에 누수 있음")

print(f'{cnt_total}개의 파일 중 {len(success)}개 서버, {len(out)}개 관리제외, {len(nobasic)}개 파일명에러, {len(fail)}개 예외')
        

# if docu_except :
#     print("docu", *docu_except, sep='\n')
# if event_except :
#     print("event", *event_except, sep='\n')
# if day_except :
#     print("day", *day_except, sep='\n')
# if mark_except :
#     print("mark", *mark_except, sep='\n')
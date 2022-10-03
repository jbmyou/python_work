"""
purpose를 설정하면 test 모드로 진입
0.변수(path, dict_referFnc( ), file_list( ), total, loglist
for문
    1. 변수 : items, nameextra, depth
    2. keyFnc( ) - rm_s( )
    3-1. setDocuByEvent( ) - > docu, event, name#extra
    else 
    3-2. setDocu( ) > docu, name#extra
    4. name#extra( ) : dateFnc( ), rmNeedlessSharp( ), rm_s( ) . if len(name)<2
------------------
    5. set_depth( )
    6. re_name( )
7. write_log_csv( )
8. print
"""
import os
import sys
import shutil
import pandas as pd
from tqdm import tqdm      
import re
import traceback
from os.path import join
from datetime import datetime

def getPath(purpose:str) :
    """
    'nas' : 새 스캔파일(업로드), nas내부 주소 \n
    'done' : 검수완료, 네트워크 경로 \n
    'fileTest' : pc바탕화면/test, 로그는 파일서버개편 폴더 \n
    'logTest' : 스캔파일log/success, 로그는 파일서버개편 \n
    """
    path = r'\\192.168.0.75\스캔파일\새 스캔파일(업로드)' 
    path_server = r'\\192.168.0.75\솔림헬프'
    path_nobasic = join(path, "파일명에러")
    path_out = r'\\192.168.0.75\삭제예정파일\관리제외'
    path_log_success = r'\\192.168.0.75\스캔파일\스캔파일log\success'
    path_log_nobasic = r'\\192.168.0.75\스캔파일\스캔파일log\nobasic'
    path_log_out = r'\\192.168.0.75\스캔파일\스캔파일log\out'
    path_log_fail = r'\\192.168.0.75\스캔파일\스캔파일log\fail'
    path_df = r'\\192.168.0.75\스캔파일\스캔파일log\_project\파일\채무자조회.pkl'
    if purpose == "done" :
        path = r'\\192.168.0.75\스캔파일\새 스캔파일(업로드)\검수완료' 
        path_nobasic = join(path, "파일명에러")
    elif purpose == "fileTest" :
        path = r'C:\Users\SL\Desktop\test' 
        path_server = join(path, 'server')
        path_nobasic = join(path, "hand")
        path_out = join(path, '관리제외')
        path_log_success = r'D:\0.전산\1.진행중과업\파일서버개편\log\success'
        path_log_nobasic = r'D:\0.전산\1.진행중과업\파일서버개편\log\nobasic'
        path_log_out = r'D:\0.전산\1.진행중과업\파일서버개편\log\out'
        path_log_fail = r'D:\0.전산\1.진행중과업\파일서버개편\log\fail'
        ### dict_referFnc 안 읽어지면 절대경로 담은 변수 직접 넣으라고
    elif purpose == "logTest" : # 파일 이동이 일어나지 않는다.
        path = r'\\192.168.0.75\스캔파일\스캔파일log\success'
        path_server = r'/volume1/솔림헬프'
        path_log_success = r'D:\0.전산\1.진행중과업\파일서버개편\log\success'
    elif purpose == "nas" : pass
    
    # path = r'/volume1/스캔파일/새 스캔파일(업로드)' #############
    # path_server = r'/volume1/솔림헬프/'
    # path_nobasic = join(path, "파일명에러")
    # path_out = r'/volume1/삭제예정파일/관리제외'
    # path_log_success = r'/volume1/스캔파일/스캔파일log/success'
    # path_log_nobasic = r'/volume1/스캔파일/스캔파일log/nobasic'
    # path_log_out = r'/volume1/스캔파일/스캔파일log/out'
    # path_log_fail = r'/volume1/스캔파일/스캔파일log/fail'
    # path_df = r'/volume1/스캔파일/스캔파일log/_project/파일/채무자조회.pkl'
    # if purpose == "done" :
    #     path = r'/volume1/스캔파일/새 스캔파일(업로드)/검수완료' 
    #     path_nobasic = join(path, "파일명에러")
    # elif purpose == "fileTest" :
    #     path = r'C:\Users\SL\Desktop\test' 
    #     path_server = join(path, 'server')
    #     path_nobasic = join(path, "hand")
    #     path_out = join(path, '관리제외')
    #     path_log_success = r'D:\0.전산\1.진행중과업\파일서버개편\log\success'
    #     path_log_nobasic = r'D:\0.전산\1.진행중과업\파일서버개편\log\nobasic'
    #     path_log_out = r'D:\0.전산\1.진행중과업\파일서버개편\log\out'
    #     path_log_fail = r'D:\0.전산\1.진행중과업\파일서버개편\log\fail'
    #     ### dict_referFnc 안 읽어지면 절대경로 담은 변수 직접 넣으라고
    # elif purpose == "logTest" : # 파일 이동이 일어나지 않는다.
    #     path = r'/volume1/스캔파일/스캔파일log/success'
    #     path_server = r'/volume1/솔림헬프'
    #     path_log_success = r'D:\0.전산\1.진행중과업\파일서버개편\log\success'
    # elif purpose == "nas" : pass

    return path, path_server, path_nobasic, path_out, path_log_out, path_log_success, path_log_nobasic, path_log_out, path_log_fail, path_df

path_df = r'./파일/채무자조회.pkl' 
def dict_referFnc(path_df):
    """dict_refer["key"][0:매각사, 1:채무상태, 2:채무자성명]"""
    df_c = pd.read_pickle(path_df)
    return dict(map(lambda x : (str(x[1].채무자키),[x[1].매각사구분, x[1].채무상태, x[1].성명]), df_c.iterrows()))
dict_refer = dict_referFnc(path_df) ################## 전역변수로 둬야 함. 함수에서도 쓰고, main에서도 쓴다.

# 사해행위는 판결, 품의서, 예고서 등 다방면에 걸쳐있어 키워드로 부적합
comp = { # search는 앞에서부터 찾으니까 엄격한 것이 앞으로. 단 가압류와 압류처럼 가?압류로 표현할 수 있는 것은 순서 상관이 없음
#"1붙은 건 .*를 앞에 붙여서 아예 맨 앞으로 문서구분을 이동시켜도 괜찮겠다. 어차피 이름 중복되면 제거하니까."
        "개인정보1" : re.compile(r"신분증|(기초)?수급자?|차상위|(법인|사용)?\s?인감|기본\s?증명서?|(가족|혼인)\s?(관계|증명)|이혼|입양|친양자|졸업|병적"),
        "원인서류" : re.compile(r"원인\s?서류|(입회|가입|카드)\s?신청서|(신용)?\s?대출\s?(신청서)?|약정서|녹취록?|통화\s?(내용|내역)|(대출)?\s?원장|마이너스\s?대출,종[적족]\s?조회"),
        "양도통지서" : re.compile(r"(채권)?\s?(양도|양수)\s?통지서?|(채권)?\s?양도\s?및?\s?양수\s?(통지)?서?\s?|(?<![^가-힣][가-힣])양통|(?<=\d차)\s?(양통|양도통지서?)"), # 세양통신 해결
        "양도통지서1" : re.compile(r"종[적족]\s?(조회)?|이력"), #종적조회는 모두 양통만 있더라
        # 사건번호 내부는 이제 신경쓰지 않아도 된다.
        "파산" : re.compile(r"파산|면책|파산.*면책"), #파산에도 배당있음. 강제집행보다 먼저 나와야
        # 연도 다음에 나오는 개회가 아닌 경우, 전방탐색을 통해 '개인회생'의 '회생'이 걸리는 거 방지. count도 하자
        "개인회생" : re.compile(r"개회|(개인)?\s?회생"),
        "신용회복" : re.compile(r"(?<=[가-힣]{3})신복|[\s_]신복|신용\s?회복"), #이름에있는 신복,숫자뒤 신복은 제외. 이름다음에 띄어쓰기 없이 나온 신복은.. 
        "신용회복1" : re.compile(r"(개인)?채무\s?조정안?|.*원상\s?회복|신청인\s?현황"),
        "재산조사" : re.compile(r"재산\s?조사|재산\s?조회"), # 상세문서를 재산조사가 대체하는 게 아님에 유의
        "재산조사1" : re.compile(r"(?<!법인)\s등기|(?<!법인)등기|가압류\s?물건지|.*대장|.*등록\s?원부|.*은행거래|.*입출금"),
        
        "부채증명서" : re.compile(r"부채\s?(잔액)?증명\s?[서원]?(류|발급)?"),
        "신용조회" : re.compile(r"신용\s?조[회사]|신용\s?정보(?!( 활용| 이용|활용|이용|동의))"), # 신용조회가 있는 경우 기관이 앞에 나오건 뒤에 나오건 냅두면 되니 상관없음.
        "신용조회1" : re.compile(r"KCB|NICE|나이스", re.I), # 기관명만 있는 경우 docu만 추가하면 됨. 순서대로 검색하니 여기 왔다는 건 신용조회라는 말이 없다는 뜻

        # 등초본
        "외국인증명" : re.compile(r"\(?\s?외국인\s?증명서?\s?\)?|\(?\s?외국인\s?등록\s?사실\s?증명서?\s?\)?|\(?\s?외국인\s?등록증?\s?\)?|\(?\s?외국인\s?\)?"),
        "등초본" : re.compile(r"\(?\s?등본.{0,3}원?초본\s?\)?|\(?\s?원?초본.{0,3}등본\s?\)?|\(?\s?등\s?초본\s?\)?|\(?\s?주민\s?등록\s?등본\s?및?\s?초본\s?\)?|\(?\s?주민\s?등록\s?초본\s?및?\s?등본\s?\)?|초[.,]등|등[.,]초"), # '등본 및 초본' 때문에 .{0,3}
        # 등기부/s?등본은 조건문으로 처리하자.
        "등본" : re.compile(r"\(?\s?법인\s?등기부?\s?(등본)?\s?\)?|\(?\s?(?<!(등기부|기부 ))등본\s?\)?|\(?\s?주민\s?등록\s?등본\s?\)?"), #제적_등본 허용. 배당표등본, 결정등본은 법원서류에서 걸러지니 pass
        "초본" : re.compile(r"(?<![가-힣])원초본|(?<=원)원초본|(?<![가-힣])\(?\s?원\s?초본\s?\)?|(?<=[가-힣]{3})원초본|\(?\s?초본\s?\)?|\(?\s?주민\s?등록\s?초본\s?\)?"), # 말소자_초본
        "주민등록정보" : re.compile(r"행자부\s?(전송)?\s?(자료)?|주민\s?등록\s?정보|주소\s이력"),
        # 키워드 추가(대체x)
        "기타1" : re.compile(r"기타|집행문\s?부여의\s?소|배송[가-힣]+|(채권|양도|양수|매매).*계약|화해(?!권고)|대위변제|분할|분납|상환|감면|(상속)?\s?한정\s?승인|상속\s?포기|지방세|세목별|과세|.*내용증명|출입국사실|.*답변서|.*진술서|.*보정(서|명령)|.*인포케어|보증면탈|자동차직권말소|완제|.*품의서"), # 제거가 아니므로 첫 글자만 잘 찾으면 된다.
        #기타 제거 : 부채증명서, 신용조회, 주민등록정보
        # 판결문과 결정문이 여러곳에서 나올 수 있어 뒤로 뺌. 결정과 판결은 더욱 애매해서 제외함
        "집행권원" : re.compile(r"집행\s?권원|승계\s?(집행|결정)?(문)?|판결문|지급\s?(명령\s?(결정문)?|결정문?)|이행\s?권고|화해\s?권고|공정증서"), # count=1이 의미있게 하기 위해 올바른 표현도 넣는다.
        "집행권원 재도" : re.compile(r"(?<!\b[가-힣])재도(부여|건)?|\b재도건?|(?<!문)\s부여건?|수통\s?(부여)?|재교부건?|재발급건?"),
        "강제집행" : re.compile(r"강제\s?집행|\s?타채|압추|(채권)?\s?압류\s?및?\s?추심\s?(명령|결정)?문?|(채권)?\s?추심\s?및?\s?압류\s?(명령|결정)?문?|채권\s?(추심|압류)\s?(결정)?(문)?|(?<!개시)\s결정문?"), # 결정이라는 말이 여러곳에서 나올 수 있다.ex개인회생 회생결정
        "강제집행1" : re.compile(r"재산\s?명시|[가-힣\b]*(부?동산)?\s?(가?압류|경매)(?!물건)|[가-힣\b]*동산|[가-힣\b]*추심|[가-힣\b]*유체|[가-힣\b]*배당[가-힣]|.*진술\s?최고서?")#일반진술서는 안올려도 되는 거. 올린다면 기타로
        #p4_1=re.compile(r"(배당[가-힣]+") 차분히 만들자                            
    }

def file_listFnc(path) :
    p_extension = re.compile('(jpeg|jpg|bmp|gif|pdf|png|tif|tiff|m4a|wav)$', re.I)
    return [f.name for f in os.scandir(path) if f.is_file() and (p_extension.search(f.name))]

def file_list_test(path) :
    """return df \n
    .csv로그파일로 테스트를 하기 위함. 폴더내 모든 csv를 읽음 \n
    col = src_dir, src_file(file_list), dst_file, dst_dir \n
    """
    logs = [f.name for f in os.scandir(path) if all([f.is_file(), re.search("^[^~$].*csv$", f.name)])]
    col = ['src_dir', "src_file", "dst_file", "dst_dir"]
    df_c = pd.DataFrame(None, columns=col)
    for result in logs :
        df_temp = pd.read_csv(join(path,result), sep = ",", names=col)
        df_c = pd.concat([df_c, df_temp], axis = 0, ignore_index=True)

    return df_c

# keyFnc에서 한 번, new_f만들기 전 한 번, 함수 외부에서 공백을 _로 바꾸기
def rm_s(stem:str) :
    "반복문 처음(stem)과 끝(name,extra)에서 실행하라. 언더봐와 스페이스 > strip된 스페이스"
    #시작,중간,끝 모든 공백과 _를 > 하나의 공백으로 > strip하면 끝
    p_s = re.compile("^[_\s)]+|[_\s(]+$|[_\s]{1,}") #(로 끝나거나 )로 시작하는 경우는 제거. 짝 안 맞는 건 주)가 있으니 그냥 두자..
    stem = p_s.sub(" ", stem).strip()
    return stem

def rmNeedlessSharp(nameextra:str) :
    """무조건 # 을 넣어줘야 작동함 \n
    일련번호와 기호 제거>name, extra"""
       
    #완전제거                                                      영어와 숫자가 연속
    p_rmSerialN = re.compile(r"(?<![a-zA-Z])[a-zA-Z](?![a-zA-Z])|\d+[a-zA-Z]+|[a-zA-Z]+\d+|TAA\(회\)|\
        |SCSB|ADMIN.*Conflict|\d(?!건|통|차|채|억|천|백|급|번|길)") #3개의 숫자를 지운다. 해당글자가 나온다면 그 앞 숫자는 살린다. 
    p_sign = re.compile(r"[^#㈜()\sa-zA-Zㄱ-ㅎ가-힣\d]|\([^\w#]*\)") #반쪽 괄호만 있는 거는 어케 지우지?
    
    nameextra = p_rmSerialN.sub("", nameextra)
    nameextra = p_sign.sub(" ", nameextra)

    #분리 후 rm_s
    name = rm_s(nameextra.split("#")[0])
    extra = rm_s(nameextra.split("#")[1])
    return name, extra

# rm_s 내부실행
def keyFnc(stem:str) -> str :
    "return2(key, stem-key:앞뒤_,공백 및 이중공백 제거)"
    key = ""
    new_stem = stem

    p_key = re.compile(r"[\d]{8}(?!\d)")  # match

    if not p_key.match(new_stem) :
        return key, new_stem
    else :
        key = p_key.match(new_stem).group()
        new_stem = p_key.sub("", new_stem, count=1)
        new_stem = rm_s(new_stem) 
    
    return key, new_stem

def dateFnc(subStem:str)->str :
    "인자:key제거후, sign 제거 전  /  return2(date, stem-date)"
    date = ""
    # 컴파일
    #중간기호 : [. / - 공백]  그룹이름 필요  #####연도는 바뀔때마다 최댓값 수정 필요#### 
    p_day4 = re.compile(r'\(?\s?(?<!\d)(20)?(?P<y>[01]\d|2[0-2])[\.\/\-\s](?P<m>[1-9])[./\-\s](?P<d>[1-9])(?!\d)\s?\)?')
    p_day5 = re.compile(r'\(?\s?(?<!\d)(20)?(?P<y>[01]\d|2[0-2])[\.\/\-\s](?P<m>[1-9])[./\-\s](?P<d>[0-2][1-9]|[1-3]0|31)(?!\d)\s?\)?')
    p_day6 = re.compile(r'\(?\s?(?<!\d)(20)?(?P<y>[01]\d|2[0-2])[\.\/\-\s](?P<m>0[1-9]|1[0-2])[./\-\s](?P<d>[0-2][1-9]|[1-3]0|31)(?!\d)\s?\)?')
    #중간기호는 없고 괄호로 감싸진 경우
    p_day_4 = re.compile(r'\(\s?(20)?(?P<y>[01]\d|2[0-2])(?P<m>[1-9])(?P<d>[1-9])(?!\d)\s?\)')
    p_day_5 = re.compile(r'\(\s?(20)?(?P<y>[01]\d|2[0-2])(?P<m>[1-9])(?P<d>[0-2][1-9]|[1-3]0|31)(?!\d)\s?\)')
    p_day_6 = re.compile(r'\(\s?(20)?(?P<y>[01]\d|2[0-2])(?P<m>0[1-9]|1[0-2])(?P<d>[0-2][1-9]|[1-3]0|31)(?!\d)\s?\)')
    #괄호,중간기호 없이 (그러니 보수적으로) 숫자만 6/8자리 있는 경우.생년월일/일련번호와 겹칠 수 있으니 2020년 이후만. 4자리는 날짜인지 불확실하니 제외.
    p_day_d = re.compile(r'(?<!\d)(20)?(?P<y>2[0-2])(?P<m>0[1-9]|1[0-2])(?P<d>[0-2][1-9]|[1-3]0|31)(?!\d)')
    p_y = re.compile(r'(\d\d|\d\d\d\d)년')
    p_m = re.compile(r'(\d{1,2})월')
    p_d = re.compile(r'(\d{1,2})일')
    # 날짜 포맷 통일
    if p_day4.search(subStem):
        d = p_day4.search(subStem)
        date = d["y"] + "0" + d["m"] + "0" + d["d"]
        subStem = p_day4.sub("", subStem)
    elif p_day5.search(subStem) :
        d = p_day5.search(subStem)
        date = d["y"] + d["m"] + "0" + d["d"]
        subStem = p_day5.sub("", subStem)
    elif p_day6.search(subStem) :
        d = p_day6.search(subStem)
        date = d["y"] + d["m"] + d["d"]
        subStem = p_day6.sub("", subStem)
    elif p_day_4.search(subStem) :
        d = p_day_4.search(subStem)
        date = d["y"] + "0" + d["m"] + "0" + d["d"]
        subStem = p_day_4.sub("", subStem)
    elif p_day_5.search(subStem) :
        d = p_day_5.search(subStem)
        date = d["y"] + d["m"] + "0" + d["d"]
        subStem = p_day_5.sub("", subStem)
    elif p_day_6.search(subStem) :
        d = p_day_6.search(subStem)
        date = d["y"] + d["m"] + d["d"]
        subStem = p_day_6.sub("", subStem)
    elif p_day_d.search(subStem) :
        d = p_day_d.search(subStem)
        date = d["y"] + d["m"] + d["d"]
        subStem = p_day_d.sub("", subStem)
    elif p_y.search(subStem) or p_m.search(subStem) or p_d.search(subStem) :
        py = p_y.search(subStem)
        pm = p_m.search(subStem)
        pd = p_d.search(subStem)
        if py : 
            date = py[1]
            subStem = p_y.sub("", subStem)
        if pm :
            if len(pm[1]) == 1 : date = date + "0" + pm[1]
            else : date = date + pm[1]
            subStem = p_m.sub("", subStem)
        if pd :
            if len(pd[1]) == 1 : date = date + "0" + pd[1]
            else : date = date + pd[1]
            subStem = p_d.sub("", subStem)

    else : pass

    return date, subStem

# date, rmNeedlessSharp 내부 실행
def eventFnc(noKeyStem:str)->list:
    """
    키와 확장자 제거한 파일명을 받아 return [0|1|2, old|new stem] \n
    0 : 사건번호 없음 또는 기타에 해당, 파일명 그대로 반환 -> setDocu( ) 호출해 \n
    1 : 사건번호 있고, [docu, event, name+"#"+extra]을 두번째 요소로 반환 \n
    2 : 경정사건번호인데 정확한 문서구분없음, 파일명 그대로 반환 -> nobasic처리(hand)해 \n
        드물게 사건번호 예외일 수 있음. 역시 똑같이 직접 확인 필요
    """
    new_stem = noKeyStem
    event, eSign, docu, nSharpe = "", "", "", ""
    
    # 컴파일 # 연도의 경우 2030이하면 괜찮음
    p_event1=re.compile(r"((?<=\D)|^)(19\d\d|20[012]\d)\s?(준?재?[가나느차카타즈본징하개회라][가-힣]?)\s?([0-9]+)")
    p_event2=re.compile(r"((?<=\D)|^)([012]\d)\s?(준?재?[가나느차카타즈본징하개회라][가-힣]?)\s?([0-9]+)")
    p_court = re.compile("[가-힣]*(법원|지원|지법|서울|대전|대구|부산|광주|수원|\
        |의정부|파주|포천|동두천|가평|연천|철원|인천|김포|강화|용인|오산|광명|\
        |안성|양평|이천|춘천|홍천|양구|삼척|동해|정선|평창|태백|횡성|인제|화천|\
        |고성|양양|금산|세종|보령|서천|예산|아산|태안|당진|부여|청양|진천|보은|\
        |단양|음성|옥천|괴산|경산|칠곡|청도|영천|성주|고령|영주|봉화|구미|문경|\
        |예천|청송|군위|울진|영양|양산|울산|창원|마산|김해|함안|의령|사천|남해|\
        |하동|거제|창녕|합천|함양|산청|담양|함평|강진|구례|영광|나주|장성|화순|\
        |곡성|광양|고흥|여수|보성|무안|영암|완도|진도|전주|군산|정읍|남원|진안|\
        |김제|무주|임실|익산|부안|고창|장수|순창|제주|서귀포)[가-힣]*")
    # 사해행위는 판결, 품의서, 예고서 등 다방면에 걸쳐있어 키워드로 부적합
    dict = { #사건구분 검색어 / 삭제할 문서구분 키워드
        "집행권원" : [re.compile("가[합단소]|나|다|머|차"), comp["집행권원"]],
        "강제집행" : [re.compile("카(?!경|기|확)|타|즈|본|징|가"), comp["강제집행"]], #카경: 경정, 카기: 기타민사신청, 본(접수증)도 강제집행맞다.
        "개인회생" : [re.compile("개|회"), comp["개인회생"]], # 라 : 민사항고사건
        "파산" : [re.compile("하"), comp["파산"]], 
        "경정" : [re.compile("카경|카기전"), re.compile('집행권원|강제집행|개인회생|파산')],
        "항고" : [re.compile("라"), re.compile('집행권원|강제집행|개인회생|파산')],
        "기타" : [re.compile("느|카(기(?!전)|담|확)"), re.compile("기타")] #느(단) : 한정상속, 상속포기, 기:의사표시공시송달, 담:담보취소, 확:소송비용확정
        }

    # 관할법원 제거
    new_stem = p_court.sub("", new_stem)
    
    # event, name, extra, eSing 할당
    # 사건번호 없는 경우
    if (p_event1.search(new_stem)==None) and (p_event2.search(new_stem)==None) :
        return [0, noKeyStem]
    elif comp["기타1"].search(new_stem) :
        return [0, noKeyStem]
    #사건번호 있는 경우
    elif p_event1.search(new_stem):
        m = p_event1.search(new_stem)
        event = m[2] + m[3] + m[4]
        eSign = m[3]
        nSharpe = new_stem[:m.start()] + "#" + new_stem[m.end():]
        
    else :
        p_event2.search(new_stem)
        m = p_event2.search(new_stem)
        event = "20" + m[2] + m[3] + m[4]
        eSign = m[3]
        nSharpe = new_stem[:m.start()] + "#" + new_stem[m.end():]

    # eSign에 따라 3.docu, name과 extra에서 각각 docu키워드 제거
    for k, v in dict.items() :
        if v[0].search(eSign) :
            docu = k
            nSharpe = v[1].sub("", nSharpe, count=1)
            if (k == "집행권원") and comp["집행권원 재도"].search(nSharpe):
                docu = docu + " 재도"
                nSharpe = comp["집행권원 재도"].sub("", nSharpe)
            elif k == "경정" or k == "항고" : #몇개없으니 문서구분이 제대로 된 경우만 처리한다. 그렇지 않은 경우 hand로 보내서 문서구분 수작업 해준다.
                if v[1].search(n) : #nSharpe는 위에서 지워버렸다.
                    docu = v[1].search(n).group() + k
                else : 
                    return [2, noKeyStem]  # 경정사건인데 문서구분이 정확하지 않다.

            nSharpe = re.sub("기타", "", nSharpe)
            
            return [1, [docu, event, nSharpe]] # 잘 마무리

    return [2, noKeyStem] #반복문 끝나도 없음. 이거 탭 위치 for랑 같아야 한다!!!

def setDocu(noKeyStem:str)->list :
    """[False:다큐없음, stem]|[True:다큐있음, [docu, name+"#"+extra]"""
    n = noKeyStem
    docu, name, extra = "", "", ""
    isDocu = False
    
    for k, v in comp.items() : #comp의 순서가 의미 있음!!!!!
        if v.search(n) :
            isDocu = True
            s = v.search(n).start() # 이걸 기준으로 name과 extra만 저장하기 때문에 검색된 키워드는 첫번째것만 자연스레 제거
            e = v.search(n).end()
            # 문서구분 추가(ss)
            if k == "개인정보1" or k == "양도통지서1" or k == "신용회복1" or k == "재산조사1" or k == "강제집행1" or k == "신용조회1" or k == "기타1" : 
                docu = k[ :-1 ] # 1떼기
                name = n[ : s ]
                extra = n[ s : ] 
            # 문서구분이 검색어를 대체
            elif k == "등본" :
                docu = k
                name = n[ : s ] 
                extra = n[ e : ]
                if re.search("법인", n) :
                    docu = "법인" + "등기"
            else :
                docu = k
                name = n[ : s ] 
                extra = n[ e : ]
                if k == "집행권원" : # 집행권원 + (재도)? // 재도만 있는 경우는 상위로직 그대로 적용
                    if comp["집행권원 재도"].search(n):
                        docu = k + " 재도"
                        name = comp["집행권원 재도"].sub("", name)
                        extra = comp["집행권원 재도"].sub("", extra)
                elif k == "양도통지서" : # 차수가 있다면 docu로 살려주고, 종적조회는 extra로 넘겨서 살려준다.
                    p_차수 = re.compile(r"(?<!\d)\d차")
                    if p_차수.search(n) :
                        docu = k + " " + p_차수.search(n).group()
                        name = p_차수.sub("", name)
                        extra = p_차수.sub("", extra)
                    if comp["양도통지서1"].search(n) : # 종족조회 및 기타 제거 
                        name = comp["양도통지서1"].sub("", name)
                        extra = "종적조회"+ " "+ comp["양도통지서1"].sub("", extra)

            name = re.sub('기타', "", name, count=1)
            extra = re.sub('기타', "", extra, count=1)
            return [isDocu, [docu, name+"#"+extra]] #첫번째로 검색된 곳에서 반복문 종료
    
    return [isDocu, noKeyStem] #반복문 끝났는데도 매칭되는게 없었다면

def setDepth(new_f:str)->set :
    "return3 \n 함수 실행 후 depth1이 비었거나, wrong~~거나 out인지 확인"

    p_out = re.compile('개인회생\(면책\)|파산\(면책\)|환매|매각|종결')
    docu_folder_dict = {"원인서류": "1.원인서류", "양도통지서": "2.양도통지서", "집행권원": "3.집행권원", "강제집행": "4.강제집행", 
        "등본": "5.등초본", "법인등기" : "5.등초본", "초본": "5.등초본", "등초본":"5.등초본", "외국인증명": "5.등초본", "주민등록정보":"5.등초본", 
        "개인회생": "6.개인회생", "신용회복": "7.신용회복", "파산": "8.파산", "재산조사": "9.재산조사", "부채증명서" : "10.부채증명서", 
        "신용조회" : "11.신용조회", "개인정보":"12.개인정보", "기타": "기타"}
    depth1 = ""
    depth3 = new_f[:8] ### 3(key)
    
    try :
        depth2 = dict_refer[depth3][0]  ### 2(매각사)
    except :
        return "wrongkey", "", ""

    try :
        docu = new_f.split("_")[2]
        for key, value in docu_folder_dict.items():
            if re.search(key, docu):
                depth1=value ### 1(docu)
                break
    except :
        return "wrongdocu", "", ""

    # 관리제외 검사
    if p_out.match(dict_refer[depth3][1]):
        return "out", depth2, depth3
    else :
        return depth1, depth2, depth3

def re_name(src: str, dst: str) -> list:
    """
    파일명 변경 후 os.rename대신 사용하는 함수(중복확인과 새넘버링)
    폴더를 바꾸는 거 아니라면 파일명 다를때라는 조건문 다음에 호출
    src : dir + file
    dst : dir + new_name
    return : [src dir, src filename, dst filename, dst dir]
    """
    dir = os.path.split(dst)[0]
    f_name = os.path.split(dst)[1]
    stem = os.path.splitext(f_name)[0]
    ext = os.path.splitext(f_name)[1]

    new_name = stem + ext
    i = 1
    while os.path.exists(dir+"/"+new_name):  # 작업디렉토리가 아니므로 풀경로
        new_name = stem + "_"+"("+str(i)+")"+ext
        i += 1

    if not os.path.exists(dir):
        os.makedirs(dir)  # 미리 만들어뒀으니 mkdir해도 됨
    
    shutil.move(src, join(dir, new_name))

    return [os.path.split(src)[0],os.path.split(src)[1], new_name, dir]

def write_log_csv(log:list, path : str) -> None:
    "2차원이 최적이고 그 이상은 셀안에 리스트로"
    if not os.path.exists(path):
        os.makedirs(path)  

    if len(log) > 0 :
    
        import csv
        
        name = str(datetime.today().strftime("%Y%m%d %H%M%S")) + "_" +str(len(log)) +".csv" 
                    
        with open (join(path, name), "a", newline="", encoding='utf-8-sig') as p :
            for row in log :
                wr = csv.writer(p)
                wr.writerow(row)
    else :
        print(f'log 파일이 비어있습니다. path : {path}')


if __name__ == "__main__" :
    ############################
    #다음 중 실행목적을 입력하시오. nas, done(검수완료), pc, test
    purpose = sys.argv[1]      
    if len(sys.argv) != 2 or re.match(r"nas$|done|fileTest|logTest", sys.argv[1])==None:     
        print("'nas', 'done', 'fileTest', 'logTest'중에서 목적을 입력하세요")
        sys.exit()
    else :
        #get path
        path, path_server, path_nobasic, path_out, path_log_out, path_log_success, path_log_nobasic, path_log_out, path_log_fail, path_df = getPath(purpose)
        print(f"purpose:{purpose}---\n {path}에서 실행합니다. \n {path_log_success}에서 로그를 확인하세요")
    ############################

    file_list, df = None, None
    success, nobasic , out , fail = [], [], [], []
    if purpose == "logTest" :
        df = file_list_test(path)
        file_list = df.src_file
        success = nobasic = out = fail
    else : 
        file_list = file_listFnc(path)
    total = len(file_list)


    for f in tqdm(file_list, total=total):
        try : #기본적으로 dict_refer에서의 에러는 처리된 상태이다. 
            if purpose != "done" : 
                name_items = {"key" :"", "name" :"", "docu" :"", "event" :"", "extra" :"", "date" :""}
                name_extra = None
                depth1, depth2, depth3,  = "", "", ""

                n = os.path.splitext(f)[0]
                ext = os.path.splitext(f)[1]

                name_items["key"], n = keyFnc(n) #### rm_s() 같이 실행, key 할당####################
                if not name_items["key"] :
                    if purpose == 'logTest' :
                        nobasic.append([f, "nokey"])
                        continue
                    else :
                        temp = (re_name(join(path,f), join(path_nobasic, f)))#--------t
                        temp.append("nokey")
                        nobasic.append(temp)
                        continue

                isEvent, docuEventNameextra = eventFnc(n) ### docu, event
                if isEvent == 2 :
                    if purpose == 'logTest' :
                        nobasic.append([f, "경정사건인데 nodocu"])
                        continue
                    else :
                        temp = (re_name(join(path,f), join(path_nobasic, f)))#-------t
                        temp.append("경정사건")
                        nobasic.append(temp)
                        continue
                    
                
                elif isEvent == 1 : # 정상
                    name_items["docu"] = docuEventNameextra[0] #사건번호>>docu 할당 #############
                    name_items["event"] = docuEventNameextra[1]
                    name_extra = docuEventNameextra[2]

                else : # 사건번호 없음
                    isDocu, docuNameExtra = setDocu(n)
                    if isDocu :
                        name_items["docu"] = docuNameExtra[0]#비사건번호>>docu 할당 #############
                        name_extra = docuNameExtra[1]
                    else : # nodocu
                        if purpose == 'logTest' :
                            nobasic.append([f, "nodocu"])
                            continue
                        else :
                            temp = re_name(join(path, f), join(path_nobasic, f))#---------t
                            temp.append("nodocu")
                            nobasic.append(temp)
                            continue

                date, temp = dateFnc(name_extra) ### date, name, extra
                name, extra = rmNeedlessSharp(temp) 
                if len(name) < 2 : 
                    try :
                        name = dict_refer[name_items["key"]][2]
                    except :
                        if purpose == 'logTest' :
                            nobasic.append([f, "dict_refer 키에러 for name"])
                        else :
                            temp = re_name(join(path, f), join(path_nobasic, f))#----------t
                            temp.append("dict_refer 키에러 for name")
                            nobasic.append(temp)
                    
                    for ptrn in re.findall("[가-힣a-zA-Z]+", name) : # 새로 가져온 name에만 적용
                        extra = re.sub(ptrn+"의?", "", extra) #다행히 ')의'는 '~~(주)의'밖에 안 보이네. 혹시 하다가 예외 많으면 \b의\b를 따로 해주는 것으로
                    
                    name, extra = rmNeedlessSharp(name+"#"+extra) #list를 반환하기 때문에 
                
                name = rm_s(name)
                extra = rm_s(extra)

                name_items["name"] = name #### name, extra, date 할당 ######################
                name_items["extra"] = extra
                name_items["date"] = date
                new_f = "_".join(filter(lambda x :bool(x), name_items.values())) + ext

            else :  # purpose == done
                new_f = f 
                pass

            depth1, depth2, depth3 = setDepth(new_f)########### depth ###########

            if depth1 == "" or depth1=="wrongdocu" or depth1 == "wrongkey" :
                if purpose == 'logTest' :
                    nobasic.append([f, depth1])
                else :
                    temp = re_name(join(path, f), join(path_nobasic, new_f))#----------t
                    temp.append(depth1)
                    nobasic.append(temp)
                

            elif depth1 == "out" :
                out_dir = join(path_out, depth2, depth3)
                if purpose == 'logTest' :
                    nobasic.append([new_f, out_dir])
                else :
                    if not os.path.exists(out_dir):
                        os.makedirs(out_dir)
                    out.append(re_name(join(path, f), join(out_dir, new_f)))#----------t

            else :
                dst_dir = os.path.join(path_server, depth1, depth2, depth3)
                if purpose == 'logTest' :
                    success.append([new_f, dst_dir])
                else :
                    if not os.path.exists(dst_dir):
                        os.makedirs(dst_dir)  
                    success.append(re_name(join(path, f), join(dst_dir, new_f)))#----------t

        except Exception as e:
            print("===================================")
            time = datetime.today().strftime("%H:%M:%S")
            print(time)
            print(f, n, e.__class__, e, sep=" : ")
            print(traceback.format_exc())
            #fail.append([f, e.__class__, e])#원래 이거였는데 그냥 test건 아니건 e만 하자-------------t
            fail.append([f, e])
            continue  # 반복문 계속 돌아


    if purpose == "logTest" :
        df['result'] = [x[0] for x in success]
        df['result_dir'] = [x[1] for x in success]

        print(f'{total} 중 {len(success)}개 처리')
        
        df.to_csv(join(path_log_success, "result"+str(datetime.today().strftime("%Y%m%d %H%M%S"))+".csv"), mode='w', encoding='utf-8-sig', index=False)

    else :
        try :
            write_log_csv(success, path_log_success)
            write_log_csv(nobasic, path_log_nobasic)
            write_log_csv(out, path_log_out)
            write_log_csv(fail, path_log_fail)
        except Exception as e:
            print(e)

        if total == len(success) + len(nobasic) + len(out) + len(fail) :
            print("처리된 파일에 누수 없음")
        else :
            print("처리된 파일에 누수 있음")

        print(f'전체:{total}개, 서버:{len(success)}개, 파일명에러:{len(nobasic)}개, 관리제외:{len(out)}개, 예상치못한 예외:{len(fail)}개')
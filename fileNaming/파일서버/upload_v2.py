"""
purpose를 설정하면 test 모드로 진입
0.변수(path, dict_referFnc( ), file_list( ), total, loglist
for문
    1. 변수 : items, extra, depth
    2. keyFnc( ) - rm_s( )
    2-1. nameFnc-> name, extra, #docu에서 name과 중복될까 걱정하지 않아도 됨. 이후 매개변수는 계속 extra
    3-1. setDocuByEvent( ) - > docu, event, extra
    else 
    3-2. setDocu( ) > docu, extra
    2-2. DateFnc 사건번호를 날짜로 취하는 경우 있어서 뒤로 옮김
    4. name#extra( ) : dateFnc( ), rmNeedless( ), rm_s( )
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
import binascii
import pickle

def getPath(purpose:str) :
    """
    'nas' : 새 스캔파일(업로드), nas내부 주소 \n
    'done' : 검수완료, 네트워크 경로 \n
    'fileTest' : pc바탕화면/test, 로그는 파일서버개편 폴더 \n
    'logTest' : 모든 로그 읽어서 원래파일명을 재작업, 스캔파일log/success, 로그는 파일서버개편
    """
    path = r'\\192.168.0.75\스캔파일\새 스캔파일(업로드)' 
    path_server = r'\\192.168.0.75\솔림헬프'
    path_nobasic = join(path, "파일명에러")
    path_out = r'\\192.168.0.75\삭제예정파일\관리제외'
    path_noUp = r'\\192.168.0.75\스캔파일\새 스캔파일(업로드)\업로드대상아님'
    path_log_success = r'\\192.168.0.75\스캔파일\스캔파일log\success'
    path_log_nobasic = r'\\192.168.0.75\스캔파일\스캔파일log\nobasic'
    path_log_out = r'\\192.168.0.75\스캔파일\스캔파일log\out'
    path_log_fail = r'\\192.168.0.75\스캔파일\스캔파일log\fail'
    path_df = r'\\192.168.0.75\스캔파일\스캔파일log\_project\파일\채무자조회.pkl'
    path_dupl = r'\\192.168.0.75\삭제예정파일\중복_서버'
    if purpose == "done" :
        path = r'\\192.168.0.75\스캔파일\새 스캔파일(업로드)\검수완료' 
        path_nobasic = join(path, "파일명에러")
    elif purpose == "fileTest" :
        path = r'C:\Users\DATA\Desktop\test' 
        path_server = join(path, 'server')
        path_nobasic = join(path, "hand")
        path_out = join(path, '관리제외')
        path_log_success = r'D:\0.전산\1.진행중과업\파일서버개편\log\success'
        path_log_nobasic = r'D:\0.전산\1.진행중과업\파일서버개편\log\nobasic'
        path_log_out = r'D:\0.전산\1.진행중과업\파일서버개편\log\out'
        path_log_fail = r'D:\0.전산\1.진행중과업\파일서버개편\log\fail'
        ### dict_referFnc 안 읽어지면 절대경로 담은 변수 직접 넣으라고
    elif purpose == "logTest" : # 파일 이동이 일어나지 않는다.
        path = r'\\192.168.0.75\스캔파일\스캔파일log\nobasic\새 폴더' ######################### 읽을 csv들 있는 경로
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
    # path_dupl = r'/volume1/삭제예정파일/중복_서버'
    # if purpose == "done" :
    #     path = r'/volume1/스캔파일/새 스캔파일(업로드)/검수완료' 
    #     path_nobasic = join(path, "파일명에러")
    # elif purpose == "fileTest" :
    #     path = r'C:\Users\DATA\Desktop\test' 
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
    
    return path, path_server, path_nobasic, path_out, path_noUp, path_log_out, path_log_success, path_log_nobasic, path_log_out, path_log_fail, path_df, path_dupl

y_10 = str(datetime.today().year)[2] #23년이면 2
y_10_before = str(int(y_10)-1) #23년이면 2-1 = 1
y_1 = str(datetime.today().year)[3] # 23년이면 3

path_dict = r'C:\Users\DATA\Desktop\workspace\python\fileNaming\파일' 
# dict_refer["key"][0:매각사, 1:채무상태, 2:채무자성명, 3:보증인성명]
with open(join(path_dict, "dict_refer.pkl"), 'rb') as pkl : dict_refer = pickle.load(pkl)

# outList (활용) "key" in sr.values > out
# 안 쓸거면 빈 outList를 만들면 됨
outList = pd.read_pickle(join(path_dict, "outList.pkl")) # 아래 코드 파이썬11로 업그레이드 후 오류 나서, 이렇게 바꿈. 어차피 nas에서는 빈 outList를 사용하므로 여기와는 다름
# with open(join(path_dict, "outList.pkl"), 'rb') as pkl : outList = pickle.load(pkl)

def crc32_checksum(filename):
    buf = open(filename,'rb').read()
    buf = (binascii.crc32(buf) & 0xFFFFFFFF)
    return "%08X" % buf

path_kcs = r'\\192.168.0.75/스캔파일/스캔파일log/_project/파일/중복조사/kcs별 파일정보_솔림헬프.pkl'
def read_cks(path_kcs) :
    with open(path_kcs, "rb") as pkl :
        return pickle.load(pkl)
dict_kcs = read_cks(path_kcs)

def isDuplFnc(root:str, f:str) :
    isDupl = False
    fullPath = join(root, f)
    crc32 = str(crc32_checksum(fullPath))
    size = str(os.path.getsize(fullPath))
    kcs = f[:8] + crc32 + size 
    
    #서버에 있음
    if kcs in dict_kcs : isDupl = True
    
    return isDupl, kcs

def write_cks(path_kcs, dict_kcs) :
    with open(path_kcs, "wb") as pkl :
        pickle.dump(dict_kcs, pkl)


# 사해행위는 판결, 품의서, 예고서 등 다방면에 걸쳐있어 키워드로 부적합
comp = { # search는 앞에서부터 찾으니까 엄격한 것이 앞으로. 단 가압류와 압류처럼 가?압류로 표현할 수 있는 것은 순서 상관이 없음
#"1붙은 건 .*를 앞에 붙여서 아예 맨 앞으로 문서구분을 이동시켜도 괜찮겠다. 어차피 이름 중복되면 제거하니까."
# 세부종류에 해당하는 것은 1에 써넣어서 세부종류 문구를 살린다.
        "원인서류" : re.compile(r"(원인|대출)\s?서류|(신용)?\s?대출[약신원승확상거][가-힣]*|(?<!분할)약정서|(대출)?\s?원장|마이너스\s?대출"),
        "원인서류1" : re.compile(r"보증서류?|(입회|가입|카드)\s?신청서|(?=녹취)|통화\s?(내용|내역)|근보증서?"),
        "개인정보" : re.compile(r"개인정보(?!활용|이용| 활| 이|동의| 동)"),
        "개인정보1" : re.compile(r"신분증|운전면허|주민등록증|(기초)?수급자?|차상위|(법인|사용)?\s?인감|기본\s?증명서?|(가족|혼인)\s?(관계|증명)|이혼|입양|친양자|졸업|병적|장애인(등록)?증|복지카드"),
        # 등초본
        "외국인증명" : re.compile(r"\(?\s?외국인\s?증명서?\s?\)?|\(?\s?외국인\s?등록\s?사실\s?증명서?\s?\)?|\(?\s?외국인\s?등록증?\s?\)?|\(?\s?외국인\s?\)?|국내거소"),
        "등초본" : re.compile(r"\(?\s?등본.{0,3}원?초본\s?\)?|\(?\s?원?초본.{0,3}등본\s?\)?|\(?\s?등[\s.,]?초본?\s?\)?|\(?\s?초[\s.,]?등본?\s?\)?|\(?\s?주민\s?등록\s?등본\s?및?\s?초본\s?\)?|\(?\s?주민\s?등록\s?초본\s?및?\s?등본\s?\)?"), # '등본 및 초본' 때문에 .{0,3}
        # 등기부\s?등본은 조건문으로 처리하자.
        "초본" : re.compile(r"(?<![가-힣])원초본|(?<=원)원초본|(?<![가-힣])\(?\s?원\s?초본\s?\)?|(?<=[가-힣]{3})원초본|\(?\s?(?<!법원)초본\s?\)?|\(?\s?주민\s?등록\s?초본\s?\)?"), # 말소자_초본
        "주민등록정보" : re.compile(r"행자부\s?(전송)?\s?(자료)?|주민\s?등록\s?정보|주소\s?이력|법원초본"),
        # 키워드 추가(대체x)
        "기타" : re.compile(r"기타"),
        "기타1" : re.compile(r"집행문\s?부여|배송[가-힣]+|(채권|양도|양수|매매).*계약|화해(?!권고)|대위변제|분할|분납|상환(?!일정)(?!유예)|감면|(상속)?\s?한정\s?승인|\
            |상속\s?포기|지방세|세목별|과세|.*내용증명|출입국사실|.*답변서|.*진술서|.*보정(서|명령)|.*인포케어|보증면탈|자동차직권말소|완제|.*품의서|금전\s?공탁|\
            |배분\s?[계산|기일|내역]|[가-힣\s]*(예고|답변|준비)|이의\s?신청|의사표시(?!용)|소송고지|위임장|개별공시|주택가격|(채무)?\s?(종결|면제)\s?확인서?|개인\s?정보\s?(활용|이용|동의)|\
            |소장|진단서|사용\s?증명원|해제\s?통지서|의견\s?청취서|느단|즈기|추심\s?요청서?|안내문?|개문|채무\s?이행\s?통지|신탁(해지|해제)?통지서?|최고장|사실조회|잔액증명"), # 제거가 아니므로 첫 글자만 잘 찾으면 된다.
        #기타 제거 : 부채증명서, 신용조회, 주민등록정보
        "양도통지서" : re.compile(r"(채권)?\s?(양도|양수)\s?통지서?|(채권)?\s?양도\s?및?\s?양수\s?(통지)?서?\s?|(?<![^가-힣][가-힣])양통|(?<=\d차)\s?(양통|양도통지서?)|(?<=환매)통지서?"), # 세양통신 해결
        "양도통지서1" : re.compile(r"종[적족]\s?(조회)?|(?<!주소|소\s)이력"), #종적조회는 모두 양통만 있더라
        # 사건번호 내부는 이제 신경쓰지 않아도 된다.
        "파산" : re.compile(r"파산|면책|파산.*면책"), #파산에도 배당있음. 강제집행보다 먼저 나와야
        # 연도 다음에 나오는 개회가 아닌 경우, 전방탐색을 통해 '개인회생'의 '회생'이 걸리는 거 방지. count도 하자
        "개인회생" : re.compile(r"개회|(개인)?\s?회생"),
        "신용회복" : re.compile(r"(?<=[가-힣]{3})신복|[\s_]신복|신용\s?회복"), #이름에있는 신복,숫자뒤 신복은 제외. 이름다음에 띄어쓰기 없이 나온 신복은.. 
        "신용회복1" : re.compile(r"(개인)?채무\s?조정안?|.*원상\s?회복|(신청인|채무자|고객)\s?현황|채권신고\s?및?\s?의견서|상환일정표"),
        "재산조사" : re.compile(r"재산\s?조사|재산\s?조회"), # 상세문서를 재산조사가 대체하는 게 아님에 유의
        "재산조사1" : re.compile(r"(?<!법인)\s등기|(?<!법인)등기|가압류\s?물건지|.*대장|.*등록\s?원부|.*은행거래|.*입출금"),
        
        "부채증명서" : re.compile(r"부채\s?(잔액)?증명\s?[서원]?(류|발급)?"),
        "신용조회" : re.compile(r"신용\s?조[회사]|신용\s?정보(?!( 활용| 이용|활용|이용|동의))"), # 신용조회가 있는 경우 기관이 앞에 나오건 뒤에 나오건 냅두면 되니 상관없음.
        "신용조회1" : re.compile(r"KCB|NICE|나이스|주거래(기관|은행)", re.I), # 기관명만 있는 경우 docu만 추가하면 됨. 순서대로 검색하니 여기 왔다는 건 신용조회라는 말이 없다는 뜻

        # 판결문과 결정문이 여러곳에서 나올 수 있어 뒤로 뺌. 결정과 판결은 더욱 애매해서 제외함
        "집행권원" : re.compile(r"집행\s?권원|집행문(?!부여|이의)|승계\s?(집행|결정)?(문)?|양수금\s?(판결)?문?|(가단|가합|가소|차전)\s?(결정|판결)?문?"), # count=1이 의미있게 하기 위해 올바른 표현도 넣는다.
        "집행권원1" : re.compile(r"사해행위\s?(취소)?\s?(소송)?|공정증서|지급\s?(명령|결정문?)|이행\s?(권고|결정문?)|화해\s?(권고|결정문?)|판결문"),
        "집행권원 재도" : re.compile(r"(?<!\b[가-힣])재도(부여|건)?|\b재도건?|(?<![문통])\s부여건?|재교부건?|재발급건?"),    # 사해행위는 집행권원이지만, 답변서, 예고, 준비서면등이 있어서 키워드로 등록하기가..카단이랑도 엮인경우동 있고..
        # 수통은 수통대로(230308)
        "강제집행" : re.compile(r"강제\s?집행|압추|(채권)?\s?압류\s?및?\s?추심\s?(명령|결정)?문?|(채권)?\s?추심\s?및?\s?압류\s?(명령|결정)?문?|채권\s?(추심|압류)\s?(결정)?문?|(카단|카명|카합|카담|타채|타경|타기)\s?(결정)?문?"), # 결정이라는 말이 여러곳에서 나올 수 있어 단독사용은 불가
        "강제집행1" : re.compile(r"재산\s?명시|[가-힣\b]*(부?동산)?\s?(가?압류|경매)(?!물건)|[가-힣\b]*동산|[가-힣\b]*추심|[가-힣\b]*유체|[가-힣\b]*배당[가-힣]|.*진술\s?최고서?|3자이의|공매"),#3자이의는 '가단'이라도 관련사건이 강제집행임 #일반진술서는 안올려도 되는 거(최고진술서=3채무자).그냥 최고서와 또 다름. 올린다면 기타로
        "등본" : re.compile(r"\(?\s?법인\s?등기부?\s?(등본)?\s?\)?|\(?\s?(?<!(등기부|기부 |배당표|당표 |.결정|결정 ))등본\s?\)?|\(?\s?주민\s?등록\s?등본\s?\)?|제적\s?(등본)?|재적(?!증명)") #법인등본, 제적등본(재적으로 잘못쓰는 경우 더러 있음. 재적(학적)증명서와 구분) setDocu에서 따로 처리, 등본이 법원서류에서 자꾸 나오니 젤 아래로...
    }

def file_listFnc(path) :
    # 업데이트시 dupl.py의 fileInfo()도 업데이트 해야
    p_extension = re.compile('(jpeg|jpg|bmp|gif|pdf|png|tif|tiff|m4a|wav|mp[34])$', re.I) 
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

def rmNeedless(extra:str) :
    """name과 extra가 확정되면 그때 한 번 해준다. 공백으로 대체가 있으므로 rm_s()를 내포했다.
    일련번호와 기호 제거"""
       
    #1) 완전제거                                                      영어와 숫자가 연속
    # p_rmSerialN = re.compile(r"(?<![a-zA-Z])[a-zA-Z](?![a-zA-Z])|\d+[a-zA-Z]+|[a-zA-Z]+\d+|TAA\(회\)|\
    #     |SCSB|ADMIN.*Conflict|(?<!외)\d(?!건|통|차|채|자|억|천|백|급|번|회|길|명|8염|염)|복사본") # 모든 숫자를 지운다. 해당글자가 나온다면 그 앞 숫자는 살린다. 
    
    #2) 이제 숫자는 남김도록 수정한거
    p_rmSerialN = re.compile(r"(?<![a-zA-Z])[a-zA-Z](?![a-zA-Z])|\d+[a-zA-Z]+|[a-zA-Z]+\d+|TAA\(회\)|\
         |SCSB|ADMIN.*Conflict|복사본") # 모든 숫자를 지운다. 해당글자가 나온다면 그 앞 숫자는 살린다. 
    p_sign = re.compile(r"[^㈜()\sa-zA-Zㄱ-ㅎ가-힣\d]|\([^\w]*\)") #반쪽 괄호만 있는 거는 어케 지우지?

    # ...(3)... 이런건 지우자
    p_numbering = re.compile(r"\(\s?\d{1,2}\s?\)")
    extra = p_numbering.sub(" ", extra)
    
    extra = p_rmSerialN.sub("", extra)
    extra = p_sign.sub(" ", extra)

    extra = rm_s(extra)

    return extra

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

def pwFnc(stem:str) :
    new_stem = stem
    p_pw = re.compile(r"(비번|pw)\s?\d*", re.I)
    pw = ""

    if p_pw.search(new_stem) :
        pw = p_pw.search(new_stem).group()
        new_stem = p_pw.sub("", new_stem)
    
    return pw, new_stem

def ptrnFnc(ptrn) : # 정규식 함수에서 () 인식을 위한 함수
    ptrn = re.sub("\(", "\\(", ptrn) #search에서 ()인식을 위해
    ptrn = re.sub("\)", "\\)", ptrn)
    return ptrn

def nameFnc(stem : str, debtorName : str, grtName : str) :
    """return : name, extra \n
    name은 rm까지 다 해서 리턴. 바로 저장하면 됨."""
    
    name = ""
    extra = stem
    
    debtorList = []
    grtList = []
    debtor, grt = "", ""
    
    #index를 안 쓰면 바로 바로 지워주면 되네. #기호를 구분자로 했기 때문에 괄호는 살리지 못함
    for ptrn in re.findall("[가-힣a-zA-Z]+|\([가-힣]\)|㈜", debtorName) : #(주)를 안 썼을 수도 있으니 따로 빼준다.
        if ptrn == "㈜" :
            m = re.search("\(주\)|㈜", stem)
            if m :
                debtorList.append(m.group())
                extra = re.sub("망?\s?" + ptrnFnc(m.group()) + r"\s?(의(?=\s))?", " ", extra)
        else : 
            ptrn = ptrnFnc(ptrn)
            p = re.search(ptrn, stem)
            if p :
                debtorList.append(p.group())
                extra = re.sub("망?\s?" + ptrnFnc(p.group()) + r"\s?(의(?=\s))?", " ", extra) # 의를 지우되 보증인이 의로 시작하는 경우 있어 (?=\s)추가, 대신 앞뒤공백 다 제거할 수 있어서 " "로 대체
    
    if grtName != "" : 
        for ptrn in re.findall("[가-힣a-zA-Z]+|\([가-힣]\)|㈜", grtName) : #(주)를 안 썼을 수도 있으니 따로 빼준다.
            if ptrn == "㈜" :
                m = re.search("\(주\)|㈜", stem)
                if m :
                    grtList.append(m.group())
                    extra = re.sub("망?\s?" + ptrnFnc(m.group()) + r"\s?(의(?=\s))?", "", extra)
            else :
                ptrn = ptrnFnc(ptrn)
                p = re.search(ptrn, stem)
                if p :
                    grtList.append(p.group())
                    extra = re.sub("망?\s?" + ptrnFnc(p.group()) + r"\s?(의(?=\s))?", " ", extra)
    
    # name에 debtor 추가
    if len(debtorList) == 1 :
        name = debtorList[0]
    elif len(debtorList) > 1 :
        debtor = " ".join(debtorList)
        # 괄호 앞 뒤로 공백 없애기
        debtor = re.sub("\s\(", "(", debtor)
        debtor = re.sub("\)\s", ")", debtor)
        debtor = re.sub("\s?㈜\s?", "㈜", debtor)
        name = debtor

    # name에 grt 추가
    if len(grtList) == 1 :
        grt = grtList[0]
        name = name + " 보증인 " + grt
        extra = re.sub("보증인", "", extra)
    elif len(grtList) > 1 :
        grt = " ".join(grtList)
        grt = re.sub("\s\(", "\(", grt)
        grt = re.sub("\)\s", "\)", grt)
        grt = re.sub("\s?㈜\s?", "㈜", grt)
        name = name + " 보증인 " + grt
        extra = re.sub("보증인", "", extra)
    
    if not re.search("[가-힣a-zA-Z]{2}", name) : # 한글이나 영어가 2글자 이상이 되지 못한다면
        name = debtorName

    if re.search("[()]", name) != None and re.search(".*[()]", name).group()[-1] == "(" : #닫는 괄호 추가해주기
        name = name + ")"

    name = rmNeedless(name)
    name = rm_s(name)

    return name, extra

def dateFnc(subStem:str)->str :
    "2000년 이후/기호없으면 2015년 이후 / 인자:key제거후, sign 제거 전  /  return2(date, stem-date)"
    date = ""
    # 컴파일
    #중간기호 : [. / - 공백]  그룹이름 필요  #####연도는 바뀔때마다 최댓값 수정 필요#### 
    year_com = '(?P<y>[0-'+ y_10_before +']\d|'+y_10+'[0-'+y_1+'])'
    p_day4 = re.compile('\(?\s?(?<!\d)(20)?'+year_com+'[\.\/\-\s](?P<m>[1-9])[\.\/\-\s](?P<d>[1-9])(?!\d)\s?\)?')
    p_day5 = re.compile('\(?\s?(?<!\d)(20)?'+year_com+'[\.\/\-\s](?P<m>[1-9])[\.\/\-\s](?P<d>[0-2][1-9]|[1-3]0|31)(?!\d)\s?\)?') 
    p_day5d = re.compile('\(?\s?(?<!\d)(20)?'+year_com+'[\.\/\-\s](?P<m>10|11|12)[\.\/\-\s](?P<d>[1-9])(?!\d)\s?\)?')
    p_day6 = re.compile('\(?\s?(?<!\d)(20)?'+year_com+'[\.\/\-\s](?P<m>0[1-9]|1[0-2])[\.\/\-\s](?P<d>[0-2][1-9]|[1-3]0|31)(?!\d)\s?\)?')
    #중간기호는 없고 괄호로 감싸진 경우
    p_day_4 = re.compile('\(\s?(20)?'+year_com+'(?P<m>[1-9])(?P<d>[1-9])(?!\d)\s?\)')
    p_day_5 = re.compile('\(\s?(20)?'+year_com+'(?P<m>[1-9])(?P<d>[0-2][1-9]|[1-3]0|31)(?!\d)\s?\)')#(22119)가 1월19일인지 11월9일인지 알수 없고, 어차피 먼저 나온 거에 걸리므로 5d는 필요가 없다.
    p_day_6 = re.compile('\(\s?(20)?'+year_com+'(?P<m>0[1-9]|1[0-2])(?P<d>[0-2][1-9]|[1-3]0|31)(?!\d)\s?\)')
    #괄호,중간기호 없이 (그러니 보수적으로) 숫자만 6/8자리 있는 경우.생년월일/일련번호와 겹칠 수 있으니 2015년 이후만. 4자리는 날짜인지 불확실하니 제외.
    p_day_d = re.compile('(?<!\d)(20)?(?P<y>1[5-9]|'+y_10+'[0-'+y_1+'])(?P<m>0[1-9]|1[0-2])(?P<d>[0-2][1-9]|[1-3]0|31)(?!\d)')
    # 글자 '년','월','일'이 있는 경우
    p_y = re.compile(r'(\d\d|\d\d\d\d)년')
    p_m = re.compile(r'(\d{1,2})월')
    p_d = re.compile(r'(\d{1,2})일')
    # 날짜 포맷 통일
    if p_day4.search(subStem):
        d = p_day4.search(subStem)
        date = d["y"] + "0" + d["m"] + "0" + d["d"]
        subStem = p_day4.sub("", subStem)
    elif p_day5.search(subStem) : #월이 한자리
        d = p_day5.search(subStem)
        date = d["y"] + "0" + d["m"] + d["d"]
        subStem = p_day5.sub("", subStem)
    elif p_day5d.search(subStem) : #일이 한자리
        d = p_day5d.search(subStem)
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
    elif p_day_5.search(subStem) : #월이 한자리
        d = p_day_5.search(subStem)
        date = d["y"] + "0" + d["m"] + d["d"]
        subStem = p_day5.sub("", subStem)
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
            if len(py[1]) == 2 : date = py[1]
            else : date = py[1][2:]
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

# date, rmNeedless 내부 실행
def eventFnc(noKeyStem:str, testMode :bool = False)->list:
    """
    키와 확장자 제거한 파일명을 받아 return [0|1|2, old|new stem] \n
    0 : 사건번호 없음 또는 기타에 해당, ["", "", extra] 반환 -> setDocu( ) 호출해 \n
    1 : 사건번호 있고, [docu, event, extra]을 두번째 요소로 반환 \n
    2 : 경정사건번호인데 정확한 문서구분없음 ["", event, extra] 반환 -> setDocu() \n
    사건번호 2개일 때 : 사건번호 두개가 모두 집행권원, 강제집행일때 하나는 지워짐 \n (line482 extra = v[1].sub("", extra, count=1)) \n 지워지는게 맞음
    """
    new_stem = noKeyStem
    event, eSign, docu, extra = "", "", "", ""
    
    # 컴파일
    year_com = "([0-"+ y_10_before +"]\d|"+y_10+"[0-"+y_1+"])"
    # y-es-sn
    p_event1=re.compile("((?<=\D)|^)(?P<y>19\d\d|20"+year_com+")\s?(?P<es>준?재?[가간금나느다머차카타즈본징하개회라정][가-힣]{0,2})\s?(?P<sn>[0-9]+)") # 4자리 연도
    p_event2=re.compile("((?<=\D)|^)(?P<y>[7-9][0-9]|"+year_com+")\s?(?P<es>준?재?[가간금나느다머차카타즈본징하개회라정][가-힣]{0,2})\s?(?P<sn>[0-9]+)") # 2자리 연도
    # y-es
    # "\\b" 를 하거나 r"\b"를 해야함
    p_event3=re.compile("((?<=\D)|^)(?P<y>19\d\d|20"+year_com+")\s?(?P<es>준?재?[가간금나느다머차카타즈본징하개회라정][가-힣]{0,2})\\b") #반드시 띄어쓰기를 하거나 끝이나야.(& 1보다 먼저 if문에서 나오면 안됨.)
    p_event4=re.compile("((?<=\D)|^)(?P<y>[7-9][0-9]|"+year_com+")\s?(?P<es>준?재?[가간금나느다머차카타즈본징하개회라정][가-힣]{0,2})\\b") #반드시 띄어쓰기를 하거나 끝이나야.(& 1보다 먼저 if문에서 나오면 안됨.)
    p_court = re.compile("[가-힣]*(법원|지원|지법|서울|대전|대구|부산|광주|수원|\
        |의정부|파주|포천|동두천|가평|연천|철원|인천|김포|강화|용인|오산|광명|\
        |안성|양평|이천|춘천|홍천|양구|삼척|동해|정선|평창|태백|횡성|인제|화천|\
        |고성|양양|금산|세종|보령|서천|예산|아산|태안|당진|(?<![도통문])부여|청양|진천|보은|\
        |단양|음성|옥천|괴산|경산|칠곡|청도|영천|성주|고령|영주|봉화|구미|문경|\
        |예천|청송|군위|울진|영양|양산|울산|창원|마산|김해|함안|의령|사천|남해|\
        |하동|거제|창녕|합천|함양|산청|담양|함평|강진|구례|영광|나주|장성|화순|\
        |곡성|광양|고흥|여수|보성|무안|영암|완도|진도|전주|군산|정읍|남원|진안|\
        |김제|무주|임실|익산|부안|고창|장수|순창|제주|서귀포)[가-힣]*")
    # 사해행위는 판결, 품의서, 예고서 등 다방면에 걸쳐있어 키워드로 부적합
    # 수통부여, 재도부여 때문에 '부여'수정(230308)
    dict = { #사건구분 검색어 / 삭제할 문서구분 키워드
        "집행권원" : [re.compile("가[합단소]|나|다|머|차"), comp["집행권원"]],
        "강제집행" : [re.compile("카(?!경|기|확)|타|즈(?!기)|본|금|징|가|정[가|명|라|마|고|드|브|스|기]"), comp["강제집행"]], #카경: 결정,판결경정, 카기, 즈기: 기타민사신청(심판경정), 본(접수증)은 강제집행맞다.
        "개인회생" : [re.compile("개|회"), comp["개인회생"]], # 라 : 민사항고사건
        "파산" : [re.compile("하"), comp["파산"]], 
        "경정" : [re.compile("카경|카기전|즈기"), re.compile('집행권원|강제집행')],
        "항고" : [re.compile("라"), re.compile('집행권원|강제집행|개인회생|파산|기타')],
        "기타" : [re.compile("느|카(기(?!전)|담|확)"), re.compile("기타")] #느(단) : 한정상속, 상속포기, 기:의사표시공시송달, 담:담보취소, 확:소송비용확정
        }

    # 관할법원 제거
    new_stem = p_court.sub("", new_stem)
    if testMode : print("관할법원 제거후 : ", new_stem)
    
    # event, name, extra, eSing 할당
    #사건번호 있는 경우
    whichCase = ""
    if p_event1.search(new_stem):
        whichCase = "1"
        m = p_event1.search(new_stem)
        event = m["y"] + m["es"] + m["sn"]
        eSign = m["es"]
        extra = new_stem[:m.start()] + new_stem[m.end():]
    elif p_event2.search(new_stem) :
        whichCase = "2"
        m = p_event2.search(new_stem)
        if int(m["y"])  <= int(y_10 + y_1):
            event = "20" + m["y"] + m["es"] + m["sn"]
        else :
            event = m["y"] + m["es"] + m["sn"] # 1900년대는 사건번호에 19안 붙음
        eSign = m["es"]
        extra = new_stem[:m.start()] + new_stem[m.end():]
    elif p_event3.search(new_stem) :
        whichCase = "3"
        m = p_event3.search(new_stem)
        event = m["y"] + m["es"]
        eSign = m["es"]
        extra = new_stem[:m.start()] + new_stem[m.end():]
    elif p_event4.search(new_stem) :
        whichCase = "4"
        m = p_event4.search(new_stem)
        if int(m["y"])  <= int(y_10 + y_1):
            event = "20" + m["y"] + m["es"]
        else :
            event = m["y"] + m["es"]
        eSign = m["es"]
        extra = new_stem[:m.start()] + new_stem[m.end():]
    else :
        if testMode : print("사건번호 없음")
        return [0, ["", "", noKeyStem]]

    if testMode : print(f"사건번호 있음. event = {event}, extra = {extra}, whichCase = {whichCase}")

    # eSign에 따라 3.docu, name과 extra에서 각각 docu키워드 제거
    for k, v in dict.items() : # k = 문서종류, v[0] = 사건구분자 컴파일, v[1] = (집행권원, 강제집행)키워드, 문서종류
        if v[0].search(eSign) : # 개회, 파산은 무조건 개회, 파산
            if (k == "개인회생" or k=="파산") : 
                docu = k
                extra = v[1].sub("", extra, count=1) # v[1] = comp["집행권원..."]
                extra = re.sub("기타", "", extra)
                if testMode :
                    print(f"개인회생,파산에 걸림. docu = {docu}, extra={extra}")
                return [1, [docu, event, extra]]

            else :
                if comp["기타1"].search(extra) : # 실제 기타로 처리하는 키워드가 있는 경우
                    docu = "기타"
                    extra = re.sub("기타", "", extra)
                    if testMode : print(f"기타에걸림 [d,e,ex]리턴 {docu, event, extra}")
                    return [1, [docu, event, extra]] # 1번으로 처리해도 되지만 여기가 복잡해지니 setdocu로 넘기자

                # ('기타'라는 말을 제외하곤)기타에 해당하는 키워드가 없는 경우 >> 사건구분자 문서구분
                else :
                    if (k == "집행권원") : 
                        if eSign == "가단" and re.search("3자이의", extra) : #3자이의는 유체동산 관련
                            docu = "강제집행"
                            extra = re.sub("강제집행", "", extra)
                        elif comp["집행권원 재도"].search(extra): # 재도일때
                            docu = k + " 재도"
                            extra = comp["집행권원 재도"].sub("", extra)
                        else :
                            docu = k
                        extra = v[1].sub("", extra, count=1) # v[1] = comp["집행권원..."]
                        if testMode : print(f"집행권원에 걸림 [d,e,ex]리턴 {docu, event, extra}")
                    elif k == "경정" or k == "항고" : 
                        if v[1].search(extra) : # 정식구분(집행권원...)에 해당하는 경우
                            docu = v[1].search(extra).group()
                            extra = v[1].sub("", extra)
                            extra = re.sub(k, "", extra) + k # 있든 없든 붙여주기 위해서
                            if testMode : print(f"경정에 왔고, docukey있음. [d,e,ex]리턴 {docu, event, extra}")
                        else : # setDocu로 해결
                            extra = re.sub(k, "", extra) + " " + k # 있든 없든 붙여주기 위해서
                            if testMode : print(f"경정docukey는 없음 [d,e,ex]리턴 {docu, event, extra}")
                            return [2, ["", event, extra]]  # 경정/항고사건인데 문서구분이 정확하지 않다.
                    else :
                        docu = k
                        extra = v[1].sub("", extra, count=1) # v[1] = comp["집행권원..."]
                        if testMode : print(f"else사실상 강제집행에 걸림 [d,e,ex]리턴 {docu, event, extra}")

                    extra = re.sub("기타", "", extra)
                    if testMode : print(f"extra에서 기타를 제거하고 1번을 리턴함 [d,e,ex]={docu, event, extra}")
                    return [1, [docu, event, extra]] # 잘 마무리
    if testMode : print(f"for문이 끝났지만 아무것도 걸리지않음. 이럴 수는 없는데 {docu, event, extra}")
    return [2, ["", event, extra]] #사건구분 포섭 실패. 이거 탭 위치 for랑 같아야 한다!!!

def setDocu(noKeyStem:str)->list :
    """[False:다큐없음, stem]|[True:다큐있음, [docu, name+"#"+extra]"""
    extra = noKeyStem
    docu = ""
    isDocu = False
    
    for k, v in comp.items() : #comp의 순서가 의미 있음!!!!!
        if v.search(extra) :
            isDocu = True
            # 문서구분 추가(ss)
            if k == "개인정보1" or k == "양도통지서1" or k == "신용회복1" or k == "재산조사1" or k == "강제집행1" or k == "신용조회1" or k == "기타1" or k=="집행권원1" or k=="원인서류1" : 
                docu = k[:-1] # 1떼기
                # extra = re.sub(comp[docu], "", extra, count=1) # "개인정보"와 "개인정보1"의 순서가 바뀌는 경우엔 필요
            # 문서구분이 검색어를 대체
            elif k == "등본" :
                if re.search("법인", extra) :
                    docu = "법인등기"
                elif re.search("제적", extra) :
                    docu = "제적등본"
                else :
                    docu = k
                extra = v.sub("", extra)
            else :
                docu = k
                extra = v.sub("", extra)
                if k == "집행권원" : # 집행권원 + (재도)? // 재도만 있는 경우는 상위로직 그대로 적용
                    if comp["집행권원 재도"].search(extra):
                        docu = docu + " 재도"
                        extra = comp["집행권원 재도"].sub("", extra)
                elif k == "양도통지서" : # 차수가 있다면 docu로 살려주고, 종적조회는 extra로 넘겨서 살려준다.
                    p_차수 = re.compile(r"(?<!\d)\d차")
                    if p_차수.search(extra) :
                        docu = k + " " + p_차수.search(extra).group()
                        extra = p_차수.sub("", extra)
                    if comp["양도통지서1"].search(extra) : # 종족조회 및 기타 제거 
                        extra = "종적조회"+ " "+ comp["양도통지서1"].sub("", extra)

            extra = re.sub('기타', "", extra, count=1)
            return [isDocu, [docu, extra]] #첫번째로 검색된 곳에서 반복문 종료
    
    return [isDocu, noKeyStem] #반복문 끝났는데도 매칭되는게 없었다면

def setDepth(new_f:str)->set :
    "return3 \n 함수 실행 후 depth1이 비었거나, wrong~~거나 out인지 확인"

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
    if depth3 in outList.values :
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

def write_log_csv(log:list, path : str, addInfo : str = "") -> None: #################################
    "2차원이 최적이고 그 이상은 셀안에 리스트로"
    if not os.path.exists(path):
        os.makedirs(path)  

    if len(log) > 0 :
    
        import csv
        
        if len(addInfo) > 0 : addInfo = "_" + addInfo

        name = str(datetime.today().strftime("%Y%m%d %H%M%S")) + "_" +str(len(log)) + addInfo +".csv" ################################
                    
        with open (join(path, name), "a", newline="", encoding='utf-8-sig') as p :
            for row in log :
                wr = csv.writer(p)
                wr.writerow(row)
    else :
        for name in globals() : 
            if globals()[name] is log :
                print(f'log 파일이 비어있습니다. path : {path}')

def no_upload(f) :
    p = re.compile(r"NOUPLOAD")
    if p.search(f) : return True
    else : return False


if __name__ == "__main__" :
    ############################
    #다음 중 실행목적을 입력하시오. nas, done(검수완료), pc, test
    purpose = sys.argv[1]      
    if len(sys.argv) != 2 or re.match(r"nas|done|fileTest|logTest", purpose)==None:     
        print("'nas', 'done', 'fileTest', 'logTest'중에서 목적을 입력하세요")
        sys.exit()
    else :
        #get path
        path, path_server, path_nobasic, path_out, path_noUp, path_log_out, path_log_success, path_log_nobasic, path_log_out, path_log_fail, path_df, path_dupl = getPath(purpose)
        print(f"purpose:{purpose}---\n {path}에서 실행합니다. \n {path_log_success}에서 로그를 확인하세요")
    ############################
    
    before_len_dict_cks = len(dict_kcs)
    
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
                
                if no_upload(f) : # 업로드 대상 아님################
                    if purpose == 'logTest' :
                        out.append([f, "업로드대상아님"])
                    else :
                        if not os.path.exists(path_noUp):
                            os.makedirs(path_noUp)
                        temp = re_name(join(path, f), join(path_noUp, f))#----------t
                        temp.append("업로드대상아님")
                        out.append(temp)
                    continue
            
                f_name_items = {"key" :"", "name" :"", "docu" :"", "event" :"", "sub_event" : "", "extra" :"", "date" :"", "pw" : ""}
                depth1, depth2, depth3, allName  = "", "", "", ""
                extra = os.path.splitext(f)[0] # extr = stem 여기서 하나씩 항목 제외시키므로 최종적으로 남는 건 말 그대로 extra
                ext = os.path.splitext(f)[1]
                ext = ext.lower()

                f_name_items["key"], extra = keyFnc(extra) #### rm_s() 같이 실행, key 할당####################
                if not f_name_items["key"] :
                    if purpose == 'logTest' :
                        nobasic.append([f, "nokey"])
                        continue
                    else :
                        temp = (re_name(join(path,f), join(path_nobasic, f)))#--------t
                        temp.append("nokey")
                        nobasic.append(temp)
                        continue
                
                try :
                    debtorName = dict_refer[f_name_items["key"]][2] 
                    grtName = dict_refer[f_name_items["key"]][3]
                except :
                    if purpose == 'logTest' :
                        nobasic.append([f, "dict_refer 키에러 for name"])
                        continue
                    else :
                        temp = re_name(join(path, f), join(path_nobasic, f))#----------t
                        temp.append("dict_refer 키에러 for name")
                        nobasic.append(temp)
                        continue

                f_name_items["pw"], extra = pwFnc(extra)  ###### pw 할당

                f_name_items["name"], extra = nameFnc(extra, debtorName, grtName) #### name 할당 ####################

                isEvent, docuEventExtra = eventFnc(extra) ### docu, event

                if isEvent == 1 : # 정상
                    f_name_items["docu"] = docuEventExtra[0] #사건번호>>docu 할당 #############
                    f_name_items["event"] = docuEventExtra[1]
                    extra = docuEventExtra[2]

                    # 사건번호 하나 더 있는지 확인
                    sub_isEvent, sub_docuEventExtra = eventFnc(extra)
                    if sub_isEvent == 1 :
                        f_name_items["sub_event"] = sub_docuEventExtra[1]
                        extra = sub_docuEventExtra[2]
            
                else : # 사건번호 없거나(0) docu 정의할 수 없는 경정사건(2)
                    extra = docuEventExtra[2]
                    CallSetDocu = False # setdocu 부른 경우, is애쳐
                    
                    if isEvent == 2 : #경정사건
                        f_name_items["event"] = docuEventExtra[1]

                        # 사건번호 하나 더 있는지 확인
                        sub_isEvent, sub_docuEventExtra = eventFnc(extra)
                        if sub_isEvent == 1 : # subEvent로 docu 정의 
                            f_name_items["docu"] = sub_docuEventExtra[0] #사건번호>>docu 할당 #############
                            f_name_items["sub_event"] = sub_docuEventExtra[1]
                            extra = sub_docuEventExtra[2]
                        else : # subEvent로도 docu 정의 불가할 때 setDocu 호출
                            isDocu, docuExtra = setDocu(extra)  #비사건번호>>docu 할당 #############
                            CallSetDocu = True
                    else : # 사건번호 없을 때 setDocu 호출
                        isDocu, docuExtra = setDocu(extra)  #비사건번호>>docu 할당 #############
                        CallSetDocu = True

                    # setDocu 호출했을 때만 실행하는 영역
                    if CallSetDocu : 
                        if isDocu :
                            f_name_items["docu"] = docuExtra[0]
                            extra = docuExtra[1]
                        else : # nodocu
                            if purpose == 'logTest' :
                                nobasic.append([f, "nodocu"])
                                continue
                            else :
                                temp = re_name(join(path, f), join(path_nobasic, f))#---------t
                                temp.append("nodocu")
                                nobasic.append(temp)
                                continue
                
                f_name_items["date"], extra = dateFnc(extra) ### date 할당

                extra = rmNeedless(extra)
                extra = rm_s(extra)
                f_name_items["extra"] = extra #### extra #################

                new_f = "_".join(filter(lambda x :bool(x), f_name_items.values())) + ext # 최종 파일 이름 #######################

            else :  # purpose == done
                new_f = f 
                pass

            depth1, depth2, depth3 = setDepth(new_f)########### depth ###########

            if depth1 == "" or depth1=="wrongdocu" or depth1 == "wrongkey" : # if-elif-else로 반복문 끝나므로 continue 안 해줘도 됨
                if purpose == 'logTest' :
                    nobasic.append([f, depth1])
                else :
                    temp = re_name(join(path, f), join(path_nobasic, new_f)) #----------t
                    temp.append(depth1)
                    nobasic.append(temp)
                    
                    

            elif depth1 == "out" :
                out_dir = join(path_out, depth2, depth3)
                if purpose == 'logTest' :
                    nobasic.append([new_f, out_dir])
                else :
                    if not os.path.exists(out_dir):
                        os.makedirs(out_dir)
                    temp = re_name(join(path, f), join(out_dir, new_f))#----------t
                    temp.append("out")
                    out.append(temp)

            else :
                dst_dir = os.path.join(path_server, depth1, depth2, depth3)
                if purpose == 'logTest' :
                    success.append([new_f, dst_dir])
                else : # 업로드 하는 곳

                    isDupl, kcs = isDuplFnc(path, f)  ############### 중복검사
                    if isDupl : # 중복인 경우
                        dst_dir = join(path_dupl, depth2, kcs)
                        if not os.path.exists(dst_dir):
                            os.makedirs(dst_dir) 
                        temp = re_name(join(path, f), join(dst_dir, new_f))
                        temp.append(kcs)
                        out.append(temp)
                    
                    else : # 중복이 아닌 경우 (isDuplFnc에서 dict에 추가는 했다.)
                        if not os.path.exists(dst_dir):
                            os.makedirs(dst_dir)  
                        success.append(re_name(join(path, f), join(dst_dir, new_f)))#----------t
                                                
                        # dict에 추가
                        dict_kcs[kcs] = join(dst_dir, new_f)

        except Exception as e:
            print("===================================")
            time = datetime.today().strftime("%H:%M:%S")
            print(time)
            print(f, extra, e.__class__, e, sep=" : ")
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
            print(traceback.format_exc())
            pass
        
        try :
            # pkl 수정
            after_len_dict_cks = len(dict_kcs)
            write_cks(path_kcs, dict_kcs)
        except Exception as e:
            print(e)
            print(traceback.format_exc())
            pass

        if total == len(success) + len(nobasic) + len(out) + len(fail) :
            print("처리된 파일에 누수 없음")
        else :
            print("처리된 파일에 누수 있음")

        print(f'전체:{total}개, 서버:{len(success)}개, 파일명에러:{len(nobasic)}개, 관리제외 및 중복:{len(out)}개, 예상치못한 예외:{len(fail)}개')
        print(f'dict_kcs 변화 : {after_len_dict_cks} - {before_len_dict_cks} = {after_len_dict_cks - before_len_dict_cks}')
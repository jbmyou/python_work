import os
import shutil
import time
import pandas as pd
from tqdm.notebook import tqdm      #tqdm(filelist, total = len(file_list), position=0, leave=True)
import re
from pathlib import Path
import traceback
from os.path import join
from datetime import datetime
from io import StringIO
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser

#########################################
# 파일 목록 불러오기
#########################################

def read_filelist(path) :
    # 업데이트시 dupl.py의 fileInfo()도 업데이트 해야
    p_extension = re.compile('(jpeg|jpg|bmp|gif|pdf|png|tif|tiff|m4a|wav|mp[34])$', re.I) 
    return [f.name for f in os.scandir(path) if f.is_file() and (p_extension.search(f.name))]


#########################################
# pdf파일 > str
#########################################

def read_pdf(file_name):
    """애초에 pdf로 만들어져야 읽어지는듯. tif를 pdf로 바꾸니 안 읽어진다."""
    output_string = StringIO()
    with open(file_name, 'rb') as f:
        parser = PDFParser(f)
        doc = PDFDocument(parser)
        rsrcmgr = PDFResourceManager()
        device = TextConverter(rsrcmgr, output_string, laparams=LAParams())
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        for page in PDFPage.create_pages(doc):
            interpreter.process_page(page)
    return output_string.getvalue()


#########################################
# pdf류 아닌 파일 모두 이동시키기
#########################################


def not_pdf(path: str, dst_root: str, s_index: int):  # 문자변환 여부 주의
    """
    os.walk
    path : 작업최상위 dir
    dst_root : 비스캔파일 모을 폴더
    s_index : path에서 제외할 글자수(c:/를 제외한다면 3)
    """
    # pdf류 확장자 리스트
    p_extension = re.compile('.jpeg|.jpg|.bmp|.gif|.pdf|.png|.tif|.xps', re.I)

    for root, __dir__, files in os.walk(path):

        for f in files:
            ext = os.path.splitext(f)[1]

            if p_extension.match(ext) == None:

                src = join(root, f)
                dst_dir = join(dst_root + root[s_index:])
                dst = join(dst_dir, f)

                if not os.path.exists(dst_dir):
                    os.makedirs(dst_dir)

                shutil.move(src, dst)


#########################################
# move / rename
#########################################
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
    stem = os.path.splitext(f_name)[0].strip()
    ext = os.path.splitext(f_name)[1]

    # date = re.sub("[\D][\d]{6}") 
    numbering = r'(_[(][\d]{1,2}[)]_?|_[\d]{1,2}|[\s]*[(][\d]{1,2}[)]_?|[\s]+[\d]{1,2}_?)$'
    temp = re.sub(numbering, "", stem)  # 모든 넘버링 및 _로 끝나는 경우 제거 _ , 괄호, 공백 뒤에 나오는 숫자 1~2자리
    new_name = temp + ext

    i = 1
    while os.path.exists(dir+"/"+new_name):  # 작업디렉토리가 아니므로 풀경로
        new_name = temp + "_"+"("+str(i)+")"+ext
        i += 1

    if not os.path.exists(dir):
        os.makedirs(dir)  # 미리 만들어뒀으니 mkdir해도 됨
    
    shutil.move(src, join(dir, new_name))

    

    return [os.path.split(src)[0],os.path.split(src)[1], new_name, dir]

def re_name_upload(src: str, dst: str) -> list:
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


#########################################
# 채무자조회.xlsx -> dict
#########################################
def debtorInfoDict():
    """
    채무자조회.xlsx파일을 읽어 채무자키를 key로 하고
    나머지컬럼을 value로 하는 dict반환
    value = dict["키"].컬럼명 // 따옴표의 불편함
    전체 1회 조회속도 df보다 0.1초 빠름..
    """
    path = r'C:\Users\SL\Desktop\채무자조회.xlsx'
    df_c = pd.read_excel(path, index_col=0)
    dict = {}
    for key, row in df_c.iterrows():
        dict[str(key)] = row[1:]
    # row는 시리즈. 채무자키는 dict의 키로 넣었으니 row[1:]을 value로 넣자.
    # 시리지는 .컬럼명으로 읽으면 되니까 최종적으로
    # dict["20495151"].성명 이렇게 읽으면 된다.

    # 2차원 딕셔너리는 row[1:] 대신 아래를 사용
    # {'매각사구분':row.매각사구분, '성명' : row.성명, \
        # '주민번호인':row.주민번호인, '관리자기타':str(row.관리자기타), '보증인성명':row.보증인성명}
    return dict


#########################################
# 생성일, 수정일 보기 + time을 보기 좋게
#########################################
def get_mtime(path):
    """
    (pdf)수정일=최초생성일!! 이거 써(이동/파일명 변경해도 안 바뀜)
    path : (walk)dir + file 또는 (listdir) file
    """
    a = time.ctime(os.path.getmtime(path))
    b = time.strptime(a)
    c = time.strftime('%Y.%m.%d - %H:%M:%S', b)
    return c


def get_ctime(path):
    """
    이동,복사일이니까 쓰지마(파일명 변경으론 바뀌지 않음)
    walk로 작업시 path는 dir+file
    listdir로 작업시 cwd설정 했다면 path는 file명만 와도 됨
    """
    a = time.ctime(os.path.getctime(path))
    b = time.strptime(a)
    c = time.strftime('%Y.%m.%d - %H:%M:%S', b)
    return c


#########################################
# 전체 파일 폴더트리 포함 이동
#########################################
def move_all(path: str, dst_root: str) -> None:
    """
    파일이 있는 하위 폴더를 그대로 복사(덮어쓰기, 깊은 복사)
    path : 작업 최상위 폴더 경로
    dst_root : 복사할 최상위 폴더
    """
    s_index = len(path) + 1
    # +1을 해주지 않으면 하위폴더의 경우 root[s_index:]가 /로 시작해버려서 c로 가버림
    for root, __dirs__, files in os.walk(path):
        for f in files:
            src = join(root, f)
            dst_dir = join(dst_root, root[s_index:])
            dst = join(dst_dir, f)

            if not os.path.exists(dst_dir):
                os.makedirs(dst_dir)
            shutil.move(src, dst)


#########################################
# copy
#########################################
def copy_all(path: str, dst_root: str) -> None:
    """
    파일이 있는 하위 폴더를 그대로 복사
    path : 작업 최상위 폴더 경로
    dst_root : 복사할 최상위 폴더
    """
    s_index = len(path) + \
        1  # +1을 해주지 않으면 하위폴더의 경우 root[s_index:]가 /로 시작해버려서 c로 가버림
    for root, __dirs__, files in os.walk(path):
        for f in files:
            src = join(root, f)
            dst_dir = join(dst_root, root[s_index:])
            dst = join(dst_dir, f)

            if not os.path.exists(dst_dir):
                os.makedirs(dst_dir)
            shutil.copy2(src, dst)


#########################################
# 파일명에서 특정 단어를 끝으로 이동, 위치 수정해서 재사용
#########################################
def move_word(word: str, filelist: list, dst: str = "root"):
    """
    모든 하위 파일에서 특정 단어의 위치를 옮겨 파일명 변경하고자 할 때
    filelist : os.listdir(walk) or os.walk(path) or list of path
    dst : dir. 안적으면 src의 root
    """
    _ = "_"
    index = 0
    result = []

    for root, __dirs__, files in filelist:
        for f in files:
            if re.search(word, f):  # word가 원래 끝에; 있었어도 _ 추가때문에 파일명은 항상 달라짐
                stem = os.path.splitext(f)[0]
                ext = os.path.splitext(f)[1]

                # re.sub("[^가-힣]+$", "", stem)  # 모든 넘버링 제거
                numbering = r'(_[(][\d]{1,2}[)]|_[\d]{1,2}|[\s]*[(][\d]{1,2}[)][\s]*|[\s]+[\d]{1,2}[\s]*)$'
                temp = re.sub(numbering, "", stem)  # 모든 넘버링 제거
                temp = re.sub(word, "", temp)  # word 제거
                new_name = temp + _ + word + ext  # word 재배치
                new_name = re.sub('_{2,}', '_', new_name)  # __ > _ 있어야됨

                f_s = root + "\\" + f

                if dst == "root":
                    dst = root

                f_d = dst + "\\" + new_name
                re_name(f_s, f_d)

                index += 1
                result.append([f, new_name])

    print(str(index)+"건의 파일 이름 수정 완료")
    print(*result, sep="\n")


#########################################
# 특정단어를 대체하여 파일명만 바꾸기
#########################################
def change_word(word: str, word_to_change: str, file_list: list) -> list:
    """
    파일리스트에서 특정 단어를 다른 단어로 대체
    """
    p1 = re.compile(word)
    count = 0
    list = []

    for root, __dirs__, files in tqdm(file_list):
        for f in files:
            new_name = f
            if p1.search(new_name):
                new_name = p1.sub(new_name, word_to_change)
            if new_name != f:
                list.append([f, new_name])
                re_name(join(root, f), join(root, new_name))
                count += 1

    print(count, "개 파일이름 변경")
    return list


#########################################
# 최종확인1 - 자기 폴더에서 공백과 언더바 점검후 넘버링도 새롭게(마지막 숫자가 +될수도 있음)
#########################################

def final_rename(path):
    """"
    자기 폴더에서 공백과 언더바 점검후 넘버링도 새롭게(마지막 숫자가 +될수도 있음)
    """
    f_d = path
    os.chdir(path)
    file_list = os.listdir(path)
    index = 0
    changed = []
    error = []
    p0 = re.compile(r'\s')
    p1 = re.compile('_{2,}')
    p2 = re.compile('복사본')
    numbering = r'(_[(][\d]{1,2}[)]|_[\d]{1,2}|[\s]*[(][\d]{1,2}[)][\s]*|[\s]+[\d]{1,2}[\s]*)$'
    p3 = re.compile(numbering)  # 모든 넘버링 제거

    docu_kind = '원인서류|양도통지서|판결문|지급명령|이행권고|화해권고|타채|결정문|등본|초본|등,초본|등초본|외국인|개회|신복|파산'
    etc_kind = '보증인|재도|1차|2차|3차|4차'
    p_key1 = re.compile("[0-9]{8}")
    p_key2 = re.compile("[0-9]{9}")
    p_docu = re.compile(docu_kind)
    p_etc = re.compile(etc_kind)

    try:
        for f in tqdm(file_list):

            if os.path.isfile(f) & (f != "Thumbs.db"):

                fullname = Path(Path.cwd() / f)
                temp = fullname.stem
                new_name = ""

                # 키 뒤에, 문서종류, 기타정보 앞에 _ 없는 경우 수정
                res_d = p_docu.search(f)
                res_e = p_etc.search(f)

                if (p_key1.match(temp) != None) & (p_key2.match(temp) == None):
                    if temp[8] != "_":  # key 뒤에 언더바 없는 경우
                        temp = temp[:8] + "_" + temp[8:]
                else:
                    error.append(f)

                if res_d != None:
                    if temp[res_d.start()-1] == "_":
                        pass
                    else:  # 문서종류 앞이 _가 아닌경우
                        temp = temp[:res_d.start()] + "_" + \
                            temp[res_d.start():]

                    # 키와 문서 종류 사이에 언더바가 여러개인경우
                    name_before = temp[9:res_d.start()-1]  # _namebefore_
                    name_after = re.sub("_", "", name_before)
                    if name_after != name_before:
                        temp = temp[:9] + name_after + temp[res_d.start()-1:]

                else:
                    error.append(f)

                if res_e != None:
                    if temp[res_e.start()-1] == "_":
                        pass
                    else:  # 기타키워드 앞이 _가 아닌경우
                        temp = temp[:res_e.start()] + "_" + \
                            temp[res_e.start():]

                # 공백
                temp = temp.strip()
                temp = p0.sub("_", temp)

                # 연속 언더바
                temp = p1.sub('_', temp)

                # 끝이 한글이 아니거나 '복사본'인 경우(즉 모든 넘버링 및 기호 지우기)
                temp = p2.sub("", temp)
                temp = p3.sub("", temp)

                new_name = temp + fullname.suffix  # 넘버링 제외된 파일명 + 확장자 붙여서 비교

                if new_name == f:  # 달라진게 없다면
                    pass
                else:

                    i = 1
                    while os.path.exists(f_d+"/"+new_name):  # 작업디렉토리가 아니므로 풀경로
                        new_name = temp + "_"+"("+str(i)+")"+fullname.suffix
                        i += 1

                    f_d_final = f_d + "/" + new_name
                    shutil.move(f, f_d_final)
                    changed.append([f, new_name])
                index += 1

            else:
                pass

    except:
        print(traceback.format_exc())
        print(index, "번째 파일까지 처리하고 에러")

    print(index, "개의 파일 이름 변경 완료")
    print("error : ", *error, sep="\n")
    print("파일명 변경 목록 : ", *changed, sep="\n")
    os.chdir('c:/')


#########################################
# 최종확인2 - 마지막으로 3개 양식과 언버가 개수 확인
#########################################

# 등,초본이 초본을 포함해버린다. 이거 수정해야

def final_check(path):
    """
    채무자키 8 자리 있는지
    문서종류 키워드 있는지
    언더바가 5개 이상인지
    """
    os.chdir(path)
    file_list = os.listdir(path)
    lista = []  # 메인 3개 항목 중 문제 발생
    listb = []  # 언더바 개수에 이상
    index = 0
    docu_kind = '원인서류|양도통지서|판결문|지급명령|이행권고|화해권고|타채|결정문|등본|초본|외국인증명|개회|신복|파산'  # 기타는 안 넣었음
    p0 = re.compile(
        r'[\d]{8}_[a-zA-Z가-힣,()]+_('+docu_kind+')')
    p_ = re.compile('_')

    # try:
    for f in tqdm(file_list):

        if os.path.isfile(f) & (f != "Thumbs.db"):

            fullname = Path(Path.cwd() / f)
            temp = fullname.stem

            cond1 = (p0.match(temp) == None)
            num_ = len(p_.findall(temp))
            # cond1을 통화했다면 num_ 은 반드시 2이상. 6덩이는 나올 수 있으나 수가 적을 것이므로 같이 보자.
            cond2 = num_ >= 5

            if cond1:
                lista.append(f)
                index += 1
            else:
                pass

            if cond2:
                listb.append(f)
                index += 1
            else:
                pass

    print(index, "개의 이상 탐색")
    print("메인3항목중 이상 발견", *lista, sep="\n")
    print("언더바 개수 이상 발견", *listb, sep="\n")
    os.chdir('c:/')


#########################################
# 파일 정보를 딕셔너리로, 무엇을 추가할지는 수정해서 쓰면 됨
#########################################
def fileInfoDict(path) -> dict:
    """
    파일 정보를 2차원 딕셔너리로, 무엇을 추가할지는 수정해서 쓰면 됨
    key의 타입은 모두 str임
    """
    filelist = os.walk(path)
    v2_dict = {}  # 중복파일명 숫자를 카운트 할 딕셔너리

    for root, __dirs__, files in filelist:

        for f in files:
            fullpath = join(root, f)
            split_list = f.split("_")

            key = str(split_list[0])
            size = str(os.path.getsize(fullpath))

            if key not in v2_dict:
                v2_dict[key] = {size: fullpath}
            else:
                v2_dict[key][size] = fullpath

    return v2_dict


def moveFilesToRootFolderAndRmDirs(path):
    """
    모든 하위 폴더의 파일들을 path로
    그리고 빈 폴더는 지워버리기
    """
    index = 0
    for root, __dirs__, files in os.walk(path):
        for f in files:
            src = join(root, f)
            dst = join(path, f)
            re_name(src, dst)
            index += 1

    print(index, "개의 파일 이동 완료")
    rmSubDirs(path)
    print("빈 폴더 삭제 완료")


def rmSubDirs(path: str):
    """모든 비어있는 폴더 삭제 os.walk"""
    for root, __dir__, __files__ in os.walk(path):
        try:
            os.rmdir(root)
        except:
            print(root)
            print(traceback.format_exc())
            print("")
            continue

def write_log_csv(log:list, path : str) -> None:
    "2차원이 최적이고 그 이상은 셀안에 리스트로"
    if not os.path.exists(path):
        os.makedirs(path)  

    if len(log) > 0 :
    
        import csv
        
        name = str(datetime.today().strftime("%Y%m%d %H%M%S")) + "_" +str(len(log)) + ".csv" 
                    
        with open (join(path, name), "a", newline="", encoding='utf-8-sig') as p :
            for row in log :
                wr = csv.writer(p)
                wr.writerow(row)
    else :
        print(f'log 파일이 비어있습니다. path : {path}')

def write_log_csv_upload(log:list, path : str, addInfo : str = "") -> None: #################################
    "2차원이 최적이고 그 이상은 셀안에 리스트로"
    if not os.path.exists(path):
        os.makedirs(path)  

    if len(log) > 0 :
    
        import csv
        
        name = str(datetime.today().strftime("%Y%m%d %H%M%S")) + "_" +str(len(log)) + "_" + addInfo +".csv" ################################
                    
        with open (join(path, name), "a", newline="", encoding='utf-8-sig') as p :
            for row in log :
                wr = csv.writer(p)
                wr.writerow(row)
    else :
        print(f'log 파일이 비어있습니다. path : {path}')


def twoDimensionListToTxt(log, path) :
    """
    (2 x any)리스트를 받아 txt파일로 만들어주는 함수
    파일명은 날짜, index와 현재 시간을 본문에 추가
    """
    day = str(datetime.today().strftime("%Y%m%d"))
    time = str(datetime.today().strftime("%H:%M"))

    if len(log)>0 :
        #log_path = join(path,"success")
        name = day +".txt"
        index = 1

        with open(join(path,name), "a") as lf :
            for row in log :
                lf.write(str(index))
                lf.write('\t')
                lf.write(time)
                lf.write('\t')
                for col in row :
                    
                    lf.write(str(col))
                    lf.write('\t')
                lf.write('\n')
                index += 1





# 불요 ######################################################################################

# #########################################
# # 채무자조회.xlsx -> df, index = 채무자키
# #########################################
# def debtorInfoDF():
#     """
#     채무자조회.xlsx파일을 df로
#     value = df.looc[12345678].보증인성명 // .loc의 불편함
#     """
#     path = r'C:\Users\SL\Desktop\채무자조회.xlsx'
#     df_c = pd.read_excel(path, index_col=0)
#     return df_c


# #########################################
# # 모든 파일의 정보를 추출하여 csv로 저장, df 반환. 중복파일명을 기준으로 하고 있는데 파일 사이즈를 기준으로 하는 것으로 수정해야 할듯
# #########################################
# def all_files(path, local):
#     os.chdir(path)

#     z = "/"
#     f_name_dict = {}  # 중복파일명 숫자를 카운트 할 딕셔너리
#     f_dir = []
#     f_name = []
#     name_count = []
#     f_size = []
#     create_time = []
#     modify_time = []
#     extension = []

#     for root, __dirs__, files in tqdm(os.walk(path)):

#         for f in files:
#             path = root + z + f

#             if f not in f_name:
#                 f_name_dict[f] = 1
#             else:
#                 f_name_dict[f] = f_name_dict[f]+1

#             f_dir.append(root)
#             f_name.append(f)
#             name_count.append(f_name_dict[f])
#             f_size.append(os.path.getsize(path))
#             create_time.append(get_ctime(path))
#             modify_time.append(get_mtime(path))
#             extension.append(os.path.splitext(f)[1])

#     # 매각사 칼럼 추가하기
#     import re
#     df_matching = pd.read_excel(
#         r'D:\전산\workspace\python_work\파일\매각사 이름매칭.xlsx')
#     sell = []

#     for i in f_dir:
#         for __index__, row in df_matching.iterrows():
#             if re.search(row[0], i):
#                 sell.append(row[1])
#                 continue

#     df = pd.DataFrame({'경로': f_dir, '매각사': sell, '파일명': f_name, '중복수': name_count,
#                       '크기': f_size, '생성일': create_time, '수정일': modify_time, '확장자': extension})
#     df.to_csv(r'C:\Users\SL\Desktop/'+local +
#               ' 모든 파일 정보.csv', encoding='utf-8-sig')

#     return df


# #########################################
# # 사이즈 같은 파일들만 딕셔너리로
# #########################################
# def same_size(path: str) -> dict:
#     """
#     사이즈 같은 파일 dict로
#     """
#     dict_size = {}

#     for root, __dir__, files in os.walk(path):
#         for f in files:
#             size = os.path.getsize(f)
#             fullname = join(root, f)
#             if size not in dict_size:
#                 dict_size[size] = [fullname]
#             else:
#                 dict_size[size].append(fullname)

#     for key, value in dict_size.items():
#         if len(value) == 1:
#             del dict_size[key]
#         else:  # 중복사이즈인데 수정일이 다른 경우 있는지
#             dict_mtime = {}
#             for fullname in value:
#                 mtime = os.path.getmtime(fullname)
#                 if mtime not in dict_mtime:
#                     dict_mtime[mtime] = [fullname]
#                 else:
#                     dict_mtime[mtime].append(fullname)

#     return dict_size


# #########################################
# # 다른 폴더로 이동
# #########################################
# def move(path, f_d):
#     os.chdir(path)
#     file_list = os.listdir(path)
#     index = 0
#     p1 = re.compile(r'[^가-힣]+$')

#     for f in tqdm(file_list):
#         if os.path.isfile(f) & (f != "Thumbs.db"):
#             stem = os.path.splitext(f)[0]
#             ext = os.path.splitext(f)[1]

#             temp = p1.sub("", stem)
#             new_name = temp + ext  # 넘버링 제외된 파일명 + 확장자 붙여서 비교

#             i = 1
#             while os.path.exists(f_d+"/"+new_name):  # 작업디렉토리가 아니므로 풀경로
#                 new_name = temp + "_"+"("+str(i)+")"+ext
#                 i += 1

#             f_d_final = f_d + "/" + new_name
#             shutil.move(f, f_d_final)
#             index += 1

#     print(index, "개 파일 이동 완료")
#     os.chdir('c:/')


#########################################
# 파일명 구분기호별 분류하기( _, space...)
#########################################
# def filename_split(path) :
#   all = os.listdir(path)
#   files = [Path(path +'/'+x).stem for x in all if os.path.isfile(path +'/'+x)]
#           ########################확장자 포함하려면 이걸 그냥 x로
#   #files = [Path(path +'/'+x).stem for x in all if os.path.isfile(path +'/'+x)]
#   f_split_list = [f.split('_') for f in files]
#   return f_split_list

# %%
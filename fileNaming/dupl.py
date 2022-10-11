import os
import shutil
import pandas as pd
import re
import binascii # 내장모듈
import pickle # 내장모듈
from os.path import join
from tqdm import tqdm
#import traceback
from upload_v2 import write_log_csv

def fileNameScore(filename:str) -> int :
    """파일명에서 특정 항목이 있는지 추리는 함수
    채무자키 있음 : 10만
    대체키 있음 : 1만
    사건번호 있음 : 1000
    문서구분 있음 : 100
    길이 : stem 길이
    """
    score = 0
    f = filename
    # compile
    
    # 채무자키 match
    p_key = re.compile(r"[\d]{8}(?!\d)") 
    # 사업자/주민번호/관리자키
    p_extraKey = re.compile(r'([\d]{3})-\d\d-\d\d\d\d\d[\D]+|([\d]{6})-([\d]{6})[\D]+|[\d]{10}[\D]|[\D][\d]{4}-[\d]{6}[\D]')
    # 사건번호
    p_event1=re.compile(r"((?<=\D)|^)(19\d\d|20[012]\d)\s?(준?재?[가나느차카타즈본징하개회][가-힣]?)\s?([0-9]+)")
    p_event2=re.compile(r"((?<=\D)|^)([012]\d)\s?(준?재?[가나느차카타즈본징하개회][가-힣]?)\s?([0-9]+)")
    # 문서구분
    p_docu = re.compile(r'원인|양도통지서|양통|종적|승계|집행|판결|명령|이행|화해|재도|재부여|압류|압추|추심|유체|동산|배당|타채|결정|(등|초)본|외국인|개회|신복|파산|내용|신용|등기|부채|재산|대장')
    
    if p_key.match(f) : score += 100000 # 날짜같은것과 구분짓기 위해 match필수
    else :
        if p_extraKey.search(f) : score += 10000 # 채무자키 없을 때만, 채무자키 있으면 쓸모없는 정보이므로 점수 x
    if p_event1.search(f) or p_event2.search(f) : score += 1000
    if p_docu.search(f) : score += 100

    real_f = f
    real_f = p_key.sub("", real_f)
    real_f = p_extraKey.sub("", real_f)
    real_f = p_event1.sub("", real_f)
    real_f = p_event2.sub("", real_f)
    real_f = p_docu.sub("", real_f)
    score += len(os.path.splitext(real_f)[0]) # score에 선반영 된 것들 제외한 파일명 길이 점수

    return score

def crc32_checksum(filename):
    buf = open(filename,'rb').read()
    buf = (binascii.crc32(buf) & 0xFFFFFFFF)
    return "%08X" % buf

def file_info(path:str,savePath = "/volume1/스캔파일/스캔파일log/_project/파일/중복조사"):
    """
    하위경로포함 모든 파일에 대해 size,cre32로 중복 검사 후 
    모든 파일 정보 및 중복파일 목록을 excel, pickle로 파일폴더에 저장
    path : 중복검사할 최상위 디렉토리
    savePath : 검사 결과 파일을 저장할 디렉토리
    """
    savePath = savePath
    dict_sc = {}  #중복파일끼리 묶을 딕셔너리(size+cre)
    df = None
    data = []
    p_extension = re.compile('jpeg|jpg|bmp|gif|pdf|png|tif|tiff|m4a|wav|mp[34]|xps$', re.I)
    df_matching = pd.read_excel('./파일/매각사 이름매칭.xlsx')

    for root, __dirs__, files in tqdm(os.walk(path)):
        for f in files:
            if (p_extension.search(f)!=None) and (re.match("[~$]", f) == None) :
                fullPath = join(root, f)
                #key
                size = str(os.path.getsize(fullPath))
                #value
                mtime = str(os.path.getmtime(fullPath))
                crc32 = str(crc32_checksum(fullPath))
                sell = "매각사" # 검색결과가 없을 수 있으니
                for __index__, row in df_matching.iterrows():
                    if re.search(str(row[0]), str(root)):  
                        sell = row[1]
                        break            
                stem = os.path.splitext(f)[0]
                ext = os.path.splitext(f)[1]
                
                score = fileNameScore(f) ##
                sc = size + crc32

                temp = {"sc":sc, "score" : score, "root" : root, "stem":stem, "ext":ext, "fullPath" : fullPath, "size":size, "crc32": crc32, "mtime":mtime, "sell":sell}
                data.append(temp)
                
                # 고유한 size, crc를 키로 하는 2중 딕셔너리 만들기
                if sc not in dict_sc:
                    dict_sc[sc] = [temp]
                else:
                    dict_sc[sc].append(temp)
            
            
    # df = pd.DataFrame(data)
    # sc_dupl = df.duplicated(['size', 'crc32'], keep=False) # 중복파일은 모두 마크하기(series)
    # sc_dupl.name = "sc_dupl" # 칼럼이름
    # df = pd.concat([df, sc_dupl], axis=1) # 새로운 열로 결합하기
    #df_sc_dupl = df[df["sc_dupl"]] # dupl인 것만 새로운 df에 담기 return용
    
    #add_dir = path.split("\\")[-1]
    # if not os.path.exists(join(savePath,add_dir)):
    #     os.mkdir(join(savePath,add_dir))
    
    # 파일 내보내기1 : path내 모든 파일(만일을 위해)
    if not os.path.exists(savePath) : 
        os.makedirs(savePath)

    # df.to_excel(join(savePath, "전체 파일정보.xlsx"))
    # df.to_pickle(join(savePath,"전체 파일정보.pkl"))
    # total = len(df.index) 
    
    # 파일 내보내기2 : s+c끼리 묶은 2중 dict
    with open(join(savePath,"sc별 파일정보.pkl"), "wb") as pkl :
        pickle.dump(dict_sc, pkl)

    return

def sort_dupl(pathOfDict:str, nameOfDict:str) :

    with open(join(pathOfDict, nameOfDict), "rb") as pkl :
        dict = pickle.load(pkl)
    
    total = len(dict)
    
    for __key__, ListOfSubDict in tqdm(dict.items(), total=total) : # 파일 그룹 단위 반복
    #"score" : score, "root" : root, "stem":stem, "ext":ext, "fullPath" : fullPath, "size":size, "crc32": crc32, "mtime":mtime, "sell":sell
        num = len(ListOfSubDict)
        if num == 1 :
            pass
        else :
            for i in range(num):
                for j in range((i+1), num) :
                    if ListOfSubDict[i]["score"] < ListOfSubDict[j]["score"] : # 바뀌는 경우
                        ListOfSubDict[i], ListOfSubDict[j] = ListOfSubDict[j], ListOfSubDict[i] # 딕셔너리 내부에서도 작동함.

    with open(join(pathOfDict,("정렬된 "+nameOfDict)), "wb") as pkl :
        pickle.dump(dict, pkl)

def readDictOnebyOne(pathOfDict:str, nameOfDict:str) :
    with open(join(pathOfDict, nameOfDict), "rb") as pkl :
        dict = pickle.load(pkl)
    total = len(dict)
    print(f"전체 그룹 수는 {total}")

    for key, list in dict.items() :
        print(key)
        templist = []
        for x in list : 
            templist.append(x["score"])
        print("sore  : ", *templist, sep="  ")
        print("마지막 파일 pathOfDict", x["fullPath"])
        print("더 읽을까 말까? y or n")
        go = input()
        if go == "y" :
            pass
        else :
            break

def changeDictForNas(pathOfDict, nameOfDict) :
    with open(join(pathOfDict, nameOfDict), "rb") as pkl :
        dict = pickle.load(pkl)
    total = len(dict)

    for __key__, list in tqdm(dict.items(), total=total) :
        for x in list : 
            x["root"] = re.sub(r"D:\\", "/volume1/스캔파일/", x["root"])
            x["root"] = re.sub(r"\\", "/", x["root"])
            x["fullPath"] = re.sub(r"D:\\", "/volume1/스캔파일/", x["fullPath"])
            x["fullPath"] = re.sub(r"\\", "/", x["fullPath"])

    with open(join(pathOfDict,("nas용 "+nameOfDict)), "wb") as pkl :
        pickle.dump(dict, pkl)

def moveDuplFile(pathOfDict, nameOfDict, basePathToMove) : 
    with open(join(pathOfDict, nameOfDict), "rb") as pkl :
        dict = pickle.load(pkl)
    total = len(dict)
    print(f"{total}개의 그룹에 대해 중복 파일 이동을 실시합니다.")
    dupl_fail = []
    duplCnt = 0
    
    for __key__, list in tqdm(dict.items(), total=total) :
        num = len(list)
        if num > 1 : # 중복파일
            for i in range(1, num) :
                try :
                    src = list[i]["fullPath"]
                    #dir = os.path.split(src)[0]
                    file = os.path.split(src)[1]

                    dst_dir = join(basePathToMove, file) # 대표파일이름으로 폴더 만들어 중복파일을 넣는다
                    dst = join(dst_dir, file)

                    if not os.path.exists(dst_dir) :
                        os.makedirs(dst_dir)
                    shutil.move(src, dst)   
                    duplCnt += 1

                except Exception as e :
                    dupl_fail.append([src, dst, e.__class__, e])
                    continue  # 코드 계속 진행

    print(f"중복파일 이동 : {duplCnt}건,  이동실패 : {len(dupl_fail)}건")
    write_log_csv(dupl_fail, pathOfDict)
    return

def moveKeyFile(pathOfDict, nameOfDict, basePathToMove, path_gu) :
    with open(join(pathOfDict, nameOfDict), "rb") as pkl :
        dict = pickle.load(pkl)
    total = len(dict)
    gu = len(path_gu) + 1
    print(f"{total}개의 그룹에 대해 key가 있는 파일 이동을 실시합니다.")
    noKeyList = []
    move_fail = []
    keyCnt, extraKeyCnt = 0, 0

    for __key__, list in tqdm(dict.items(), total=total) :
        try :
            # 1) 점수가 가장 높고, key 있는 파일 채무자키폴더로 이동
            # root - path -1(디렉토리 구분자 제거) = subdir
            if list[0]["score"] >= 100000 :
                src = list[0]["fullPath"]
                #dir = os.path.split(src)[0]
                file = os.path.split(src)[1]

                dst_dir = join(basePathToMove, file) # 대표파일이름으로 폴더 만들어 중복파일을 넣는다
                dst = join(dst_dir, file)

                dst_dir = join(basePathToMove, "key", list[0]["root"][gu:])
                dst = join(dst_dir, file)

                if not os.path.exists(dst_dir) :
                    os.makedirs(dst_dir)
                shutil.move(src, dst)
                keyCnt += 1

            # 2) 점수가 가장 높지만, extraKey 있는 파일 extraKey폴더로 이동
            elif list[0]["score"] >= 10000 :
                src = list[0]["fullPath"]
                #dir = os.path.split(src)[0]
                file = os.path.split(src)[1]

                dst_dir = join(basePathToMove, "extraKey", list[0]["root"][gu:])
                dst = join(dst_dir, file)

                if not os.path.exists(dst_dir) :
                    os.makedirs(dst_dir)
                shutil.move(src, dst)
                extraKeyCnt += 1

            # 점수가 가장 높지만, key가 전혀 없는 것 root, stem, ext, fullPath(\\192...으로 변경해야 하니까 원형 유지를 위해 root, fileName도 저장한다.)
            # 를 df에 담아 for문 끝난 후 엑셀로 저장
            # fullPath 하이퍼 링크 걸어주고, 편하게 파일 보면서 newName 저장
            else :
                noKeyList.append([list[0]["root"], list[0]["stem"], list[0]["ext"], list[0]["fullPath"]])
        
        except Exception as e :
            move_fail.append([src, dst, e.__class__, e])
            continue  # 코드 계속 진행
    
    print(f"채무자키 : {keyCnt}건,  기타키 : {extraKeyCnt}건,  이동실패 : {len(move_fail)}건")
    write_log_csv(move_fail, pathOfDict)

    # 제 자리에 두는 파일 목록 엑셀로 변환
    df_noKey = pd.DataFrame(noKeyList, columns=["root", "stem", "ext", "fullPath"])
    df_noKey.to_excel(join(pathOfDict, "remain.xlsx"))
    return



if __name__ == "__main__" :

    #####################################################
    pathOfDict = "/volume1/스캔파일/스캔파일log/_project/파일/중복조사"
    nameOfDict = "nas용 정렬된 sc별 파일정보.pkl"
    basePathToMoveDupl = '/volume1/삭제예정파일/중복_스캔파일'
    basePathToMoveKey = '/volume1/스캔파일'
    path_gu = '/volume1/스캔파일/구 스캔파일'
    #####################################################
    
    moveDuplFile(pathOfDict, nameOfDict, basePathToMoveDupl)

    # moveKeyFile(pathOfDict, nameOfDict, basePathToMoveKey, path_gu)

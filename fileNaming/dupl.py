import os
import shutil
import pandas as pd
import re
import binascii # 내장모듈
import pickle # 내장모듈
from os.path import join
from tqdm import tqdm
import traceback

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
    
    real_f = f
    if p_key.match(f) : 
        score += 100000 # 날짜같은것과 구분짓기 위해 match필수
        real_f = p_key.sub("", real_f)
    else :
        if p_extraKey.search(f) :  
            score += 10000 # 채무자키 없을 때만, 채무자키 있으면 쓸모없는 정보이므로 점수 x
            real_f = p_extraKey.sub("", real_f)
    if p_event1.search(f) or p_event2.search(f) : 
        score += 1000
        real_f = p_event1.sub("", real_f)
        real_f = p_event2.sub("", real_f)
    if p_docu.search(f) : 
        score += 100
        real_f = p_docu.sub("", real_f)

    score += len(os.path.splitext(real_f)[0]) # score에 선반영 된 것들 제외한 파일명 길이 점수

    return score

def crc32_checksum(filename):
    buf = open(filename,'rb').read()
    buf = (binascii.crc32(buf) & 0xFFFFFFFF)
    return "%08X" % buf

def file_info(path:str,savePath = "./파일/중복조사"):
    """
    하위경로포함 모든 파일에 대해 size,cre32로 중복 검사 후 
    모든 파일 정보 및 중복파일 목록을 excel, pickle로 파일폴더에 저장
    path : 중복검사할 최상위 디렉토리
    savePath : 검사 결과 파일을 저장할 디렉토리
    """
    dict_sc = {}  #중복파일끼리 묶을 딕셔너리(size+cre)
    # df = None
    # data = []
    p_extension = re.compile('jpeg|jpg|bmp|gif|pdf|png|tif|tiff|m4a|wav|mp[34]|xps$', re.I)

    for root, __dirs__, files in tqdm(os.walk(path)):
        
        for f in files:
            if (p_extension.search(f)!=None) and (re.match("[~$]", f) == None) :

                fullPath = join(root, f)
                #key
                size = str(os.path.getsize(fullPath))
                #value
                mtime = str(os.path.getmtime(fullPath))
                crc32 = str(crc32_checksum(fullPath))
                stem = os.path.splitext(f)[0]
                ext = os.path.splitext(f)[1]
                
                score = fileNameScore(f) ##
                sc = size + crc32

                temp = {"sc":sc, "score" : score, "root" : root, "stem":stem, "ext":ext, "fullPath" : fullPath, "size":size, "crc32": crc32, "mtime":mtime}
                # data.append(temp)
                
                # 고유한 size, crc를 키로 하는 2중 딕셔너리 만들기
                if sc not in dict_sc:
                    dict_sc[sc] = [temp]
                else:
                    dict_sc[sc].append(temp)
            
            
    # df = pd.DataFrame(data)
    # sc_dupl = df.duplicated(['size', 'crc32'], keep=False) # 중복파일은 모두 마크하기(series)
    # sc_dupl.name = "sc_dupl" # 칼럼이름
    # df = pd.concat([df, sc_dupl], axis=1) # 새로운 열로 결합하기
    # df_sc_dupl = df[df["sc_dupl"]] # dupl인 것만 새로운 df에 담기 return용
    
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

    return dict_sc

if __name__ == "__main__" :
    #####################################################
    path = r'D:\구 스캔파일' 
    path_base = r'D:\\구 스캔파일_key'
    path_dupl = r'D:\\구 스캔파일_dupl'
    total = 2527559
    #####################################################
    savePath = r"./파일/중복조사"
    dict = file_info(path)

    # with open("./파일/중복조사/sc별 파일정보.pkl", "rb") as pkl :
    #     dict = pickle.load(pkl)
    print(f"파일 그룹 개수(중복되지 않은 파일 개수) : {len(dict)}") # 파일 그룹 개수

    noKeyList = []
    keyCnt, extraKeyCnt, duplCnt = 0, 0, 0

    print("파일이동을 시작합니다.")
    for key, ListOfSubDict in tqdm(dict.items()) : # 파일 그룹 단위 반복
    #"score" : score, "root" : root, "stem":stem, "ext":ext, "fullPath" : fullPath, "size":size, "crc32": crc32, "mtime":mtime, "sell":sell
        try :
            num = len(ListOfSubDict)
            if num == 1 :
                pass
            else :
                for i in range(num):
                    highscore = i
                    for j in range((i+1), num) :
                        if ListOfSubDict[i]["score"] < ListOfSubDict[j]["score"] : # 바뀌는 경우
                            ListOfSubDict[i], ListOfSubDict[j] = ListOfSubDict[j], ListOfSubDict[i] # 딕셔너리 내부에서도 작동함.

                # 점수가 가장 높지 않은 것(num > 2) 중복파일 폴더로 이동. 파일이름 같은 거 삭제.
                for k in range(1, num) : # range(1,1) 이라도 오류는 안 남
                    try : 
                        src = ListOfSubDict[k]["fullPath"]
                        dst_dir = join(path_dupl, ListOfSubDict[0]["stem"]) # 대표파일 폴더 아래에 중복파일을 넣기 때문에 [0][stem]
                        dst = join(dst_dir, os.path.split(src)[1])

                        if not os.path.exists(dst_dir) :
                            os.makedirs(dst_dir)
                        shutil.move(src, dst)   
                        duplCnt += 1
                    except Exception as e :
                        print("===================================")
                        print(e.__class__, e, sep=" : ")
                        print(traceback.format_exc())
                        continue  # 코드 계속 진행
                    
            # 1. 하나의 파일 그룹에 대한 정렬이 끝났고, 중복파일은 다 이동했다. 여기서부턴 대표파일(subDict[0])을 key 유무에 따라 분류
            
            # 1.1) 점수가 가장 높고, key 있는 파일 채무자키폴더로 이동
            # root - path -1(디렉토리 구분자 제거) = subdir
            if ListOfSubDict[0]["score"] >= 100000 :
                src = ListOfSubDict[0]["fullPath"]
                dst_dir = join(path_base, "key", ListOfSubDict[0]["root"][len(path)+1:])
                dst = join(dst_dir, os.path.split(src)[1])

                if not os.path.exists(dst_dir) :
                    os.makedirs(dst_dir)
                shutil.move(src, dst)
                keyCnt += 1

            # 1.2) 점수가 가장 높지만, extraKey 있는 파일 extraKey폴더로 이동
            elif ListOfSubDict[0]["score"] >= 10000 :
                src = ListOfSubDict[0]["fullPath"]
                dst_dir = join(path_base, "extraKey", ListOfSubDict[0]["root"][len(path)+1:])
                dst = join(dst_dir, os.path.split(src)[1])

                if not os.path.exists(dst_dir) :
                    os.makedirs(dst_dir)
                shutil.move(src, dst)
                extraKeyCnt += 1

            # 점수가 가장 높지만, key가 전혀 없는 것 root, stem, ext, fullPath(\\192...으로 변경해야 하니까 원형 유지를 위해 root, fileName도 저장한다.)
            # 를 df에 담아 for문 끝난 후 엑셀로 저장
            # fullPath 하이퍼 링크 걸어주고, 편하게 파일 보면서 newName 저장
            else :
                noKeyList.append([ListOfSubDict[0]["root"], ListOfSubDict[0]["stem"], ListOfSubDict[0]["ext"], ListOfSubDict[0]["fullPath"]])
        
        except Exception as e :
            print("===================================")
            print(e.__class__, e, sep=" : ")
            print(traceback.format_exc())
            continue  # 코드 계속 진행

    # 제 자리에 두는 파일 목록 엑셀로 변환
        df_noKey = pd.DataFrame(noKeyList, columns=["root", "stem", "ext", "fullPath"])
    try : 
        df_noKey.to_excel(join(savePath, "remain.xlsx"))
    except : pass
    df_noKey.to_pickle(join(savePath, "remain.pkl")) # 엑셀 행 많아서 오류 날 수 있으니 

    actCnt = keyCnt + extraKeyCnt + len(noKeyList) + duplCnt
    print(f"total : {total}개.  key : {keyCnt}, extraKey : {extraKeyCnt}, noKeyRemain : {len(noKeyList)}, dupl : {duplCnt}") 
    print(f"누수 파일 {total - actCnt}개")    
    
    



import os
import shutil
import time
import pandas as pd
from tqdm import tqdm      #tqdm(filelist, total = len(file_list), position=0, leave=True)
import re
from pathlib import Path
import traceback
from os.path import join
import file_function as ff
from datetime import datetime


start = time.time()########################################

src_path = r'\\192.168.0.75\sollim광주\test1\original'
dst_path = r'\\192.168.0.75\삭제예정파일\test2'
log_path = r'\\192.168.0.75\sollim광주\test1'

result = []
error = []
cnt = 0
file_list =[f.name for f in os.scandir(src_path) if f.is_file() and (f.name != 'Thumbs.db')]

p_day4 = re.compile(r'\s?\(?\d{0,2}(?P<y>\d{2})[.-/\s](?P<m>\d{1})[.-/\s](?P<d>\d{1})(?!\d)\)?')
p_day5 = re.compile(r'\s?\(?\d{0,2}(?P<y>\d{2})[.-/\s](?P<m>\d{1})[.-/\s](?P<d>\d{2})\)?')
p_day6 = re.compile(r'\s?\(?\d{0,2}(?P<y>\d{2})[.-/\s](?P<m>\d{2})[.-/\s](?P<d>\d{2})\)?')


for f in tqdm(file_list, total=len(file_list)) : 
    try :
        stem = os.path.split(f)[0]
        ext = os.path.split(f)[1]
        n = stem
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
        
        result.append(ff.re_name(join(src_path, f), join(dst_path, (n+ext))))       
        cnt += 1

    except Exception as e:
        print("===================================")
        print(datetime.today().strftime("%H:%M:%S"))
        print(f, e.__class__, e, sep=" : ")
        print(traceback.format_exc())
        error.append([f, e.__class__, e])
        continue # 반복문 계속 돌아


ff.write_log_csv(result, log_path)
end = time.time()########################################

print(f'{cnt}개 파일 이동에 {end-start}초 소요')


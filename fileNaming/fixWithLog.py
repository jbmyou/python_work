import os, sys
from tqdm import tqdm      
import re
import pandas as pd
from os.path import join
import upload_v2 as v2

def log_list_test(path) :
    """return df \n
    .csv로그파일로 테스트를 하기 위함. 폴더내 모든 csv를 읽음 \n
    col = src_dir, src_file(file_list), dst_file, dst_dir \n
    """
    logs = [f.name for f in os.scandir(path) if all([f.is_file(), re.search("^[^~$].*csv$", f.name)])]
    col = ['src_dir', "src_file", "dst_file", "dst_dir"]
    df_c = pd.DataFrame(None, columns=col)
    log_list = []
    acc_index = 0
    for log in logs :
        df_temp = pd.read_csv(join(path,log), sep = ",", names=col)
        acc_index += len(df_temp.index)
        log_list.append([log, acc_index])
        df_c = pd.concat([df_c, df_temp], axis = 0, ignore_index=True)

    return df_c, log_list



#################################
pathOfLogsToRead = r"\\192.168.0.75\스캔파일\스캔파일log\success\미확인"
pathoFLogToWrite = r"\\192.168.0.75\스캔파일\스캔파일log\success"

# pathOfLogsToRead = "/volume1/스캔파일/스캔파일log/success"
# pathoFLogToWrite = "/volume1/스캔파일/스캔파일log/success/"

#################################


if __name__ == "__main__" :
############################
    #다음 중 실행목적을 입력하시오. fix / search
    purpose = sys.argv[1]      
    if len(sys.argv) != 2 or re.match(r"fix|search", sys.argv[1])==None:     
        print("'fix', 'search' 중에서 목적을 입력하세요")
        sys.exit()

    df, log_list = log_list_test(pathOfLogsToRead)
    total = len(df.index)
    result = []

    if purpose == "fix" :
        ######################
        # dst_dir = "/volume1/스캔파일/새 스캔파일(업로드)/검수완료"
        ######################
        for i in tqdm(range(total)) :
            try :
                src_dir = df.iloc[i,3]
                f = df.iloc[i,2]

                n = os.path.splitext(f)[0]
                ext = os.path.splitext(f)[1]
                ########### logic ###############

                n = re.sub("등본", "주민등록정보", n)
                
                #################################
                new_f = n+ext

                if f != new_f :                                     #######
                    result.append(v2.re_name(join(src_dir, f), join(src_dir, new_f)))
            except Exception as e:                                  #######
                print("===================================")
                print(f, n, e.__class__, e, sep=" : ")
                pass

        v2.write_log_csv(result, pathoFLogToWrite)

    else :
        ########### logic ###############
        
        search_col = "dst_file"
        search_str = "20421982_양현주_등본_부.pdf"

        #################################
        a = df.where(df[search_col] == search_str).dropna()
        b = a.index[0]
        print(a)
        for l, i in log_list :
            if b < i :
                print(f"검색된 파일 : {l}")
                break

        




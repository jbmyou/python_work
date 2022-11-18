import os
from os.path import join
import re
import xlwings as xw
from tqdm import tqdm

def file_listFnc(path) :
    # 업데이트시 dupl.py의 fileInfo()도 업데이트 해야
    p_extension = re.compile('xls|xlsx$', re.I) 
    return [f.name for f in os.scandir(path) if f.is_file() and (p_extension.search(f.name))]

if __name__ == "__main__" :
    print("경로를 입력하세요(엑셀파일이 있는 폴더를 열어 주소창의 경로를 복사 붙여넣기 하세요")
    path = input()
    print("===================================")
    print("패스워드를 입력하세요")
    pw = input()
        
    file_list = file_listFnc(path)
    total = len(file_list)
    save_dir_path = join(path, "lock")
    complete_dir_path = join(path, "완료")
    if not os.path.exists(save_dir_path) :
        os.mkdir(save_dir_path)
    if not os.path.exists(complete_dir_path) :
        os.mkdir(complete_dir_path)

    app = xw.App(visible=False)
    for f in tqdm(file_list, total=total) : 
        try :
            load_file_path = join(path, f)
            book = xw.Book(load_file_path)
            book.api.SaveAs(join(save_dir_path, f), Password = pw)
            os.rename(load_file_path, join(complete_dir_path, f))
        except Exception as e :
            print(f)
            print(e)
            pass
    app.kill()
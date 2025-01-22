import os
import win32print
import win32api
import time
from sys import exit

def print_pdfs_in_folder(folder_path, delay_time):
    # 폴더에서 PDF 파일만 가져오기
    pdf_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.pdf')]

    # 파일명 기준으로 정렬
    pdf_files.sort()

    # 프린터 정보 가져오기
    printer_name = win32print.GetDefaultPrinter()

    print(f"사용 중인 기본 프린터: {printer_name}")

    for pdf_file in pdf_files:
        pdf_path = os.path.join(folder_path, pdf_file)

        # PDF 파일 인쇄
        try:
            result = win32api.ShellExecute(0, "print", pdf_path, None, folder_path, 0)
            if result > 32:  # 성공적인 요청 코드 확인
                print(f"{pdf_file} 인쇄 요청 성공")
                time.sleep(delay_time)  # 인쇄 요청 간 대기 시간 추가
            else:
                print(f"{pdf_file} 인쇄 요청 실패: 코드 {result}")
        except Exception as e:
            print(f"{pdf_file} 인쇄 실패: {e}")

if __name__ == "__main__":
    folder_path = input("PDF 파일이 있는 폴더 경로를 입력하세요: ").strip()
    try:
        delay_time = int(input("파일 간 대기 시간을 초 단위로 입력하세요(6초 이상) : ").strip())
    except ValueError:
        print("대기 시간은 숫자로 입력해야 합니다.")
        exit(1)

    if os.path.exists(folder_path) and os.path.isdir(folder_path):
        print_pdfs_in_folder(folder_path, delay_time)
        print('폴더 내의 pdf파일 출력이 완료되었습니다.')
    else:
        print("유효한 폴더 경로를 입력해주세요.")
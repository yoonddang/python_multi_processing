import numpy as np
import csv

# 파일 읽기, 탭으로 구분, 라인배열로 리턴 받음
data = open('melon0925.csv', encoding='utf-8')
reader = csv.reader(data, delimiter='\t')
lines = list(reader)

# 곡명을 변수로 받으면 좋을듯
# 특정 곡만 찾아서 csv 저장
songName = '조금 취했어 (Prod. 2soo)'
print(songName)

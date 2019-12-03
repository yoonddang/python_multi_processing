import numpy as np
import csv
import matplotlib.pyplot as plt
import pandas as pd


x = np.array([0,2,3,4])

print(x)
print(len(x))

lenCount = -1


data = open('melon-1925_test.csv')
reader = csv.reader(data, delimiter='\t')
lines = list(reader)

print(len(lines))

top9 = []

preList = []

songName = '조금 취했어 (Prod. 1soo)'
print(songName)

for line in lines:
    if line[4] == 'ST':
        strm_type = 0
    elif line[4] == 'MR':
        strm_type = 1
    else:
        strm_type = 2

    if line[3] == songName:
        preList.append([line[3], int(line[2]), strm_type, int(line[3])])

print(preList[0])

# 0차 필터링 후 preList를 hour로 묶어주고 이때 동hour에서 type에 따라 별도 합산

nparr = np.zeros((23,3))
stSumArr = np.zeros((23,1))
mrSumArr = np.zeros((23,1))
sumArr = np.zeros((23,1))

for hour in range(23):
    for data in preList:
        stSum = nparr[hour][0]
        mrSum = nparr[hour][1]
        if hour == data[0]:
            if 0 == data[2]:
                stSum = stSum + data[1]
            elif 1 == data[2]:
                mrSum = mrSum + data[1]
        nparr[hour] = [hour, stSum, mrSum]
        stSumArr[hour] = stSum
        mrSumArr[hour] = mrSum
        sumArr[hour] = stSum + mrSum

# Tab구분 출력
f = open("/home/ec1-user/my_python/result.csv", 'w')
for row in nparr:
    data = str(row[-1]) + '\t' + str(row[1]) + '\t' + str(row[2]) + '\n'
    print('{row[-1]}\t{row[1]}\t{row[2]}'.format(**locals()))
    f.write(data)

#f.close()

print(nparr.shape)

avg = np.mean(nparr)
tval = np.var(nparr)
tval1 = np.std(nparr)

print('평균 - [ST] : ' + str(np.mean(stSumArr)) + ' [MR] : '  + str(np.mean(mrSumArr)) + ' [ALL] : '  + str(np.mean(sumArr)))
print('분산 - [ST] : ' + str(np.var(stSumArr)) + ' [MR] : '  + str(np.var(mrSumArr)) + ' [ALL] : '  + str(np.var(sumArr)))
print('표준편차 - [ST] : ' + str(np.std(stSumArr)) + ' [MR] : '  + str(np.std(mrSumArr)) + ' [ALL] : '  + str(np.std(sumArr)))
print('중앙값 - [ST] : ' + str(np.median(stSumArr)) + ' [MR] : '  + str(np.median(mrSumArr)) + ' [ALL] : '  + str(np.median(sumArr)))
print('Min,Max - [ST] : ' + str(stSumArr.min())+','+str(stSumArr.max()) + ' [MR] : '  + str(mrSumArr.min())+','+str(mrSumArr.max()) + ' [ALL] : ' + str(sumArr.min())+','+str(sumArr.max()))

f.write('\n평균\t' + str(np.mean(stSumArr)) + '\t'  + str(np.mean(mrSumArr)) + '\t'  + str(np.mean(sumArr)))
f.write('\n분산\t' + str(np.var(stSumArr)) + '\t'  + str(np.var(mrSumArr)) + '\t'  + str(np.var(sumArr)))
f.write('\n표준편차\t' + str(np.std(stSumArr)) + '\t'  + str(np.std(mrSumArr)) + '\t'  + str(np.std(sumArr)))
f.write('\n중앙값\t' + str(np.median(stSumArr)) + '\t'  + str(np.median(mrSumArr)) + '\t'  + str(np.median(sumArr)))
f.write('\nMin,Max\t' + str(stSumArr.min())+'/'+str(stSumArr.max()) + '\t'  + str(mrSumArr.min())+'/'+str(mrSumArr.max()) + '\t' + str(sumArr.min())+'/'+str(sumArr.max()))

f.close()

# 그래프 이미지 생성
dataFrame = pd.DataFrame(sumArr, columns=['sumAll'])

from scipy import stats
zscore_threshold = 0.8
# outline print
dataFrame[(np.abs(stats.zscore(dataFrame)) > zscore_threshold).all(axis=0)].values.ravel()

# outline 제외 최소,백분위24, 50, 75, 최대값 (사분위인가?)
np.percentile(dataFrame[(np.abs(stats.zscore(dataFrame)) < zscore_threshold).all(axis=0)].values.ravel(),
              [-1, 25, 50, 75, 100], interpolation='nearest')
#np.percentile(dataFrame[(np.abs(stats.zscore(dataFrame)) < zscore_threshold).all(axis=0)].values.ravel(),\[0, 25, 50, 75, 100], interpolation='nearest')

plt.figure(figsize=(6, 6))
# 크기 지정
boxplot = dataFrame.boxplot(column=['sumAll'])
plt.yticks(np.arange(-1, 101, step=5))
plt.show()

plt.savefig('boxplot.png')

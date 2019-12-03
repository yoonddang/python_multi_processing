import csv
import errno
import os
import multiprocessing as mp
from functools import partial


def regex_check_file_name(file_name):
    return str(file_name)


def get_data_list_for_csv(file_name):
    csv_file = open(file_name, encoding='utf-8')
    file_reader = csv.reader(csv_file, delimiter='\t')
    data_list = list(file_reader)
    return data_list


def make_dir(directory_name):
    try:
        if not (os.path.isdir(directory_name)):
            os.makedirs(os.path.join(directory_name))
    except OSError as e:
        if e.errno != errno.EEXIST:
            print("Failed to create directory!!!!!")
            raise


def write_csv_del_tab(file_name, data_list):
    if len(data_list) > 0:
        try:
            with open(file_name + '.csv', 'w', encoding='utf-8', newline='') as csvfile:
                writer = csv.writer(csvfile, delimiter='\t')
                for data in data_list:
                    try:
                        writer.writerow(data)
                    except Exception as detail:
                        print(type(detail))
                        print(detail)

            csvfile.close()
        except Exception as e:
            print(file_name)


def find_specific_song_data(song_name, row_data):
    song_data = []
    for row in row_data:
        # 스트리밍 타입 구분
        if row[5] == 'ST':
            strm_type = 0
        elif row[5] == 'MR':
            strm_type = 1
        else:
            strm_type = 2
        # 필요한 데이터 저장
        if row[4] == song_name:
            # songName, userIdx, strmCount, strmHour, strmDate, strmType, purchaseType, regDate
            song_data.append([row[4], row[6], int(row[3]), int(row[2]), row[1], strm_type, row[0], int(row[3])])
    return song_data


def get_strm_type_code(str_type):
    # 스트리밍 타입 구분
    if str_type == 'ST':
        return 0
    elif str_type == 'MR':
        return 1
    else:
        return 2


def make_song_vo(row):
    return [row[4], row[6], int(row[3]), int(row[2]), row[1], get_strm_type_code(row[5]), row[0], row[7]]


def filterd_song_name_by_row_data(row_data, song_dic):
    #song_list = {}
    for row in row_data:
        # songName, userIdx, strmCount, strmHour, strmDate, strmType, purchaseType, regDate
        #   4           6       3           2       1           5           0           7
        song_name = row[4]
        song = make_song_vo(row)
        if song_name in song_dic:
            song_dic[song_name].append(song)
        else:
            song_dic[song_name] = [song]
    return song_dic


# songName, userIdx, strmCount, strmHour, strmDate, strmType, purchaseType, regDate, maxStrmCount, maxStrmCountHour, minStrmCount, minStrmCountHour
#   0           1       2           3       4           5           6           7           8               9           10              11

def make_user_data(user_log):
    user = user_log[0]
    user[2] = 0
    user[3] = 0
    user.append(0)
    user.append('')
    user.append(float('inf'))
    user.append('')

    for log in user_log:
        strm_count = log[2]
        strm_hour = log[3]
        max_count = user[8]
        min_count = user[10]
        if max_count < strm_count:
            user[8] = strm_count
            user[9] = strm_hour
        if min_count > strm_count:
            user[10] = strm_count
            user[11] = strm_hour
        user[2] = user[2] + log[2]

    return user


def parral_check_user_exists(data, userList):
    func = partial(make_user_data, data)
    pool = mp.Pool(processes=4)
    result = pool.map(func, userList)
    pool.close()
    pool.join()
    print(result)


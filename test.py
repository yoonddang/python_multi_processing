import htMultiProcessing as htmp
import classifyUser as cu
import os
import time


def filtered_by_song_name(data_list, song_dic):
    start_time = time.time()
    cu.filterd_song_name_by_row_data(data_list, song_dic)
    print('----%s time ---' % (time.time() - start_time), 'data_list : ', len(data_list), 'song_dic : ', len(song_dic))


# songName, userIdx, strmCount, strmHour, strmDate, strmType, purchaseType, regDat
#     0         1       2           3          4        5           6          7
def filtered_by_day(data_list, daily_dic):
    for data in data_list:
        # strmCount 형변환
        data[2] = int(data[2])
        # 날짜별 분류
        if data[4] in daily_dic:
            day = daily_dic[data[4]]
            # 날짜별 리스트에서 유저 검색
            if data[1] in day:
                bef_user_data = day[data[1]]
                day[data[1]] = cu.make_user_data([bef_user_data, data])
            else:
                # 존재 안할경우 해당 유저 값 생성
                day[data[1]] = data
        else:
            # 날짜별 리스트 없을 경우 최초 생성
            daily_dic[str(data[4])] = {data[1]: data}
    return daily_dic


def test(file_name):
    # 데이터 읽기
    row_data = cu.get_data_list_for_csv(file_name + '.csv')
    print('[READ] row_data', len(row_data))

    # 곡명으로 데이터 분할
    # 입력 데이터 분할 리스트 생성
    slice_arr = htmp.chunk_divider(row_data, 1000)
    # 멀티프로세싱 초기화 및 실행
    htmp.init()
    htmp.do_processing(filtered_by_song_name, slice_arr)
    htmp.stop_processing()
    song_dic = htmp.ht_mp.get('result')
    print('result : ', len(song_dic))

    htmp.init()
    for song_name in song_dic.keys():
        start_time = time.time()
        # 입력 데이터 분할 리스트 생성
        slice_arr = htmp.chunk_divider(song_dic[song_name], 1000)
        # 멀티프로세싱 초기화 및 실행
        htmp.do_processing(filtered_by_day, slice_arr)
        daily_dic = htmp.ht_mp.get('result')
        # print('result : ', len(daily_dic))

        user_dic_len = 0
        for day in daily_dic.keys():
            user_dic_len = user_dic_len + len(daily_dic[day])
            now_path = os.path.join(os.getcwd(), 'parsed', day + '_' + song_name)
            cu.write_csv_del_tab(now_path, daily_dic[day])

        print('----%s time ---' % (time.time() - start_time), 'song_name : ', song_name
              , 'user_log_list : ', len(song_dic[song_name]), 'user_dic_len : ', user_dic_len)

    htmp.stop_processing()


if __name__ == "__main__":
    test('melon0925')

import classifyUser as cu
import os
import time
import htMultiProcessing as htmp


def group_by_user(song_data):
    daily_list = {}
    for data in song_data:
        data[2] = int(data[2])
        if data[4] in daily_list:
            day = daily_list[data[4]]
            if data[1] in day:
                day[data[1]].append(data)
            else:
                day[data[1]] = [data]
        else:
            daily_list[str(data[4])] = {data[1]: [data]}
    return daily_list


song_dic = {}


def work_filter_song(arg_arr, job_id):
    print('job', job_id, len(arg_arr[0]), song_dic)
    cu.filterd_song_name_by_row_data(arg_arr[0], song_dic)
    print(song_dic)


def work_filter_user(arg_arr, job_id):
    filter_user_by_song_data(arg_arr[0], arg_arr[1])


def filter_song_name(file_name):
    row_data = cu.get_data_list_for_csv(file_name + '.csv')
    print('row_data', len(row_data))

    slice_job_map([row_data, song_dic], 10, 6, work_filter_song)

    print(len(song_dic))
    # 곡별로 분류한 로그를 사용자별로 재분류


#    for song_name in song_dic:
#        slice_job_map([song_name, song_dic[song_name]], 10, 6, work_filter_user)


def filter_user_by_song_data(song_name, song_data):
    start_time = time.time()

    daily_list = group_by_user(song_data)
    for day in daily_list:
        day_user_list = []
        for user_log in daily_list[day].values():
            # user_log -> user 객체 하나로 grouping
            day_user_list.append(cu.make_user_data(user_log))
            now_path = os.path.join(os.getcwd(), 'parsed', day + '_' + song_name)
            cu.write_csv_del_tab(now_path, day_user_list)

    print("----%s time ---" % (time.time() - start_time))




if __name__ == "__main__":
    filter_song_name('melon0925_test')

# song_name = '조금 취했어 (Prod. 2soo)'
# print(song_name)
# song_data = cu.get_data_list_for_csv(song_name + '.csv')


def test():
    # 테스트 데이터 생성
    num_arr = []
    for num in range(1000):
        node = []
        for copy in range(7):
            node.append(str(num))

        num_arr.append(node)

    # 입력 데이터 분할 리스트 생성
    slice_arr = chunk_divider(num_arr, 10)
    # 멀티프로세싱 초기화 및 실행
    init()
    result = do_processing(test_func, slice_arr)
    print(result)

    print(ht_mp.get('shared_dic'))


if __name__ == "__main__":
    test()



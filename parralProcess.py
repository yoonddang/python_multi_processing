import classifyUser as cu
import os
import time
from functools import partial
import multiprocessing as mp


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


def slicer(l, n):
    print('slicer', len(l), n)
    if n == 0:
        n = 1
        print('slicer is 0', len(l), n)
    return [l[i:i + n] for i in range(0, len(l), n)]


def slice_job_map(data, job_num, pool_size, do_job):
    total = len(data[0])
    slice_len = int(total / job_num)
    print('total : ', total, 'job_num : ', job_num, 'slice_len : ', slice_len)
    slice = slicer(data[0], slice_len)
    slice_arr = []

    for i, s in enumerate(slice):
        slice_arr.append(s)
        now_pool_size = i % pool_size
        print(i, now_pool_size, range(i - now_pool_size, i))
        if now_pool_size == 0 and i > 0:
            func = partial(do_job, slice_arr)
            p = mp.Pool(pool_size)
            p.map_async(func, range(i - pool_size, i)).get()
            p.close()
            p.join()
            slice_arr = []
        elif i == len(slice) - 1:
            func = partial(do_job, slice_arr)
            p = mp.Pool(now_pool_size)
            p.map_async(func, range(i - now_pool_size, i)).get()
            p.close()
            p.join()
            slice_arr = []


if __name__ == "__main__":
    filter_song_name('melon0925_test')

# song_name = '조금 취했어 (Prod. 2soo)'
# print(song_name)
# song_data = cu.get_data_list_for_csv(song_name + '.csv')

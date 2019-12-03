import multiprocessing as mp
import os
from functools import partial


def find_num(input_num, target_arr):
    # print(target_arr)
    # print("{} == {}".format(get_num, input_num))
    if target_arr == str(input_num):
        print(target_arr)
        return target_arr
    return False


def parral_check_user_exists(input_num, target_arr):
    f_num = 0
    print(input_num)
    func = partial(find_num, input_num)
    pool = mp.Pool()
    f_num = pool.map(func, target_arr)
    pool.close()
    pool.join()
    return f_num


# if __name__ == '__main__':
#    f = parral_check_user_exists(333, num_arr)
#    print(f)

# parral_check_user_exists(900, num_arr)
# parral_check_user_exists(3333, num_arr)

def slicer(l, n):
    print('slicer', len(l), n)
    if n == 0:
        n = 1
        print('slicer is 0', len(l), n)
    return [l[i:i + n] for i in range(0, len(l), n)]


def do_job(data_slice, job_id):
    print('job', job_id, len(data_slice[0]))
    for slice in data_slice:
        print('slice', max(slice), min(slice))
        for item in slice:
            if item == 999999:
                print("job", job_id, item)


#        print("job", job_id, item)


def last_func(data_list, arg_dic):
    #print(arg_dic)
    for data in data_list:
        try:
            data_dic[data[0][0]] = data
        except Exception as e:
            print(e)
            return


def work_filter_song(arg_arr, data_dic):
    # 필요한 값은 job_id, data_list 이다
    # static_val - partial, func 시에 전달
    # arg_arr[] - arg_arr[job_id][data_list] 형태로 pool 시에 전달
    # data_dic - pool.manager 를 통해 전달
    print('work job', arg_arr, data_dic)
    #last_func(arg_list, data_dic)


def slice_job(data, job_num, job):
    total = len(data)
    slice_len = int(total / job_num)
    slice = slicer(data, slice_len)
    jobs = []

    for i, s in enumerate(slice):
        j = mp.Process(target=job, args=(i, s))
        jobs.append(j)
    for j in jobs:
        j.start()


def doMultiProcessing()


def slice_job_map(arg_arr, job_num, pool_size, do_job):
    total = len(arg_arr[0])
    slice_len = int(total / job_num)
    print('total : ', total, 'job_num : ', job_num, 'slice_len : ', slice_len)
    slice = slicer(arg_arr[0], slice_len)
    slice_arr = []

    for i, s in enumerate(slice):
        slice_arr.append(s)
        now_pool_size = (i + 1) % pool_size
        print(i, now_pool_size, range((i+1) - pool_size, (i+1)))
        if now_pool_size == 0 and i > 0:
            print(len(slice_arr), min(slice_arr[0]), max(slice_arr[3]))


            p = mp.Pool(pool_size)
            p.map_async(func, slice_arr).get()
            p.close()
            p.join()
            slice_arr = []
        elif i == len(slice) - 1:
            func = partial(do_job, arg_arr, range((i+1) - now_pool_size, (i+1)))
            p = mp.Pool(now_pool_size)
            p.map_async(func, slice_arr).get()
            p.close()
            p.join()
            slice_arr = []


if __name__ == "__main__":

    # 테스트 데이터 생성
    num_arr = []
    for num in range(1000):
        node = []
        for copy in range(7):
            node.append(str(num))

        num_arr.append(node)

    slice_job_map([num_arr, data_dic], 10, processors, work_filter_song)

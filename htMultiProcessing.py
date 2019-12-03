import multiprocessing as mp
import os
from functools import partial
from itertools import repeat

ht_mp = {}


def init(num_core=None):
    if num_core is None or num_core == 0:
        num_core = mp.cpu_count()

    ht_mp['pool'] = mp.Pool(num_core)
    ht_mp['manager'] = mp.Manager()


def set_shared_dictionary(shared_dic):
    manager = ht_mp.get('manager')
    d = manager.dict()
    d = shared_dic
    ht_mp['shared_dic'] = d


def do_processing(func, input_list):
    pool = ht_mp.get('pool')
    manager = ht_mp.get('manager')
    shared_dic = ht_mp.get('shared_dic')

    print(input_list)
    print(shared_dic)

    if shared_dic is None or shared_dic == 0:
        print('shared_dic exists')
        pool.starmap(func, zip(input_list, repeat(shared_dic)))
    else:
        print('shared_dic not exists')
        func_p = partial(func, list(range(0, 4)))
        pool.map_async(func_p, input_list).get()

    pool.close()
    result = pool.join
    return result


def test_func(input_shared_dic, input_list):
    print('test_func', len(input_shared_dic), len(input_list), min(input_list)[0], max(input_list)[0])
    last_func(input_shared_dic, input_list)


def last_func(input_shared_dic, input_list):
    print(input_shared_dic)
    for data in input_list:
        try:
            print(data, data[0])
            input_shared_dic[data[0]+'th'] = data
        except Exception as e:
            print(e, data[0])
            return


def slicer(l, n):
    print('slicer', len(l), n)
    if n == 0:
        n = 1
        print('slicer is 0', len(l), n)
    return [l[i:i + n] for i in range(0, len(l), n)]


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


def test():

    # 테스트 데이터 생성
    num_arr = []
    for num in range(1000):
        node = []
        for copy in range(7):
            node.append(str(num))

        num_arr.append(node)

    total = len(num_arr)
    slice_len = int(total / 10)
    print('total : ', total, 'job_num : ', 10, 'slice_len : ', slice_len)
    slice = slicer(num_arr, slice_len)


    init()
    set_shared_dictionary({})
    # result = do_processing(test_func, list(range(0, 10)))
    result = do_processing(test_func, slice)
    print(result)


if __name__ == "__main__":
    test()
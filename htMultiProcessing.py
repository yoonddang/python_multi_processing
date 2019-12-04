import multiprocessing as mp
from functools import partial
from itertools import repeat

ht_mp = {}


def init(num_core=None):
    if num_core is None or num_core == 0:
        num_core = mp.cpu_count()

    print('num_core : ', num_core, num_core)
    ht_mp['pool'] = mp.Pool(num_core)
    ht_mp['manager'] = mp.Manager()


def set_shared_dic(shared_dic):
    ht_mp['shared_dic'] = shared_dic


# 프로세서 할당, 공유 딕셔너리 설정
# func - 수행 될 작업 함수, input_list - 분할된 데이터 리스트
def do_processing(func, input_list):
    pool = ht_mp.get('pool')
    manager = ht_mp.get('manager')

    # 공유 딕셔너리 설정
    if 'shared_dic' not in ht_mp:
        shared_dic = manager.dict()
    else:
        shared_dic = ht_mp.get('shared_dic')

    print('input_list', len(input_list), 'shared_dic', shared_dic, pool, manager)
    pool.starmap(func, zip(input_list, repeat(shared_dic)))
    # pool.close()
    # pool.join()
    ht_mp['result'] = shared_dic


def stop_processing():
    pool = ht_mp.get('pool')
    pool.close()
    pool.join()


def slicer(l, n):
    # print('slicer', len(l), n)
    if n == 0:
        n = 1
        # print('slicer is 0', len(l), n)
    return [l[i:i + n] for i in range(0, len(l), n)]


def chunk_divider(data_list, chunk_count):
    total = len(data_list)
    slice_len = int(total / chunk_count)
    print('total : ', total, 'chunk_count : ', chunk_count, 'chunk_len : ', slice_len)
    return slicer(data_list, slice_len)

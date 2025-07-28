import pandas as pd
from joblib import Parallel, delayed


def divide_TOCC_optimized(df: pd.DataFrame) -> list:
    """
    使用扫描线算法优化 TOCC (Tightly Overlapped Call-chain-sets) 的划分。
    时间复杂度: O(N log N)，其中 N 是 CICP 的数量。

    :param df: 一个包含所有CICP事件的DataFrame。
               必须包含 'ts_begin', 'ts_end', 和 'tid' 列。
    :return: 一个列表，其中每个元素都是一个代表TOCC的DataFrame。
    """
    if df.empty:
        return []

    # 1. 创建事件点列表
    #    将每个 [ts_begin, ts_end] 区间拆分为一个“开始”事件和一个“结束”事件。
    #    事件类型: 1 代表 START, -1 代表 END。
    #    itertuples() 比 iterrows() 快得多。
    events = []
    for cicp in df.itertuples():
        # (时间戳, 事件类型,  CICP完整信息)
        events.append((cicp.ts_begin, 1, cicp))
        events.append((cicp.ts_end, -1, cicp))

    # 2. 按时间对所有事件点进行排序
    events.sort()

    tocc_list = []
    active_threads = set()
    current_tocc_cicps = []

    # 3. 扫描时间线
    for i, (ts, event_type, cicp) in enumerate(events):
        # 遇到 START 事件
        if event_type == 1:
            # 如果这是第一个活跃的线程，说明一个新的潜在TOCC窗口开始了。
            if not active_threads:
                # 清空上一轮的CICP列表
                current_tocc_cicps = []

            active_threads.add(cicp.tid)
            current_tocc_cicps.append(cicp)

        # 遇到 END 事件
        else:  # event_type == -1
            # 必须先尝试移除，因为可能一个线程有多个CICP在本TOCC内
            try:
                active_threads.remove(cicp.tid)
            except KeyError:
                # 如果一个线程的多个CICP重叠，其tid可能已经被移除了，是正常现象
                pass

            # 如果移除后，活跃线程集合变空，意味着一个完整的、独立的重叠窗口刚刚结束。
            if not active_threads:
                # 检查这个窗口内是否有超过一个CICP事件，才算真正的"竞争"
                if len(current_tocc_cicps) > 1:
                    # 使用Index从原始DataFrame中高效地提取行，构建TOCC
                    tocc_indices = [c.Index for c in current_tocc_cicps]
                    tocc_df = df.loc[tocc_indices].copy()

                    # 避免重复添加同一个TOCC
                    tocc_list.append(tocc_df)

                # 重置，为下一个窗口做准备
                current_tocc_cicps = []

    # 按TOCC中的线程数排序，线程数多的更有可能是瓶颈
    tocc_list.sort(key=len, reverse=True)

    return tocc_list


def _process_single_thread_cics(thread_df: pd.DataFrame) -> pd.DataFrame:
    """处理单个线程的DataFrame，识别CICP"""
    if thread_df.empty:
        return pd.DataFrame()

    compare_key = thread_df["user_defined_indentical_call_stacks"]
    thread_df["token"] = (compare_key != compare_key.shift()).cumsum()

    # 聚合，找出CICP
    processed_df = thread_df.groupby(["token"], as_index=False).aggregate(
        lambda x: list(x)
    )

    # 清理和格式化结果
    processed_df["duration_length"] = processed_df[
        "user_defined_indentical_call_stacks"
    ].apply(lambda x: len(x))
    processed_df["tid"] = processed_df["tid"].apply(lambda x: x[0])
    processed_df["command"] = processed_df["command"].apply(lambda x: x[0])
    processed_df["event"] = processed_df["event"].apply(lambda x: x[0])
    processed_df["ts_begin"] = processed_df["timestamp"].apply(lambda x: x[0])
    processed_df["ts_end"] = processed_df["timestamp"].apply(lambda x: x[-1])
    processed_df["user_defined_indentical_call_stacks"] = processed_df[
        "user_defined_indentical_call_stacks"
    ].apply(lambda x: x[0])
    processed_df["function_call_stack"] = processed_df["function_call_stack"].apply(
        lambda x: x[0]
    )
    processed_df["call_stack"] = processed_df["call_stack"].apply(lambda x: x[0])

    return processed_df


def identify_consecutive_identical_call_stacks_parallel(
    perf_records_df: pd.DataFrame,
    degrees_of_freedom: int,
    direction: int,
    n_jobs: int = -1,
) -> pd.DataFrame:
    """
    并行化版本的CICP识别函数。

    :param n_jobs: 使用的CPU核心数，-1代表使用所有核心。
    """
    # 准备工作
    column_name = "call_stack"
    perf_records_df["user_defined_indentical_call_stacks"] = perf_records_df[
        column_name
    ].apply(
        lambda x: get_user_defined_indentical_call_stacks(
            x, degrees_of_freedom, direction
        )
    )
    perf_records_df["top_function"] = perf_records_df[
        "user_defined_indentical_call_stacks"
    ].apply(lambda x: x.split(";")[-1])

    # 将DataFrame按线程ID拆分成一个列表
    thread_groups = [group for name, group in perf_records_df.groupby("tid")]

    # 使用joblib并行处理每个线程的DataFrame
    # delayed()将函数调用包装成一个可并行执行的任务
    results_list = Parallel(n_jobs=n_jobs)(
        delayed(_process_single_thread_cics)(group) for group in thread_groups
    )

    # 将并行处理的结果合并成一个最终的DataFrame
    records_df = pd.concat(results_list)
    return records_df


def identify_consecutive_identical_call_stacks(
    perf_records_df: pd.DataFrame, degrees_of_freedom: int, direction: int
) -> pd.DataFrame:
    """

    :param perf_records_df: timing call stacks
    :param degrees_of_freedom: The number of user-defined call stack layers. For the number of recognized layers of
                               the call stack per thread, -1 means that the entire call stack is recognized.
    :param direction: bottom up 0 / top down 1。
    :return:
    """

    column_name = "call_stack"
    perf_records_df["user_defined_indentical_call_stacks"] = perf_records_df[
        column_name
    ].apply(
        lambda x: get_user_defined_indentical_call_stacks(
            x, degrees_of_freedom, direction
        )
    )
    perf_records_df["top_function"] = perf_records_df[
        "user_defined_indentical_call_stacks"
    ].apply(lambda x: x.split(";")[-1])

    threads_list = perf_records_df.tid.unique()
    records_df_list = []
    for thread in threads_list:
        thread_df = perf_records_df[perf_records_df.tid == thread]
        # Message: sometimes we use " compare_key = tmp_thread_df.loc[:, ('user_defined_indentical_call_stacks')] "
        compare_key = thread_df["user_defined_indentical_call_stacks"]
        thread_df.loc[:, ("token")] = (compare_key != compare_key.shift()).cumsum()
        thread_df = thread_df.groupby(["token"], as_index=False).aggregate(
            lambda x: list(x)
        )
        thread_df["duration_length"] = thread_df[
            "user_defined_indentical_call_stacks"
        ].apply(lambda x: len(x))
        thread_df["tid"] = thread_df["tid"].apply(lambda x: x[0])
        thread_df["command"] = thread_df["command"].apply(lambda x: x[0])
        thread_df["event"] = thread_df["event"].apply(lambda x: x[0])
        thread_df["ts_begin"] = thread_df["timestamp"].apply(lambda x: x[0])
        thread_df["ts_end"] = thread_df["timestamp"].apply(lambda x: x[-1])
        thread_df["user_defined_indentical_call_stacks"] = thread_df[
            "user_defined_indentical_call_stacks"
        ].apply(lambda x: x[0])
        thread_df["function_call_stack"] = thread_df["function_call_stack"].apply(
            lambda x: x[0]
        )
        thread_df["call_stack"] = thread_df["call_stack"].apply(lambda x: x[0])
        records_df_list.append(thread_df)
    records_df = pd.concat(records_df_list)
    return records_df


def get_user_defined_indentical_call_stacks(
    function_list: list, degrees_of_freedom: int, direction: int
) -> str:
    """

    :param function_list: a column of dataframs
    :param degrees_of_freedom:
    :param direction: bottom up 0 / top down 1
    :return: string of the function call chain
    """
    original_length = len(function_list)
    length = min(degrees_of_freedom, original_length)

    # degrees_of_freedom -1 means max
    if length == -1:
        length = len(function_list)

    # direction 0 mean bottom up
    if direction == 0:
        function_list = function_list[0:length]
    else:
        function_list = function_list[(original_length - length) : length]
    res_function_call = ";".join([" ".join(j.split()[1:]) for j in function_list])
    return res_function_call


def filtering_operation(df: pd.DataFrame) -> pd.DataFrame:
    """Filter out data from threads where the consecutive identical call stacks do not occur consistently.
        Support presetting of thread IDs/process names that do not care.

    :param df:
    :return:
    """
    threshold_duration_length = 1
    df = df[df.duration_length > threshold_duration_length]
    useless_command_list = ["swapper"]
    for command in useless_command_list:
        df = df[df.command.str.contains(command) == False]
    # df = df.sort_values(by=['duration_length', 'ts_begin'], ascending=[False, True])
    return df


def divide_TOCC(df: pd.DataFrame) -> list:
    """

    :param df:
    :return:
    """
    length = len(df)
    df = df.sort_values(by=["ts_begin"])
    ts_begin_dic = {}
    tocc_list = []
    for i in range(length - 1):
        current_ts_begin = df.iloc[i]["ts_begin"]
        current_ts_end = df.iloc[i]["ts_end"]
        if str(current_ts_begin) in ts_begin_dic.keys():
            continue
        else:
            ts_begin_dic[str(current_ts_begin)] = 1
            ts_begin_bound = current_ts_begin
            ts_end_bound = current_ts_end
            tocc = []
            tocc.append(ts_begin_bound)
            for j in range(i + 1, length):
                tmp_ts_begin = df.iloc[j]["ts_begin"]
                tmp_ts_end = df.iloc[j]["ts_end"]
                if str(tmp_ts_begin) in ts_begin_dic.keys():
                    continue
                else:
                    if (ts_begin_bound > tmp_ts_end) or (ts_end_bound < tmp_ts_begin):
                        continue
                    else:
                        ts_begin_dic[str(tmp_ts_begin)] = 1
                        tocc.append(tmp_ts_begin)
                        if ts_begin_bound > tmp_ts_begin:
                            ts_begin_bound = tmp_ts_begin
                        if ts_end_bound < tmp_ts_end:
                            ts_end_bound = tmp_ts_end
            if len(tocc) > 1:
                tocc_list.append(tocc)

    tocc_df_list = []
    tocc_list.sort(key=lambda x: len(x), reverse=True)
    for tocc in tocc_list:
        tmp_tocc_df_list = []
        for i in range(len(tocc)):
            tmp_df = df[df.ts_begin == tocc[i]]
            tmp_tocc_df_list.append(tmp_df)
        tmp_tocc_df = pd.concat(tmp_tocc_df_list)
        tocc_df_list.append(tmp_tocc_df)
    return tocc_df_list


def distinguish_execution_mode(df_list: list) -> list:
    """

    :param df_list: each DataFrame in the list is each item in the TOCC.
    :return: Return a list with two DataFrames nested within each item, one for the user mode and one for the kernel mode.
        for example：
        [ [ df_kernel_1, df_user_1 ],
          [ df_kernel_2, df_user_2 ] ]
    """
    tocc_df_list = []
    for each_df in df_list:
        each_df["execution_mode"] = each_df["call_stack"].apply(
            lambda x: judge_execution_mode(x)
        )
        kernel_tocc_df = each_df[each_df.execution_mode == 0]
        user_tocc_df = each_df[each_df.execution_mode == 1]
        kernel_user_tocc_df_list = [kernel_tocc_df, user_tocc_df]
        tocc_df_list.append(kernel_user_tocc_df_list)
    return tocc_df_list


def judge_execution_mode(call_stack: list) -> int:
    """Determine the mode of operation of the function executed by the thread.

    :param call_stack: call stack
    :return:
    """
    top_function = call_stack[-1]
    top_function_list = top_function.split(" ")
    # Here the length of top_function_list is 3,
    # 0 is the function address,
    # 1 is the function name,
    # 2 is the function source code corresponding path
    function_address = top_function_list[0]
    execution_mode = 1  # kernel 0, user 1

    # 64bit kernel space: 0xffffffff80000000~0xffffffffffffffff
    # 64bit user space: 0x0000000000000000~0x00007fffffffffff
    # 32bit kernel space: 0xc0000000~0xffffffff
    # 32bit user space: 0x00000000~0xbfffffff
    # TODO: Remove magic number.
    function_address_len = len(function_address)
    if function_address_len > 10:
        if (
            function_address >= "ffffffff80000000"
            and function_address <= "ffffffffffffffff"
        ):
            execution_mode = 0
    else:
        if function_address >= "c0000000" and function_address <= "xffffffff":
            execution_mode = 0

    return execution_mode


def pruning_by_threshold(df_list: list, duration_length_threshold: int) -> list:
    """Implementation of filter pruning, mainly for the setting and implementation of custom thresholds

    :param df_list:
    :param threshold:
    :return:
        example：
        [df_1, df_2, df_3, ...]
    """
    res_df_list = []
    for each_df in df_list:
        if duration_length_threshold == -1:
            tid_num = len(each_df)
            duration_length_sum = each_df["duration_length"].sum()
            duration_length_threshold = duration_length_sum / tid_num
        tmp_df = each_df[each_df.duration_length >= duration_length_threshold]
        res_df_list.append(tmp_df)
    return res_df_list


def duration_threshold_setting(
    df_list: list, duration_length_threshold: int = -1
) -> list:
    """Customize thresholds for filtering.

    :param df_list: Each item in the list is a DataFrame List to be filtered, which depends
                    largely on the processing time when the threads of
                     different time periods are stored in different DataFrames.
    :param duration_length_threshold: Custom set threshold value. We specify: -1 means use the mean value.
    :return:
        example of results ：
        [ [df_kernel_1, df_kernel_2, ...],
          [df_user_1, df_user_2, ...] ]
    """
    kernel_tocc_df_list = []
    user_tocc_df_list = []
    for each_df_list in df_list:
        if len(each_df_list[0]) > 0:
            kernel_tocc_df_list.append(each_df_list[0])
        if len(each_df_list[1]) > 0:
            user_tocc_df_list.append(each_df_list[1])
    res_df_list = []
    tmp_res_df_list = [kernel_tocc_df_list, user_tocc_df_list]
    for each_df_list in tmp_res_df_list:
        tmp_res_df = pruning_by_threshold(each_df_list, duration_length_threshold)
        res_df_list.append(tmp_res_df)

    return res_df_list


def divide_each_subTOCC(df_list: list) -> list:
    """Time overlap division for threads inside a single DataFrame

    :param df_list:
    :return:
        example：
        [df_1, df_2, df_3, ...]
    """
    each_subTOCC_df_list = []
    for each_df in df_list:
        tmp_subTOCC_df_list = divide_TOCC(each_df)  # redivide
        each_subTOCC_df_list += tmp_subTOCC_df_list

    # NOTE: We believe that the more threads in a DataFrame, the more likely there is competition and therefore prioritize display.
    each_subTOCC_df_list = sorted(
        each_subTOCC_df_list, key=lambda x: len(x), reverse=True
    )
    return each_subTOCC_df_list


def divide_subTOCC(df_list: list) -> list:
    """divide subTOCC

    :param df_list:
    :return:
        example：
         [ [df_kernel_1, df_kernel_2, ...],
          [df_user_1, df_user_2, ...] ]
    """
    # Handles kernel mode and user mode in TOCC, so here the df_list length is 2.
    subTOCC_df_list = []
    for each_df_list in df_list:
        # The first one is a list of df's in kernel mode, the second one is in user mode.
        tmp_subTOCC_df_list = divide_each_subTOCC(each_df_list)
        subTOCC_df_list.append(tmp_subTOCC_df_list)
    return subTOCC_df_list

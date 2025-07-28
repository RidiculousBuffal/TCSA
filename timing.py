import re

import pandas as pd


class DataPreparation:
    """Handle performance data such as function call stacks collected by Linux perf."""

    def __init__(self) -> None:
        pass

    def data_loading(self, input_file_path: str) -> pd.DataFrame:
        """Loading data.

        :param file_path:
        :return:
        """

        top_message = []
        perf_records = []
        call_stack = []  # temp call stacks
        command, timestamp, event_value, event, cpu, tid = "", "", "", "", "", ""
        with open(input_file_path, "r") as input_file:
            perf_file = input_file.readlines()
            for each_line in perf_file:
                if each_line[0] == "#":
                    top_message.append(each_line)
                elif each_line[0] == "\t":
                    call_stack.append(each_line.replace("\t", "").replace("\n", ""))
                elif each_line[0] == "\n":
                    call_stack.reverse()
                    perf_records.append(
                        (timestamp, command, tid, cpu, event, call_stack)
                    )
                    call_stack = []
                    command, timestamp, event, cpu, tid = "", "", "", "", ""
                else:
                    # Note: different versions of perf and different parameters collect different types of data
                    # swapper     0 [000] 691089.368816:     250000 cpu-clock:pppH:
                    position_flag = each_line.rfind("[")
                    if position_flag != -1:
                        tmp_perf_record = each_line[: position_flag - 1].split()
                        command = " ".join(
                            str(x) for x in tmp_perf_record[: len(tmp_perf_record) - 1]
                        )
                        tid = tmp_perf_record[len(tmp_perf_record) - 1]
                        tmp_perf_record = (
                            each_line[position_flag:]
                            .replace(":", "")
                            .replace("[", "")
                            .replace("]", "")
                            .split()
                        )
                        # tmp_perf_record: ['000', '694345.695642', '10101010', 'cpu-clockpppH']
                        cpu = tmp_perf_record[0]
                        timestamp = tmp_perf_record[1]
                        event = tmp_perf_record[2]
                    else:
                        tmp_perf_record = each_line.strip().split()
                        timestamp = tmp_perf_record[-3]
                        event = tmp_perf_record[-1]
                        tid = tmp_perf_record[-4]
                        command = "".join(str(x) for x in tmp_perf_record[:-4])
        # Transform DataFrame
        title_columns = ["timestamp", "command", "tid", "cpu", "event", "call_stack"]
        perf_records_df = pd.DataFrame(perf_records, columns=title_columns)
        return perf_records_df

    def data_loading_optimized(self, input_file_path: str) -> pd.DataFrame:
        """
        [优化版] 使用生成器和正则表达式以流式方式高效加载数据。

        :param input_file_path: perf script 输出的文本文件路径。
        :return: 一个包含所有 perf 记录的 Pandas DataFrame。
        """

        # 1. 预编译正则表达式，用于高效解析事件头
        #    这个表达式能更好地处理空格和不同格式
        header_regex = re.compile(
            r"^(?P<command>.+?)\s+(?P<tid>\d+)\s+\[(?P<cpu>\d+)\]\s+(?P<timestamp>[\d\.]+):\s+.*"
        )

        # 2. 定义一个生成器函数，以流式方式处理文件
        def record_generator(path: str):
            call_stack = []
            current_record = {}

            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    if line.startswith("#"):
                        continue  # 忽略注释行

                    # 遇到调用栈行 (以制表符或多个空格开头)
                    elif line.startswith(("\t", "  ")):
                        call_stack.append(line.strip())

                    # 遇到空行，代表一个事件的结束
                    elif not line.strip():
                        if current_record and call_stack:
                            # 标准的 perf script 输出是调用者在下，被调用者在上
                            # 代码默认的行为是在加载后 reverse()。
                            # 这里我们可以在加载时就保持这个顺序，或者在生成时 reverse
                            # 为了与原代码保持一致，这里不 reverse
                            current_record["call_stack"] = call_stack
                            yield current_record

                        # 重置状态，为下一个事件做准备
                        call_stack = []
                        current_record = {}

                    # 遇到新的事件头
                    else:
                        match = header_regex.match(line)
                        if match:
                            current_record = match.groupdict()
                            # 确保从字典中提取的字段存在
                            current_record["event"] = line.split(":")[
                                -1
                            ].strip()  # 简单获取事件类型

            # 处理文件末尾最后一个可能的事件
            if current_record and call_stack:
                current_record["call_stack"] = call_stack
                yield current_record

        # 3. 从生成器高效创建 DataFrame
        #    pd.DataFrame.from_records() 可以直接处理字典的迭代器
        perf_records_df = pd.DataFrame.from_records(record_generator(input_file_path))

        # 如果DataFrame为空，则提前返回
        if perf_records_df.empty:
            return perf_records_df

        # 数据清洗 (可选，但推荐)
        # 确保关键列存在，防止后续代码出错
        required_cols = ["timestamp", "command", "tid", "cpu", "event", "call_stack"]
        for col in required_cols:
            if col not in perf_records_df.columns:
                perf_records_df[col] = None  # 或 pd.NA

        return perf_records_df

    def data_processing(self, perf_records_df: pd.DataFrame) -> pd.DataFrame:
        # DataFrame -> ['timestamp', 'command', 'tid', 'cpu', 'event', 'call_stack']
        perf_records_df["tid"] = perf_records_df["tid"].astype(int)
        perf_records_df["cpu"] = perf_records_df["cpu"].astype(int)
        perf_records_df["timestamp"] = perf_records_df["timestamp"].astype(float)
        perf_records_df["top_function"] = perf_records_df.apply(
            lambda x: x.call_stack[0] if len(x.call_stack) > 0 else [], axis=1
        )
        perf_records_df["top_function"] = perf_records_df["top_function"].astype(str)
        perf_records_df["function_call_stack"] = perf_records_df["call_stack"].apply(
            lambda x: ";".join([" ".join(j.split()[1:-1]) for j in x])
        )
        perf_records_df["function_call_stack"] = perf_records_df[
            "function_call_stack"
        ].astype(str)

        return perf_records_df


if __name__ == "__main__":
    result = DataPreparation().data_loading_optimized(
        r"E:\codes\TCSA\mock\perf_quick_test.txt"
    )
    print(result)
    result = DataPreparation().data_processing(result)
    print(result)

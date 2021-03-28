import hashlib
import os
import re
import shutil
import json
from enum import Enum
from datetime import datetime

import pandas as pd

from py.Util import run_async


class FailLogsOption(Enum):
    # Keep log at original location, without merge
    KEEP_MERGE = 1 #pass
    # Extract logs and create separate file
    CREATE_SEP = 2 #pass
    # Append failed logs lines to previous normal log line
    APPEND_LINE = 3 #pass
    # Re-order failed log lines and attach to end of the dataframe
    # TODO: DO we need line break sep?
    REORDER_ATTACH = 4 #pass


class LogBlock:
    def __init__(self, log_format, logName, indir='./', outdir='./result/', rex=[], sep_split='||', sep_count='<*>', line_break='~~', disable_step=None):
        self.path = indir
        self.logName = logName
        self.savePath = outdir
        self.df_log = None
        self.log_format = log_format
        self.rex = rex
        self.failtomatchList = []
        self.init()
        self.sep_split = sep_split
        self.sep_count = sep_count
        self.line_break = line_break
        self.failed_logs_option = FailLogsOption.APPEND_LINE
        self.freq = 0.1
        self.disable_step = disable_step #Skip which step

    def outputCSV(self, list, file_name, sep='\n'):
        print('[START] writing %s' % file_name)
        list = [str(x) for x in list]

        try:
            with open(file_name, 'w') as w:
                w.write(sep.join(list))
            w.close()
        except UnicodeEncodeError:
            # If failed then write bytes after encoding
            with open(file_name, 'wb') as w:
                w.write(sep.join(list).encode('utf-8'))
            w.close()
        print('[END] writing %s' % file_name)

    def step1_check_unique(self, l_list, isStartWithFail):
        """
        Step1: Process if current column contains only unique value
        :param l_list:
        :param isStartWithFail:
        :return:
        """
        if len(set(l_list)) == 1:
            # If only single value in list
            # Return single value add its appearance
            if isStartWithFail:
                return [self.sep_count.join([l_list[0], str(len(l_list))]) + 'E'], True
            else:
                return [self.sep_count.join([l_list[0], str(len(l_list))])], True
        else:
            return l_list, False

    def step2_delta_encoding_for_integers(self, l_list, isStartWithFail):
        """
        If a line contains only integers, represent with delta value
        This saves space for colunms like timestamp
        :param l_list:
        :param isStartWithFail:
        :return:
        """
        try:
            t_list = list(map(int, l_list))
            # Delta encoding
            t_list = [str(t_list[x+1] - t_list[x]) if x != 0 else str(t_list[0]) for x in range(len(t_list) - 1)]
            if isStartWithFail:
                # t_list[0] = '<delta>E' + t_list[0]
                t_list.insert(0, '<delta>E')
            else:
                #t_list[0] = '<delta>' + t_list[0]
                t_list.insert(0, '<delta>')
            return t_list, True
        except ValueError:
            # If cannot be convert to integers
            return l_list, False

    def step3_extract_frequent_words(self, l_list, isStartWithFail):
        """
        If column contains frequent words (larger than a frequency threshold), extract & build dictionary & replace those words
        :param l_list:
        :param isStartWithFail:
        :param freq:
        :return:
        """
        if len(set(l_list)) <= int(self.freq * len(l_list)):
            # Create dictionary
            dict_lv = {}
            count = 0
            for lv in set(l_list):
                dict_lv[lv] = count
                count += 1
            d_list = [str(dict_lv[x]) for x in l_list]
            #d_list[0] = json.dumps(dict_lv) + d_list[0]
            if isStartWithFail:
                #d_list[0] = json.dumps(dict_lv) + 'E' + d_list[0]
                d_list.insert(0, json.dumps(dict_lv) + 'E')
            else:
                # d_list[0] = json.dumps(dict_lv) + d_list[0]
                d_list.insert(0, json.dumps(dict_lv))
            return d_list, True
        else:
            return l_list, False


    def step4_extract_common_prefix_string(self, l_list, isStartWithFail):
        """
        If a column contains values with the same prefix string, then extract that
        For example, if all values start with "com.hadoop."
        :param l_list:
        :param isStartWithFail:
        :return:
        """
        common_prefix_str = os.path.commonprefix(l_list)
        #if len(common_prefix_str) >= 0.4 * len(l_list[0]):
        if len(common_prefix_str) > 0:
            comm_list = [x[len(common_prefix_str):] for x in l_list]
            if isStartWithFail:
                #comm_list[0] = '<pre' + common_prefix_str + '/>E' + comm_list[0]
                comm_list.insert(0, '<pre' + common_prefix_str + '/>E')
            else:
                #comm_list[0] = '<pre' + common_prefix_str + '/>' + comm_list[0]
                comm_list.insert(0, '<pre' + common_prefix_str + '/>')
            return comm_list, True
        else:
            return l_list, False

    def check_disable_steps(self, process_funcs):
        """
        Skip preprocessing steps
        :param process_funcs:
        :return:
        """
        dis = self.disable_step
        if dis is None:
            return process_funcs
        else:
            if isinstance(dis, int):
                del process_funcs[dis-1]
            elif isinstance(dis, list):
                dis = sorted(dis, reverse=True)
                for x in dis:
                    del process_funcs[x-1]
            else:
                raise TypeError('To disable step(s), you should only use an integer or a list of integers')
            return process_funcs

    def convert_list_to_shorter(self, l_list):
        # 0. Check if first row is null
        # This happens to log chunks that start with failed logs
        isStartWithFail = False
        if l_list[0] == '':
            l_list = l_list[1:]
            isStartWithFail = True

        # 1. Check if unique
        # 2. If list contains integers only
        # 3. If contains repetitive values can be regard as levels (< 10%) for example
        # 4. Extract repetitive prefix strings
        process_funcs = [self.step1_check_unique,
                 self.step2_delta_encoding_for_integers,
                 self.step3_extract_frequent_words,
                 self.step4_extract_common_prefix_string]

        # Check disable steps
        process_funcs = self.check_disable_steps(process_funcs)

        for p_func in process_funcs:
            # Process columns according to different steps
            l, processed = p_func(l_list, isStartWithFail)
            if processed: return l

        # If reach here, it means the list cannot be processed from above approaches, recover it
        if isStartWithFail:
            l_list.insert(0, '')
        return l_list


    def trans_by_cols(self, df, cols, output_ext):
        #TODO: Decide if we need to introduce parallel processing
        merge_list = []

        for col in cols:
            df_colList = df[col]
            # If contains NA
            # check if none in list, replace none with ''
            if df_colList.isnull().values.any():
                df_colList = ['' if x is None else x for x in df_colList]
            else:
                df_colList = list(df_colList)

            df_colList = self.convert_list_to_shorter(df_colList)

            merge_list.append(self.sep_split.join(df_colList))

        self.outputCSV(file_name=os.path.join(self.savePath, self.logName + output_ext), list=merge_list)


    def run(self):
        """
        Transpose logs and content
        :return:
        """

        # Load data
        self.load_data()

        self.trans_by_cols(
            df=self.df_log,
            cols=self.headers,
            output_ext="_trans_file.txt"
        )

        # Output unmatched lines
        if len(self.failtomatchList) != 0:
            fail_df = pd.DataFrame(self.failtomatchList, columns=['Line', 'Content'])

            self.trans_by_cols(
                df=fail_df,
                cols=list(fail_df.columns),
                output_ext='_failmatch.txt'
            )

    def init(self):
        if not os.path.exists(self.savePath):
            os.makedirs(self.savePath)
        else:
            # Clean folder
            shutil.rmtree(self.savePath)
            os.makedirs(self.savePath)

    def load_data(self):
        headers, regex = self.generate_logformat_regex(self.log_format)
        self.headers = headers
        self.df_log = self.log_to_dataframe(os.path.join(self.path, self.logName), regex,
                                            headers, self.log_format)

    def preprocess(self, line):
        for currentRex in self.rex:
            line = re.sub(currentRex, '<*>', line)
        return line

    def log_to_dataframe(self, log_file, regex, headers, logformat, encode="utf-8"):
        """ Function to transform log file to dataframe
        """
        try:
            log_messages = []
            linecount = 0

            # Record temporary line number
            temp_linecount = 0
            temp_line_list = []

            line_id_list_real = []

            with open(log_file, 'r', encoding=encode) as fin:

                for line in fin.readlines():
                    # print(line)
                    temp_linecount += 1
                    try:

                        match = regex.search(line.strip())
                        message = [match.group(header) for header in headers]

                        log_messages.append(message)

                        # Dump saved failed lines
                        if temp_line_list != []:
                            # Add except extra
                            self.failtomatchList += temp_line_list
                            temp_line_list = []

                        linecount += 1
                        line_id_list_real.append(temp_linecount)

                    except Exception as e:
                        if self.failed_logs_option == FailLogsOption.CREATE_SEP:
                            # Record lines that cannot be processed
                            if temp_line_list == []:
                                temp_line_list.append([str(temp_linecount), line.strip()])
                            else:
                                temp_line_list.append(['', line.strip()])
                        elif self.failed_logs_option == FailLogsOption.REORDER_ATTACH:
                            if temp_line_list == []:
                                temp_line_list.append(str(temp_linecount) + ' ' + line.strip())
                            else:
                                temp_line_list[-1] += self.line_break + line.strip()
                                #temp_line_list.append(line.strip())

                        elif self.failed_logs_option == FailLogsOption.KEEP_MERGE:
                            # If merge failed log lines, regard the failed log line as content, with empty headers
                            message = ['' if header != "Content" else line.strip() for header in headers]
                            log_messages.append(message)
                        elif self.failed_logs_option == FailLogsOption.APPEND_LINE:


                            content_index = headers.index("Content")
                            if len(log_messages) > 0:
                                # IF has a prior log line, append to the end of line
                                log_messages[-1][content_index] += self.line_break + line.strip()
                            else:
                                # If first line failed
                                message = ['' if header != "Content" else line.strip() for header in headers]
                                log_messages.append(message)

            fin.close()

            # Final check if temp_line_text is empty
            if temp_line_list != []:
                self.failtomatchList += temp_line_list
                del temp_line_list

            if self.failed_logs_option == FailLogsOption.REORDER_ATTACH:
                for f_line in self.failtomatchList:
                    message = ['' if header != "Content" else f_line for header in headers]
                    log_messages.append(message)
                # Clear
                self.failtomatchList = []

            logdf = pd.DataFrame(log_messages, columns=headers)

            return logdf
        except UnicodeDecodeError:
            return self.log_to_dataframe(log_file, regex, headers, logformat, encode="ISO-8859-1")

    def generate_logformat_regex(self, logformat):
        """ Function to generate regular expression to split log messages
        """
        headers = []
        splitters = re.split(r'(<[^<>]+>)', logformat)
        regex = ''
        for k in range(len(splitters)):
            if k % 2 == 0:
                splitter = re.sub(' +', '\\\s+', splitters[k])
                regex += splitter
            else:
                header = splitters[k].strip('<').strip('>')

                h_list = header.split(':')
                if len(h_list) > 1:
                    header = h_list[0]
                    digit = h_list[1]

                    regex += '(?P<%s>.{%s})' % (header, digit)
                else:
                    regex += '(?P<%s>.*?)' % header
                headers.append(header)
        regex = re.compile('^' + regex + '$')
        return headers, regex



if __name__ == '__main__':

    logpath = '/Users/yaokundi/Documents/Project/2019/LogPreprocess/result/78_Windows.log'
    out_dir = 'data'
    transposer = LogBlock(
        log_format='<Year:4>-<Month:2>-<Day:2> <Hour:2>:<Minute:2>:<Second:2>, <Level>                  <Component>    <Content>',
        indir=os.path.dirname(logpath),
        logName=os.path.basename(logpath),
        outdir=out_dir,
        rex=[r'0x.*?\s'],
    )
    transposer.run()
import pickle
import random
import re
import os
import shutil
import subprocess
import sys
import glob
import ntpath
import socket
import signal
import platform
import queue
import logging
from datetime import datetime, timedelta
from enum import Enum
import pandas as pd
sys.path.insert(1, os.path.realpath(os.path.pardir))
from py.Util import elpased_time, _getAllFiles, run_async, run_async_multiprocessing, time_limit

# Import write lock
try:
    from readerwriterlock import rwlock
    lock = rwlock.RWLockWrite().gen_wlock()
except ImportError:
    from threading import Lock
    lock = Lock()

# Set working directory under the py directory
file_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(file_dir)

log_dir='../runtimeLog'
if not os.path.isdir(log_dir):
    os.makedirs(log_dir)
logging.basicConfig(
    filename=os.path.join(log_dir, 'CompressionResult.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger("CompressionResult")

# We adopt the logpai benchmark

benchmark_settings = {
    'HDFS': {
        'log_file': 'HDFS/HDFS_10M.log',
        'log_format': '<Year:2><Month:2><Day:2> <Hour:2><Minute:2><Second:2> <Pid> <Level> <Component>: <Content>',
        'regex': [r'blk_-?\d+', r'(\d+\.){3}\d+(:\d+)?'],
        'st': 0.5,
        'depth': 4
        },

    'Hadoop': {
        'log_file': 'Hadoop/Hadoop_10M.log',
        'log_format': '<Year:4>-<Month:2>-<Day:2> <Hour:2>:<Minute:2>:<Second:2>,<Millisecond:3> <Level> \[<Process>\] <Component>: <Content>',
        'regex': [r'(\d+\.){3}\d+'],
        'st': 0.5,
        'depth': 4
        },

    'Spark': {
        'log_file': 'Spark/Spark_10M.log',
        'log_format': '<Year:2>/<Month:2>/<Day:2> <Hour:2>:<Minute:2>:<Second:2> <Level> <Component>: <Content>',
        'regex': [r'(\d+\.){3}\d+', r'\b[KGTM]?B\b', r'([\w-]+\.){2,}[\w-]+'],
        'st': 0.5,
        'depth': 4
        },

    'Zookeeper': {
        'log_file': 'Zookeeper/Zookeeper_10M.log',
        'log_format': '<Year:4>-<Month:2>-<Day:2> <Hour:2>:<Minute:2>:<Second:2>,<Millisecond:3> - <Level>  \[<Node>:<Component>@<Id>\] - <Content>',
        'regex': [r'(/|)(\d+\.){3}\d+(:\d+)?'],
        'st': 0.5,
        'depth': 4
        },

    'BGL': {
        'log_file': 'BGL/BGL_10M.log',
        'log_format': '<Label> <Timestamp> <Year1:4>.<Month1:2>.<Day1:2> <Node> <Year:4>-<Month:2>-<Day:2>-<Hour:2>.<Minute:2>.<Second:2>.<Millisecond:6> <NodeRepeat> <Type> <Component> <Level> <Content>',
        'regex': [r'core\.\d+'],
        'st': 0.5,
        'depth': 4
        },

    'HPC': {
        'log_file': 'HPC/HPC_10M.log',
        'log_format': '<LogId> <Node> <Component> <State> <Time> <Flag> <Content>',
        'regex': [r'=\d+'],
        'st': 0.5,
        'depth': 4
        },

    'Thunderbird': {
        'log_file': 'Thunderbird/Thunderbird_10M.log',
        'log_format': '<Label> <Timestamp> <DYear:4>.<DMonth:2>.<DDay:2> <User> <Month> <Day> <Hour:2>:<Minute:2>:<Second:2> <Location> <Component>(\[<PID>\])?: <Content>',
        'regex': [r'(\d+\.){3}\d+'],
        'st': 0.5,
        'depth': 4
        },

    'Windows': {
        'log_file': 'Windows/Windows_10M.log',
        'log_format': '<Year:4>-<Month:2>-<Day:2> <Hour:2>:<Minute:2>:<Second:2>, <Level>                  <Component>    <Content>',
        'regex': [r'0x.*?\s'],
        'st': 0.7,
        'depth': 5
        },

    'Linux': {
        'log_file': 'Linux/Linux_10M.log',
        'log_format': '<Month> <Date> <Hour:2>:<Minute:2>:<Second:2> <Level> <Component>(\[<PID>\])?: <Content>',
        'regex': [r'(\d+\.){3}\d+', r'\d{2}:\d{2}:\d{2}'],
        'st': 0.39,
        'depth': 6
        },

    'Android': {
        'log_file': 'Android/Android_10M.log',
        'log_format': '<Month:2>-<Day:2> <Hour:2>.<Minute:2>.<Second:2>.<Millisecond:3>  <Pid>  <Tid> <Level> <Component>: <Content>',
        'regex': [r'(/[\w-]+)+', r'([\w-]+\.){2,}[\w-]+', r'\b(\-?\+?\d+)\b|\b0[Xx][a-fA-F\d]+\b|\b[a-fA-F\d]{4,}\b'],
        'st': 0.2,
        'depth': 6
        },

    'HealthApp': {
        'log_file': 'HealthApp/HealthApp_10M.log',
        'log_format': '<Year:4><Month:2><Day:2>-<Hour:2>:<Minute:2>:<Second:2>:<Millisecond:3>\|<Component>\|<Pid>\|<Content>',
        'regex': [],
        'st': 0.2,
        'depth': 4
        },

    'Apache': {
        'log_file': 'Apache/Apache_10M.log',
        'log_format': '\[<Date:3> <Month:3> <Day:2> <Hour:2>:<Minute:2>:<Second:2> <Year:4>\] \[<Level>\] <Content>',
        'regex': [r'(\d+\.){3}\d+'],
        'st': 0.5,
        'depth': 4
        },

    'Proxifier': {
        'log_file': 'Proxifier/Proxifier_10M.log',
        'log_format': '\[<Month:2>.<Day:2> <Hour:2>:<Minute:2>:<Second:2>\] <Program> - <Content>',
        'regex': [r'<\d+\ssec', r'([\w-]+\.)+[\w-]+(:\d+)?', r'\d{2}:\d{2}(:\d{2})*', r'[KGTM]B'],
        'st': 0.6,
        'depth': 3
        },

    'OpenSSH': {
        'log_file': 'OpenSSH/OpenSSH_10M.log',
        'log_format': '<Date> <Day> <Hour:2>:<Minute:2>:<Second:2> <Component> sshd\[<Pid>\]: <Content>',
        'regex': [r'(\d+\.){3}\d+', r'([\w-]+\.){2,}[\w-]+'],
        'st': 0.6,
        'depth': 5
        },

    'OpenStack': {
        'log_file': 'OpenStack/OpenStack_10M.log',
        'log_format': 'nova-<ComponentName>.log(.<Index>)?.<Year1:4>-<Month1:2>-<Day1:2>_<Hour1:2>:<Minute1:2>:<Second1:2> <Year:4>-<Month:2>-<Day:2> <Hour:2>:<Minute:2>:<Second:2>.<Millisecond:3> <Pid> <Level> <Component> \[<ADDR>\] <Content>',
        'regex': [r'((\d+\.){3}\d+,?)+', r'/.+?\s', r'\d+'],
        'st': 0.5,
        'depth': 5
        },

    'Mac': {
        'log_file': 'Mac/Mac_10M.log',
        'log_format': '<Month>  <Date> <Hour:2>:<Minute:2>:<Second:2> <User> <Component>\[<PID>\]( \(<Address>\))?: <Content>',
        'regex': [r'([\w-]+\.){2,}[\w-]+'],
        'st': 0.7,
        'depth': 6
        },
}

compressor_config = {
    # Remember to save compressed file in the same folder
    'gzip': {
        'extension': 'gz',
        'compress_cmd': 'gzip {path}',
        'decompress_cmd': 'gzip -d {path_compressed}',
        'compress_folder_cmd': 'gzip -r {path_dir}/*',
        'decompress_folder_cmd': 'gzip -dr {path_dir}/*'
    },
    'deflate': {
        'extension': '7z',
        'compress_cmd': '7z a -m0=Deflate {path_compressed} {path} && rm {path}',
        'decompress_cmd': '7z x -m0=Deflate {path_compressed} -o{path_pardir} && rm {path_compressed}',
        'compress_folder_cmd': '7z a -m0=Deflate {path_dir}/all_log_ppmd.7z {path_dir}/*.log && rm {path_dir}/*.log',
        'decompress_folder_cmd': '7z x -m0=Deflate {path_dir}/all_log_ppmd.7z -o{path_dir} && rm {path_dir}/all_log_ppmd.7z'
    },
    'lz4': {
        'extension': 'lz4',
        'compress_cmd': 'lz4 --rm {path} {path_compressed}',
        'decompress_cmd': 'unlz4 --rm {path_compressed} {path}',
        'compress_folder_cmd': 'find {path_dir} -exec lz4 --rm {}',
        'decompress_folder_cmd': 'find {path_dir} -exec unlz4 --rm {}',
    }
}

def convert_size(size, round_digit = 2, isConversion = False):
    """
    Convert size from bytes to MB/KB/GB...
    Convert size value from float/int to str
    :param size:
    :param round_digit:
    :param isConversion:
    :return:
    """
    if isConversion:
        level = ['B', 'K', 'M', 'G', 'T', 'P']
        lv_count = 0

        while size/1024 >= 1:
            size = round(float(size/1024))
            lv_count += 1

        return str(round(size, round_digit)) + level[lv_count]
    else:
        return str(round(size, round_digit))

def revert_size(size):
    """
    Convert a size value from str to float
    :param size:
    :return:
    """
    level = ['B', 'K', 'M', 'G', 'T', 'P']

    return float(size[:-1]) * pow(1024, level.index(size[-1]))

getsize = os.path.getsize


def read_file_to_list(file):
    try:
        with open(file, 'r') as r:
            line_list = r.read().split('\n')
        r.close()
    except UnicodeError:
        with open(file, 'rb') as r:
            line_list = r.read().decode('utf-8').split('\n')
        r.close()
    return line_list


def cmd_execute_time(input, cwd=None, isformatTime=False, timeThresh=None, *args, **kwargs):
    # Add a time threshold; if the processing time of a processing time exceeds the threshold, then skip
    start_time = datetime.now()

    if isinstance(input, str):
        if cwd is None:
            subprocess.Popen(input, shell=True).wait()
        else:
            subprocess.Popen(input, shell=True, cwd=cwd).wait()
    elif isinstance(input, list):
        for inp in input:
            if cwd is None:
                subprocess.Popen(inp, shell=True).wait()
            else:
                subprocess.Popen(inp, shell=True, cwd=cwd).wait()
    elif callable(input):
        if timeThresh:
            try:
                with time_limit(timeThresh):
                    input(*args, **kwargs)
            except TimeoutError as e:
                return None
        else:
            input(*args, **kwargs)
    end_time = datetime.now()

    if isformatTime:
        return '{!s}'.format(end_time - start_time)
    else:
        return (end_time-start_time).total_seconds()


def get_num_of_templates(folder, ext):
    with open(_getAllFiles(folder=folder, ext=ext)[0], 'r') as r:
        num_buckets = len(r.readlines()) - 1
    r.close()
    return num_buckets


def compress_data(path, compressor_name, repeat=1, type=1):
    settings = compressor_config[compressor_name]

    path_compressed = path + '.' + settings['extension']
    if os.path.isfile(path):


        ori_compress_time = cmd_execute_time(input=settings['compress_cmd'].format(path=path, path_compressed=path_compressed))

        compressed_size = getsize(path_compressed)

        ori_decompress_time = cmd_execute_time(input=settings['decompress_cmd'].format(path=path,
                                                                                 path_compressed=path_compressed,
                                                                                 path_pardir=os.path.dirname(path)))

        return {
            "Original Size Compreseed": convert_size(compressed_size),
            "Original Compress Time": seconds_to_hh_mm_ss(ori_compress_time),
            "Original Decompress Time": seconds_to_hh_mm_ss(ori_decompress_time)
        }

    elif os.path.isdir(path):

        path_dir = path

        num_buckets = len(glob.glob1(path, '*.log'))

        if num_buckets == 0:
            # If number of buckets is zero, this probably because output the variable extraction result
            dict = compress_var_extraction_result(dirpath=path_dir, compressor_settings=settings, repeat=repeat, type=type)

            try:
                dict["Number Of Buckets"] = str(get_num_of_templates(folder=path_dir, ext='template.csv'))
            except Exception:
                pass
            return dict

        else:

            prep_compress_time = cmd_execute_time(input=settings['compress_folder_cmd'].format(path_dir=path_dir))

            prep_size = sum([getsize(file) for file in glob.glob(path+'/**/*.%s' % settings['extension'], recursive=True)])

            prep_decompress_time = cmd_execute_time(input=settings['decompress_folder_cmd'].format(path_dir=path_dir))

        return {
            "PrepSize Compressed": convert_size(prep_size),
            "Prep CompressTime": prep_compress_time,
            "Prep DecompressTime": prep_decompress_time,
            "Number Of Buckets": str(num_buckets)
        }

    else:
        raise FileNotFoundError("[ERROR] Input path %s cannot be compressed!" % path)


def generate_repeat_files(path, repeat, temp_dir='../temp'):

    if os.path.isfile(path):
        output_dir = os.path.join(temp_dir, os.path.basename(path).split('.')[0])
        files_list = [path]
    elif os.path.isdir(path):
        files_list = _getAllFiles(path, ext='')
        output_dir = os.path.join(temp_dir, path.split(os.path.sep)[-1])

    if not os.path.isdir(output_dir):
        print(output_dir)
        os.makedirs(output_dir)

    for file in files_list:
        file_basename = os.path.basename(file)
        for i in range(0, repeat):
            shutil.copyfile(file, os.path.join(output_dir, file_basename + str(i)))

    return output_dir


def get_compress_info(path, bucket_dir, dataset, compressor_list, repeat=1, isRepeat=True, isOriOnly=False):
    """
    Measure the compression performance of a given log or directory
    :param path:
    :param bucket_dir:
    :param dataset:
    :param compressor_list:
    :param repeat:
    :param isRepeat:
    :param isOriOnly:
    :return:
    """

    if os.path.isfile(path):
        ori_size = getsize(path)


        file_info_dict =  {
            "File": dataset,
            "Original Size": convert_size(ori_size)
        }

        dict_list = []

        if isRepeat:
            # This part of code repeat data many times according to their sizes
            # So we can capture the time
            # We use following thresholds
            # 1. repeat 50 times if < 10M; 2. repeat 20 times if < 50M; 3. repeat 10 time if < 100M
            # if ori_size <= 10 * 1024 * 1024:
            #     repeat = 50
            # elif 10 * 1024 * 1024 < ori_size <= 50 * 1024 * 1024:
            #     repeat = 20
            # elif 50 * 1024 * 1024 < ori_size <= 100 * 1024 * 1024:
            #     repeat = 10
            # else:
            #     repeat = 1


            ori_output = generate_repeat_files(path=path, repeat=repeat)
            prep_output = generate_repeat_files(path=bucket_dir, repeat=repeat)

            for compressor in compressor_list:
                # Type 0 for original data and type 1 for prep data
                ori_dict = compress_data(path=ori_output, compressor_name=compressor, repeat=repeat, type=0)

                if isOriOnly:

                    dict_list.append({**file_info_dict, **ori_dict, **{'Compressor': compressor}})
                else:

                    prep_dict = compress_data(path=prep_output, compressor_name=compressor, repeat=repeat, type=1)

                    if "Number Of Buckets" not in prep_dict.keys():
                        try:
                            prep_dict["Number of Buckets"] = str(get_num_of_templates(folder=bucket_dir, ext='template.csv'))
                        except Exception:
                            # There's no template file
                            pass

                    prep_dict['Compressor'] = compressor

                    dict_list.append({**file_info_dict, **ori_dict, **prep_dict})

            shutil.rmtree(ori_output)
            shutil.rmtree(prep_output)

        else:

            for compressor in compressor_list:
                # Compress & Decompress original file
                ori_dict = compress_data(path, compressor_name=compressor)

                if isOriOnly:
                    dict_list.append({**file_info_dict, **ori_dict, **{'Compressor': compressor}})
                else:
                    # Compress & Decompress preprocessed file
                    prep_dict = compress_data(bucket_dir, compressor_name=compressor)
                    prep_dict['Compressor'] = compressor

                    dict_list.append({**file_info_dict, **ori_dict, **prep_dict})

        return dict_list

    else:
        raise FileNotFoundError("[ERROR] Input path %s is not recognizable!" % path)

def get_total_lines(path):
    """Get total number of lines of a text file"""
    proc = subprocess.Popen("wc -l %s | awk '{print $1}'" % path, shell=True, stdout=subprocess.PIPE)
    output = str(proc.stdout.read())

    try:
        # Kill process
        proc.kill()
        # remove non-numeric characters
        total_lines = int(re.sub(r"\D", "", str(output)))
        return total_lines
    except TypeError:
        print('[ERROR] Cannot access a valid line count number from file: %s; the output is: %s' % (
            path, output))
        sys.exit(1)
    except OSError:
        pass

def getUpperBound(total_lines, chunkSize, path):
    """
    Get max allowed upper bound
    """
    get_bound_cmd = 'tail -c %s %s | wc -l' % (chunkSize, path)

    proc = subprocess.Popen(get_bound_cmd, shell=True, stdout=subprocess.PIPE)
    output = str(proc.stdout.read())

    try:
        # Kill process
        proc.kill()
        # remove non-numeric characters
        bound_counts = int(re.sub(r"\D", "", str(output)))

        if bound_counts == total_lines:
            print('[WARN] The size of file %s is less than or equal to your cutoff threshold. Please check original file size! ' % path)

        return (total_lines-bound_counts)

    except TypeError:
        print('[ERROR] Cannot access a valid boundary line count number from file: %s; the output is: %s' % (
            path, output))
        sys.exit(1)
    except OSError:
        pass


def truncate_chunk(in_path, out_path, start, end, chunkSize):

    # Truncate first chunkSize logs and remove last line, since it might be imcomplete
    cmd = 'sed -n \'%d, %dp\' %s | head -c %s | sed \'$d\' > %s' % (start, end, in_path, chunkSize, out_path)
    subprocess.Popen(cmd, shell=True).wait()
    print('Chunk created at %s' % out_path)


def init_chunk_size(chunkSize):

    if platform.system() == 'Darwin':
        conver_dict = {
            "B": 0,
            "K": 1,
            "M": 2,
            "G": 3,
            "T": 4,
            "P": 5
        }

        for key in conver_dict.keys():
            if chunkSize.endswith(key):
                # Since head command does not support non-digit in Mac
                chunkSize = str(int(chunkSize.rstrip(key)) * pow(1024, conver_dict[key]))
                break

    return chunkSize


def generate_random_chunk(path, repeat, dataset, temp_dir='../temp', chunkSize='16K'):

    temp_dir = os.path.sep.join([temp_dir, 'chunk_%s' % chunkSize, dataset])
    if not os.path.isdir(temp_dir):
        os.makedirs(temp_dir)

    # Set seed
    random.seed(1)

    # Mac does not support non-digit head/tail command
    chunkSize_str = chunkSize
    chunkSize = init_chunk_size(chunkSize)

    # Get total lines
    total_lines_count = get_total_lines(path)
    upper_bound = getUpperBound(total_lines=total_lines_count, path=path, chunkSize=chunkSize)

    # To ensure the correctness, we use 5 times of the line counts at approximate size
    approximate_lines_of_chunk = total_lines_count-upper_bound
    expand_param = 5


    q = queue.Queue()
    jobs_truncate = []
    for i in range(repeat):

        @run_async
        def truncate(i, q_queue, ignoreCreated=True):
            output_file = os.path.join(temp_dir, '%d_%s' % (i, os.path.basename(path)))
            if os.path.isfile(output_file) and ignoreCreated:
                q_queue.put(output_file)
                print('Chunk already exists at %s' % output_file)
                return

            # Start and end of the chunk, then select X size from chunk

            start = random.randint(0, upper_bound)
            end = approximate_lines_of_chunk * expand_param + start

            if end > total_lines_count:
                end = total_lines_count

            truncate_chunk(out_path=output_file,
                           in_path=path,
                           start=start,
                           end=end,
                           chunkSize=chunkSize
                           )

            # Check size, if not 16K, redo
            if not convert_size(getsize(output_file), None, True) == chunkSize_str:
                truncate(i, q_queue, ignoreCreated=False)
            else:
                q_queue.put(output_file)

        jobs_truncate.append(truncate(i, q))

    [j.join() for j in jobs_truncate]

    # Return a list of processed logs
    return list(q.queue)



@elpased_time
def logblock(dataset, setting, output_root_dir, compressor, chunkSizeList, isDisable = False, repeat=100):
    from LogBlock.LogBlock import LogBlock

    dict_list = []

    original_log_path = os.path.join(input_dir, setting['log_file'])

    ori_log_size = get_total_size_of_extension(os.path.dirname(original_log_path), os.path.basename(original_log_path))

    for chunkSize in chunkSizeList:

        if revert_size(chunkSize) > ori_log_size:
            break

        log_path_list = generate_random_chunk(path=original_log_path, repeat=repeat, dataset=dataset, chunkSize=chunkSize)

        for logpath in log_path_list:

            if not isDisable:

                out_dir = os.path.sep.join([output_root_dir, 'bucket_%s_%s' % (dataset, chunkSize),
                                            "bucket_" + ntpath.basename(logpath).split('.')[0]])
                # Generate a random chunk
                lblock = LogBlock(
                    log_format=setting['log_format'],
                    indir=os.path.dirname(logpath),
                    logName=os.path.basename(logpath),
                    outdir=out_dir,
                    rex=setting['regex'],
                )

                prep_time = cmd_execute_time(lblock.run)

                # Compress from here
                d = get_compress_info(
                    path=logpath,
                    bucket_dir=out_dir,
                    compressor_list=compressor,
                    dataset=dataset,
                    repeat=1
                )
                print(prep_time)

                chunk_id = os.path.basename(logpath).split('_')[0]

                if isinstance(d, list):
                    for x in d:
                        x["PreprocessingTime"] = seconds_to_hh_mm_ss(prep_time)
                        x["Chunk"] = chunkSize
                        x["ChunkId"] = chunk_id
                    dict_list += d
                elif isinstance(d, dict):
                    d["PreprocessingTime"] = seconds_to_hh_mm_ss(prep_time)
                    d["Chunk"] = chunkSize
                    d["ChunkId"] = chunk_id
                    dict_list.append(d)

            else:
                # From step1 to step4
                for step in range(0, 5):

                    if step == 0:
                        skip_step = None
                    else:
                        skip_step = step

                    out_dir = os.path.sep.join([output_root_dir, 'bucket_%s_%s' % (dataset, chunkSize),
                                                "bucket_" + ntpath.basename(logpath).split('.')[0],
                                                "SkipStep_%s" % str(step)])
                    # Generate a random chunk
                    lblock = LogBlock(
                        log_format=setting['log_format'],
                        indir=os.path.dirname(logpath),
                        logName=os.path.basename(logpath),
                        outdir=out_dir,
                        rex=setting['regex'],
                        disable_step=skip_step
                    )

                    prep_time = cmd_execute_time(lblock.run)

                    # Compress from here
                    d = get_compress_info(
                        path=logpath,
                        bucket_dir=out_dir,
                        compressor_list=compressor,
                        dataset=dataset,
                        repeat=1
                    )
                    print(prep_time)

                    chunk_id = os.path.basename(logpath).split('_')[0]

                    if isinstance(d, list):
                        for x in d:
                            x["PreprocessingTime"] = seconds_to_hh_mm_ss(prep_time)
                            x["Chunk"] = chunkSize
                            x["ChunkId"] = chunk_id
                            x["SkipStep"] = str(step)
                        dict_list += d
                    elif isinstance(d, dict):
                        d["PreprocessingTime"] = seconds_to_hh_mm_ss(prep_time)
                        d["Chunk"] = chunkSize
                        d["ChunkId"] = chunk_id
                        d["SkipStep"] = str(step)
                        dict_list.append(d)
    return dict_list


def read_log_characters(structured_log):
    ### New add, characterstics of log chunk
    try:
        # Total lines, equal to number of templates
        #total_num_templates = structured_log.shape[0]

        # Number of unique templates
        unique_num_templates = len(set(structured_log['EventId']))

        # Number of variables
        all_variables = []
        for param_row in list(structured_log['ParameterList'].dropna()):
            all_variables += list(param_row)

        # Number of unique variables
        total_num_unique_variables = len(set(all_variables))

        # Get event templates
        event_template_list = list(structured_log['EventTemplate'])

        # Clean event template list
        if 'NoMatch' in event_template_list:
            event_template_list = [x for x in event_template_list if x != 'NoMatch']
            # Cause they will give NoMatch an ID
            unique_num_templates -= 1

        # Size of templates
        size_of_templates = sys.getsizeof(''.join(event_template_list))

        # Size of dynamic variables
        size_of_variables = sys.getsizeof(''.join(all_variables))

        characters = {
            'UniqueNumTemplates': unique_num_templates,
            'UniqueNumVariables': total_num_unique_variables,
            'TemplateSize': size_of_templates,
            'VariableSize': size_of_variables
        }
    except Exception as e:
        # If error happens
        # Set 0 to all
        characters = {
            'UniqueNumTemplates': 0,
            'UniqueNumVariables': 0,
            'TemplateSize': 0,
            'VariableSize': 0
        }
    return characters


def get_total_size_of_extension(dir, ext):
    return sum([getsize(x) for x in _getAllFiles(folder=dir, ext=ext)])

def seconds_to_hh_mm_ss(seconds):
    #return '{:02}:{:02}:{:02}'.format(int(seconds//3600), int(seconds%3600//60), seconds%60)
    return seconds


def getFormat(logformat):
    """
    Function to generate regular expression to split log messages
    Content will later be replaced by dict_template + var
    """
    headers = []
    splitters = re.split(r'(<[^<>]+>)', logformat)
    regex = ''
    for k in range(len(splitters)):
        if k % 2 == 0:
            splitter = splitters[k]
            regex += splitter
        else:
            header = splitters[k].strip('<').strip('>')
            # if header != 'Content':
            regex += '(.*)'
            headers.append(header)
    return headers, regex


def compress_var_extraction_result(dirpath, compressor_settings, repeat=1, dict=None, type=1):

    if dict is None:
        dict = {}

    total_compressed_size = 0
    total_compress_time = 0.0
    total_decompress_time = 0.0

    ext = compressor_settings['extension']

    if type == 1:
        max_ext_num = repeat - 1
        d_type_ext = {
            'Variables': 'vars.txt%d' % max_ext_num,
            'Fail': 'failmatch.txt%d' % max_ext_num,
            'HashId': 'hashId.txt%d' % max_ext_num,
            'LineId': 'lineId.txt%d' % max_ext_num,
            'Template': 'template.csv%d' % max_ext_num
        }

        d_type_ext['TransposedFile'] = [f for f in _getAllFiles(folder=dirpath, ext='_trans_file.txt%d' % max_ext_num) if f.split('log_')[-1] not in d_type_ext.values()]

    filetype = ''

    inp_comp = [compressor_settings['compress_cmd'].format(
            path=file,
            path_compressed=file + '.%s' % ext)
            for file in _getAllFiles(dirpath, filetype)]

    total_compress_time += cmd_execute_time(input=inp_comp, isformatTime=False)

    filetype_compressed_size = get_total_size_of_extension(dirpath, filetype + '.%s' % ext)

    inp_decomp = [compressor_settings['decompress_cmd'].format(
        path=file[:-len('.'+ext)],
        path_compressed=file,
        path_pardir=os.path.dirname(file))
        for file in _getAllFiles(dirpath, filetype + '.%s' % ext)]

    total_decompress_time += cmd_execute_time(input=inp_decomp, isformatTime=False)

    total_compressed_size += filetype_compressed_size

    if type == 0:
        dict["Original Size Compreseed"] = convert_size(total_compressed_size/repeat)
        dict["Original Compress Time"] = seconds_to_hh_mm_ss(seconds=total_compress_time/repeat)
        dict["Original Decompress Time"] = seconds_to_hh_mm_ss(seconds=total_decompress_time/repeat)
    else:
        dict['Prep: Total compressed size'] = convert_size(total_compressed_size/repeat)
        dict['Prep: Total compression time'] = seconds_to_hh_mm_ss(seconds=total_compress_time/repeat)
        dict['Prep: Total decompression time'] = seconds_to_hh_mm_ss(seconds=total_decompress_time/repeat)

    return dict


if __name__ == '__main__':

    compressor = list(compressor_config.keys())

    output_root_dir = '../data'
    input_dir = '../logs'
    repeat = 100

    chunkSize = ['16K', '32K', '64K', '128K']

    # If disable, this will block step1 to step4 one by one
    # Only works for evaluating LogBlock
    isDisable = False

    result_out_path = '../result/logblock_preprocess_result.csv'
    if not os.path.isdir('../result'):
        os.makedirs('../result')

    # Remove if exist
    if os.path.isfile(result_out_path): os.remove(result_out_path)

    start_time = datetime.now()

    jobs = []

    input_dir = '../logs'

    for dataset, setting in benchmark_settings.items():
        inputs = sys.argv

        if len(inputs) == 1:
            # Dataset to be evaluated
            eval_datasets = benchmark_settings.keys()
        else:
            eval_datasets = inputs[1].split(',')

        if dataset in eval_datasets:
            # Disable multiprocessing to capture time info;
            #@run_async_multiprocessing
            def parallel_processing(
                    dataset,
                    setting,
                    output_root_dir,
                    compressor,
                    chunkSize,
                    repeat,
                    ):
                output_data_dir = os.path.join(output_root_dir, 'logblock')
                if not os.path.isdir(output_data_dir):
                    os.makedirs(output_data_dir)
                dict = logblock(
                    dataset=dataset,
                    setting=setting,
                    output_root_dir=output_data_dir,
                    compressor=compressor,
                    chunkSizeList=chunkSize,
                    repeat=repeat,
                    isDisable=isDisable
                )
                if len(dict) == 0:
                    return None

                df = pd.DataFrame()
                df = df.append(dict, ignore_index=True)

                df = df.reindex(sorted(df.columns), axis=1)

                with lock:
                    if os.path.isfile(result_out_path):
                        df.to_csv(result_out_path, mode='a', index=False, header=False)
                    else:
                        df.to_csv(result_out_path, mode='w', index=False, header=True)


            jobs.append(parallel_processing(
                    dataset=dataset,
                    setting=setting,
                    output_root_dir=output_root_dir,
                    compressor=compressor,
                    chunkSize=chunkSize,
                    repeat=repeat,))

    # Wait all till finished
    #[j.join() for j in jobs]

    print('Pre-processing done. [Time taken: {!s}]'.format(datetime.now() - start_time))
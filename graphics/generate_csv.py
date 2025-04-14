import json
import os
import sys
import pandas as pd
import re

from os import listdir
from os.path import isfile, join, isdir
import re


# pd.options.mode.chained_assignment = None


def get_path_file(filename, subfolder=""):
    file_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(file_dir, subfolder, filename)
    return file_path


def oos(line):
    return 'Rejected Transactions (Server Retry)' in line or \
        'Rejected Transactions (Retry Different)' in line or \
        'Unexpected SQL Errors' in line or \
        'Unknown Status Transactions' in line


def is_out_of_scope(name, subfolder=""):
    file_path = get_path_file(name, subfolder)
    f = open(file_path, 'r')
    content = f.read()
    content = content.split('\n')
    indexes = [i for i in range(0, len(content))
               if oos(content[i])]
    empty = [i for i in indexes if '<EMPTY>' in content[i + 1]]
    return len(empty) != 4


def has_deadlock(name, subfolder=""):
    file_path = get_path_file(name, subfolder)
    f = open(file_path, 'r')
    content = f.read()
    return 'org.postgresql.util.PSQLException: ERROR: deadlock detected' in content


def load_csv(name, subfolder=""):
    file_path = get_path_file(name, subfolder)
    return pd.read_csv(file_path, sep=';')


def load_json(name, subfolder=""):
    file_path = get_path_file(name, subfolder)
    f = open(file_path, "r")
    jsonfile = json.loads(f.read())
    f.close()
    return jsonfile


def setSourceDir():
    os.chdir(os.path.expanduser(".."))


def getPath(folder):
    return os.getcwd() + "/" + folder


def fix_encoding(path, csvfile):
    file = path + "/" + csvfile
    file_path = get_path_file(file, "")
    f = open(file_path, 'r')
    content = f.read().split('\n')
    f.close()
    new_content = []
    for line in content:
        new_content.append(re.sub('([,](?![^[]*\]))', ';', line))
    new_content = '\n'.join(new_content)

    f = open(file_path, 'w')
    f.write(new_content)
    f.close()


def process_files(path, csvfile, jason_file, output, i):
    jfile = path + "/" + jason_file
    ofile = path + "/" + output

    file = path + "/" + csvfile

    df = load_csv(file)
    data = load_json(jfile)
    df = df.rename(columns=lambda x: x.strip())

    sizes = list(map(int, df['Sizes'].iloc[0].strip(' ][').split(', ')))
    df['Case'] = int(i)
    df['Number of transactions'] = (int(df['Total Size']) - sizes[0]) / 3
    df['Running Time (ms)'] = data['Elapsed Time (nanoseconds)'] / 1000000
    df['Timeout'] = df['Timeout'].apply(lambda x: x.strip())
    df['Consistent'] = df['Consistent'].apply(lambda x: x.strip())
    df['OOS'] = str(is_out_of_scope(ofile))
    df['Deadlock'] = str(has_deadlock(ofile))
    return df


def process_avg_case(df_case, case):
    df_map = {}
    df_map['Case'] = int(case)
    df_map['Evaluation Time (ms)'] = df_case['Evaluation Time (ms)'].mean()
    df_map['Creation Time (ms)'] = df_case['Creation Time (ms)'].mean()
    df_map['Running Time (ms)'] = df_case['Running Time (ms)'].mean()
    df_map['Time (ms)'] = df_case['Time (ms)'].mean()
    df_map['Number of transactions'] = df_case['Number of transactions'].mean()
    df_map['Timeout'] = (df_case['Timeout'] != 'true').all().astype(bool)
    df_map['Consistent'] = (df_case['Consistent'] != 'Unknown').all().astype(bool)
    return pd.DataFrame.from_records([df_map], index=[0])


def process_init_case():
    df_map = {}
    df_map['Evaluation Time (ms)'] = 0
    df_map['Creation Time (ms)'] = 0
    df_map['Running Time (ms)'] = 0
    df_map['Time (ms)'] = 0
    df_map['Number of transactions'] = int(1)
    df_map['Timeout'] = True
    df_map['Consistent'] = True
    df_map['Real'] = False
    df_map['Case'] = 1
    return pd.DataFrame.from_records([df_map], index=[0])


def getFileMap(folder):
    path = getPath(folder)
    case_names = [f for f in listdir(path) if isdir(join(path, f))]
    files = {}
    jsons = {}
    outputs = {}

    for c in case_names:
        path_case = path + "/" + c
        n = len(c) - 5
        case = c[-n:-3]
        example = c[-3:]

        if case not in files:
            files[case] = {}
            jsons[case] = {}
            outputs[case] = {}

        files[case][example] = [f for f in listdir(path_case)
                            if (isfile(join(path_case, f)) and "histories.csv" in join(path_case, f))]
        jsons[case][example] = [f for f in listdir(path_case)
                                if (isfile(join(path_case, f)) and "summary" in join(path_case, f))]
        outputs[case][example] = [f for f in listdir(path_case)
                                  if (isfile(join(path_case, f)) and "output" in join(path_case, f))]

        if not files[case][example] or not jsons[case][example]:
            print("ERROR with case " + case + example)
            del files[case][example]
            del jsons[case][example]
            del outputs[case][example]
        elif len(files[case][example]) > 4 : #to be fixed
            print("SOS " + case + ", " + example)
        else:
            f = files[case][example]
            j = jsons[case][example][0]
            o = outputs[case][example][0]
            files[case][example] = f, j, o


    return files



def generateDF(folder, benchmarkName, isolation, files):
    path = getPath(folder)

    df = pd.DataFrame(columns=['Number of transactions', 'Real', 'Timeout', 'Consistent'])
    df_all = pd.DataFrame(columns=['Number of transactions', 'Case', 'Sub-case'])

    df['Timeout'] = df['Timeout'].astype(bool)
    df['Consistent'] = df['Consistent'].astype(bool)
    df['Real'] = df['Real'].astype(bool)

    deadlocks = 0
    oos = 0

    map_case = {}

    total_cases = 0

    for c, example_map in files.items():
        # df_case = pd.DataFrame(columns=['Case'])
        if not bool(example_map.items()):
            continue
        for e, (fileCSV, jason, output) in example_map.items():
            total_cases += 1
            path_case = path + "/case-" + c + e

            df_results = process_files(path_case, fileCSV, jason, output, c)
            # df_case = pd.concat([df_case, df_results], ignore_index=True)

            num_case = int(c)

            if num_case not in map_case:
                map_case[num_case] = \
                    pd.DataFrame(columns=['Case'])

            map_case[num_case] = pd.concat([map_case[num_case], df_results])

            df_results['Sub-case'] = e
            df_all = pd.concat([df_all, df_results], ignore_index=True)
            if df_results['Deadlock'].iloc[0] == 'True':
                # print("Deadlock at case: " + c + e)
                deadlocks += 1
            if df_results['OOS'].iloc[0] == 'True':
                # print("OOS at case: " + c + e)
                oos += 1

    max_key = max(map_case) + 1
    delta = process_init_case()
    for case in range(1, max_key):
        if case in map_case:
            df_case = map_case[case]
            df_case_avg = process_avg_case(df_case, case)
            df_case_avg['Real'] = True

        else:
            df_case_avg = delta
            df_case_avg['Number of transactions'] = case
            df_case_avg['Real'] = False

        df_case_avg['Real'] = df_case_avg['Real'].astype(bool)

        df = pd.concat([df, df_case_avg], ignore_index=True)
        delta = df_case_avg


    df['Case'] = df['Case'].astype(int)
    df_all['Case'] = df_all['Case'].astype(int)
    df = df.sort_values(by=['Case'])
    df_all = df_all.sort_values(by=['Case', 'Sub-case'])

    df = df[['Case', 'Number of transactions', 'Time (ms)', 'Evaluation Time (ms)',
             'Creation Time (ms)', 'Running Time (ms)']]
    #df = df.round(3)
    #df_all = df_all.round(3)
    df.to_csv(folder + '/' + benchmarkName + '-' + isolation + '-data.csv', index=False, encoding='utf-8', sep=";",
              float_format='%.3f')
    df_all.to_csv(folder + '/' + benchmarkName + '-' + isolation + '-data-all.csv', index=False, encoding='utf-8',
                  sep=";", float_format='%.3f')

    print('Deadlocks ' + str(deadlocks))
    print('OOS ' + str(oos))
    timeouts = len(df_all[df_all['Timeout'] == 'true'])
    print('Timeouts ' + str(timeouts) + ' (' +str(100*timeouts/total_cases) +'%)')

def listFiles(folder, isolation):
    folder = folder + "/" + isolation
    return getFileMap(folder)




def get_csob_calls(path, output):
    ofile = path + "/" + output
    file_path = get_path_file(ofile)
    f = open(file_path, 'r')
    content = f.read()
    f.close()

    if 'timeout' in content:
        to = True
    else:
        to = False

    if 'Unique CSOB call' in content:
        calls = 1
    elif 'Multiple CSOB calls' not in content:
        calls = -1
    else:
        content = content.split('\n')
        lines = [content[i] for i in range(0, len(content))
                 if ('checkSOBound - Call #' in content[i]
                    and ': ' not in content[i])]
        endlines = [int(i.partition('checkSOBound - Call #')[2]) for i in lines]

        calls = max(endlines) + 1

    return to, calls


def compute_statistics(folder, benchmarkName, size):
    isolations = ['SER', 'SI', 'RC', 'SER+RC', 'SI+RC']
    stat = {}
    for iso in isolations:
        folder_case = folder + '/' + iso
        files = getFileMap(folder_case)
        path = getPath(folder_case)

        stat[iso] = {}
        for i in range(0,size):
            stat[iso][i] = {}
            stat[iso][i]['Timeout Xh'] = 0
            stat[iso][i]['Timeout Unique'] = 0
            stat[iso][i]['Timeout Multiple'] = 0
            stat[iso][i]['Good Unique'] = 0
            stat[iso][i]['Good Multiple'] = 0
            stat[iso][i]['Timeout'] = 0


        for c, example_map in files.items():
            # df_case = pd.DataFrame(columns=['Case'])
            if not bool(example_map.items()):
                continue
            for e, (fileCSV, jason, output) in example_map.items():
                path_case = path + "/case-" + c + e
                to, calls = get_csob_calls(path_case, output)
                id = (int(c) - 1) // 10
                if to and calls == -1:
                    stat[iso][id]['Timeout Xh'] += 1
                    stat[iso][id]['Timeout'] += 1
                elif to and calls == 1:
                    stat[iso][id]['Timeout Unique'] += 1
                    stat[iso][id]['Timeout'] += 1
                elif to and calls > 1:
                    stat[iso][id]['Timeout Multiple'] += 1
                    stat[iso][id]['Timeout'] += 1
                elif not to and calls == 1:
                    stat[iso][id]['Good Unique'] += 1
                elif not to and calls > 1:
                    stat[iso][id]['Good Multiple'] += 1
                else:
                    print('Error in case ' + c + e + ': '
                          + str(to) + ', ' + str(calls))

    df = pd.DataFrame()

    for k, v in stat.items():

        df_iso = pd.DataFrame.from_dict(v, orient='index').reset_index()
        df_iso = df_iso.rename(columns={'index': 'Group'})
        df_iso['Isolation'] = k
        df = pd.concat([df, df_iso], ignore_index=True)
        print(df_iso.sum())


    columns = ['Isolation', 'Group', 'Timeout', 'Timeout Xh', 'Timeout Unique',
               'Timeout Multiple', 'Good Unique', 'Good Multiple']
    df = df[columns]
    #df = df.round(3)
    df.to_csv(folder + '/' + benchmarkName + '-stats.csv', index=False, encoding='utf-8', sep=";", float_format='%.3f')


def generate_scala(benchmark, case, isolations, lim):
    #isolations = ['SI+RC']
    #isolations = ['SER', 'RC', 'SI+RC']
    #isolations = ['SER', 'SI', 'RC', 'SER+RC', 'SI+RC']

    folder_basic = "results/testFiles/" + case + "/" + benchmark +"Histories"

    for iso in isolations:
        print(case + " -  " + benchmark + " - " + iso)
        fileMap = listFiles(folder_basic, iso)
        file_map_ = {}
        for c, examples in fileMap.items():
            if int(c) > int(lim):
                continue
            elif c not in file_map_:
                file_map_[c] = {}
            for e, (files, j, o) in examples.items():
                file_map_[c][e] = files[0], j, o
        folder = folder_basic + "/" + iso
        generateDF(folder, benchmark, iso, file_map_)

    #compute_statistics(folder_basic, benchmark, 5)


def generate_invalid(benchmark, case, lim):

    folder = "results/testFiles/" + case + "/" + benchmark +"Histories"

    file_map = listFiles(folder, 'Naive-vs-CheckSOBound')
    naives = {}
    csobs = {}
    for c, examples in file_map.items():
        if int(c) > int(lim):
            continue
        if c not in naives:
            naives[c] = {}
            csobs[c] = {}
        for e, (files, j, o) in examples.items():
            for f in files:
                if "Naive" in f:
                    naives[c][e] = f, j, o
                else:
                    csobs[c][e] = f, j, o
    folder += "/" + "Naive-vs-CheckSOBound"
    generateDF(folder, benchmark, 'Naive', naives)
    generateDF(folder, benchmark, 'CSOB', csobs)



if __name__ == "__main__":
    setSourceDir()
    pd.options.display.precision = 3
    pd.options.styler.format.precision = 3
    pd.set_option('float_format', '{:.3f}'.format)

    print('Number of arguments:', len(sys.argv), 'arguments.')
    print('Argument List:', str(sys.argv))
    for i in range(1, len(sys.argv), 5):
        scala=sys.argv[i+4]
        isolations=sys.argv[i + 2].split(',')
        print(isolations)
        if scala == 'true':
            generate_scala(sys.argv[i], sys.argv[i+1], isolations, sys.argv[i+3])
        else:
            generate_invalid(sys.argv[i], sys.argv[i+1], sys.argv[i+3])

    #generate_invalid("twitter", 20)
    #generate_invalid("tpcc", 20)




"""
twitter
Transaction-Scalability
18
tpcc
Transaction-Scalability
18
tpccPC
Transaction-Scalability
18
"""

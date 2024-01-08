import json
import os
import sys
import pandas as pd
import re

from os import listdir
from os.path import isfile, join, isdir


# pd.options.mode.chained_assignment = None


def get_path_file(filename, subfolder=""):
    file_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(file_dir, subfolder, filename)
    return file_path


def has_deadlock(name, subfolder=""):
    file_path = get_path_file(name, subfolder)
    f = open(file_path, 'r')
    content = f.read()
    return 'org.postgresql.util.PSQLException: ERROR: deadlock detected' in content

def load_csv(name, subfolder=""):
    file_path = get_path_file(name, subfolder)
    return pd.read_csv(file_path, sep=',')

def load_json(name, subfolder=""):
    file_path = get_path_file(name, subfolder)
    f = open (file_path, "r")
    return json.loads(f.read())


def setSourceDir():
    os.chdir(os.path.expanduser("../target/benchbase-postgres"))

def getPath(folder):
    return os.getcwd() + "/" + folder


def process_file(path, csvfile, jason_file, output, i):
    file = path + "/" + csvfile
    jfile = path + "/" + jason_file
    ofile = path + "/" + output

    if has_deadlock(ofile):
        return pd.DataFrame({'Case' : []})

    df = load_csv(file)
    data = load_json(jfile)
    df['Case'] = int(i)
    df['Running Time (ms)'] = data['Elapsed Time (nanoseconds)'] / 1000000
    df = df.rename(columns=lambda x: x.strip())
    df['Timeout'] = df['Timeout'].apply(lambda x: x.strip())
    df['Consistent'] = df['Consistent'].apply(lambda x: x.strip())
    return df


def process_avg_case(df_case, case):
    df_map = {}
    df_map['Evaluation Time (ms)'] = df_case['Evaluation Time (ms)'].mean()
    df_map['Creation Time (ms)'] = df_case['Creation Time (ms)'].mean()
    df_map['Running Time (ms)'] = df_case['Running Time (ms)'].mean()
    df_map['Time (ms)'] = df_case['Time (ms)'].mean()
    df_map['Case'] = int(case)
    df_map['Timeout'] = (df_case['Timeout'] != 'true').all().astype(bool)
    df_map['Consistent'] = (df_case['Consistent'] != 'Unknown').all().astype(bool)
    return pd.DataFrame.from_records([df_map], index=[0])


def listFiles(folder, name):
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
                            if (isfile(join(path_case, f)) and "histories" in join(path_case, f))]
        jsons[case][example] = [f for f in listdir(path_case)
                            if (isfile(join(path_case, f)) and "summary" in join(path_case, f))]
        outputs[case][example] = [f for f in listdir(path_case)
                                if (isfile(join(path_case, f)) and "output" in join(path_case, f))]

        if not files[case][example] or not jsons[case][example]:
            print("ERROR with case " + case + "(" + example + ")")
            del files[case][example]
            del jsons[case][example]
            del outputs[case][example]
        else:
            f = files[case][example][0]
            j = jsons[case][example][0]
            o = outputs[case][example][0]
            files[case][example] = f, j, o

    df = pd.DataFrame(columns=['Case', 'Timeout', 'Consistent'])
    df_all = pd.DataFrame(columns=['Case'])

    df['Timeout'] = df['Timeout'].astype(bool)
    df['Consistent'] = df['Consistent'].astype(bool)

    deadlocks = 0

    for c, example_map in files.items():
        df_case = pd.DataFrame(columns=['Case'])
        if not bool(example_map.items()):
            continue
        for e, (fileCSV, jason, output) in example_map.items():
            path_case = path + "/case-" + c + e
            df_results = process_file(path_case, fileCSV, jason, output, c)
            df_all = pd.concat([df_all, df_results], ignore_index=True)
            df_case = pd.concat([df_case, df_results], ignore_index=True)
            if df_results.empty:
                print("Deadlock at case: " + c + e)
                deadlocks += 1

        df_case = process_avg_case(df_case, c)

        df = pd.concat([df, df_case], ignore_index=True)


    # print(df.head())
    df = df.sort_values(by=['Case'])
    df_all = df_all.sort_values(by=['Case'])

    df.to_csv(folder + '/' + name + '-data.csv', index=False, encoding='utf-8', sep=";")
    df_all.to_csv(folder + '/' + name + '-data-all.csv', index=False, encoding='utf-8', sep=";")

    print(deadlocks)


if __name__ == "__main__":
    setSourceDir()
    print('Number of arguments:', len(sys.argv), 'arguments.')
    print('Argument List:', str(sys.argv))
    for i in range(1, len(sys.argv)):

        folder = "results/testFiles/" + sys.argv[i]

        listFiles(folder, sys.argv[i])

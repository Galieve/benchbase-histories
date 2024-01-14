import csv
import os
import sys
import time
from datetime import datetime
from math import log10

import pandas
from matplotlib.ticker import MaxNLocator

import pandas as pd
import ast
import numpy as np
import warnings
import matplotlib.pyplot as plt
from datetime import timedelta
from matplotlib import collections as matcoll
import matplotlib.ticker as tick
from textwrap import wrap
from mpl_toolkits.axes_grid1 import make_axes_locatable

pd.options.mode.chained_assignment = None


def get_path_file(filename, subfolder=""):
    file_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(file_dir, subfolder, filename)
    return file_path


def set_source_dir():
    os.chdir(os.path.expanduser("../target/benchbase-postgres"))


def get_path(folder):
    return os.getcwd() + "/" + folder


def load_csv(name, subfolder=""):
    file_path = get_path_file(name, subfolder)
    return pd.read_csv(file_path, sep=';')


def set_font(names):
    plt.rcParams["font.family"] = names


def time_to_int(time_string):
    h, m, s = time_string.split(':')
    return int(h) * 3600 + int(m) * 60 + int(s)


def time_to_int_nan(time_string):
    t = time_to_int(time_string)
    if t >= 1800 - 1:
        return np.nan
    else:
        return t


def values_if_nan(h, e, t, m):
    if np.isnan(t):
        return np.nan, np.nan, np.nan
    else:
        return int(h), int(e), int(m)


def prepare_dataframe_cactus(file, n):
    df = load_csv(file + ".csv")

    df['Time'] = df['Time'].map(lambda x: time_to_int_nan(x) / 60)
    df['Histories'], df['End States'], df['Memory'] = \
        df.apply(lambda r: values_if_nan(r['Histories'], r['End States'], r['Time'], r['Memory']), axis=1).str

    df['Histories'] = df['Histories'].map(lambda x: x / 1000)
    df['End States'] = df['End States'].map(lambda x: x / 1000)
    df['Memory'] = df['Memory'].map(lambda x: (x) / 1024)

    return process_dataframe_application(df)


def prepare_dataframe_parameters(file, n):
    df = load_csv(file + ".csv")
    df['Time'] = df['Time'].map(lambda x: time_to_int_nan(x))
    df['Histories'], df['End States'], df['Memory'] = \
        df.apply(lambda r: values_if_nan(r['Histories'], r['End States'], r['Time'], r['Memory']), axis=1).str

    df['Histories'] = df['Histories'].map(lambda x: x / 1000)
    df['End States'] = df['End States'].map(lambda x: x / 1000)
    df['Memory'] = df['Memory'].map(lambda x: x)


def prepare_dataframe_scalability(file, parameter, n):
    df = load_csv(file + ".csv")

    df['Time'] = df['Time'].map(lambda x: time_to_int(x))
    df['Histories'], df['End States'], df['Memory'] = df.apply(
        lambda r: values_if_nan(r['Histories'], -1, r['Time'], r['Memory']), axis=1).str

    df['Histories'] = df['Histories'].map(lambda x: x / 1000)
    df['Memory'] = df['Memory'].map(lambda x: x / 1024)
    return process_dataframe_trso(df, parameter, n)


def process_dataframe_application(df):
    labels_app = [('Causal', 'Causal'), ('Causal', 'SnapshotIsolation'),
                  ('Causal', 'Serializable'), ('ReadAtomic', 'Causal'),
                  ('ReadCommitted', 'Causal'), ('Trivial', 'Causal'),
                  ('Naive', 'Causal')]

    i = 1
    # 'Case', 'Base ISO', 'True ISO', 'Histories', 'End States', 'Time', 'Memory'
    other_df = pd.DataFrame()
    other_df['Benchmark'] = df['Case']
    other_df = other_df.drop_duplicates()
    other_df.set_index('Benchmark')

    i = 1
    for base, true in labels_app:
        aux_df = df.loc[(df['Base ISO'] == base) & (df['True ISO'] == true)]
        other_df['Histories' + str(i)] = aux_df['Histories'].values
        other_df['Time' + str(i)] = aux_df['Time'].values
        other_df['Mem.' + str(i)] = aux_df['Memory'].values
        other_df['End states' + str(i)] = aux_df['End States'].values

        i += 1
    other_df = other_df.reset_index()
    return other_df


def process_dataframe_trso(df, parameter, n):
    labels_app = [str(i) for i in range(1, n + 1)]

    i = 1
    # 'Case', 'Base ISO', 'True ISO', 'Histories', 'End States', 'Time', 'Memory'
    other_df = pd.DataFrame()
    other_df['Benchmark'] = df['Case']
    other_df = other_df.drop_duplicates()
    other_df.set_index('Benchmark')

    i = 1
    for case in labels_app:
        aux_df = df.loc[(df[parameter] == i)]
        other_df['Histories' + case] = aux_df['Histories'].values
        other_df['Time' + case] = aux_df['Time'].values
        other_df['Mem.' + case] = aux_df['Memory'].values
        other_df['End states' + case] = aux_df['End States'].values

        i += 1

    return other_df


def plot_cactus(file, saved_file, field, labels, colors):
    n = len(labels) + 1

    df = prepare_dataframe_cactus(file, n)

    fig = plt.figure()
    # plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=None, hspace=None)

    ax = fig.add_axes((0.1, 0.1, 0.85, 0.85))

    plt.rc('figure', titlesize=20)
    ax.set_xlabel('Number of benchmarks')
    ax.set_ylabel('Time (min)')
    plt.gca().set_prop_cycle(plt.cycler('color', plt.cm.get_cmap('Dark2_r')(np.linspace(0, 1, n))))
    plt.xlim([0 - 0.25, len(df.index) - 1 + 0.25])

    for i_s in range(1, n):
        i = str(i_s)
        algo = df[['Benchmark', 'Histories' + i, 'End states' + i, 'Time' + i, 'Mem.' + i]]
        algo = algo.sort_values(by=[field + i])
        algo['Time' + i] = algo['Time' + i].cumsum()
        # algo = algo.dropna(subset='Time'+i)
        # lines = [(i, t) for (i,t) in zip( algo['Time'+i], df.index)]
        plt.plot(df.index, algo[field + i], label=labels[i_s - 1], linewidth=3, color=colors[i_s - 1])

    plt.legend(loc="lower right", borderpad=0.1, fontsize=11)

    # plt.show()
    print('Saving...')
    fig.savefig(saved_file + '.eps', format='eps', bbox_inches='tight')

    print('Saved! :)')

    return


def plot_cactus_mem(file, saved_file, field, labels, colors):
    n = len(labels) + 1

    df = prepare_dataframe_cactus(file, n)

    fig = plt.figure()
    ax = fig.add_axes((0.15, 0.15, 0.8, 0.8))

    plt.rc('figure', titlesize=20)
    # plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=None, hspace=None)

    ax.set_xlabel('Number of benchmarks')
    ax.set_ylabel('Memory (GB)')
    plt.gca().set_prop_cycle(plt.cycler('color', plt.cm.get_cmap('Dark2_r')(np.linspace(0, 1, n))))
    plt.xlim([0 - 0.25, len(df.index) - 1 + 0.25])
    plt.ylim([0 - 0.25, 32 + 0.25])

    for i_s in range(1, n):
        i = str(i_s)
        algo = df[['Benchmark', 'Histories' + i, 'End states' + i, 'Time' + i, 'Mem.' + i]]
        algo = algo.sort_values(by=[field + '.' + i])
        algo['Mem' + i] = algo['Mem.' + i].cumsum()
        # print(algo)
        # lines = [(i, t) for (i,t) in zip( algo['Time'+i], df.index)]

        plt.plot(df.index, algo[field + i], label=labels[i_s - 1], linewidth=3, color=colors[i_s - 1])

    plt.legend(loc="upper right", borderpad=0.1, fontsize=11)

    print('Saving...')

    fig.savefig(saved_file + '.eps', format='eps', bbox_inches='tight')
    # plt.show()

    print('Saved! :)')

    return


def plot_cactus_histories(file, saved_file, labels, colors):
    n = len(labels) + 1

    df = prepare_dataframe_cactus(file, n)

    fig = plt.figure()
    ax = fig.add_axes((0.1, 0.1, 0.85, 0.85))

    plt.rc('figure', titlesize=20)
    # plt.subplots_adjust(left=0.1, bottom=None, right=None, top=None, wspace=None, hspace=None)

    ax.set_xlabel('Number of benchmarks')
    ax.set_ylabel('Number of histories ($\\times 10^3$)')
    plt.gca().set_prop_cycle(plt.cycler('color', plt.cm.get_cmap('Dark2_r')(np.linspace(0, 1, n))))
    plt.xlim([0 - 0.25, len(df.index) - 1 + 0.25])

    for i_s in range(1, n):
        i = str(i_s)
        if i_s > 1:
            df_labels = ['Benchmark', 'Histories' + i, 'End states' + i, 'Time' + i, 'Mem.' + i, 'Histories1']
        else:
            df_labels = ['Benchmark', 'Histories' + i, 'End states' + i, 'Time' + i, 'Mem.' + i]
        algo = df[df_labels]
        if i_s <= 3:
            field = 'Histories'
        else:
            field = 'End states'

        algo = algo.sort_values(by=[field + i])

        algo[field + i] = algo[field + i].cumsum()

        plt.plot(df.index, algo[field + i], label=labels[i_s - 1], linewidth=3, color=colors[i_s - 1])

    plt.legend(loc="upper right", borderpad=0.1, fontsize=11)

    print('Saving...')
    fig.savefig(saved_file + '.eps', format='eps', bbox_inches='tight')
    # plt.show()

    print('Saved! :)')

    return


def plot_scalability(file, parameter, saved_file, object_name, n):
    df = prepare_dataframe_scalability(file, parameter, n - 1)

    bench = len(df.index)
    fig = plt.figure()
    ax = fig.add_axes((0.15, 0.15, 0.85, 0.85))
    ax2 = ax.twinx()

    plt.rc('figure', titlesize=20)
    ax.set_xlabel('Number of ' + object_name)
    ax.set_ylabel('Time (mins)')
    ax2.set_ylabel('Memory (GB)', labelpad=10)
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    ax2.yaxis.set_major_locator(MaxNLocator(integer=True))

    cmap = plt.cm.get_cmap('Dark2')

    plt.xlim([1 - 0.05, n - 1 + 0.05])
    ax2.set_ylim([0 - 0.25, 30 + 0.25])

    ax2.set_ylim([0 - 0.25, 16 - 0.25])

    # plt.ylim([0 - 0.25, 30 + 0.25])

    memory = [df['Mem.' + str(i)].mean() for i in range(1, n)]
    time = [df['Time' + str(i)].mean() / 60 for i in range(1, n)]

    t_lab = ax.plot(range(1, n), time, label='Avg. time', linewidth=4, color=cmap(4 / 8 - 0.05))
    m_lab = ax2.plot(range(1, n), memory, label='Avg. memory', linewidth=4, color=cmap(1 / 8 - 0.05))
    lns = t_lab + m_lab
    labs = [l.get_label() for l in lns]
    plt.legend(lns, labs, loc="upper left")

    print('Saving...')
    fig.savefig(saved_file + '.eps', format='eps', bbox_inches='tight')

    print('Saved! :)')

    return


def calculate_parameters(mode, labels):
    n = len(labels) + 1

    folder = get_path(mode)
    file = folder + '/data'

    df = prepare_dataframe_cactus(file, n)

    results = {}
    for i_s in range(1, n):
        i = str(i_s)
        if i_s > 1:
            algo = df[['Benchmark', 'Histories' + i, 'End states' + i, 'Time' + i, 'Mem.' + i, 'Time1']]
            algo['Mem.' + i] = algo['Mem.' + i].map(lambda x: (x) * 1024)

            algo.loc[:, 'SpeedUp' + i] = algo['Time' + i] / algo['Time1']
            results['SpeedUp' + i + '-average'] = algo['SpeedUp' + i].mean()
        else:
            algo = df[['Benchmark', 'Histories' + i, 'End states' + i, 'Time' + i, 'Mem.' + i]]
            algo['Mem.' + i] = algo['Mem.' + i].map(lambda x: (x) * 1024)

            results['SpeedUp' + i + '-average'] = 1

        results['Time' + i + '-average'] = algo['Time' + i].map(lambda x: x).mean().squeeze()

        # results['Time'+i+'-min'] = algo['Time'+i].min()
        # results['Time'+i+'-max'] = algo['Time'+i].max()
        # results['SpeedUp'+i+'-min'] = algo['SpeedUp'+i].min()
        # results['SpeedUp'+i+'-max'] = algo['SpeedUp'+i].max()
        # results['Mem.'+i+'-min'] = algo['Mem.'+i].min()
        # results['Mem.'+i+'-max'] = algo['Mem.'+i].max()
        results['Mem.' + i + '-average'] = algo['Mem.' + i].mean()
        if i_s <= 3:
            field = 'Histories'
        else:
            field = 'End states'

        algo[field + i] = algo[field + i].map(lambda x: x)

        results['Histories' + i + '-average'] = algo[field + i].mean()
        # results['Histories'+i+'-min'] = algo[field+i].min()
        # results['Histories'+i+'-max'] = algo[field+i].max()

    results = {k: "%.5f" % v for k, v in results.items()}

    with open(folder + '/results.csv', 'w') as f:  # You will need 'wb' mode in Python 2.x
        w = csv.DictWriter(f, results.keys())
        w.writeheader()
        w.writerow(results)


def calculate_parameters_2(file):
    n = 5 + 1
    df = prepare_dataframe_scalability(file, n)

    memory = [df['Mem.' + str(i)].map(lambda x: x * 1024).mean() for i in range(1, n)]
    print(np.array(memory).mean())


def plot_depending_on_mode(mode):
    folder = get_path(mode)
    labels = ["\\texttt{CC}", "\\texttt{CC} + \\texttt{SI}",
              "\\texttt{CC} + \\texttt{SER}", "\\texttt{RA} + \\texttt{CC}",
              "\\texttt{RC} + \\texttt{CC}", "\\texttt{true} + \\texttt{CC}", "DFS(\\texttt{CC})"]

    colors = ['#734d26', '#0066ff', '#cc9900', '#009933', '#d147a3', '#5900b3', '#e63900']
    n = 6
    if mode.startswith('demo'):
        n = 4
    if mode.endswith('application-scalability'):
        plot_cactus(folder + '/data', folder + '/app-scala-Time', 'Time', labels, colors)
        plot_cactus_mem(folder + '/data', folder + '/app-scala-Mem', 'Mem', labels, colors)
        plot_cactus_histories(folder + '/data', folder + '/app-scala-histories', labels, colors)
    elif mode.endswith('session-scalability'):
        plot_scalability(folder + '/data', 'Session', folder + '/se-scala', 'sessions', n)
    elif mode.endswith('transaction-scalability'):
        plot_scalability(folder + '/data', 'Transaction', folder + '/tr-scala', 'transactions per session', n)


def filter(values):
    i = 0;
    result = [];

    while (i < len(values)):
        element = values[i]
        if element > 0:
            result.add(element)
    ++i

    return result


def parallel_filter(values):
    m = len(values) / 2
    result = []
    thread1 = Thread(filter, values[:m], result)
    thread2 = Thread(filter, values[m:], result)

    # execute threads
    ...
    return result


def plot_curve(folder, name, isolation, n):
    path = get_path(folder)
    data_path = path + name + '-' + isolation + '-data.csv'
    df = load_csv(data_path)

    maxX = df['Case'].max()
    plt.figure()
    plt.rc('figure', titlesize=20)

    df = df[['Running Time (ms)', 'Evaluation Time (ms)']]
    #df = df[['Running Time (ms)', 'Creation Time (ms)', 'Evaluation Time (ms)']]
    ax = df.plot.area()

    maxY = max(60000, sum(df.max(axis=0)))
    # ax = df["Time (ms)"].plot()
    # df['Running time (ms)'].plot()
    plt.title('Benchmark ' + name +'(' + isolation + ')'+ ' with ' + n + ' sessions')

    ax.set_xlabel('Number of transactions per session')
    ax.set_ylabel('Time (ms)')

    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    plt.xlim([1 - 0.05, maxX - 1 + 0.05])
    plt.ylim([0 - 0.25, maxY + 0.25])
    print('Saving...')

    plt.savefig(path + name + '-' + isolation + '.eps', format='eps', bbox_inches='tight')
    plt.savefig(path + name + '-' + isolation + '.png', format='png', bbox_inches='tight')

    print('Saved! :)')

    return


def aux():
    fig = plt.figure()
    ax = fig.add_axes((0.15, 0.15, 0.85, 0.85))

    plt.rc('figure', titlesize=20)
    ax.set_xlabel('Number of transaction per session')
    ax.set_ylabel('Time (ms)')
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))

    cmap = plt.cm.get_cmap('Dark2')

    plt.xlim([1 - 0.05, n - 1 + 0.05])

    time = df['Time (ms)']

    t_lab = ax.plot(range(1, n), time, label='Avg. time', linewidth=4, color=cmap(4 / 8 - 0.05))
    lns = t_lab + m_lab
    labs = [l.get_label() for l in lns]
    plt.legend(lns, labs, loc="upper left")

    print('Saving...')
    fig.savefig(saved_file + '.eps', format='eps', bbox_inches='tight')

    print('Saved! :)')

    return


def plot_timeout(folder, name, isolation, n):
    path = get_path(folder)
    data_path = path + name +'-' + isolation+ '-data-all.csv'
    df = load_csv(data_path)

    maxX = df['Case'].max()
    plt.figure()
    plt.rc('figure', titlesize=20)

    df = df[['Case', 'Timeout']]
    cases = list(set(map(lambda x: int(x), df['Case'].tolist())))
    timeouts = {}
    for c in cases:
        c_t = df[df['Case'] == c]
        t_case = c_t[c_t['Timeout'] == True]
        timeouts[c] = t_case['Timeout'].count() * 20

    plt.plot(timeouts.keys(), timeouts.values())
    ax = plt.gca()
    maxY = 100
    # ax = df["Time (ms)"].plot()
    # df['Running time (ms)'].plot()
    ax.set_title('Benchmark ' + name +'(' + isolation + ')'+ ' with ' + n + ' sessions')

    ax.set_xlabel('Number of transactions per session')
    ax.set_ylabel('Timeout probabilty')

    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    plt.xlim([1 - 0.05, maxX - 1 + 0.05])
    plt.ylim([0 - 0.25, maxY + 0.25])
    print('Saving...')

    plt.savefig(path + name+'-' + isolation + '-timeouts.eps', format='eps', bbox_inches='tight')
    plt.savefig(path + name +'-' + isolation+ '-timeouts.png', format='png', bbox_inches='tight')

    print('Saved! :)')

    return


def plot_oos(folder, name, isolation, n):
    path = get_path(folder)
    data_path = path + name +'-' + isolation+ '-data-all.csv'
    df = load_csv(data_path)

    maxX = df['Case'].max()
    plt.figure()
    plt.rc('figure', titlesize=20)

    df = df[['Case', 'OOS']]
    cases = list(set(map(lambda x: int(x), df['Case'].tolist())))
    timeouts = {}
    for c in cases:
        c_t = df[df['Case'] == c]
        t_case = c_t[c_t['OOS'] == True]
        timeouts[c] = t_case['OOS'].count() * 20

    plt.plot(timeouts.keys(), timeouts.values())
    ax = plt.gca()
    maxY = 100
    # ax = df["Time (ms)"].plot()
    # df['Running time (ms)'].plot()
    ax.set_title('Benchmark ' + name +'(' + isolation + ')'+ ' with ' + n + ' sessions')

    ax.set_xlabel('Number of transactions per session')
    ax.set_ylabel('OOS probabilty')

    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    plt.xlim([1 - 0.05, maxX - 1 + 0.05])
    plt.ylim([0 - 0.25, maxY + 0.25])
    print('Saving...')

    plt.savefig(path + name+'-' + isolation + '-oos.eps', format='eps', bbox_inches='tight')
    plt.savefig(path + name +'-' + isolation+ '-oos.png', format='png', bbox_inches='tight')

    print('Saved! :)')

    return



if __name__ == "__main__":
    plt.rc('text', usetex=True)
    font = {'family': 'serif', 'size': 16, 'serif': ['computer modern roman']}
    plt.rc('font', **font)
    plt.rc('legend', **{'fontsize': 14})

    set_source_dir()

    for i in range(1, len(sys.argv), 3):
        name = sys.argv[i]
        isolation = sys.argv[i + 1]
        sessions = sys.argv[i + 2]
        folder = "results/testFiles/" + name + "/" + isolation +"/"

        # calculate_parameters(mode, labels)
        # plot_depending_on_mode(mode)
        plot_curve(folder, name, isolation, sessions)
        plot_timeout(folder, name, isolation, sessions)
        plot_oos(folder, name, isolation, sessions)


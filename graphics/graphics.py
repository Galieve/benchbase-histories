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
from matplotlib.ticker import PercentFormatter, MultipleLocator
import seaborn as sns

pd.options.mode.chained_assignment = None


def get_path_file(filename, subfolder=""):
    file_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(file_dir, subfolder, filename)
    return file_path


def set_source_dir():
    os.chdir(os.path.expanduser(".."))


def get_path(folder):
    return os.getcwd() + "/" + folder


def load_csv(name, subfolder=""):
    file_path = get_path_file(name, subfolder)
    return pd.read_csv(file_path, sep=';')


def set_font(names):
    plt.rcParams["font.family"] = names


def plot_statistics(folder, name, name_plot, fig_name, n):
    path = get_path(folder)
    data_path = path + name + '-stats.csv'
    df = load_csv(data_path)
    df['Group'] = df['Group'] * 10
    df = df[['Group', 'Isolation', 'Timeout']]
    pivoted = df.pivot('Group', 'Isolation', 'Timeout')
    pivoted = pivoted[['SER', 'SER+RC', 'SI+RC', 'RC']]

    # df.set_index('Isolation configuration', inplace=True)

    plt.rc('figure', titlesize=20)
    ax = pivoted.plot.bar()
    # ax = df.plot(kind='bar', stacked=True, rot=0)

    # df = df[['Running Time (ms)', 'Creation Time (ms)', 'Evaluation Time (ms)']]

    maxY = 51
    # ax = df["Time (ms)"].plot()
    # df['Running time (ms)'].plot()
    plt.title('Benchmark ' + name_plot + ' with ' + n + ' sessions')

    ax.set_xlabel('Transactions per session')
    ax.set_ylabel('Timeout')
    ax.set_ylim([0 - 0.25, maxY + 0.25])

    ax.set_yticks(np.arange(0, 51, 10))

    # ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    # ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    # plt.xlim([1 - 0.05, maxX - 1 + 0.05])
    print('Saving...')

    path = get_path(folder)

    plt.savefig(path + fig_name + '-stats.eps', format='eps', bbox_inches='tight')
    plt.savefig(path + fig_name + '-stats.png', format='png', bbox_inches='tight')

    print('Saved! :)')
    plt.close()

    return


def plot_benchmark(folder, name, isolations, name_plot, fig_name, x_label, n):


    df_all = pd.DataFrame()

    for isolation in isolations:
        folder_iso = folder + isolation + "/"
        path = get_path(folder_iso)
        data_path = path + name + '-' + isolation + '-data.csv'
        df = load_csv(data_path)
        if 'Case' not in df_all:
            df_all['Case'] = df['Case']
        df_all[isolation] = df['Evaluation Time (ms)'] / 1000

    maxX = max(df_all['Case'])
    plt.rc('figure', titlesize=20)

    # df = df[['Running Time (ms)', 'Creation Time (ms)', 'Evaluation Time (ms)']]
    ax = df_all.plot(x='Case', y=isolations)

    maxY = 61
    # ax = df["Time (ms)"].plot()
    # df['Running time (ms)'].plot()
    plt.title('Benchmark ' + name_plot + ' with ' + n + ' sessions')

    ax.set_xlabel(x_label)
    ax.set_ylabel('Time (s)')
    ax.set_ylim([0 - 0.25, maxY + 0.25])

    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    ax.set_xlim([1 - 0.05, maxX + 0.05])
    ax.set_yticks(np.arange(0, 61, 10))

    # plt.ylim([0 - 0.25, maxY + 0.25])
    print('Saving...')

    path = get_path(folder)

    plt.savefig(path + fig_name + '.eps', format='eps', bbox_inches='tight')
    plt.savefig(path + fig_name + '.png', format='png', bbox_inches='tight')

    print('Saved! :)')
    plt.close()

    return


def plot_curve(folder, name, name_plot, isolation, n):
    path = get_path(folder)
    data_path = path + name + '-' + isolation + '-data.csv'
    df = load_csv(data_path)

    maxX = df['Case'].max()
    plt.figure()
    plt.rc('figure', titlesize=20)

    df = df[['Running Time (ms)', 'Evaluation Time (ms)']]
    # df = df[['Running Time (ms)', 'Creation Time (ms)', 'Evaluation Time (ms)']]
    ax = df.plot.area()

    maxY = max(60000, sum(df.max(axis=0)))
    # ax = df["Time (ms)"].plot()
    # df['Running time (ms)'].plot()
    plt.title('Benchmark ' + name_plot + '(' + isolation + ')' + ' with ' + n + ' sessions')

    ax.set_xlabel('Number of transactions per session')
    ax.set_ylabel('Time (ms)')

    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    plt.xlim([1 - 0.05, maxX - 1 + 0.05])
    plt.ylim([0 - 0.25, maxY + 0.25])
    print('Saving...')

    plt.savefig(path + name_plot + '-' + isolation + '.eps', format='eps', bbox_inches='tight')
    plt.savefig(path + name_plot + '-' + isolation + '.png', format='png', bbox_inches='tight')

    print('Saved! :)')

    return


def plot_timeout_oos(folder, name, name_plot, n, isTimeout):
    isolations = ['SER', 'SER+RC', 'SI+RC', 'RC']
    df_all = pd.DataFrame(columns=['SER', 'SER+RC', 'SI+RC', 'RC'])
    mode = "OOS"
    if isTimeout:
        mode = "Timeout"

    for isolation in isolations:
        folder_iso = folder + isolation + "/"
        path = get_path(folder_iso)
        data_path = path + name + '-' + isolation + '-data-all.csv'
        df = load_csv(data_path)
        df = df.sort_values(by=['Case', 'Sub-case'])

        if 'Case' not in df_all:
            df_all['Case'] = df['Case'].unique()

        cases = list(set(map(lambda x: int(x), df['Case'].tolist())))
        timeouts = {}
        delta = 0
        for c in cases:
            c_t = df[df['Case'] == c]
            t_case = c_t[c_t[mode] == True]
            timeouts[c] = (delta * 5 * c + t_case[mode].count()) / (5 * (c + 1))
            delta = timeouts[c]
        df_all[isolation] = df_all['Case'].map(timeouts)

    plt.figure()
    plt.rc('figure', titlesize=20)

    # df = df[['Running Time (ms)', 'Creation Time (ms)', 'Evaluation Time (ms)']]
    ax = df_all.plot(x='Case', y=isolations)

    maxX = 100
    maxY = 1
    plt.rc('figure', titlesize=20)

    # ax = df["Time (ms)"].plot()
    # df['Running time (ms)'].plot()
    ax.set_title('Benchmark ' + name_plot + ' with ' + n + ' sessions')

    ax.set_xlabel('Number of transactions per session')
    ax.set_ylabel('Timeout ratio')

    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    plt.xlim([1 - 0.05, maxX - 1 + 0.05])
    plt.ylim([0 - 0.25, maxY + 0.25])
    print('Saving...')

    path = get_path(folder)

    plt.savefig(path + name_plot + '-' + mode + '.eps', format='eps', bbox_inches='tight')
    plt.savefig(path + name_plot + '-' + mode + '.png', format='png', bbox_inches='tight')

    print('Saved! :)')
    plt.close()

    return


def plot_oos(folder, name, name_plot, isolation, n):
    path = get_path(folder)
    data_path = path + name + '-' + isolation + '-data-all.csv'
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
    ax.set_title('Benchmark ' + name_plot + '(' + isolation + ')' + ' with ' + n + ' sessions')

    ax.set_xlabel('Number of transactions per session')
    ax.set_ylabel('OOS probabilty')

    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    plt.xlim([1 - 0.05, maxX - 1 + 0.05])
    plt.ylim([0 - 0.25, maxY + 0.25])
    print('Saving...')

    plt.savefig(path + name_plot + '-' + isolation + '-oos.eps', format='eps', bbox_inches='tight')
    plt.savefig(path + name_plot + '-' + isolation + '-oos.png', format='png', bbox_inches='tight')

    print('Saved! :)')

    return


if __name__ == "__main__":
    plt.rc('text', usetex=True)
    font = {'family': 'serif', 'size': 16, 'serif': ['computer modern roman']}
    plt.rc('font', **font)
    plt.rc('legend', **{'fontsize': 14})

    set_source_dir()

    print_names = {'twitter': 'Twitter',
                   'tpcc': 'TPC-C',
                   'tpccPC': 'TPC-C PC',
                   'seats': 'Seats',
                   }

    figures_names = {'twitter': 'Twitter',
                     'tpcc': 'TPC-C',
                     'tpccPC': 'TPC-C-PC',
                     'seats': 'Seats',
                     }


    benchmark = ['Transaction-Scalability', 'Session-Scalability']

    for i in range(1, len(sys.argv), 4):
        name = sys.argv[i]
        case = sys.argv[i+1]
        isolations = sys.argv[i + 2].split(',')
        sessions = sys.argv[i + 3]
        folder = "results/testFiles/" + case + "/" + name + "Histories/"

        plot_benchmark(folder, name, isolations, print_names[name], figures_names[name] + '-scala-sessions', 'Sessions', sessions)

#!/usr/bin/python

import sys
import json

from metric import *

exec_class_map = {
    'CSVScanner': CSVscannerMetric,
    'RawFileScanner': RawFileScannerMetric,
    'SeqScanExec': SeqScanExecMetric,
    'HashShuffleFileWriteExec': HashShuffleFileWriteExecMetric,
    'HashJoinExec': HashJoinExecMetric,
    'StoreTableExec': StoreTableExecMetric,
    'total': TotalMetric
}


def usage():
    print 'Usage : %s -f <input:json file> <output:csv file>' % (sys.argv[0],)
    sys.exit(0)


def process_class_data(exec_class, data):
    # making class instance
    mm = exec_class()

    for line in data:
        try:
            method_name = line[0].split('.')[1]
            mm.add_method_data(method_name, line[1])
        except IndexError:
            if line[0] == 'total' or line[0] == 'fetch':
                mm.add_method_data(line[0], line[1])

    return mm


if __name__ == '__main__':

    if len(sys.argv) != 4 or sys.argv[1] != '-f':
        usage()

    in_file = open(sys.argv[2])
    out_file = open(sys.argv[3], 'w')

    json_obj = json.load(in_file)

    eblocks = []

    for eb_data in json_obj:
        eb = ExecutionBlock(eb_data[0])
        # print '----', eb_data[0], '----'
        for each_class_data in eb_data[1:]:
            class_name = each_class_data[0][0].split('.')[0]
            try:
                metric = process_class_data(exec_class_map[class_name], each_class_data)
                # print metric
                eb.add_metric(metric)

            except KeyError:
                print 'Error : not implemented for',class_name

        eblocks.append(eb)

    total = 0
    for x in eblocks:
        total = total + x.get_sum()

    print 'total = ', total

    out_file.write('EBID, MODULE_NAME, NANO_TIME, IN_RECORD, OUT_RECORD, REAL_NANOTIME, EB RATE, RATE\n\n')
    for x in eblocks:
        x.write_csv_file(out_file, total)

    out_file.write(',Total,,'+str(total))

    in_file.close()
    out_file.close()
#!/usr/bin/python

import sys, json, sqlite3

from metric import *

metric_map = {
    'CSVScanner': CSVscannerMetric,
    'RawFileScanner': RawFileScannerMetric,
    'SeqScanExec': SeqScanExecMetric,
    'HashShuffleFileWriteExec': HashShuffleFileWriteExecMetric,
    'HashJoinExec': HashJoinExecMetric,
    'StoreTableExec': StoreTableExecMetric,
    'ExternalSortExec': ExternalSortExecMetric,
    'HashAggregateExec': HashAggregateExecMetric,
    'RangeShuffleFileWriteExec': RangeShuffleFileWriteExecMetric,
    'total': TotalMetric
}


def usage():
    print 'Usage : %s -f <input:json file> <output:csv file>' % (sys.argv[0],)
    sys.exit(0)


def process_class_data(class_name, data, cur, ebid):
    # making class instance
    class_metric = metric_map[class_name]()

    for line in data:
        try:
            method_name = line[0].split('.')[1]
            class_metric.add_method_data(method_name, line[1], cur, ebid)
        except IndexError:
            if line[0] == 'total' or line[0] == 'fetch':
                class_metric.add_method_data(line[0], line[1], cur, ebid)

    return class_metric


def make_db():
    global conn, cur

    conn = sqlite3.connect(':memory:')

    cur = conn.cursor()

    cur.execute('CREATE TABLE eb (seq INT, ebid INT)')

    cur.execute('''CREATE TABLE class_data
			(seq INT,
			class TEXT,
			nanotime LONG, 
			realnano LONG,
			in_record INT,
			out_record INT,
			ebid INT) ''')

    cur.execute(''' CREATE TABLE method_data
			(class TEXT, 
			method TEXT,
			nanotime LONG, 
			realnano LONG,
			ebid INT) ''')

	
if __name__ == '__main__':

    if len(sys.argv) != 4 or sys.argv[1] != '-f':
        usage()

    in_file = open(sys.argv[2])
    out_file = open(sys.argv[3], 'w')

    json_obj = json.load(in_file)

    eblocks = []

    make_db()

    seq = 1
    for eb_data in json_obj:
        ebid = int(eb_data[0][2:])

        eb = ExecutionBlock(ebid, cur)

        cur.execute('INSERT INTO eb values (?, ?)', (seq, ebid))
        # print '----', eb_data[0], '----'
        for each_class_data in eb_data[1:]:

            class_name = each_class_data[0][0].split('.')[0]
            cur.execute('INSERT INTO class_data (seq, class, ebid) values (?, ?, ?)', (seq, class_name, ebid))
            seq = seq + 1

            try:
                metric = process_class_data(class_name, each_class_data, cur, ebid)
                eb.add_metric(metric)
            except KeyError:
                print 'Error : not implemented for',class_name

        eblocks.append(eb)

  #  for x in cur.execute('SELECT * FROM method_data'):
   #     print x

    total = cur.execute('SELECT sum(nanotime) FROM method_data WHERE class=? and method=?', ('Total', 'total')).fetchone()[0]

    out_file.write('EBID, MODULE_NAME, NANO_TIME, IN_RECORD, OUT_RECORD, REAL_NANOTIME, EB RATE, RATE\n\n')

    for eb in eblocks:
        class_records = [x for x in cur.execute('SELECT * FROM class_data WHERE ebid=? ORDER BY seq', (eb.id,))]
        eb_total = cur.execute('SELECT nanotime FROM class_data WHERE ebid=? and class=?', (eb.id, 'total')).fetchone()[0]
        out_file.write('EB'+str(eb.id))
        prev_sum = 0
        for record in class_records:
            cname = record[1]
            csv_string = metric_map[cname].get_method_csv_string(cur, eb.id, eb_total, total, prev_sum)

            out_file.write(csv_string['method_str'])

            real_sum = csv_string['sum_val']
            try:
                sum_str = ',SUM,%ld,%ld,%ld,%ld,%f,%f\n' % (record[2], record[4], record[5], real_sum
                                                                      , real_sum*100 / float(eb_total)
                                                                      , real_sum*100 / float(total))
            except TypeError:
                sum_str = ',SUM,%ld,,,%ld,%f,%f\n' % (record[2], real_sum, real_sum*100 / float(eb_total), real_sum*100 / float(total))

            prev_sum = record[2]
            
            out_file.write(sum_str)

        out_file.write('\n')

    out_file.write(',Total,,'+str(total))

    cur.close()
    in_file.close()
    out_file.close()



class ExecutionBlock:
    def __init__(self, id, cur):
        self.cur = cur
        self.id = id
        self.metrics = []

    def add_metric(self, metric):
        self.metrics.append(metric)

    def write_csv_file(self, out_file, total):
        out_file.write(self.id)

        prev_sum = 0
        for x in self.metrics:
            out_file.write(x.get_csv_string(self.get_sum(), total, prev_sum))
            prev_sum = x.nanotime

        out_file.write('\n')

SQL_GET_METHOD_NANOTIME='SELECT nanotime FROM method_data WHERE ebid=? and class=? and method=?'

def make_basic_csv_str(name, data, real_data, eb_total, total):
    return ',%s,%ld,,,%ld,%f,%f\n' % (name, data, real_data, real_data * 100 / float(eb_total), real_data * 100 / float(total))

class BaseModuleMetric:
    def __init__(self, name):
        self.nanotime = 0
        self.in_record = 0
        self.out_record = 0
        self.class_name = name
        self.each_method_data = {}

    def add_method_data(self, name, data, cur, ebid):
        if name == 'next':
            cur.execute('UPDATE class_data SET nanotime=? WHERE ebid = ? and class=?', (data, ebid, self.class_name))
            cur.execute('INSERT INTO method_data values (?, ?, ?, null, ?)', (self.class_name, name, data, ebid))
        elif name == 'inTuples':
            cur.execute('UPDATE class_data SET in_record=? WHERE ebid = ? and class=?', (data, ebid, self.class_name))
        elif name == 'outTuples':
            cur.execute('UPDATE class_data SET out_record=? WHERE ebid = ? and class=?', (data, ebid, self.class_name))
        elif name == 'numInTuple':
            cur.execute('UPDATE class_data SET in_record=?, out_record=? WHERE ebid = ? and class=?', (data, data, ebid, self.class_name))
        else:
            cur.execute('INSERT INTO method_data values (?, ?, ?, null, ?)', (self.class_name, name, data, ebid))

    def __unicode__(self):
        result = u'>>> '+self.class_name+'\n'
        for method in self.each_method_data:
            result += method + ' : ' + unicode(self.each_method_data[method]) + '\n'

        return result

    def __str__(self):
        return unicode(self).encode('utf-8')


class CSVscannerMetric(BaseModuleMetric):
    def __init__(self):
        BaseModuleMetric.__init__(self, 'CSVScanner')

    def add_method_data(self, name, data, cur, ebid):
        BaseModuleMetric.add_method_data(self, name, data, cur, ebid)

    @staticmethod
    def get_method_csv_string(cur, ebid, eb_total, total, prev_sum):
        page_data = cur.execute(SQL_GET_METHOD_NANOTIME, (ebid, 'CSVScanner', 'page')).fetchone()[0]
        page_str = make_basic_csv_str('CSVScanner.page', page_data, page_data, eb_total, total)

        make_tuple_data = cur.execute(SQL_GET_METHOD_NANOTIME, (ebid, 'CSVScanner', 'makeTuple')).fetchone()[0]
        make_tuple_str = make_basic_csv_str('CSVScanner.makeTuple', make_tuple_data, make_tuple_data, eb_total, total)

        next_data = cur.execute(SQL_GET_METHOD_NANOTIME, (ebid, 'CSVScanner', 'next')).fetchone()[0]
        real_next_data = next_data - page_data - make_tuple_data
        next_str = make_basic_csv_str('CSVScanner.next', next_data, real_next_data, eb_total, total)

        real_sum = sum((page_data, make_tuple_data, real_next_data))

        return {'method_str':page_str+make_tuple_str+next_str, 'sum_val':real_sum}


class SeqScanExecMetric(BaseModuleMetric):
    def __init__(self):
        BaseModuleMetric.__init__(self, 'SeqScanExec')

    def add_method_data(self, name, data, cur, ebid):
        if name == 'init':
            pass
        else:
            BaseModuleMetric.add_method_data(self, name, data, cur, ebid)

    @staticmethod
    def get_method_csv_string(cur, ebid, eb_total, total, prev_sum):
        try:
            bineval_data = cur.execute(SQL_GET_METHOD_NANOTIME, (ebid, 'SeqScanExec', 'BinaryEval')).fetchone()[0]
            bineval_str = make_basic_csv_str('SeqScanExec.BinaryEval', bineval_data, bineval_data, eb_total, total)
        except TypeError:
            bineval_data = 0
            bineval_str = ''

        project_data = cur.execute(SQL_GET_METHOD_NANOTIME, (ebid, 'SeqScanExec', 'project')).fetchone()[0]
        project_str = make_basic_csv_str('SeqScanExec.project', project_data, project_data, eb_total, total)

        next_data = cur.execute(SQL_GET_METHOD_NANOTIME, (ebid, 'SeqScanExec', 'next')).fetchone()[0]
        real_next_data = next_data - project_data - bineval_data - prev_sum
        next_str = make_basic_csv_str('SeqScanExec.next', next_data, real_next_data, eb_total, total)

        real_sum = sum((bineval_data, project_data, real_next_data))

        return {'method_str':bineval_str+project_str+next_str, 'sum_val':real_sum}


class HashShuffleFileWriteExecMetric(BaseModuleMetric):
    def __init__(self):
        BaseModuleMetric.__init__(self, 'HashShuffleFileWriteExec')

    def add_method_data(self, name, data, cur, ebid):
        if name == 'init':
            pass
        else:
            BaseModuleMetric.add_method_data(self, name, data, cur, ebid)

    @staticmethod
    def get_method_csv_string(cur, ebid, eb_total, total, prev_sum):
        next_data = cur.execute(SQL_GET_METHOD_NANOTIME, (ebid, 'HashShuffleFileWriteExec', 'next')).fetchone()[0]
        real_next_data = next_data - prev_sum
        next_str = make_basic_csv_str('HashShuffleFileWriteExec.next', next_data, real_next_data, eb_total, total)

        flush_data = cur.execute(SQL_GET_METHOD_NANOTIME, (ebid, 'HashShuffleFileWriteExec', 'flush')).fetchone()[0]
        flush_str = make_basic_csv_str('HashShuffleFileWriteExec.flush', flush_data, flush_data, eb_total, total)

        real_sum = sum((real_next_data, flush_data))

        return {'method_str':next_str+flush_str, 'sum_val':real_sum}


class RawFileScannerMetric(BaseModuleMetric):
    def __init__(self):
        BaseModuleMetric.__init__(self, 'RawFileScanner')

    def add_method_data(self, name, data, cur, ebid):
        if name == 'seek':
            pass
        else:
            BaseModuleMetric.add_method_data(self, name, data, cur, ebid)

    @staticmethod
    def get_method_csv_string(cur, ebid, eb_total, total, prev_sum):

        read_data = cur.execute(SQL_GET_METHOD_NANOTIME, (ebid, 'RawFileScanner', 'read')).fetchone()[0]
        read_str = make_basic_csv_str('RawFileScanner.read', read_data, read_data, eb_total, total)

        make_tuple_data = cur.execute(SQL_GET_METHOD_NANOTIME, (ebid, 'RawFileScanner', 'makeTuple')).fetchone()[0]
        make_tuple_str = make_basic_csv_str('RawFileScanner.makeTuple', make_tuple_data, make_tuple_data, eb_total, total)

        next_data = cur.execute(SQL_GET_METHOD_NANOTIME, (ebid, 'RawFileScanner', 'next')).fetchone()[0]
        real_next_data = next_data - read_data - make_tuple_data - prev_sum
        next_str = make_basic_csv_str('RawFileScanner.next', next_data, real_next_data, eb_total, total)

        real_sum = sum((read_data, make_tuple_data, real_next_data))

        return {'method_str':read_str+make_tuple_str+next_str, 'sum_val':real_sum}


class HashJoinExecMetric(BaseModuleMetric):
    def __init__(self):
        BaseModuleMetric.__init__(self, 'HashJoinExec')

    def add_method_data(self, name, data, cur, ebid):
        if name == 'nanoTimeLoadRight':
            cur.execute('INSERT INTO method_data values (?, ?, ?, null, ?)', (self.class_name, 'loadRight', data, ebid))
        elif name == 'nanoTimeLeftNext':
            cur.execute('INSERT INTO method_data values (?, ?, ?, null, ?)', (self.class_name, 'leftNext', data, ebid))
        elif name == 'nanoTimeLoadRightNext':
            pass
        else:
            BaseModuleMetric.add_method_data(self, name, data, cur, ebid)

    @staticmethod
    def get_method_csv_string(cur, ebid, eb_total, total, prev_sum):

        load_right_data = cur.execute(SQL_GET_METHOD_NANOTIME, (ebid, 'HashJoinExec', 'loadRight')).fetchone()[0]
        left_next_data = cur.execute(SQL_GET_METHOD_NANOTIME, (ebid, 'HashJoinExec', 'leftNext')).fetchone()[0]
        lr_avg = (load_right_data+left_next_data-prev_sum) / 2.0

        load_right_str = make_basic_csv_str('HashJoinExec.loadRight', load_right_data, lr_avg, eb_total, total)
        left_next_str = make_basic_csv_str('HashJoinExec.leftNext', left_next_data, lr_avg, eb_total, total)

        next_data = cur.execute(SQL_GET_METHOD_NANOTIME, (ebid, 'HashJoinExec', 'next')).fetchone()[0]
        real_next_data = next_data - lr_avg*2 - prev_sum
        next_str = make_basic_csv_str('HashJoinExec.next', next_data, real_next_data, eb_total, total)

        real_sum = sum((lr_avg*2, real_next_data))

        return {'method_str':load_right_str+left_next_str+next_str, 'sum_val':real_sum}


class StoreTableExecMetric(BaseModuleMetric):
    def __init__(self):
        BaseModuleMetric.__init__(self, 'StoreTableExec')

    def add_method_data(self, name, data, cur, ebid):
        if name == 'inTuples' or name == 'init':
            pass
        else:
            BaseModuleMetric.add_method_data(self, name, data, cur, ebid)

    @staticmethod
    def get_method_csv_string(cur, ebid, eb_total, total, prev_sum):

        next_data = cur.execute(SQL_GET_METHOD_NANOTIME, (ebid, 'StoreTableExec', 'next')).fetchone()[0]
        real_next_data = next_data - prev_sum
        next_str = make_basic_csv_str('StoreTableExec.next', next_data, real_next_data, eb_total, total)

        return {'method_str':next_str, 'sum_val':real_next_data}


class RangeShuffleFileWriteExecMetric(BaseModuleMetric):
    def __init__(self):
        BaseModuleMetric.__init__(self, 'RangeShuffleFileWriteExec')

    def add_method_data(self, name, data, cur, ebid):
        if name == 'init':
            pass
        else:
            BaseModuleMetric.add_method_data(self, name, data, cur, ebid)

    @staticmethod
    def get_method_csv_string(cur, ebid, eb_total, total, prev_sum):

        next_data = cur.execute(SQL_GET_METHOD_NANOTIME, (ebid, 'RangeShuffleFileWriteExec', 'next')).fetchone()[0]
        real_next_data = next_data - prev_sum
        next_str = make_basic_csv_str('RangeShuffleFileWriteExec.next', next_data, real_next_data, eb_total, total)

        return {'method_str':next_str, 'sum_val':real_next_data}


class HashAggregateExecMetric(BaseModuleMetric):
    def __init__(self):
        BaseModuleMetric.__init__(self, 'HashAggregateExec')

    def add_method_data(self, name, data, cur, ebid):
        if name == 'computeJoin':
            pass
        else:
            BaseModuleMetric.add_method_data(self, name, data, cur, ebid)

    @staticmethod
    def get_method_csv_string(cur, ebid, eb_total, total, prev_sum):

        compute_data = cur.execute(SQL_GET_METHOD_NANOTIME, (ebid, 'HashAggregateExec', 'compute')).fetchone()[0]
        real_compute_data = compute_data - prev_sum
        compute_str = make_basic_csv_str('HashAggregateExec.compute', compute_data, real_compute_data, eb_total, total)

        next_data = cur.execute(SQL_GET_METHOD_NANOTIME, (ebid, 'HashAggregateExec', 'next')).fetchone()[0]
        real_next_data = next_data - compute_data
        next_str = make_basic_csv_str('HashAggregateExec.next', next_data, real_next_data, eb_total, total)

        real_sum = sum((real_compute_data, real_next_data))

        return {'method_str':compute_str+next_str, 'sum_val':real_sum}


class ExternalSortExecMetric(BaseModuleMetric):
    def __init__(self):
        BaseModuleMetric.__init__(self, 'ExternalSortExec')

    def add_method_data(self, name, data, cur, ebid):
        if (name == 'Sort' or name == 'SortScan' or name == 'MemorySort' or name == 'SortWrite') and data == 0:
            pass
        elif name == 'init':
            pass
        else:
            BaseModuleMetric.add_method_data(self, name, data, cur, ebid)

    @staticmethod
    def get_method_csv_string(cur, ebid, eb_total, total, prev_sum):

        try:
            sortscan_data = cur.execute(SQL_GET_METHOD_NANOTIME, (ebid, 'ExternalSortExec', 'SortScan')).fetchone()[0]
            real_sortscan_data = sortscan_data - prev_sum
            sortscan_str = make_basic_csv_str('ExternalSortExec.SortScan', sortscan_data, real_sortscan_data, eb_total, total)
        except TypeError:
            sortscan_data = real_sortscan_data = 0
            sortscan_str = ''

        next_data = cur.execute(SQL_GET_METHOD_NANOTIME, (ebid, 'ExternalSortExec', 'next')).fetchone()[0]
        real_next_data = next_data - sortscan_data
        next_str = make_basic_csv_str('ExternalSortExec.next', next_data, real_next_data, eb_total, total)

        real_sum = sum((real_sortscan_data, real_next_data))

        return {'method_str':sortscan_str+next_str, 'sum_val':real_sum}


class TotalMetric(BaseModuleMetric):
    def __init__(self):
        BaseModuleMetric.__init__(self, 'Total')

    def add_method_data(self, name, data, cur, ebid):
        if name == 'total':
            cur.execute('UPDATE class_data SET nanotime=? WHERE ebid = ? and class=?', (data, ebid, 'total'))
            cur.execute('INSERT INTO method_data values (?, ?, ?, null, ?)', (self.class_name, name, data, ebid))
        elif name == 'fetch' and data < 0:
            cur.execute('INSERT INTO method_data values (?, ?, ?, null, ?)', (self.class_name, name, 0, ebid))
        else:
            cur.execute('INSERT INTO method_data values (?, ?, ?, null, ?)', (self.class_name, name, data, ebid))

    @staticmethod
    def get_method_csv_string(cur, ebid, eb_total, total, prev_sum):
        fetch_data = cur.execute(SQL_GET_METHOD_NANOTIME, (ebid, 'Total', 'fetch')).fetchone()[0]
        fetch_str = ',Fetch, %ld,,,%ld, %f, %f\n' % (fetch_data, fetch_data, fetch_data*100 / float(eb_total), fetch_data*100 / float(total))

        total_data = cur.execute(SQL_GET_METHOD_NANOTIME, (ebid, 'Total', 'total')).fetchone()[0]
        real_total_data = total_data - fetch_data - prev_sum
        total_str = ',Total, %ld,,,%ld,%f,%f\n' % (total_data, real_total_data, real_total_data*100 / float(eb_total), real_total_data*100 / float(total))

        real_sum = real_total_data+fetch_data

        return {'method_str':fetch_str+total_str, 'sum_val':real_sum}
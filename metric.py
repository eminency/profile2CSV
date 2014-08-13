

class ExecutionBlock:
    def __init__(self, id):
        self.id = id
        self.metrics = []

    def add_metric(self, metric):
        self.metrics.append(metric)

    def get_sum(self):
        total_module = self.metrics[-1]
        if total_module.class_name != 'Total':
            print 'No Total Module!!'
            return 0

        return total_module.get_sum()

    def write_csv_file(self, out_file, total):
        out_file.write(self.id)

        prev_sum = 0
        for x in self.metrics:
            out_file.write(x.get_csv_string(self.get_sum(), total, prev_sum))
            prev_sum = x.nanotime

        out_file.write('\n')

class BaseModuleMetric:
    def __init__(self, name):
        self.nanotime = 0
        self.in_record = 0
        self.out_record = 0
        self.class_name = name
        self.each_method_data = {}

    def get_sum(self):
        return self.nanotime

    @classmethod
    def make_basic_csv_str(self, name, data, real_data, eb_total, total):
        return ',%s,%ld,,,%ld,%f,%f\n' % (name, data, real_data, real_data * 100 / float(eb_total), real_data * 100 / float(total))

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

    def add_method_data(self, name, data):
        if name == 'next':
            self.nanotime = data
            self.each_method_data[name] = data
        elif name == 'numInTuple':
            self.in_record = self.out_record = data
        else:
            self.each_method_data[name] = data

    def get_csv_string(self, eb_total, total, prev_sum):
        page_data = self.each_method_data['page']
        page_str = self.make_basic_csv_str('CSVScanner.page', page_data, page_data, eb_total, total)

        make_tuple_data = self.each_method_data['makeTuple']
        make_tuple_str = self.make_basic_csv_str('CSVScanner.makeTuple', make_tuple_data, make_tuple_data, eb_total, total)

        next_data = self.each_method_data['next']
        real_next_data = next_data - page_data - make_tuple_data
        next_str = self.make_basic_csv_str('CSVScanner.next', next_data, real_next_data, eb_total, total)

        real_sum = sum((page_data, make_tuple_data, real_next_data))
        sum_str = ',SUM,%ld,%ld,,%ld,%f,%f\n' % (self.nanotime, self.in_record, real_sum
                                                                      , real_sum*100 / float(eb_total)
                                                                      , real_sum*100 / float(total))

        return page_str+make_tuple_str+next_str+sum_str


class SeqScanExecMetric(BaseModuleMetric):
    def __init__(self):
        BaseModuleMetric.__init__(self, 'SeqScanExec')

    def add_method_data(self, name, data):
        if name == 'next':
            self.nanotime = data
            self.each_method_data[name] = data
        elif name == 'inTuples':
            self.in_record = data
        elif name == 'outTuples':
            self.out_record = data
        elif name == 'init':
            pass
        else:
            self.each_method_data[name] = data

    def get_sum_data(self, eb_total, total):
        return ('SUM', self.nanotime, self.in_record, self.out_record, self.nanotime
                , self.nanotime / float(eb_total), self.nanotime / float(total))

    def get_csv_string(self, eb_total, total, prev_sum):

        if self.each_method_data.has_key('BinaryEval'):
            bineval_data = self.each_method_data['BinaryEval']
            bineval_str = self.make_basic_csv_str('SeqScanExec.BinaryEval', bineval_data, bineval_data, eb_total, total)
        else:
            bineval_data = 0
            bineval_str = ''

        project_data = self.each_method_data['project']
        project_str = self.make_basic_csv_str('SeqScanExec.project', project_data, project_data, eb_total, total)

        next_data = self.each_method_data['next']
        real_next_data = next_data - project_data - bineval_data - prev_sum
        next_str = self.make_basic_csv_str('SeqScanExec.next', next_data, real_next_data, eb_total, total)

        real_sum = sum((bineval_data, project_data, real_next_data))
        sum_str = ',SUM,%ld,%ld,%ld,%ld,%f,%f\n' % (self.nanotime, self.in_record, self.out_record
                                                                      , real_sum
                                                                      , real_sum*100 / float(eb_total)
                                                                      , real_sum*100 / float(total))

        return bineval_str+project_str+next_str+sum_str

class HashShuffleFileWriteExecMetric(BaseModuleMetric):
    def __init__(self):
        BaseModuleMetric.__init__(self, 'HashShuffleFileWriteExec')

    def add_method_data(self, name, data):
        if name == 'next':
            self.nanotime = data
            self.each_method_data[name] = data
        elif name == 'inTuples':
            self.in_record = data
        elif name == 'outTuples':
            self.out_record = data
        elif name == 'init':
            pass
        else:
            self.each_method_data[name] = data

    def get_sum_data(self, eb_total, total):
        return ('SUM', self.nanotime, self.in_record, self.out_record, self.nanotime
                , self.nanotime / float(eb_total), self.nanotime / float(total))

    def get_csv_string(self, eb_total, total, prev_sum):

        next_data = self.each_method_data['next']
        real_next_data = next_data - prev_sum
        next_str = self.make_basic_csv_str('HashShuffleFileWriteExec.next', next_data, real_next_data, eb_total, total)

        flush_data = self.each_method_data['flush']
        flush_str = self.make_basic_csv_str('HashShuffleFileWriteExec.flush', flush_data, flush_data, eb_total, total)

        real_sum = sum((real_next_data, flush_data))
        sum_str = ',SUM,%ld,%ld,%ld,%ld,%f,%f\n' % (self.nanotime, self.in_record, self.out_record, real_sum
                                                                      , real_sum*100 / float(eb_total)
                                                                      , real_sum*100 / float(total))

        return next_str+flush_str+sum_str


class RawFileScannerMetric(BaseModuleMetric):
    def __init__(self):
        BaseModuleMetric.__init__(self, 'RawFileScanner')

    def add_method_data(self, name, data):
        if name == 'next':
            self.nanotime = data
            self.each_method_data[name] = data
        elif name == 'numInTuple':
            self.in_record = self.out_record = data
        elif name == 'seek':
            pass
        else:
            self.each_method_data[name] = data

    def get_sum_data(self, eb_total, total):
        return ('SUM', self.nanotime, self.in_record, self.out_record, self.nanotime
                , self.nanotime / float(eb_total), self.nanotime / float(total))

    def get_csv_string(self, eb_total, total, prev_sum):

        read_data = self.each_method_data['read']
        read_str = self.make_basic_csv_str('RawFileScanner.read', read_data, read_data, eb_total, total)

        make_tuple_data = self.each_method_data['makeTuple']
        make_tuple_str = self.make_basic_csv_str('RawFileScanner.makeTuple', make_tuple_data, make_tuple_data, eb_total, total)

        next_data = self.each_method_data['next']
        real_next_data = next_data - read_data - make_tuple_data - prev_sum
        next_str = self.make_basic_csv_str('RawFileScanner.next', next_data, real_next_data, eb_total, total)

        real_sum = sum((read_data, make_tuple_data, real_next_data))
        sum_str = ',SUM,%ld,%ld,%ld,%ld,%f,%f\n' % (self.nanotime, self.in_record, self.out_record, real_sum
                                                                      , real_sum*100 / float(eb_total)
                                                                      , real_sum*100 / float(total))

        return read_str+make_tuple_str+next_str+sum_str

class HashJoinExecMetric(BaseModuleMetric):
    def __init__(self):
        BaseModuleMetric.__init__(self, 'HashJoinExec')

    def add_method_data(self, name, data):
        if name == 'next':
            self.nanotime = data
            self.each_method_data[name] = data
        elif name == 'inTuples':
            self.in_record = data
        elif name == 'outTuples':
            self.out_record = data
        elif name == 'nanoTimeLoadRight':
            self.each_method_data['loadRight'] = data
        elif name == 'nanoTimeLeftNext':
            self.each_method_data['leftNext'] = data
        elif name == 'nanoTimeLoadRightNext' or name == 'init':
            pass
        else:
            self.each_method_data[name] = data

    def get_csv_string(self, eb_total, total, prev_sum):

        load_right_data = self.each_method_data['loadRight']
        left_next_data = self.each_method_data['leftNext']
        lr_avg = (load_right_data+left_next_data-prev_sum) / 2.0

        load_right_str = self.make_basic_csv_str('HashJoinExec.loadRight', load_right_data, lr_avg, eb_total, total)
        left_next_str = self.make_basic_csv_str('HashJoinExec.leftNext', left_next_data, lr_avg, eb_total, total)

        next_data = self.each_method_data['next']
        real_next_data = next_data - lr_avg*2 - prev_sum
        next_str = self.make_basic_csv_str('HashJoinExec.next', next_data, real_next_data, eb_total, total)

        real_sum = sum((lr_avg*2, real_next_data))
        sum_str = ',SUM,%ld,%ld,%ld,%ld,%f,%f\n' % (self.nanotime, self.in_record, self.out_record, real_sum
                                                                      , real_sum*100 / float(eb_total)
                                                                      , real_sum*100 / float(total))

        return load_right_str+left_next_str+next_str+sum_str


class StoreTableExecMetric(BaseModuleMetric):
    def __init__(self):
        BaseModuleMetric.__init__(self, 'StoreTableExec')

    def add_method_data(self, name, data):
        if name == 'next':
            self.nanotime = data
            self.each_method_data[name] = data
        elif name == 'outTuples':
            self.out_record = data
        elif name == 'inTuples' or name == 'init':
            pass
        else:
            self.each_method_data[name] = data

    def get_csv_string(self, eb_total, total, prev_sum):

        next_data = self.each_method_data['next']
        real_next_data = next_data - prev_sum
        next_str = self.make_basic_csv_str('StoreTableExec.next', next_data, real_next_data, eb_total, total)

        sum_str = ',SUM,%ld,%ld,%ld,%ld,%f,%f\n' % (self.nanotime, self.in_record, self.out_record, real_next_data
                                                                      , real_next_data*100 / float(eb_total)
                                                                      , real_next_data*100 / float(total))

        return next_str+sum_str


class RangeShuffleFileWriteExecMetric(BaseModuleMetric):
    def __init__(self):
        BaseModuleMetric.__init__(self, 'RangeShuffleFileWriteExec')

    def add_method_data(self, name, data):
        if name == 'next':
            self.nanotime = data
            self.each_method_data[name] = data
        elif name == 'inTuples':
            self.in_record = data
        elif name == 'outTuples':
            self.out_record = data
        elif name == 'init':
            pass
        else:
            self.each_method_data[name] = data

    def get_csv_string(self, eb_total, total, prev_sum):
        next_data = self.each_method_data['next']
        real_next_data = next_data - prev_sum
        next_str = self.make_basic_csv_str('RangeShuffleFileWriteExec.next', next_data, real_next_data, eb_total, total)

        real_sum = real_next_data
        sum_str = ',SUM,%ld,%ld,%ld,%ld,%f,%f\n' % (self.nanotime, self.in_record, self.out_record, real_sum
                                                                      , real_sum*100 / float(eb_total)
                                                                      , real_sum*100 / float(total))

        return next_str+sum_str


class HashAggregateExecMetric(BaseModuleMetric):
    def __init__(self):
        BaseModuleMetric.__init__(self, 'HashAggregateExec')

    def add_method_data(self, name, data):
        if name == 'next':
            self.nanotime = data
            self.each_method_data[name] = data
        elif name == 'inTuples':
            self.in_record = data
        elif name == 'outTuples':
            self.out_record = data
        elif name == 'computeJoin':
            pass
        else:
            self.each_method_data[name] = data

    def get_csv_string(self, eb_total, total, prev_sum):
        compute_data = self.each_method_data['compute']
        real_compute_data = compute_data - prev_sum
        compute_str = self.make_basic_csv_str('HashAggregateExec.compute', compute_data, real_compute_data, eb_total, total)

        next_data = self.each_method_data['next']
        real_next_data = next_data - compute_data
        next_str = self.make_basic_csv_str('ExternalSortExec.next', next_data, real_next_data, eb_total, total)

        real_sum = sum((real_compute_data, real_next_data))
        sum_str = ',SUM,%ld,%ld,%ld,%ld,%f,%f\n' % (self.nanotime, self.in_record, self.out_record, real_sum
                                                                      , real_sum*100 / float(eb_total)
                                                                      , real_sum*100 / float(total))

        return compute_str+next_str+sum_str


class ExternalSortExecMetric(BaseModuleMetric):
    def __init__(self):
        BaseModuleMetric.__init__(self, 'ExternalSortExec')

    def add_method_data(self, name, data):
        if name == 'next':
            self.nanotime = data
            self.each_method_data[name] = data
        elif name == 'inTuples':
            self.in_record = data
        elif name == 'outTuples':
            self.out_record = data
        elif (name == 'Sort' or name == 'SortScan' or name == 'MemorySort' or name == 'SortWrite') and data == 0:
            pass
        elif name == 'init':
            pass
        else:
            self.each_method_data[name] = data

    def get_csv_string(self, eb_total, total, prev_sum):

        if self.each_method_data.has_key('SortScan'):
            sortscan_data = self.each_method_data['SortScan']
            real_sortscan_data = sortscan_data - prev_sum
            sortscan_str = self.make_basic_csv_str('ExternalSortExec.SortScan', sortscan_data, real_sortscan_data, eb_total, total)
        else:
            sortscan_data = real_sortscan_data = 0
            sortscan_str = ''

        next_data = self.each_method_data['next']
        real_next_data = next_data - sortscan_data
        next_str = self.make_basic_csv_str('ExternalSortExec.next', next_data, real_next_data, eb_total, total)

        real_sum = sum((real_sortscan_data, real_next_data))
        sum_str = ',SUM,%ld,%ld,%ld,%ld,%f,%f\n' % (self.nanotime, self.in_record, self.out_record, real_sum
                                                                      , real_sum*100 / float(eb_total)
                                                                      , real_sum*100 / float(total))

        return sortscan_str+next_str+sum_str


class TotalMetric(BaseModuleMetric):
    def __init__(self):
        BaseModuleMetric.__init__(self, 'Total')

    def add_method_data(self, name, data, cur, ebid):
        if name == 'total':
            self.nanotime = data
            cur.execute('UPDATE class_data SET nanotime=? WHERE ebid = ? and class=?', (data, ebid, 'total'))
            self.each_method_data[name] = data
        elif name == 'fetch' and data < 0:
            self.each_method_data[name] = 0
        else:
            self.each_method_data[name] = data

    def get_csv_string(self, eb_total, total, prev_sum):
        fetch_data = self.each_method_data['fetch']
        fetch_str = ',Fetch, %ld,,,%ld, %f, %f\n' % (fetch_data, fetch_data, fetch_data*100 / float(eb_total), fetch_data*100 / float(total))

        total_data = self.each_method_data['total']
        real_total_data = total_data - fetch_data - prev_sum
        total_str = ',Total, %ld,,,%ld,%f,%f\n' % (total_data, real_total_data, real_total_data*100 / float(eb_total), real_total_data*100 / float(total))

        real_total_data = real_total_data+fetch_data
        sum_str = ',SUM, %ld,,,%ld,%f,%f\n' % (total_data, real_total_data, real_total_data*100 / float(eb_total), real_total_data*100 / float(total))

        return fetch_str+total_str+sum_str
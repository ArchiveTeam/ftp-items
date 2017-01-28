import codecs
import json
import os
import sys

import warc


class Deduplicate(object):
    """
    Class to deduplicate WARCs with FTP records. This class should only be
    used to deduplicate FTP records.
    """

    records = {}

    def __init__(self, f):
        if not os.path.isfile('records.json'):
            print('No records.json file was found.')
            print('We need the records.json for deduplication!')
            if 'n' in raw_input('Continue? [y/n]').lower():
                sys.exit(1)
        else:
            self.load_records()

        self.input_filename = f
        self.input_file = warc.WARCFile(self.input_filename)
        self.input_file_size = os.path.getsize(self.input_filename)

        self.output_filename = self.input_filename[:-8] \
            + '-deduplicated.warc.gz'
        self.output_file = warc.WARCFile(self.output_filename, 'w')

        self.output_log_filename = self.input_filename[:-8] \
            + '-deduplicated.log'
        self.output_log = []

    def deduplicate(self):
        info_record = self.input_file.read_record()
        info_record.header['WARC-Filename'] = self.output_filename
        del info_record.header['WARC-Block-Digest']

        self.output_file.write_record(warc.WARCRecord(
            payload=info_record.payload.read(),
            header=info_record.header,
            defaults=False))

        while self.input_file_size > self.input_file.tell():
            for record in self.input_file:
                if record.type == 'resource':
                    record = self.deduplicate_record(record)
                else:
                    record = warc.WARCRecord(
                        header=record.header,
                        payload=record.payload.read(),
                        defaults=False)
                self.output_file.write_record(record)

        self.input_file.close()
        self.output_file.close()

        with codecs.open(self.output_log_filename, 'w') as output_log_file:
            json.dump(self.output_log, output_log_file, ensure_ascii=False,
                indent=4)

        if self.double_check(self.input_filename):
            os.remove(self.input_filename)
        else:
            os.remove(self.output_filename)
            os.remove(self.output_log_filename)

        self.dump_records()

    def deduplicate_record(self, record):
        record_check = self.check_record(record)

        if record_check:
            record_headers = []

            record.header['Content-Length'] = '0'
            record.header['WARC-Refers-To'] = \
                record_check['WARC-Record-ID']
            record.header['WARC-Refers-To-Date'] = \
                record_check['WARC-Date']
            record.header['WARC-Refers-To-Target-URI'] = \
                record_check['WARC-Target-URI']
            record.header['WARC-Type'] = 'revisit'
            record.header['WARC-Truncated'] = 'length'
            record.header['WARC-Profile'] = \
                'http://netpreserve.org/warc/1.0/revisit/identical-payload-digest'
            record.header['WARC-Payload-Digest'] = \
                record.header['WARC-Block-Digest']

            del record.header['WARC-Block-Digest']

            self.output_log.append({
                'WARC-Record-ID': record.header['WARC-Record-ID'],
                'WARC-Target-URI': record.header['WARC-Target-URI'],
                'WARC-Date': record.header['WARC-Date'],
                'Content-Length': record_check['Content-Length'],
                'Duplicate-Of': {
                    'WARC-Record-ID': record_check['WARC-Record-ID'],
                    'WARC-Target-URI': record_check['WARC-Target-URI'],
                    'WARC-Date': record_check['WARC-Date'],
                    'Content-Length': record_check['Content-Length']
                }
            })

            return warc.WARCRecord(
                header=record.header,
                payload='',
                defaults=False)
        else:
            return warc.WARCRecord(
                header=record.header,
                payload=record.payload.read(),
                defaults=False)

    @classmethod
    def check_record(cls, record):
        record_hash = record.header['WARC-Block-Digest'] \
            .split(':', 1)[1]
        record_url = record.header['WARC-Target-URI']
        record_id = record.header['WARC-Record-ID']
        record_date = record.header['WARC-Date']
        record_length = record.header['Content-Length']

        if record_length == '0':
            return False

        element = ';'.join([record_length, record_hash])
        previous_record = cls.records.get(element)

        if previous_record and previous_record['WARC-Record-ID'] != record_id:
            return previous_record

        cls.records[element] = {'WARC-Target-URI': record_url,
            'WARC-Record-ID': record_id,
            'WARC-Date': record_date,
            'Content-Length': record_length}

        return False

    @classmethod
    def double_check(cls, f):
        input_file = warc.WARCFile(f)
        input_file_size = os.path.getsize(f)
        input_file_records = 0
        output_filename = f[:-8] + '-deduplicated.warc.gz'
        output_file = warc.WARCFile(output_filename)
        output_file_size = os.path.getsize(output_filename)
        output_file_records = 0

        while input_file_size > input_file.tell():
            for record in input_file:
                input_file_records += 1

        while output_file_size > output_file.tell():
            for record in output_file:
                output_file_records += 1

        input_file.close()
        output_file.close()

        return input_file_records == output_file_records

    @classmethod
    def dump_records(cls):
        with open('records.json', 'w') as f:
            json.dump(cls.records, f, ensure_ascii=False)

    @classmethod
    def load_records(cls):
        with open('records.json', 'r') as f:
            cls.records = json.load(f)

for f in os.listdir(sys.argv[1]):
    path = os.path.join(sys.argv[1], f)

    if not (os.path.isfile(path) and path.endswith('.warc.gz')):
        continue

    if path.endswith('-deduplicated.warc.gz'):
        continue

    print('Deduplicating ' + path + '.')

    deduplicate_record = Deduplicate(path)
    deduplicate_record.deduplicate()

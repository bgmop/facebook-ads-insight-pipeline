import os
import gzip
import datetime
import sys
import logging


today = datetime.date.today()
PATH = "your_file_path".format(today)

logger = logging.getLogger('data_quality_log')
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler(filename="your_log_path")
fmt = logging.Formatter(
    fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %I:%M:%S %p'
)
handler.setFormatter(fmt)
logger.addHandler(handler)


def check_row_count():
    total_rows = 0
    file_cnt = 0
    for gf in os.listdir(PATH):
        if gf.endswith('.gz'):
            file_cnt += 1
            with gzip.open(PATH + gf) as f:
                each_r = sum(1 for _ in f)
                # print each_r
                total_rows += each_r
            f.close()

    print 'total row count: {0}'.format(str(total_rows))
    print 'average row count per file: {0}'.format(str(total_rows/file_cnt))
    logger.debug('total row count: {0}'.format(str(total_rows)))
    logger.debug('average row count per file: {0}'.format(str(total_rows/file_cnt)))
    if total_rows < 70000:
        return 1
    if total_rows/file_cnt < 1000:
        return 3
    return 0


def check_file_size():
    total_size = 0
    file_cnt = 0
    for gf in os.listdir(PATH):
        if gf.endswith('.gz'):
            file_cnt += 1
            total_size += os.path.getsize(PATH + gf)

    print 'total size: {0}'.format(str(total_size))
    print 'average file size: {0}'.format(str(total_size/file_cnt))
    logger.debug('total size: {0}'.format(str(total_size)))
    logger.debug('average file size: {0}'.format(str(total_size/file_cnt)))
    if total_size < 12582912: # 12 MB
        return 5
    if total_size/file_cnt < 201400: # 100 kB
        return 7
    return 0


def main():
    stat = 0
    stat += check_row_count()
    stat += check_file_size()
    if stat > 0:
        logger.error('some quality check failed with value {0}, please check log for possible data defect'.format(stat))
        exit(199)


if __name__ == '__main__':
    main()



import multiprocessing
from multiprocessing import freeze_support
import copy_reg
import types
import logging
import datetime
import time
import pymssql
from _mssql import MSSQLException
import os
import json
from facebookads.api import FacebookAdsApi
from facebookads.adobjects.adaccount import AdAccount
from facebookads.adobjects.adreportrun import AdReportRun
from facebookads.exceptions import FacebookRequestError, FacebookError
import gzip
import boto3
import botocore
import shutil


MYSERVER = 'your_sql_server'
MYDB = 'your_database'

class AdInsights(object):
    def __init__(self):
        configs = json.load(open(os.path.join(os.path.dirname(__file__), "facebook_configs.json")))
        self.app_id = configs["app_id"]
        self.app_secret = configs["app_secret"]
        self.access_token = configs["access_token"]
        self.account_ids = configs["account_ids"]
        self.params = configs["insights"]["params"]
        self.fields = configs["insights"]["fields"]
        self.report_local_dir = configs["report_local_dir"]
        self.log_local_dir = configs["log_local_dir"]
        self.reprot_remote_dir = configs["report_remote_dir"]
        
        try:
            FacebookAdsApi.init(self.app_id, self.app_secret, self.access_token)
        except FacebookError:
            print 'got an error when initializing the api, probably due to access token expiring.'
            # logger.debug('got an error when initializing the api, probably due to access token expiring.')
        

    def get_report(self, fields, params, file_dir):
        """
            Uses asynchronous calls to avoid time out. Pins every 15 seconds to check status. Once done, pull 
            the data, and dump data into a local file
            :param fields: all valid fields we can retrieve from an Ad account
            :param params: specifies report level, report dates, breakdowns, filters
            :param file_dir: specifies the downloading location
            :return: None
        """
        with gzip.open(file_dir, 'wb') as outfile:
            for account_id in self.account_ids:
                account = AdAccount('act_' + account_id)

                async_job = account.get_insights(fields=fields, params=params, async=True)
                async_job.remote_read()
                while async_job[AdReportRun.Field.async_status] != 'Job Completed':
                    time.sleep(5)
                    async_job.remote_read()
                time.sleep(1)

                for result in async_job.get_result():
                    json.dump(result.export_all_data(), outfile)
                    outfile.write('\n')
        outfile.close()

    def async_daily_call(self, data_date):
        """
            Tries to pull data from facebook server, if there's an api related error, retries 
            until a successful read, or stop after 5 attempts.
            :param data_date: specifies the date of data that we are pulling in YYYY-MM-DD format
            :return: the same date if succeed, None otherwise
        """
        self.__init__()
        
        logger = logging.getLogger('test_log')
        logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler(filename=self.log_local_dir)
        f = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %I:%M:%S %p'
            )
        handler.setFormatter(fmt=f)
        logger.addHandler(handler)
        
        snapshot_date = datetime.date.today()
        local_file_dir = self.report_local_dir + 'snapshot_date={0}\\'.format(snapshot_date)
        filename = local_file_dir + data_date + '.json.gz'

        self.params['time_range'] = {
            'since': data_date,
            'until': data_date
        }

        report_start = time.time()
        # print 'start generating report for {0}'.format(data_date)
        logger.debug('start pulling data for {0} with pid: {1}'.format(data_date, os.getpid()))

        initial_pause = 2
        for retry_attempt in range(1, 6):
            try:
                # print 'attempt {0} for date {1}'.format(retry_attempt, data_date)
                logger.debug('attempt {0} for date {1}'.format(retry_attempt, data_date))
                self.get_report(self.fields, self.params, filename)
                # create a .done file
                donefile = local_file_dir + data_date + '.done'
                open(donefile, 'w').close()
                break
            except FacebookRequestError as er:
                if retry_attempt == 5:
                    # print 'Failed 5 times. report for {0} failed...'.format(data_date)
                    logger.debug(FacebookRequestError.body(er))
                    logger.debug('Process for {0} terminating after 5 failed runs'.format(data_date))
                    return None
                else:
                    # print 'Failing due to an API error: {0}'.format(FacebookRequestError.body(er))
                    # print 'Retrying... attempt {0}, time into {1}'.format(retry_attempt + 1, time.time() - report_start)
                    logger.debug('Failing due to an API error: {0}'.format(FacebookRequestError.body(er)))
                    logger.debug('Retrying... attempt {0}, time into {1}'.format(retry_attempt + 1, time.time() - report_start))
                    time.sleep(initial_pause ** retry_attempt)

        # log finish of a report
        # print 'report date: {0} done'.format(data_date)
        logger.debug('report date: {0} done'.format(data_date))

        return data_date


class UploadProcess(object):
    uploaded_dates = []

    def callback_success(self, date):
        self.uploaded_dates.append(date)

    def upload(self, data_dates):
        # print 'uploading...'
        logger.debug('uploading...')
        config_json = json.load(open(os.path.join(os.path.dirname(__file__), "facebook_configs.json")))

        with connect_to_sql_server() as conn:
            with conn.cursor() as cursor:
            	cursor.execute("use DWConfig")
                cursor.execute("exec DWConfig.dbo.Decrypt 21")
                access_key = cursor.fetchone()
                cursor.execute("exec DWConfig.dbo.Decrypt 22")
                secret_key = cursor.fetchone()
            conn.close()

        session = boto3.Session(
            aws_access_key_id=access_key[0],
            aws_secret_access_key=secret_key[0]
        )
        s3 = session.resource('s3')
        s3bucket = s3.Bucket(config_json["s3_bucket"])

        snapshot_date = datetime.date.today()
        report_local_dir = config_json["report_local_dir"]
        report_remote_dir = config_json["report_remote_dir"]

        for data_date in data_dates:
            # print 'start uploading report for {0}'.format(data_date)
            logger.debug('start uploading report for {0}'.format(data_date))

            filename = report_local_dir + 'snapshot_date={0}\\'.format(snapshot_date) \
                                        + '{0}.json.gz'.format(data_date)
            if os.path.isfile(filename):
                s3_key = report_remote_dir + 'snapshot_date={0}/'.format(snapshot_date) \
                                                + 'date={0}/'.format(data_date) \
                                                + '{0}.json.gz'.format(data_date)
                try:
                    s3bucket.upload_file(filename, s3_key, Callback=self.callback_success(data_date))
                except botocore.exceptions.ClientError as e:
                    logger.error("boto3 error: {0}".format(e))
                    raise

                # print 'finished uploading report {0}'.format(filename)
                # print 'report uploaded to {0}'.format(s3_key)
                logger.debug('finished uploading report {0}'.format(filename))
                logger.debug('report uploaded to {0}'.format(s3_key))
            else:
                # print 'file {0} not found'.format(filename)
                logger.debug('file {0} not found'.format(filename))

        print 'upload process finish'
        logger.debug('upload process finish')

        return self.uploaded_dates


def connect_to_sql_server():
    try:
       conn = pymssql.connect(server=MYSERVER, database=MYDB)
    except MSSQLException:
       print 'got mssql error, aborting...'
       logging.debug('got mssql error {0}, aborting...'.format(MSSQLException.message))
       raise

    return conn


def sproc_daily_records_create():
    with connect_to_sql_server() as conn:
        cursor = conn.cursor()
        cursor.callproc('audit.AdTechAPICallCreate')
        conn.commit()
    conn.close()


def get_downloading_dates():
    dates = {}
    conn = connect_to_sql_server()
    cursor = conn.cursor()
    cursor.execute("select AdTechApiCallID, DataDate "
                   "from Audit.AdTechAPICall "
                   "where (Downloaded = 0 and cast(DateCreated AS DATE) = cast(GetDATE() AS DATE)) "
                   "or (Required = 1 and Downloaded = 0) "
                   "order by DataDate")
    temp = cursor.fetchall()
    conn.close()

    for t in temp:
        dates[t[1]] = t[0]

    return dates


def get_uploading_dates():
    dates = {}
    conn = connect_to_sql_server()
    cursor = conn.cursor()
    cursor.execute("select AdTechApiCallID, DataDate "
                   "from Audit.AdTechAPICall "
                   "where (UPloadedToS3 = 0 and cast(DateCreated AS DATE) = cast(GetDATE() AS DATE)) "
                   "and Downloaded = 1 "
                   "order by DataDate")
    temp = cursor.fetchall()
    conn.close()

    for t in temp:
        dates[t[1]] = t[0]

    return dates


def sproc_execution_start(api_id=1):
    """
    call the ExecutionStart sproc, returns a single integer as the execution id
    :return:
    """
    with connect_to_sql_server() as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.callproc('audit.AdTechApiExecutionStart', (api_id, ))
            for row in cursor:
                _execution_id = row['id']
        conn.commit()
    conn.close()
    return _execution_id


def sproc_download_start(_call_ids):
    with connect_to_sql_server() as conn:
        with conn.cursor(as_dict=True) as cursor:
            for _id in _call_ids:
                cursor.callproc('audit.AdTechAPICallDownloadStart', (_id, ))
        conn.commit()
    conn.close()


def download_start(_download_jobs):
    # print 'plan to download {0} days of data'.format(len(_download_jobs))
    logger.debug('plan to download {0} days of data'.format(len(_download_jobs)))

    # use parallel process for a list of api calls
    configs = json.load(open(os.path.join(os.path.dirname(__file__), "facebook_configs.json")))
    fb_session = AdInsights()
    copy_reg.pickle(types.MethodType, _pickle_method)

    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())

    download_dir = configs["report_local_dir"]
    download_dir += 'snapshot_date={0}\\'.format(datetime.date.today())

    downloaded_but_not_updated = []
    
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    else:
        exist_dates = []
        for subdir, dirs, files in os.walk(download_dir):
            for f in files:
                if f.endswith('.done'):
                    exist_dates.append(f.split('.')[0])
        for date in exist_dates:
            if date and date in _download_jobs:
                downloaded_but_not_updated.append(_download_jobs.pop(date))
        # print 'found {0} done dates, should not re-download those data.'.format(len(downloaded_but_not_updated))
        logger.debug('found {0} date files that are downloaded but not updated in db.'.format(len(downloaded_but_not_updated)))

        logger.debug('still need to download {0} days of data'.format(len(_download_jobs)))
                
    downloaded_dates = []

    t1 = time.time()
    # pool handles a list of dates, pass each one to fb api call
    try:
        downloaded_dates = pool.map(fb_session.async_daily_call, _download_jobs.keys())
    except TypeError:
        # print TypeError.message
        logging.debug(TypeError.message)

    pool.close()
    pool.join()

    # print 'api call finishes, got {0} days of data done'.format(len(downloaded_dates))
    # print 'it took {0} seconds'.format(time.time() - t1)
    logger.debug('api call finishes, got {0} days of data done'.format(len(downloaded_dates)))
    logger.debug('it took {0} seconds'.format(time.time() - t1))

    # find the dates that are downloaded, get the rowids to update corresponding rows
    _downloaded_call_ids = []

    for d in downloaded_dates:
        if d and d in _download_jobs:
            _downloaded_call_ids.append(_download_jobs[d])

    if len(downloaded_but_not_updated) != 0:
        _downloaded_call_ids += downloaded_but_not_updated

    return _downloaded_call_ids


def upload_start(_upload_jobs):

    # start upload process
    # print 'initializing boto3 for s3 upload process'
    logger.debug('initializing boto3 for s3 upload process')
    s3session = UploadProcess()
    
    # print 'starting to upload {0} days of data'.format(len(_upload_jobs))
    logger.debug('starting to upload {0} days of data'.format(len(_upload_jobs)))

    uploaded_dates = s3session.upload(_upload_jobs.keys())
    _uploaded_call_ids = []
    for date in uploaded_dates:
        if date in _upload_jobs:
            _uploaded_call_ids.append(_upload_jobs[date])

    # print 'upload finishes, uploaded {0} days of data'.format(len(_uploaded_call_ids))
    logger.debug('upload finishes, uploaded {0} days of data'.format(len(_uploaded_call_ids)))

    return _uploaded_call_ids    


def sproc_download_end(_call_ids):
    with connect_to_sql_server() as conn:
        with conn.cursor(as_dict=True) as cursor:
            for _id in _call_ids:
                cursor.callproc('audit.AdTechAPICallDownloadEnd', (_id, ))
        conn.commit()
    conn.close()


def sproc_upload_start(_call_ids):
    with connect_to_sql_server() as conn:
        with conn.cursor(as_dict=True) as cursor:
            for _id in _call_ids:
                cursor.callproc('audit.AdTechAPICallUploadStart', (_id, ))
        conn.commit()
    conn.close()


def sproc_upload_end(_call_ids):
    with connect_to_sql_server() as conn:
        with conn.cursor(as_dict=True) as cursor:
            for _id in _call_ids:
                cursor.callproc('audit.AdTechAPICallUploadEnd', (_id, ))
        conn.commit()
    conn.close()


def sproc_execution_end(_execution_id):
    with connect_to_sql_server() as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.callproc('audit.AdTechApiExecutionEnd', (_execution_id,))
        conn.commit()
    conn.close()


def _pickle_method(m):
    """
    This is a workaround for multiprocessing.pool to pickle unpickable items. It makes pickle
    handle the methods and register it with copy_reg
    :param m:
    :return:
    """
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)


def is_finished_for_the_day():
    download_jobs = get_downloading_dates()
    upload_jobs = get_uploading_dates()

    if len(download_jobs) == 0 and len(upload_jobs) == 0:
        return True
    else:
        return False


def download_is_finished():
    if len(get_downloading_dates()) != 0:
        return False
    else:
        return True


def download_process():
    print 'Download starting...'

    download_jobs = get_downloading_dates()

    sproc_download_start(download_jobs.values())

    downloaded_call_ids = download_start(download_jobs)

    print 'updating Downloaded column'
    sproc_download_end(downloaded_call_ids)
    print 'updating finished'

    print 'Download ending...'


def upload_is_finished():
    if len(get_uploading_dates()) != 0:
        return False
    else:
        return True


def upload_process():

    print 'Upload starting...'

    upload_jobs = get_uploading_dates()

    sproc_upload_start(upload_jobs.values())
    
    uploaded_call_ids = upload_start(upload_jobs)

    print 'updating UploadedToS3 column'
    sproc_upload_end(uploaded_call_ids)
    print 'updating finished'

    print 'Upload ending...'


def remove_local_files():
    config = json.load(open(os.path.join(os.path.dirname(__file__), "facebook_configs.json")))
    report_local_dir = config["report_local_dir"]
    two_weeks_ago = datetime.date.today() - datetime.timedelta(weeks=2)
    for root, dirs, files in os.walk(report_local_dir, topdown=True):
        for d in dirs:
            date = d.split('=')
            if datetime.datetime.strptime(date[1], "%Y-%m-%d").date() < two_weeks_ago:
                print "{0} is older than two weeks, deleting...".format(d)
                try:
                    shutil.rmtree(report_local_dir + d)
                    print '{0} deleted'.format(d)
                except OSError as e:
                    print 'got some error: {0}'.format(e)


def drop_done_file_on_s3():
    with connect_to_sql_server() as conn:
        with conn.cursor() as cursor:
            cursor.execute("exec DWConfig.dbo.Decrypt 21")
            access_key = cursor.fetchone()
            cursor.execute("exec DWConfig.dbo.Decrypt 22")
            secret_key = cursor.fetchone()
    conn.close()

    snapshot_date = str(datetime.date.today())
    config_json = json.load(open(os.path.join(os.path.dirname(__file__), "facebook_configs.json")))
    status_remote_dir = config_json["status_remote_dir"]
    s3key = status_remote_dir + snapshot_date + '.done'
    
    status_local_dir = config_json["status_local_dir"]
    logger.debug('generating done file')
    donefile = status_local_dir + snapshot_date + '.done'
    try:
        open(donefile, 'w').close()
    except IOError as e:
        logger.error("I/O error {0}: {1}".format(e.errno, e.strerror))
    
    try:
        logger.debug('uploading done file to s3')
        session = boto3.Session(
            aws_access_key_id=access_key[0],
            aws_secret_access_key=secret_key[0]
        )
        s3 = session.resource('s3')
        s3bucket = s3.Bucket("zstaging")
        s3bucket.upload_file(donefile, s3key)
    except botocore.exceptions.ClientError as e:
        logger.error("boto3 error: {0}".format(e))
        raise


if __name__ == '__main__':
    """
    Makes multiple connections with sql server through pymssql to read, update auditing tables.
    Uses AdInsights from facebook_api to make api calls and download data to local environment
    Uses UploadProcess from s3_upload_process to upload data to s3
    """
    config_json = json.load(open(os.path.join(os.path.dirname(__file__), "facebook_configs.json")))
    logger = logging.getLogger('test_log')
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(filename=config_json["log_local_dir"])
    f = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %I:%M:%S %p'
    )
    handler.setFormatter(fmt=f)
    logger.addHandler(handler)

    # print 'Process starting...'
    logger.info('Process starting...')

    sproc_daily_records_create()

    if not is_finished_for_the_day():
        execution_id = sproc_execution_start(1)

        if not download_is_finished():
            download_process()
        else:
            # print 'nothing to download'
            logger.info('nothing to download')

        # print 'sleep 10 seconds between download and upload'
        # time.sleep(10)

        if not upload_is_finished():
            upload_process()
        else:
            # print 'nothing to upload'
            logger.info('nothing to upload')

        sproc_execution_end(execution_id)

        
    if is_finished_for_the_day():
        # print 'done for the day'
        logger.info('done for the day')
        remove_local_files()
        drop_done_file_on_s3()

    # print 'Process ending...'
    logger.info('Process ending...')


import logging
from logging.handlers import RotatingFileHandler
from logging.handlers import TimedRotatingFileHandler
import sys
import datetime
import pygrametl
from pygrametl.tables import CachedDimension, TypeOneSlowlyChangingDimension
from pygrametl.datasources import SQLSource


# rotate log based on time
def create_timed_rotating_log(path):
    handler = TimedRotatingFileHandler(path, when="MIDNIGHT", interval=1, backupCount=5)
    return handler


# prepare for logging
log_file = "etl_process.log"

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.DEBUG,
                    datefmt='%Y/%m/%d %I:%M:%S %p',
                    handlers=[create_timed_rotating_log(log_file)])
logger = logging.getLogger(__name__)


# function to look up row ()
def add_foreign_keys(row, keyrefs, dimensions):
    for keyref in keyrefs:
        dim_name = get_lookup_args(keyref)
        row[keyref] = dimensions[dim_name].lookup(row)
        if not row[keyref]:
            logger.warning("{} was not present in the {}".format(keyref, dim_name))
            raise ValueError("{} was not present in the {}".format(keyref, dim_name))
    return row


# function to get dimension name
def get_lookup_args(keyref):
    dim_name = 'dim_' + keyref.replace('_id', '')
    return dim_name


def progress(count, total, status=''):
    bar_len = 50
    filled_len = int(round(bar_len * count / float(total)))
    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)
    sys.stdout.write('{} {}% {}/{} {}\r'.format(bar, percents, count, total, status))
    sys.stdout.flush()


def load_dimensions(output_conn):
    dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=output_conn)
    ret = dict()
    ret['dim_datetime'] = CachedDimension(
        name='dim_datetime',
        key='datetime_id',
        attributes=['epoch',
                    'minute',
                    'minute_20',
                    'minute_30',
                    'hour',
                    'day_of_week',
                    'day_of_month',
                    'week',
                    'month',
                    'year',
                    'period'
                    ],
        lookupatts=['epoch'],
        size=0,
        prefill=True,
        targetconnection=dw_conn_wrapper
    )
    ret['dim_location'] = TypeOneSlowlyChangingDimension(
        name='dim_location',
        key='location_id',
        attributes=['lookup_location',
                    'initial_id',
                    'company_code',
                    'street',
                    'ward',
                    'district',
                    'city',
                    'area',
                    'country',
                    'level1flag',
                    'level2flag',
                    'level3flag',
                    'level4flag',
                    'level5flag',
                    'level6flag',
                    ],
        lookupatts=['lookup_location'],
        cachesize=0,
        prefill=True,
        targetconnection=dw_conn_wrapper
    )
    ret['dim_employee'] = TypeOneSlowlyChangingDimension(
        name='dim_employee',
        key='employee_id',
        attributes=['lookup_employee', 'initial_id', 'company_code', 'login', 'name', 'active', 'mobile', 'email'],
        lookupatts=['lookup_employee'],
        cachesize=0,
        prefill=True,
        targetconnection=dw_conn_wrapper
    )
    ret['dim_partner'] = TypeOneSlowlyChangingDimension(
        name='dim_partner',
        key='partner_id',
        attributes=['lookup_partner', 'initial_id', 'company_code', 'name', 'ref', 'is_company', 'active', 'customer',
                    'supplier',
                    'employee', 'state', 'seq',
                    'seq_order',
                    'street_id', 'classify', 'total_sh'],
        lookupatts=['lookup_partner'],
        cachesize=0,
        prefill=True,
        targetconnection=dw_conn_wrapper
    )

    ret['dim_company'] = TypeOneSlowlyChangingDimension(
        name='dim_company',
        key='company_id',
        attributes=['company_code', 'company_name'],
        lookupatts=['company_code'],
        cachesize=0,
        prefill=True,
        targetconnection=dw_conn_wrapper
    )

    return ret


def transform_handle(class_name, object_name, data_source):
    run_class = class_name()
    final_source = run_class.run_class_function(object_name=object_name, data_source=data_source)
    return final_source


def run_dimension_etl(dimension_name, class_name, pygram_dimension_factory, source_sql,
                      source_conn, output_conn,
                      create_sql):
    """
    This function can be used in any kind of workflow (for example in a celery
    task) or in a simple main program.
    """
    # TODO: add null user to employee dimension
    # print current time
    print('current time is {}'.format(datetime.datetime.now()))
    # connection wrapper
    dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=output_conn)

    # create dimension table by create_sql
    cursor = output_conn.cursor()
    logger.info('create {} if not exist'.format(dimension_name))
    print('create {} if not exist'.format(dimension_name))
    cursor.execute(create_sql)
    output_conn.commit()

    # create index for dimension
    logger.info('create index of {} if not exist'.format(dimension_name))
    print('create index of {} if not exist'.format(dimension_name))
    for lookupatt in pygram_dimension_factory['lookupatts']:
        cursor.execute('''CREATE INDEX IF NOT EXISTS {}_{}_idx
                      ON {}({})'''.format(dimension_name, lookupatt, dimension_name, lookupatt))
    output_conn.commit()

    # Create dimension
    pygram_dim_class = pygram_dimension_factory["class"]
    pygram_dim_object = pygram_dim_class(
        name=pygram_dimension_factory["name"],
        key=pygram_dimension_factory["key"],
        attributes=pygram_dimension_factory["attributes"],
        lookupatts=pygram_dimension_factory["lookupatts"],
        targetconnection=dw_conn_wrapper,
        cachesize=0,
        prefill=True)

    # TODO: handle datetime dimension here

    # Create data_source
    logger.info('start query {}'.format(dimension_name))
    print('start query {}'.format(dimension_name))
    if dimension_name in ['dim_datetime', 'dim_company', 'dim_call_center', 'dim_dong_ho_o', 'dim_dong_ho_tong',
                          'dim_hoa_don_tai_chinh']:
        final_source = source_sql

    else:
        data_source = SQLSource(connection=source_conn, query=source_sql)
        final_source = transform_handle(class_name, dimension_name, data_source)

    # Ensure row into dimension
    list_data_source = list(final_source)
    length_source = len(list_data_source)
    count = 1
    for row in list_data_source:
        pygram_dim_object.scdensure(row)
        progress(count, length_source, status='{}'.format(dimension_name))
        count += 1
    print('done')

    output_conn.commit()


def run_fact_etl(fact_name, class_name, pygram_fact_factory,
                 source_sql, source_conn, output_conn,
                 create_sql, dimensions={}):
    # print current time
    print('current time is {}'.format(datetime.datetime.now()))

    # create connection to dw
    dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=output_conn)
    # TODO: add try statement to raise error

    # create fact_table_object
    pygram_fact_class = pygram_fact_factory["class"]
    pygram_fact_object = pygram_fact_class(
        name=pygram_fact_factory["name"],
        measures=pygram_fact_factory["measures"],
        keyrefs=pygram_fact_factory["keyrefs"],
        targetconnection=dw_conn_wrapper)

    # create fact table by create_sql
    cursor = output_conn.cursor()
    logger.info('create {} if not exist'.format(fact_name))
    print('create {} if not exist'.format(fact_name))
    cursor.execute(create_sql)
    output_conn.commit()

    # create index for each item of primary key group
    logger.info('create index of {} if not exist'.format(fact_name))
    print('create index of {} if not exist'.format(fact_name))
    for keyref in pygram_fact_factory['keyrefs']:
        cursor.execute('''CREATE INDEX IF NOT EXISTS {}_{}_idx
                  ON {}({})'''.format(fact_name, keyref, fact_name, keyref))
    output_conn.commit()

    # Create data_source
    logger.info('start query {}'.format(fact_name))
    print('start query {}'.format(fact_name))
    data_source = SQLSource(connection=source_conn, query=source_sql)

    # handle fact
    final_source = transform_handle(class_name, fact_name, data_source)

    # ensure into fact table
    list_data_source = list(final_source)
    length_source = len(list_data_source)
    if length_source == 0:
        logger.info('no record in query period')
        print('no record in query period')
    else:
        count = 1
        for row in list_data_source:
            row = add_foreign_keys(
                row, pygram_fact_factory["keyrefs"], dimensions)
            # logger debug pkey and value of row
            dict_keyref = {}
            for keyref in pygram_fact_factory['keyrefs']:
                dict_keyref[keyref] = row[keyref]
            for measure in pygram_fact_factory['measures']:
                dict_keyref[measure] = row[measure]
            logger.debug('row {}:{}'.format(count, dict_keyref))
            # The row can then be inserted into the fact table
            pygram_fact_object.ensure(row)
            progress(count, length_source, status='{}'.format(fact_name))
            count += 1
    print('done')
    output_conn.commit()

import optparse
import yaml
import psycopg2
import os, glob
from etl import run_dimension_etl, run_fact_etl, load_dimensions
from dw_object_folder.objects import GetObjects


# function to get config
def get_configs(company_code):
    os.environ['SRC_COMPANY_CODE'] = company_code

    with open('{}.yaml'.format(company_code), 'r') as stream:
        data_loaded = yaml.safe_load(stream)

        dw_db_host = data_loaded['DW_DB_HOST']
        dw_db_port = data_loaded['DW_DB_PORT']
        dw_db_name = data_loaded['DW_DB_NAME']
        dw_db_user = data_loaded['DW_DB_USER']
        dw_db_password = data_loaded['DW_DB_PASSWORD']

        src_db_host = data_loaded['SRC_DB_HOST']
        src_db_port = data_loaded['SRC_DB_PORT']
        src_db_user = data_loaded['SRC_DB_USER']
        src_db_password = data_loaded['SRC_DB_PASSWORD']
        src_db_name = data_loaded['SRC_DB_NAME']

        src_string = """host='{}' port = '{}' dbname='{}' user='{}' password='{}'""".format(src_db_host,
                                                                                            src_db_port,
                                                                                            src_db_name,
                                                                                            src_db_user,
                                                                                            src_db_password)
        src_pgconn = psycopg2.connect(src_string)

        # create connection to new data warehouse
        dw_string = """host='{}' port = '{}' dbname='{}' user='{}' password='{}'""".format(dw_db_host,
                                                                                           dw_db_port,
                                                                                           dw_db_name,
                                                                                           dw_db_user,
                                                                                           dw_db_password)
        dw_pgconn = psycopg2.connect(dw_string)

    return src_pgconn, dw_pgconn


# run_object_class
run_class = GetObjects()


# main
def main(run_dimensions, run_facts, company_yaml, object_name):
    if company_yaml:
        src_pgconn, dw_pgconn = get_configs(company_yaml[:-5])

    if object_name:
        if object_name[:3] == 'dim':
            object_type = 'dimension'
            for folder_path in glob.glob(os.path.join(os.getcwd(), 'dw_object_folder', object_type, '*')):
                if run_class.get_folder_name(folder_path) == object_name:

                    transform, factory = run_class.get_transform_and_factory(object_type,object_name)
                    d = run_class.get_dictionary_object(object_type, transform, factory, folder_path,
                                                                        object_name)
                    try:
                        run_dimension_etl(dimension_name=d['name'],
                                          class_name=d['class'],
                                          pygram_dimension_factory=d["dimension_handler"],
                                          source_conn=src_pgconn,
                                          output_conn=dw_pgconn,
                                          source_sql=d["source_sql"],
                                          create_sql=d["create_sql"])
                    except ValueError:
                        pass

        if object_name[:4] == 'fact':
            list_dimensions = load_dimensions(dw_pgconn)
            object_type = 'fact'
            for folder_path in glob.glob(os.path.join(os.getcwd(), 'dw_object_folder', object_type, '*')):
                if run_class.get_folder_name(folder_path) == object_name:
                    transform, factory = run_class.get_transform_and_factory(object_type, object_name)
                    f = run_class.get_dictionary_object(object_type, transform, factory, folder_path,
                                                                        object_name)
                    try:
                        run_fact_etl(fact_name=f['name'],
                                     class_name=f['class'],
                                     pygram_fact_factory=f["fact_handler"],
                                     source_conn=src_pgconn,
                                     output_conn=dw_pgconn,
                                     source_sql=f["source_sql"],
                                     create_sql=f["create_sql"],
                                     dimensions=list_dimensions)
                    except ValueError:
                        pass

    # If run_dimensions
    if run_dimensions:
        dimension_configs = run_class.get_objects('dimension')

        for d in dimension_configs:
            if d["etl_active"]:
                try:
                    run_dimension_etl(dimension_name=d['name'],
                                      class_name=d['class'],
                                      pygram_dimension_factory=d["dimension_handler"],
                                      source_conn=src_pgconn,
                                      output_conn=dw_pgconn,
                                      source_sql=d["source_sql"],
                                      create_sql=d["create_sql"])
                except ValueError:
                    pass

    # If run_facts
    if run_facts:
        fact_configs = run_class.get_objects('fact')
        list_dimensions = load_dimensions(dw_pgconn)

        for f in fact_configs:
            if f["etl_active"]:
                try:
                    run_fact_etl(fact_name=f['name'],
                                 class_name=f['class'],
                                 pygram_fact_factory=f["fact_handler"],
                                 source_conn=src_pgconn,
                                 output_conn=dw_pgconn,
                                 source_sql=f["source_sql"],
                                 create_sql=f["create_sql"],
                                 dimensions=list_dimensions)
                except ValueError:
                    pass


if __name__ == "__main__":
    parser = optparse.OptionParser()

    parser.add_option('--run_dimensions', action="store_true",
                      default=False, help="Run all active dimension ETLs")

    parser.add_option('--run_facts', action="store_true",
                      default=False, help="Run all active fact ETLs")

    parser.add_option("-c", "--configs", action="store", type="string", dest="company_yaml",
                      help='get config from company')

    parser.add_option('-o', '--object', action='store', type='string', dest='object_name',
                      help='run only this file name')

    options, args = parser.parse_args()
    main(options.run_dimensions, options.run_facts, options.company_yaml, options.object_name)

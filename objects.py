import os, glob
import importlib


class GetObjects:
    # get the code of company
    def get_company_code(self):
        return os.getenv('SRC_COMPANY_CODE')

    # get dirpath
    def get_dir_path(self):
        return os.getcwd()

    # change snake string to camel_string
    def snake_to_camel(self, snake_str):
        components = snake_str.split('_')
        return ''.join(x.title() for x in components)

    # get_source_sql
    def get_source_name(self, folder_path, object_type):
        company_code = self.get_company_code()
        source = ''
        for file in glob.glob(os.path.join(folder_path, 'sql', 'sources_folder', company_code, '*.*')):
            if self.get_folder_name(file)[-3:] == 'sql':
                f = open(file, encoding="utf8")
                source = f.read()
            elif self.get_folder_name(file)[-2:] == 'py':
                source_module = importlib.import_module(
                    'dw_object_folder.{}.{}.sql.sources_folder.{}.source'.format(object_type,
                                                                                 self.get_folder_name(folder_path),
                                                                                 company_code))
                source = source_module.py_source()

        return source

    # get_create_sql
    def get_create_name(self, folder_path):
        company_code = self.get_company_code()
        create = ''
        for file in glob.glob(os.path.join(folder_path, 'sql', 'creates_folder', company_code, '*.*')):
            f = open(file, encoding="utf8")
            create = f.read()
        return create

    # get_folder_name inside folder_path
    def get_folder_name(self, folder_path):
        folder_name = os.path.basename(folder_path)
        return folder_name

    # get transform and factory
    def get_transform_and_factory(self, object_type, folder_name):
        transform = importlib.import_module(
            'dw_object_folder.{}.{}.transform'.format(object_type, folder_name))
        factory = importlib.import_module(
            'dw_object_folder.{}.{}.factory'.format(object_type, folder_name))
        return transform, factory

    # get object of each object
    def get_dictionary_object(self, object_type, transform, factory, folder_path, folder_name):
        if folder_name in ['dim_datetime', 'dim_company']:
            dictionary_object = {
                "name": folder_name,
                "class": getattr(transform, self.snake_to_camel(folder_name)),
                "source_sql": self.get_source_name(folder_path, object_type),
                "create_sql": self.get_create_name(folder_path),
                "{}_handler".format(object_type): getattr(factory, 'pygram_{}_factory'.format(folder_name)),
                "etl_active": False,
            }


        else:
            dictionary_object = {
                "name": folder_name,
                "class": getattr(transform, self.snake_to_camel(folder_name)),
                "source_sql": self.get_source_name(folder_path, object_type),
                "create_sql": self.get_create_name(folder_path),
                "{}_handler".format(object_type): getattr(factory, 'pygram_{}_factory'.format(folder_name)),
                "etl_active": True,
            }
        return dictionary_object

    # run_class_function
    def get_objects(self, object_type):
        object_configs = []

        for folder_path in glob.glob(os.path.join(self.get_dir_path(), 'dw_object_folder', object_type, '*')):
            if self.get_folder_name(folder_path) != '__pycache__':
                for company in glob.glob(
                        os.path.join(self.get_dir_path(), 'dw_object_folder', object_type,
                                     self.get_folder_name(folder_path),
                                     'sql',
                                     'creates_folder', '*')):
                    if self.get_folder_name(company) == self.get_company_code():
                        transform, factory = self.get_transform_and_factory(object_type,
                                                                            self.get_folder_name(folder_path))
                        new_dictionary = self.get_dictionary_object(object_type, transform, factory, folder_path,
                                                                    self.get_folder_name(folder_path))
                        object_configs.append(new_dictionary)

        return object_configs

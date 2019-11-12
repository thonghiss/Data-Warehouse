import os, glob
import datetime

class TransformBase:
    # get the code of company
    def get_company_code(self):
        return os.getenv('SRC_COMPANY_CODE')

    # get the name of current class
    def getName(self):
        return self.__class__.__name__

    # add config name of dimension
    def add_config_name(self, row):
        class_name = self.getName()
        lookup_name = class_name[3:].lower()
        row['lookup_{}'.format(lookup_name)] = str(row['initial_id']) + '_' + self.get_company_code()
        row['company_code'] = self.get_company_code()

    # def to round the time to 10 minutes
    def round_time(self, row):
        '''round a datetime object to 10 minutes (600 seconds) elapse'''
        dt = row['date']
        seconds = (dt.replace() - dt.min).seconds
        rounding = seconds // 600 * 600
        dt_round = dt + datetime.timedelta(0, rounding - seconds)
        row['epoch'] = int(dt_round.timestamp())

    # def to change a datetime object to epoch
    def datetime_to_epoch(self, row):
        '''change a datetime object to epoch'''
        dt = row['date']
        row['date_epoch'] = int(dt.timestamp())

    # def to change a date object to epoch
    def date_to_epoch(self, row):
        '''change a date object to epoch'''
        dt = row['date']
        epoch_value = datetime.datetime(dt.year, dt.month, dt.day)
        row['epoch'] = int(epoch_value.timestamp())

    # function to add month and year to table
    def add_month_and_year(self, row):
        if row['period_name']:
            row['period_year'] = int(row['period_name'][-4:])
            row['period_month'] = int(row['period_name'][:-5])
        else:
            row['period_year'] = None
            row['period_month'] = None
#!/bin/python3

import time
import datetime

class DateManager:

    def __init__( self ):
        self.date_format = '%Y-%m-%d'
        self.year_month_date_format = '%Y-%m'
        
    def get_date_format( self ):
        return self.date_format

    def string_to_date( self, _date_string ):
        return datetime.datetime.strptime( _date_string, self.date_format )

    def string_to_timestamp( self, _date_string ):
        return self.string_to_date( _date_string ).timestamp()
        
    def date_to_timestamp( self, _date ):
        return _date.timestamp()

    def timestamp_to_date( self, _timestamp ):
        #return datetime.datetime.fromtimestamp( _timestamp ).date()
        return datetime.datetime.fromtimestamp( _timestamp )

    def year_month_string_to_date( self, _year_month_string ):
        return datetime.datetime.strptime( _year_month_string, self.year_month_date_format )

    def year_month_string_to_timestamp( self, _year_month_string ):
        return self.year_month_string_to_date( _year_month_string ).timestamp()
        
    def get_min_date( self ):
        return datetime.datetime( 1970, 1, 1 )
        
    def get_max_date( self ):
        #return datetime.date.max
        return datetime.datetime.max

    def get_today( self ):
        #return datetime.date.today()
        return datetime.datetime.today()

    def get_date( self, _year, _month, _day ):
        #return datetime.date( _year, _month, _day )
        return datetime.datetime( _year, _month, _day )

    def advance_to_end_of_month( self, _date ):
        month = _date.month
        year = _date.year
        if( month == 12 ):
            month = 1
            year += 1
        else:
            month = month + 1
            
        return ( datetime.datetime( year, month, 1 ) - datetime.timedelta( days=1 ) )

    def advance_to_next_month( self, _date ):
        month = _date.month
        year = _date.year
        day = _date.day
        if( month == 12 ):
            month = 1
            year += 1
        else:
            month = month + 1

        return datetime.datetime( year, month, day )

    def month_as_string( self, _month_index ):
        month = 'ERROR'
        if( _month_index == 1 ):
            month = 'JAN'
        elif( _month_index == 2 ):
            month = 'FEB'
        elif( _month_index == 3 ):
            month = 'MAR'
        elif( _month_index == 4 ):
            month = 'APR'
        elif( _month_index == 5 ):
            month = 'MAY'
        elif( _month_index == 6 ):
            month = 'JUN'
        elif( _month_index == 7 ):
            month = 'JUL'
        elif( _month_index == 8 ):
            month = 'AUG'
        elif( _month_index == 9 ):
            month = 'SEP'
        elif( _month_index == 10 ):
            month = 'OCT'
        elif( _month_index == 11 ):
            month = 'NOV'
        elif( _month_index == 12 ):
            month = 'DEC'
        else:
            raise IndexError( 'Invalid month:', _month_index )

        return month
     

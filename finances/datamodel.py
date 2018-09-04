#!/bin/python

import finances.dates

class MonthlyIncomeVsExpensesModelManager:

    def __init__( self, _data_manager ):
        self.m_data_manager = _data_manager

    def build_monthly_income_vs_expenses_model( self, _monthly_evaluation_criteria_list ):
        result = MonthlyIncomeVsExpensesModel()
    
        date_manager = finances.dates.DateManager()
    
        # retrieve the sum of all expenses in the given time period
        result_monthly_evaluations = []
        for monthly_evaluation_criteria in _monthly_evaluation_criteria_list:
            #start_date = date_manager.get_today()
            #end_date = date_manager.get_today()
    
            start_date_string = monthly_evaluation_criteria.start_date
            end_date_string = monthly_evaluation_criteria.end_date
    
            result_monthly_evaluation = MonthlyEvaluation()

            # DEBUG
            print( 'start date: ' + start_date_string )
            print( 'end date:   ' + end_date_string )
    
            NONE = 'none'
            if( start_date_string == NONE ):
                start_date = date_manager.get_min_date()
            else:
                start_date = date_manager.year_month_string_to_date( start_date_string )
    
            if( end_date_string == NONE ):
                end_date = date_manager.get_max_date()
            else:
                end_date = date_manager.year_month_string_to_date( end_date_string )

            # Retrieve the date of the first transaction of this month
            result_monthly_evaluation.start_date = self.m_data_manager.get_first_transaction_date_since( start_date )

            # DEBUG
            print( 'monthly evaluation start date: ' + str( result_monthly_evaluation.start_date ) )
    
            # Retrieve the date of the final transaction of this month
            result_monthly_evaluation.end_date = self.m_data_manager.get_last_transaction_date_before( end_date )

            # DEBUG
            print( 'monthly evaluation end date:   ' + str( result_monthly_evaluation.end_date ) )
    
            #month_start = date_manager.get_date( min_date.year, min_date.month, 1 )
            month_start = date_manager.get_date( result_monthly_evaluation.start_date.year, result_monthly_evaluation.start_date.month, 1 )
            month_end = date_manager.advance_to_end_of_month( month_start )

            # DEBUG
            print( 'month start: ' + str( month_start ) )
            print( 'month end:   ' + str( month_end ) )
    
            months = []
            month_start_timestamp = date_manager.date_to_timestamp( month_start )
            #end_date_timestamp = date_manager.date_to_timestamp( max_date )
            #end_date_timestamp = date_manager.date_to_timestamp( end_date )
            end_date_timestamp = date_manager.date_to_timestamp( result_monthly_evaluation.end_date )

            # DEBUG
            print( 'month start timestamp: ' + str( month_start_timestamp ) )
            print( 'end_date_timestamp:    ' + str( end_date_timestamp ) )

            while( month_start_timestamp < end_date_timestamp ):
                # DEBUG
                print( str( month_start_timestamp ) + ' - ' + str( end_date_timestamp ) )

                # Update date range to the next month
                month_end = date_manager.advance_to_end_of_month( month_start )
    
                try:
                    total_income = self.m_data_manager.get_total_amount( month_start, month_end, 'income_' )
                    total_expenses = self.m_data_manager.get_total_amount( month_start, month_end, 'expense_' )
                    distinct_expenses = self.m_data_manager.get_distinct_expenses( month_start, month_end )
                    distinct_income_sources = self.m_data_manager.get_distinct_income_sources( month_start, month_end )
    
                    month = Month()
                    month.start = month_start
                    month.end = month_end
                    month.total_income = total_income
                    month.total_expenses = total_expenses
                    month.distinct_expenses = distinct_expenses
                    month.distinct_income_sources = distinct_income_sources
    
                    months.append( month )
    
                #except InvalidResultException as error_message:
                except Exception as error_message:
                    print( 'ERROR calculating totals:', error_message )
    
                month_start = date_manager.advance_to_next_month( month_start )
                month_start_timestamp = date_manager.date_to_timestamp( month_start )
    
            result_monthly_evaluation.months = months
    
            result.monthly_evaluations.append( result_monthly_evaluation )
    
        return result
    
class MonthlyIncomeVsExpensesModel:
    def __init__( self ):
        #self.months = [] # list of Month objects
        self.monthly_evaluations = [] # list of MonthlyEvaluation objects

class Month:
    def __init__( self ):
        self.start = None
        self.end = None
        self.total_income = None
        self.total_expenses = None
        self.distinct_expenses = None
        self.distinct_income_sources = None

class MonthlyEvaluationCriteria:
    def __init__( self ):
        self.start_date = None
        self.end_date = None

class MonthlyEvaluation:
    def __init__( self ):
        self.start_date = None
        self.end_date = None
        self.months = []


#!/bin/python

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as pyplot
import numpy as np
import finances.dates
import finances.utilities

class ChartBuilder:

    def build_monthly_income_vs_expenses_bar_chart( self, _monthly_evaluation ):
        print( "creating monthly income vs expenses bar chart" )

        income_totals = []
        expenses_totals = []
        labels = []
        #for month in _months:
        for month in _monthly_evaluation.months:
            # DEBUG
            print( 'month total income: ' + str( month.total_income ) )

            income_totals.append( month.total_income )
            expenses_totals.append( month.total_expenses * ( -1 ) )

            month_start = month.start
            #date_manager = dates.DateManager()
            date_manager = finances.dates.DateManager()
            
            label = date_manager.month_as_string( month_start.month ) + ' ' + str( month_start.year )
            labels.append( label )
         
        fig, ax = pyplot.subplots()
        index = np.arange( len( income_totals ) )
        bar_width = 0.35
        opacity = 0.8
         
        rects1 = pyplot.bar(
            index,
            income_totals,
            bar_width,
            alpha=opacity,
            color='#339966',
            label='income' )
         
        rects2 = pyplot.bar(
            index + bar_width,
            expenses_totals,
            bar_width,
            alpha=opacity,
            color='#993333',
            label='expenses' )
         
        pyplot.xlabel( 'Month' )
        pyplot.ylabel( 'Amount' )
        pyplot.title( 'Income vs Expenses' )
        pyplot.xticks( index + bar_width, labels )
        pyplot.legend()
         
        pyplot.tight_layout()

        #utils = utilities.Utilities()
        utils = finances.utilities.Utilities()
        path_to_file = utils.get_workspace_directory() + "/monthly_income_vs_expenses_bar_graph.png"
        pyplot.savefig( path_to_file )
        pyplot.clf()

        return path_to_file


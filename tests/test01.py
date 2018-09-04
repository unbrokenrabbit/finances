import sys
sys.path.append('../finances')

#import finances.thing
import finances.datamodel
import finances.datastore
import finances.importer
import finances.transactions
import finances.graphs

def test01():
    data_manager = finances.datastore.MongoDataManager()
    accounts = data_manager.get_accounts()

    monthly_evaluation_criteria_list = []

    monthly_evaluation_criteria = finances.datamodel.MonthlyEvaluationCriteria()
    monthly_evaluation_criteria.start_date = "none"
    monthly_evaluation_criteria.end_date = "none"

    monthly_evaluation_criteria_list.append( monthly_evaluation_criteria )

    monthly_income_vs_expenses_model_manager = finances.datamodel.MonthlyIncomeVsExpensesModelManager( data_manager )
    monthly_income_vs_expenses_model = monthly_income_vs_expenses_model_manager.build_monthly_income_vs_expenses_model( monthly_evaluation_criteria_list )

    chart_builder = finances.graphs.ChartBuilder()
    for monthly_evaluation in monthly_income_vs_expenses_model.monthly_evaluations:
        path_to_monthly_income_vs_expenses_bar_chart = chart_builder.build_monthly_income_vs_expenses_bar_chart( monthly_evaluation )

    # DEBUG
    accounts_string = ''
    prefix = ''
    for account in accounts:
        accounts_string += prefix + account
        prefix = ','

#print( finances.thing.test01() )
#print( finances.thing.test02() )
test01()

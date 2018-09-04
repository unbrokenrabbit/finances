from abc import ABC, abstractmethod
from pymongo import MongoClient
import pymongo
import finances.dates

class DataManager( ABC ):
    @abstractmethod
    def upsert_transactions( self, _account, _transactions ):
        pass

    @abstractmethod
    def get_accounts( self ):
        pass

class MongoDataManager( DataManager ):
    def get_database( self ):
        mongo_client = MongoClient( 'finances-mongodb' )
        return  mongo_client.finances_db       

    def upsert_transactions( self, _account, _transactions ):
        #mongo_client = MongoClient( 'finances-mongodb' )
        #db = mongo_client.finances_db
        #result = db.test_transactions.insert( { "name":"dave", "age":"34" } )

        #temp = ""
        #for transaction in _transactions:
        #    temp += transaction.type + ','
        #    temp += str( transaction.date ) + ','
        #    temp += str( transaction.amount ) + ','
        #    temp += str( transaction.balance ) + ','
        #    temp += transaction.description
        #    temp += "\n##########################\n"

        #mongo_client = MongoClient( 'finances-mongodb' )
        #db = mongo_client.finances_db
        db = self.get_database()
    
        new_transaction_count = 0
        updated_transaction_count = 0
        for transaction in _transactions:
            row = {}
            row[ 'account' ] = _account
            row[ 'type' ] = transaction.type
            row[ 'date' ] = transaction.date
            row[ 'amount' ] = transaction.amount
            row[ 'balance' ] = transaction.balance
            row[ 'description' ] = transaction.description

            result = db.transactions.update(
                row,
                {
                    "$set": row,
                },
                upsert = True
            )

            if( result[ 'updatedExisting' ] ):
                updated_transaction_count += 1
            else:
                new_transaction_count += 1

        #return "upserting transactions into mongodb"
        result = {}
        result[ 'updated_transaction_count' ] = updated_transaction_count
        result[ 'new_transaction_count' ] = new_transaction_count

        return result

    def get_accounts( self ):
        db = self.get_database()

        accounts = []
        results = db.accounts.find()
        for result in results:
            accounts.append( result[ "name" ] )

        return accounts

    def get_first_transaction_date_since( self, _start_date ):
        db = self.get_database()

        min_date_find_clause = {
            'date': {
                '$gte': _start_date.timestamp()
            }
        }
        #results = db.test_transactions.find( min_date_find_clause ).sort( 'date', pymongo.ASCENDING ).limit( 1 )
        results = db.transactions.find( min_date_find_clause ).sort( 'date', pymongo.ASCENDING ).limit( 1 )
        min_date_transaction = results.next()

        date_manager = finances.dates.DateManager()
        min_date = date_manager.timestamp_to_date( min_date_transaction[ 'date' ] )
        
        return  min_date

    def get_last_transaction_date_before( self, _start_date ):
        db = self.get_database()

        max_date_find_clause = {
            'date': {
                '$lte': _start_date.timestamp()
            }
        }
        #results = db.test_transactions.find( max_date_find_clause ).sort( 'date', pymongo.ASCENDING ).limit( 1 )
        results = db.transactions.find( max_date_find_clause ).sort( 'date', pymongo.DESCENDING ).limit( 1 )
        max_date_transaction = results.next()

        date_manager = finances.dates.DateManager()
        max_date = date_manager.timestamp_to_date( max_date_transaction[ 'date' ] )
        
        return  max_date


    def get_total_amount( self, _start_date, _end_date, _tag_prefix ):
        db = self.get_database()

        matches_clause = {
            '$and':
            [
                { 'date': { '$gte': _start_date.timestamp() } },
                { 'date': { '$lte': _end_date.timestamp() } },
                { 'tag': { '$regex': _tag_prefix + '.*' } }
            ]
        }
        group_clause = {
            '_id': '',
            'total': { '$sum': '$amount' }
        }
        #aggregation_results = db.test_transactions.aggregate(
        aggregation_results = db.transactions.aggregate(
            [
                { '$match': matches_clause },
                { '$group': group_clause }
            ]
        )
        aggregation_result_count = 0
        amount_total = 0.0
        for result in aggregation_results:
            aggregation_result_count += 1
            amount_total = float( result[ 'total' ] )

        if( aggregation_result_count > 1 ):
            #raise InvalidResultException( 'Aggregation result for', _tag_prefix, 'count:', aggregation_result_count, ' (should be <= 1)' )
            raise Exception( 'Aggregation result for', _tag_prefix, 'count:', aggregation_result_count, ' (should be <= 1)' )

        return amount_total

    def get_distinct_income_sources( self, _start_date, _end_date ):
        db = self.get_database()

        matches_clause = {
            '$and':
            [
                { 'date': { '$gte': _start_date.timestamp() } },
                { 'date': { '$lte': _end_date.timestamp() } },
                { 'tag': { '$regex': 'income_.*' } }
            ]
        }
        group_clause = {
            '_id': '$tag',
            'total': { '$sum': '$amount' }
        }
        #aggregation_results = db.test_transactions.aggregate(
        aggregation_results = db.test_transactions.aggregate(
            [
                { '$match': matches_clause },
                { '$group': group_clause },
                { '$sort': { 'total': -1 } }
            ]
        )
        aggregation_result_count = 0
        amount_total = 0.0
        distinct_income_sources = []
        for result in aggregation_results:
            aggregation_result_count += 1
            distinct_income_source = CategorizedTotal()
            distinct_income_source.tag = result[ '_id' ]
            distinct_income_source.total = result[ 'total' ]

            distinct_income_sources.append( distinct_income_source )

        return distinct_income_sources

    def get_distinct_expenses( self, _start_date, _end_date ):
        db = self.get_database()

        matches_clause = {
            '$and':
            [
                { 'date': { '$gte': _start_date.timestamp() } },
                { 'date': { '$lte': _end_date.timestamp() } },
                { 'tag': { '$regex': 'expense_.*' } }
            ]
        }
        group_clause = {
            '_id': '$tag',
            'total': { '$sum': '$amount' }
        }
        #aggregation_results = db.test_transactions.aggregate(
        aggregation_results = db.test_transactions.aggregate(
            [
                { '$match': matches_clause },
                { '$group': group_clause },
                { '$sort': { 'total': 1 } }
            ]
        )
        aggregation_result_count = 0
        amount_total = 0.0
        distinct_expenses = []
        for result in aggregation_results:
            aggregation_result_count += 1
            distinct_expense = CategorizedTotal()
            distinct_expense.tag = result[ '_id' ]
            distinct_expense.total = result[ 'total' ]

            distinct_expenses.append( distinct_expense )

        return distinct_expenses

class CategorizedTotal:
    def __init__( self ):
        self.tag = None
        self.total = None

#class InvalidResultException( Exception ):
#    pass


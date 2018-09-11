from abc import ABC, abstractmethod
from pymongo import MongoClient
import pymongo
from bson.objectid import ObjectId
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

    def get_untagged_transactions( self ):
        transactions = []

        db = self.get_database()

        accounts = []
        results = db.transactions.find( { 'tag' : { "$exists": False } } )
        for result in results:
            transaction = Transaction()
            transaction.account = result[ 'account' ]
            transaction.amount = result[ 'amount' ]
            transaction.balance = result[ 'balance' ]
            transaction.date = result[ 'date' ]
            transaction.description = result[ 'description' ]
            transaction.type = result[ 'type' ]

            transactions.append( transaction )

        return transactions

    def get_tags( _self ):
        db = _self.get_database()

        tags = []
        results = db.tags.find()
        for result in results:
            tag = Tag()
            tag.id = result[ '_id' ]
            tag.name = result[ 'name' ]
            tag.pattern = result[ 'pattern' ]
            tag.income_vs_expense = result[ 'income_vs_expense' ]
            tag.account = result[ 'account' ]

            tags.append( tag )

        return tags

    def upsert_tag( _self, _tag ):
        db = _self.get_database()

        row = {}
        row[ 'name' ] = _tag.name
        row[ 'pattern' ] = _tag.pattern
        row[ 'income_vs_expense' ] = _tag.income_vs_expense
        row[ 'account' ] = _tag.account

        db.tags.update(
            row,
            {
                "$set": row,
            },
            upsert = True
        )

    def apply_tags( _self ):
        _self.strip_tags()

        db = _self.get_database()

        tags = db.tags.find()

        for tag in tags:
            tag_name = tag[ 'name' ]
            tag_pattern = tag[ 'pattern' ]
            tag_account = tag[ 'account' ]
            tag_income_vs_expense = tag[ 'income_vs_expense' ]

            find_clause = {}
            find_clause[ 'description' ] = { '$regex': tag_pattern }
            find_clause[ 'account' ] = tag_account

            if( tag_income_vs_expense == 'income' ):
                find_clause[ 'amount' ] = { '$gte': 0 }
            elif( tag_income_vs_expense == 'expense' ):
                find_clause[ 'amount' ] = { '$lte': 0 }

            results = db.transactions.update(
                find_clause,
                {
                    "$set":
                    {
                        "tag": tag_name
                    }
                },
                multi=True
            )

    def strip_tags( _self ):
        db = _self.get_database()

        db.transactions.update_many(
            {},
            {
                '$unset':
                {
                    'tag': ''
                }
            }
        )

    def delete_tag( _self, _tag_id ):
        db = _self.get_database()
        result = db.tags.delete_one( {'_id': ObjectId( _tag_id ) } )

    def upsert_transactions( self, _account, _transactions ):
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

class Transaction:
    def __init__( self ):
        self.account = ''
        self.amount = None
        self.balance = None
        self.date = ''
        self.description = ''
        self.type = ''

class Tag:
    def __init__( _self ):
        _self.id = None
        _self.name = None
        _self.pattern = None
        _self.income_vs_expense = None
        _self.account = None


from abc import ABC, abstractmethod
from pymongo import MongoClient

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

from abc import ABC, abstractmethod
import datetime
import finances.transactions

class TransactionImporter( ABC ):
    @abstractmethod
    def import_transactions( self, _filename ):
        pass

class CsvTransactionImporter( TransactionImporter ):
    def __init__( self ):
        self.TRANSACTION_FORMAT_CSV_CHASE_CHECKING = 'transaction_format_csv_chase_checking'
        self.TRANSACTION_FORMAT_CSV_CHASE_CREDIT = 'transaction_format_csv_chase_credit'
        self.format_lines = {}
        self.format_lines[ self.TRANSACTION_FORMAT_CSV_CHASE_CHECKING ] = 'Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #'
        self.format_lines[ self.TRANSACTION_FORMAT_CSV_CHASE_CREDIT ] = 'Type,Trans Date,Post Date,Description,Amount'

        #type (income, expense), date, amount, balance, description
        #self.TRANSACTION_ELEMENT_KEY_TYPE = 'type'
        #self.TRANSACTION_ELEMENT_KEY_DATE = 'date'
        #self.TRANSACTION_ELEMENT_KEY_AMOUNT = 'amount'
        #self.TRANSACTION_ELEMENT_KEY_BALANCE = 'balance'
        #self.TRANSACTION_ELEMENT_KEY_DESCRIPTION = 'description'

        self.TRANSACTION_ELEMENT_TYPE_INCOME = 'income' 
        self.TRANSACTION_ELEMENT_TYPE_EXPENSE = 'expense' 
        self.CSV_CHASE_DATE_FORMAT = '%m/%d/%Y'

    def import_transactions( self, _filename ):
        transactions = []
        transactions_file = open( _filename, 'r' )

        # Determine the format of the file
        columns_line = transactions_file.readline().strip()
        input_file_format = self.determine_input_file_format( columns_line )

        # Parse each line of the file and store as a list of transactions
        for line in transactions_file:
            values = self.extract_values_from_input_line( line.strip(), input_file_format )
            transaction = self.translate_values( values, input_file_format )
            #transaction = Transaction()
            #transaction.type = line
            transactions.append( transaction )

        return transactions

    def determine_input_file_format( self, _columns_line ):
        input_file_format = 'invalid' 
        if( _columns_line == self.format_lines[ self.TRANSACTION_FORMAT_CSV_CHASE_CHECKING ] ):
            input_file_format = self.TRANSACTION_FORMAT_CSV_CHASE_CHECKING
        elif( _columns_line == self.format_lines[ self.TRANSACTION_FORMAT_CSV_CHASE_CREDIT ] ):
            input_file_format = self.TRANSACTION_FORMAT_CSV_CHASE_CREDIT

        return input_file_format

    def extract_values_from_input_line( self, _line, _input_file_format ):
        line = _line
        # Correct for any known errors
        if( 'carters, Inc' in _line ):
            line = _line.replace( 'carters, Inc', 'carters Inc' )
        elif( 'GEEKNET, INC' in _line ):
            line = _line.replace( 'GEEKNET, INC', 'GEEKNET INC' )

        values = line.split( ',' )
        if( ( _input_file_format == self.TRANSACTION_FORMAT_CSV_CHASE_CHECKING ) and ( len( values ) != 8 ) ):
            print( 'WARNING - problematic split of', self.TRANSACTION_FORMAT_CSV_CHASE_CHECKING, 'line:', _line )
            print( '        - value count:', str( len( values ) ), ' (should be 8)' )
        if( ( _input_file_format == self.TRANSACTION_FORMAT_CSV_CHASE_CREDIT ) and ( len( values ) != 5 ) ):
            print( 'WARNING - problematic split of', self.TRANSACTION_FORMAT_CSV_CHASE_CREDIT, 'line:', _line )
            print( '        - value count:', str( len( values ) ), ' (should be 5)' )

        return values

    def translate_values( self, _values, _input_file_format ):
        translated_values = {}
        if( _input_file_format == self.TRANSACTION_FORMAT_CSV_CHASE_CHECKING ):
            translated_values = self.translate_csv_chase_checking_values( _values )
        elif( _input_file_format == self.TRANSACTION_FORMAT_CSV_CHASE_CREDIT ):
            translated_values = self.translate_csv_chase_credit_values( _values )

        return translated_values

    # input values should be a list in the following order:
    #     0: Details
    #     1: Posting Date
    #     2: Description
    #     3: Amount
    #     4: Type
    #     5: Balance
    #     6: Check or Slip #
    def translate_csv_chase_checking_values( self, _values ):
        #translated_values = {} #TODO - remove
        transaction = finances.transactions.Transaction()

        # DEBUG
        #print( 'values:', _values )

        # Details
        details = _values[ 0 ]
        if( details == 'DEBIT' ):
            #translated_values[ self.TRANSACTION_ELEMENT_KEY_TYPE ] = self.TRANSACTION_ELEMENT_TYPE_INCOME 
            transaction.type = self.TRANSACTION_ELEMENT_TYPE_INCOME
        elif( details == 'CREDIT' ):
            #translated_values[ self.TRANSACTION_ELEMENT_KEY_TYPE ] = self.TRANSACTION_ELEMENT_TYPE_EXPENSE 
            transaction.type = self.TRANSACTION_ELEMENT_TYPE_EXPENSE
        else:
            print( 'ERROR - unexpected value in Details column of CSV Chase Checking input line:', details )

        # Posting Date
        posting_date = _values[ 1 ]
        timestamp = datetime.datetime.strptime( posting_date, self.CSV_CHASE_DATE_FORMAT ).timestamp()
        #translated_values[ self.TRANSACTION_ELEMENT_KEY_DATE ] = timestamp
        transaction.date = timestamp

        # Description
        #translated_values[ self.TRANSACTION_ELEMENT_KEY_DESCRIPTION ] = _values[ 2 ]
        transaction.description = _values[ 2 ]

        # Amount
        #translated_values[ self.TRANSACTION_ELEMENT_KEY_AMOUNT ] = float( _values[ 3 ] )
        transaction.amount = float( _values[ 3 ] )

        # Balance
        balance = _values[ 5 ].strip()
        if( balance != '' ):
            #translated_values[ self.TRANSACTION_ELEMENT_KEY_BALANCE ] = float( balance )
            transaction.balance = float( balance )

        #return translated_values
        return transaction

    # input values should be a list in the following order:
    #    0: Type
    #    1: Trans Date
    #    2: Post Date
    #    3: Description
    #    4: Amount
    def translate_csv_chase_credit_values( self, _values ):
        #translated_values = {}
        transaction = finances.transactions.Transaction()

        # Type
        transaction_type = _values[ 0 ]
        if( transaction_type == 'Sale' ):
            #translated_values[ self.TRANSACTION_ELEMENT_KEY_TYPE ] = self.TRANSACTION_ELEMENT_TYPE_EXPENSE 
            transaction.type = self.TRANSACTION_ELEMENT_TYPE_EXPENSE 
        elif( transaction_type == 'Return' ):
            #translated_values[ self.TRANSACTION_ELEMENT_KEY_TYPE ] = self.TRANSACTION_ELEMENT_TYPE_INCOME 
            transaction.type = self.TRANSACTION_ELEMENT_TYPE_INCOME 
        else:
            print( 'ERROR - unexpected value in Type column of CSV Chase Credit input line:', transaction_type )

        # Trans Date
        transaction_date = _values[ 1 ]
        timestamp = datetime.datetime.strptime( transaction_date, self.CSV_CHASE_DATE_FORMAT ).timestamp()
        #translated_values[ self.TRANSACTION_ELEMENT_KEY_DATE ] = timestamp
        transaction.date = timestamp

        # Post Date


        # Description
        #translated_values[ self.TRANSACTION_ELEMENT_KEY_DESCRIPTION ] = _values[ 3 ]
        transaction.description = _values[ 3 ]

        # Amount
        #translated_values[ self.TRANSACTION_ELEMENT_KEY_AMOUNT ] = float( _values[ 4 ] )
        transaction.amount = float( _values[ 4 ] )

        # Add an empty balance element
        #translated_values[ self.TRANSACTION_ELEMENT_KEY_BALANCE ] = ''
        transaction.balance = ''
        
        #return translated_values
        return transaction



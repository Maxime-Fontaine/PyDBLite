

class DataBase:
    '''
    Class to manage a SQLite 3 database
    '''


    def __init__(self, DBFile=None):
        '''
        Creator of DataBase class
        if DBFile is specified, the database 'DBFile' is opened or created if it doesn't exist
        '''
        import sys
        self.File = DBFile
        self.DataBase = None
        self.Error = 0
        if self.File != None:
            if self.Open() != 0:
                self.New()


    def Open(self, DBFile=None) :
        """
        Open a connexion with the database specified by 'DBFile'
        This method is called by the constructor if 'DBFile' has been specified at construction
        """
        import sqlite3, os
        import PyDBLite.Errors as Errors
        
        if self.File == None and DBFile == None:
            return Errors.ERROR_FILE_NOT_SPECIFIED
        elif self.File == None and DBFile != None:
            self.File = DBFile
        elif self.File != None and DBFile != None:
            if self.File != DBFile:
                return Errors.ERROR_FILE_ALREADY_SPECIFIED
        
        if os.path.exists(self.File) == True :
            try:
                self.DataBase = sqlite3.connect(self.File)
                self.DataBase.row_factory = dict_factory
            except:
                return Errors.ERROR_CANNOT_CONNECT_DATABASE
            else:
                return 0
        else :
            return Errors.ERROR_FILE_DOESNT_EXIST


    def Close(self):
        """
        Close the connexion to the database 
        """
        self.DataBase.close()


    def New(self, DBFile=None):
        """
        Create and Connect to a new database specified by 'DBFile'
        This method is called by the constructor if 'DBFile' has been specified at construction
        """
        import sqlite3, os
        import PyDBLite.Errors as Errors
        
        if self.File == None and DBFile == None:
            return Errors.ERROR_FILE_NOT_SPECIFIED
        elif self.File == None and DBFile != None:
            self.File = DBFile
        elif self.File != None and DBFile != None:
            if self.File != DBFile:
                return Errors.ERROR_FILE_ALREADY_SPECIFIED
        
        if os.path.exists(self.File) == False :
            try:
                self.DataBase = sqlite3.connect(self.File)
                self.DataBase.row_factory = dict_factory
            except:
                return Errors.ERROR_CANNOT_CONNECT_DATABASE
            else:
                return 0
        else:
            return Errors.ERROR_FILE_ALREADY_EXISTS


    def Save(self):
        '''
        Save data to database
        '''
        self.DataBase.commit()
        return 0


    def Remove(self):
        """
        Remove the database file
        """
        import os
        os.remove(self.File)
        return 0


    def CreateTable(self, Table, Fields):
        '''
        Create a table in the database
        Table is string reperesenting the Table name
        Fields is a list of dictionnaries. Each dic represents a Table field :
            field["name"] = the name of the field
            field["type"] = the type of the field ("INTEGER", "REAL" or "TEXT")
            field["unique"] = Is this field unique (True or False)
        A primary key field called 'key' is automatically created
        '''
        from PyDBLite.Errors import ERROR_ARGUMENT_TYPE, ERROR_BAD_SQL_COMMAND
        if not isinstance(Table, str):
            return ERROR_ARGUMENT_TYPE
        SQLCommand = "CREATE TABLE " + Table + " (key INTEGER PRIMARY KEY, "
        for field in Fields :
            if not isinstance(field["name"], str):
                return ERROR_ARGUMENT_TYPE
            if field["type"].upper() == "INTEGER" :
                if field["unique"] == True :
                    SQLCommand = SQLCommand + '"' + field["name"] + '"' + " INTEGER UNIQUE, "
                else :
                    SQLCommand = SQLCommand + '"' + field["name"] + '"' + " INTEGER, "
            elif field["type"].upper() == "REAL" :
                if field["unique"] == True :
                    SQLCommand = SQLCommand + '"' + field["name"] + '"' + " REAL UNIQUE, "
                else :
                    SQLCommand = SQLCommand + '"' + field["name"] + '"' + " REAL, "
            else :
                if field["unique"] == True :
                    SQLCommand = SQLCommand + '"' + field["name"] + '"' + " TEXT UNIQUE, "
                else :
                    SQLCommand = SQLCommand + '"' + field["name"] + '"' + " TEXT, "
        
        SQLCommand = SQLCommand[:-2] + ")"
        Cursor = self.Execute(SQLCommand)
        if Cursor != ERROR_BAD_SQL_COMMAND:
            return 0
        else:
            return ERROR_BAD_SQL_COMMAND


    def GetTables(self):
        '''
        Return the Tables list of the database
        '''
        from PyDBLite.Errors import ERROR_BAD_SQL_COMMAND
        SQLCommand = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        Cursor = self.Execute(SQLCommand)
        if Cursor != ERROR_BAD_SQL_COMMAND:
            return Cursor.fetchall()
        else:
            return ERROR_BAD_SQL_COMMAND


    def Table(self, TableName):
        '''
        Return a Table class of the table 'TableName'
        '''
        from PyDBLite.Table import Table
        return Table(self.DataBase, TableName)    


    def Execute(self, SQLCommand):
        '''
        Execute SQL command
        '''
        from PyDBLite.Errors import ERROR_BAD_SQL_COMMAND
        DCursor = self.DataBase.cursor()
        try :
            DCursor.execute(SQLCommand)
        except :
            return ERROR_BAD_SQL_COMMAND
        else :
            return DCursor


def dict_factory(cursor, row):
    dico = {}
    for idx, col in enumerate(cursor.description):
        dico[col[0]] = row[idx]
    return dico

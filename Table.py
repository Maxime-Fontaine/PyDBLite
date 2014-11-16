


class Table():
    '''
    Class for manipulating data in SQLite3 data base table
    '''


    def __init__(self, DataBase, TableName):
        '''
        Constructor of the Table class
        It is called by PyDBLite.DataBase.Table method
        '''
        self.DB = DataBase
        self.Name = TableName
        self.Cursor = self.DB.cursor()


    def GetFields(self):
        '''
        Return a list of dictionnaries. Each dictionnary represents a field of the table
        '''
        SQLCommand = "PRAGMA TABLE_INFO(" + self.Name + ")"
        Cursor = self.Execute(SQLCommand)
        Fields = []
        if Cursor != -1 :
            for element in Cursor.fetchall() :
                field = {}
                field["name"] = element["name"]
                if element["pk"] == 1 :
                    field["type"] = "PRIMARY KEY"
                else :
                    field["type"] = element["type"]
                Fields.append(field)
                
        return Fields


    def AddItem(self, Item):
        '''
        Add an item to the table
        Item is a dictionnary of the item to add. Keys and values of the dic are
        respectively the fields name and the fields value of this item
        '''
        TableFields = self.GetFields()
        SQLCommand = "INSERT INTO " + self.Name + " ("
        Values = "("
        for champ in Item.keys() :
            for field in TableFields :
                if field["name"] == champ :
                    SQLCommand = SQLCommand + '"' + champ + '", '
                    Values = Values + '"' + str(Item[champ]) + '", '

        SQLCommand = SQLCommand[:-2] + ")"
        Values = Values[:-2] + ")"
        SQLCommand = SQLCommand + " VALUES " + Values + ";"
        Cursor = self.Execute(SQLCommand)
        print(SQLCommand)
        if Cursor == -1:
            return -1
        else :
            return 1


    def ModifyItem(self, key, NewItem):
        '''
        Modify an item
        key is the primary key of the item to modify
        NewItem is a dictionnary of the fields to modify
        keys and values of NewItem are respectively the fields name and the fields value to modify
        '''
        TableFields = self.GetFields()
        SQL = "UPDATE " + self.Name + " SET "
        Element = " WHERE key = " + str(key)
        Results = 0
        for champ in NewItem.keys() :
            for field in TableFields :
                if field["name"] == champ :
                    SQLCommand = SQL + "" + champ + " = '" + NewItem[champ] + "'" + Element
            Cursor = self.Execute(SQLCommand)
            if Cursor == -1:
                Results = Results - 1
            else :
                Results = Results + 1

        if Results/len(NewItem) == 1:
            return 1
        else :
            return -1


    def DeleteItem(self, key):
        '''
        Modify an item
        key is the primary key of the item to delete
        '''
        if type(key) != list:
            SQLCommand = "DELETE FROM " + self.Name + " WHERE key = " + str(key)
            Cursor = self.Execute(SQLCommand)
            if Cursor == -1:
                return -1
            else :
                return 1
        else:
            Results = 0
            for k in key:
                SQLCommand = "DELETE FROM " + self.Name + " WHERE key = " + str(k)
                Cursor = self.Execute(SQLCommand)
                if Cursor == -1:
                    Results = Results - 1
                else :
                    Results = Results + 1
        
        if Cursor == -1:
            return -1
        else :
            return 1


    def SearchItem(self, Pattern):
        '''
        Return items corresponding to 'Pattern'
        Pattern is a dictionnary representing the pattern to search for
        Keys and values represents respectively the fields name and the fields pattern to search for
        '''
        TableFields = self.GetFields()
        if Pattern != {} :
            SQLCommand = "SELECT * FROM '" + self.Name + "' WHERE "
            for champ in Pattern.keys() :
                for field in TableFields :
                    if field["name"] == champ :
                        SQLCommand = SQLCommand + champ + " LIKE '" + str(Pattern[champ]) + "' AND "

            SQLCommand = SQLCommand[:-5]
        else :
            SQLCommand = "SELECT * FROM " + self.Name
        Cursor = self.Execute(SQLCommand)
        if Cursor == -1:
            return -1
        else :
            return Cursor


    def DisplayAll(self):
        '''
        Return all items present in the table
        '''
        SQLCommand = "SELECT * FROM " + self.Name
        Cursor = self.Execute(SQLCommand)
        if Cursor == -1:
            return -1
        else :
            return Cursor


    def Execute(self, SQLCommand):
        '''
        Execute a SQL command
        '''
        try :
            self.Cursor.execute(SQLCommand)
        except :
            return -1
        else :
            return self.Cursor


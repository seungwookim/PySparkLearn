#https://hdfscli.readthedocs.io/en/latest/quickstart.html#configuration
#setting : ~/.hdfscli.cfg

from hdfs import Config

class HdfsManager:
    """
    HdfsManager : mainly manageing hdfs folders
    lv1 : image, raw rext, parquet types
    lv2 : category
    lv3 : sub category
    lv4 : real files
    """
    def __init__(self):
        self.client = Config().get_client('prod')
        if(self.client.content("/tensormsa/", strict=False) == False):
            self.client.makedirs("/tensormsa/", permission=True)
        if (self.client.content("/tensormsa/dataframe/", strict=False) == False):
            self.client.makedirs("/tensormsa/dataframe/", permission=True)
        self.root = "/tensormsa/dataframe/"

    def search_all_database(self):
        """
        search all databases
        :return: database list
        """
        databases = self.client.list("/tensormsa/dataframe/")
        return databases

    def create_database(self, db_name):
        """

        :param db_name: target database name
        :return: none
        """
        try:
            self.client.makedirs("/tensormsa/dataframe/{0}".format(db_name), permission=777)
        except Exception as e:
            raise Exception(e)

    def delete_database(self, db_name):
        """

        :param db_name: target database name
        :return: none
        """
        try:
            self.client.delete("/tensormsa/dataframe/{0}".format(db_name), recursive=True)
        except Exception as e:
            raise Exception(e)

    def search_database(self, db_name):
        """
        return all tables names
        :param db_name: target database name
        :return: table list
        """
        try:
            return self.client.list("/tensormsa/dataframe/{0}".format(db_name), status=False)
        except Exception as e:
            raise Exception(e)


    def rename_database(self, db_name, change_name):
        """
        rename database
        :param db_name: as-is database name
        :param change_name: tb-be data base name
        :return:
        """
        try:
            return self.client.rename("/tensormsa/dataframe/{0}".format(db_name), "/tensormsa/dataframe/{0}".format(change_name))
        except Exception as e:
            raise Exception(e)

    def create_table(self, db_name, table_name):
        """
        create table
        :param db_name:target database name
        :param table_name:target table name
        :return:
        """
        try:
            self.client.makedirs("/tensormsa/dataframe/{0}/{1}".format(db_name, table_name) , permission=777)
        except Exception as e:
            raise Exception(e)

    def delete_table(self, db_name, table_name):
        """
        delete table
        :param db_name:target database name
        :param table_name:target table name
        :return:
        """
        try:
            self.client.delete("/tensormsa/dataframe/{0}/{1}".format(db_name, table_name), recursive=True)
        except Exception as e:
            raise Exception(e)

    def rename_table(self, db_name, table_name, rename_table):
        """
        rename table
        :param db_name:target database name
        :param table_name:target table name
        :param rename_table:to-be table name
        :return:
        """
        try:
            return self.client.rename("/tensormsa/dataframe/{0}/{1}".format(db_name, table_name), "/tensormsa/dataframe/{0}/{1}".format(db_name, rename_table))
        except Exception as e:
            raise Exception(e)



HdfsManager().create_database("test1")
HdfsManager().create_database("test2")
HdfsManager().create_table("test1", "table1")
HdfsManager().create_table("test1", "table2")


print(HdfsManager().search_all_database())
print(HdfsManager().search_database("test1"))
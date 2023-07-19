import mysql.connector as mc
import pandas as pd
import unittest
from sklearn import datasets as ds


def main():

    creds = get_credentials()
    iris = Iris(creds)
    iris.load()
    iris.display_gt(140)
    
    iris2 = Iris(creds, dbname='data_test2')
    iris2.load()
    iris2.del_observations([0, 1, 2])

    iris.update_observation(0, 'stuff', 5)

    iris.close()
    iris2.close()


def get_credentials():
    return {'user': 'root', 'password': 'big0808blue'}


class Iris:
    def __init__(self, creds, dbname='data_test', new=True):
        self.__conn = self.__get_connection(creds)
        self.__dbname = dbname

        if new:
            self.__create()
            self.__conn = mc.connect(
                host='127.0.0.1',
                user=creds['user'],
                password=creds['password'],
                database=self.__dbname
            )
            
        else:
            self.__conn.cursor.execute("""USE {};""".format(self.__dbname))

    def __create(self):
        mycursor = self.__conn.cursor()

        mycursor.execute('DROP DATABASE IF EXISTS {}'.format(self.__dbname))

        mycursor.execute('CREATE DATABASE {}'.format(self.__dbname))

        mycursor.execute('USE {}'.format(self.__dbname))

        mycursor.execute('''
                   CREATE TABLE iris_data (
                   id INT NOT NULL,
                   feature_sepal_length FLOAT NOT NULL,
                   feature_sepal_width FLOAT NOT NULL,
                   feature_petal_length FLOAT NOT NULL,
                   feature_petal_width FLOAT NOT NULL,
                   target_species VARCHAR(20) NOT NULL,
                   target_species_id INT NOT NULL
               )''')

        mycursor.close()
        print(f'Database and IRIS table created in DB {self.__dbname}')

    def close(self):
        self.__conn.close()
        print('Disconnected')

    def load(self, truncate=False):
        if truncate:
            self.__truncate_iris()
            print('Iris table truncated')

        iris_dataset = ds.load_iris()

        df_iris = pd.DataFrame(iris_dataset.data, columns=['sepal_l', 'sepal_w', 'petal_l', 'petal_w'])

        mycursor = self.__conn.cursor(buffered=True)

        df_iris['Species'] = pd.Categorical.from_codes(iris_dataset.target, iris_dataset.target_names)

        df_iris['SpeciesId'] = iris_dataset.target

        for i, row in df_iris.iterrows():
            sql = 'INSERT INTO iris_data (id, feature_sepal_length, feature_sepal_width, feature_petal_length, feature_petal_width,target_species, target_species_id) VALUES({i},{a},{b},{c},{d},"{e}",{f})'.format(
                i=i, a=row['sepal_l'], b=row['sepal_w'], c=row['petal_l'], d=row['petal_w'], e=row['Species'], f=row['SpeciesId'])

            mycursor.execute(sql)

        self.__conn.commit()

        print('Iris dataset loaded')

    def display_gt(self, n):
        mycursor = self.__conn.cursor()

        sql = 'SELECT * FROM iris_data WHERE id > {a}'.format(a=n)
        mycursor.execute(sql)
        row = mycursor.fetchone()

        while row is not None:
            print(f'{row}')
            row = mycursor.fetchone()
        mycursor.close()

    def update_observation(self, id, new_target_species, new_target_species_id):

        mycursor = self.__conn.cursor()

        mycursor.execute(
            '''
            UPDATE iris_data
            SET target_species = '{}', target_species_id = {}
            WHERE id = {}
            '''.format(new_target_species, new_target_species_id, id)
        )

        mycursor.close()

        self.__conn.commit()

    def del_observations(self, row_ids):

        mycursor = self.__conn.cursor(buffered=True)
        select_id = str('SELECT * FROM iris_data')
        mycursor.execute(select_id)
        for row in row_ids:
            delete_id = 'DELETE FROM iris_data WHERE id = {}'.format(row)
            mycursor.execute(delete_id)
        mycursor.close()

    def __truncate_iris(self):

        mycursor2 = self.__conn.cursor()
        use_db = 'USE {};'.format(self.__dbname)
        mycursor2.execute(use_db)
        trunc_tbl = str("TRUNCATE TABLE iris_data; ")
        mycursor2.execute(trunc_tbl)
        mycursor2.close()

    def __get_connection(self, creds):
        return mc.connect(user=creds['user'], password=creds['password'], host='127.0.0.1', auth_plugin='mysql_native_password')

    def get_row_count(self):

        mycursor = self.__conn.cursor(buffered=True)
        mycursor.execute('SELECT * FROM iris_data')

        count = mycursor.rowcount

        if count == 'None':
            count = 0

        print(f'Row Count is {count}')
        return count


class TestServer(unittest.TestCase):
    def test(self):
        creds = get_credentials()
        db1 = Iris(creds)
        self.assertEqual(db1.get_row_count(), 0)
        db1.load()
        self.assertEqual(db1.get_row_count(), 150)
        db1.load()
        self.assertEqual(db1.get_row_count(), 300)
        db2 = Iris(creds, dbname='data_test2')
        self.assertEqual(db2.get_row_count(), 0)
        db2.load()
        self.assertEqual(db2.get_row_count(), 150)
        db1.load(truncate=True)
        self.assertEqual(db1.get_row_count(), 150)
        db1.display_gt(148)
        db1.update_observation(149, 'stuff', 5)
        db1.display_gt(148)
        db2.display_gt(148)
        db1.del_observations([0, 1, 2, 3, 4, 5])
        self.assertEqual(db1.get_row_count(), 144)
        self.assertEqual(db2.get_row_count(), 150)


if __name__ == '__main__':
    main()
    unittest.main(argv=['first-arg-is-ignored'], exit=False)

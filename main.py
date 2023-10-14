import sqlite3
import openpyxl


class DatabaseManager:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()

    def create_tables(self):
        # Создание таблиц GOODS, COUNTRY и ISG
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS GOODS (
                ID_TOVAR INTEGER PRIMARY KEY,
                NAME_TOVAR TEXT,
                BARCOD TEXT,
                ID_COUNTRY INTEGER,
                ID_ISG INTEGER
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS COUNTRY (
                ID_COUNTRY INTEGER PRIMARY KEY AUTOINCREMENT,
                NAME_COUNTRY TEXT UNIQUE 
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS ISG (
                ID_ISG INTEGER PRIMARY KEY,
                NAME_ISG TEXT UNIQUE
            )
        ''')
        self.conn.commit()

    def insert_data(self, id_tovar, tovar, id_isg, isg, country, barcod):
        # Вставка данных в таблицы COUNTRY, ISG, GOODS
        try:
            self.cursor.execute('INSERT OR IGNORE INTO COUNTRY (NAME_COUNTRY) VALUES (?)', (country,))
        except sqlite3.IntegrityError:
            pass

        id_country = self.cursor.execute('SELECT ID_COUNTRY FROM COUNTRY WHERE NAME_COUNTRY = ?', (country,))
        id_country = id_country.fetchone()[0]
        # Вставка данных в таблицу ISG с игнорированием дубликатов
        try:
            self.cursor.execute('INSERT INTO ISG (ID_ISG, NAME_ISG) VALUES (?, ?)', (id_isg, isg))
        except sqlite3.IntegrityError:
            pass

        try:
            self.cursor.execute(
                'INSERT INTO GOODS (ID_TOVAR, NAME_TOVAR, BARCOD, ID_COUNTRY, ID_ISG) VALUES (?, ?, ?, ?, ?)', (
                    id_tovar,  # ID_TOVAR
                    tovar,  # NAME_TOVAR
                    barcod,  # BARCOD
                    id_country,  # ID_COUNTRY
                    id_isg,  # ID_ISG
                )
            )
        except sqlite3.IntegrityError:
            pass

        self.conn.commit()

    def close_connection(self):
        self.conn.close()


class ExcelProcessor(DatabaseManager):
    def __init__(self, excel_file, db_name):
        super().__init__(db_name)
        self.excel_file = excel_file
        self.create_tables()

    def process_excel(self):
        wb = openpyxl.load_workbook(self.excel_file)
        sheet = wb.active
        for row in sheet.iter_rows(min_row=2, values_only=True):
            id_tovar, tovar, id_isg, isg, country, barcod, *_ = row
            self.insert_data(id_tovar, tovar, id_isg, isg, country, barcod)

    def count_and_save_to_tsv(self):
        country_count = self.cursor.execute("""SELECT COUNTRY.NAME_COUNTRY AS СТРАНА, COUNT(GOODS.ID_TOVAR) AS КОЛИЧЕСТВО_ТОВАРОВ
                                                FROM GOODS
                                                JOIN COUNTRY ON GOODS.ID_COUNTRY = COUNTRY.ID_COUNTRY
                                                GROUP BY COUNTRY.NAME_COUNTRY;
                                            """)
        country_count = country_count.fetchall()
        with open('data.tsv', 'w') as tsv_file:
            for country, count in country_count:
                tsv_file.write(f'{country} - {count}\n')


if __name__ == '__main__':
    excel_processor = ExcelProcessor('data.xlsx', 'base.sqlite')

    excel_processor.process_excel()

    excel_processor.count_and_save_to_tsv()

    excel_processor.close_connection()

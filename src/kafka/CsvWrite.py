import tempfile
import csv
import os

class CsvWrite:

    def __init__(self,filename):
        self.temp_file = tempfile.NamedTemporaryFile(
            delete=False, 
            mode='w', 
            newline='', 
            suffix='.csv', 
            prefix=f"{filename}_"
        )
        self.file_name = self.temp_file.name
        self.writer = csv.writer(self.temp_file)
        print(f"Arquivo temporário criado: {self.file_name}")

    def write_row(self, row):
        self.temp_file.seek(0, os.SEEK_END)
        if isinstance(row, str):
            self.writer.writerow([row])  # Escreve a string em uma lista
        else:
            self.writer.writerow(row)


    def close(self):
        self.temp_file.close()
        return self.file_name
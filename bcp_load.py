import os
import uuid


class DataManipulation:

    def __init__(self):
        self.output_file = os.path.join(
            'tmp',
            f"{uuid.uuid1()}.csv"
        )

    def extract_data_bcp(self, query, ms: dict) -> str:

        output_file = self.output_file

        bash_command = f'/opt/mssql-tools/bin/bcp "{query}" ' \
                           + f'queryout {output_file} ' + f"-S {ms['host']}" + f" -U {ms['user']}" \
                           + f" -P {ms['password']} -c -COEM"

        os.system(bash_command)

from pathlib import Path

import pandas as pd
import requests
from rdflib.namespace import Namespace

from owmeta_core.datasource import DataSource, DataTransformer, Informational
from owmeta_core.data_trans.csv_ds import CSVDataSource

CONTEXT = 'http://example.org/def_context'

BASE_NAMESPACE = Namespace('http://example.org/ds_example/')


class WikipediaText(DataSource):
    '''
    A Wikipedia text
    '''
    base_namespace = BASE_NAMESPACE
    class_context = CONTEXT

    page = Informational(display_name='Page',
            description='Name or the Wikipedia page',
            multiple=False)

    section = Informational(display_name='Section',
            description='Section index within the page',
            multiple=False)

    def get_text(self):
        '''
        Gets the text of the whole page or, if available, the section
        '''
        base = 'https://en.wikipedia.org/w/api.php?action=parse'
        page = self.page.one()
        section = self.section.one()
        url = f'{base}&page={page}&prop=text&format=json'
        if section is not None:
            url += f'&section={section}'
        resp = requests.get(url)
        return resp.json()['parse']['text']['*']


class ExtractWikipediaTables(DataTransformer):
    '''
    Extracts tables from wiki text
    '''
    base_namespace = BASE_NAMESPACE
    class_context = CONTEXT

    input_type = WikipediaText
    output_type = CSVDataSource

    def transform(self, source):
        text = source.get_text()
        frames = pd.read_html(text, header=None)
        outputs = []
        for idx, frame in enumerate(frames):
            output = self.make_new_output((source,),
                    key=f'{source.identifier}#{idx}',
                    direct_key=False)
            output.file_name(f'output_{idx}.csv')
            output.source_file_path = Path(output.file_name.onedef())
            frame.to_csv(output.source_file_path)
            outputs.append(output)
        return outputs

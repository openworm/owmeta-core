'''
A replpacement for numpydoc's docscrape that doesn't require numpydoc's time-consuming
imports
'''
import re
from textwrap import dedent
from collections import namedtuple

parameter_regex = r'''
^(?P<param_name>[*]{0,2}\w+)(?:\s+:\s+(?P<param_type>.+))?\n
(?P<param_description>(^[ ]{4}(?:(\S.*|)\n))+)
'''

regex = r'''
(?P<desc>(?:(^\S.*)?\n)+(?:^\n+(?=Parameters)))?
(^Parameters\n
^-+\n
(?P<parameters>(?:{parameter_regex})+))?
'''.format(parameter_regex=parameter_regex)

first_line_unindented_regex = r'^\S.*\n+(?P<initial_white_space>\s+)'

reference_regex = r'`\s*(?P<tilde>~)?(?P<text>[^`<]+)(\s+<(?P<paren>[^>]+)>)?`'

RE = re.compile(regex, flags=re.VERBOSE | re.MULTILINE)
ParamRE = re.compile(parameter_regex, flags=re.VERBOSE | re.MULTILINE)
FLURE = re.compile(first_line_unindented_regex, flags=re.VERBOSE | re.MULTILINE)
ReferenceRE = re.compile(reference_regex, flags=re.VERBOSE | re.MULTILINE)


ParamInfo = namedtuple('ParamInfo', ('name', 'val_type', 'desc'))


def parse(text):
    resp = {}
    iws_match = FLURE.match(text)
    if iws_match:
        text = iws_match.group('initial_white_space') + text
    if text.startswith('\n'):
        text = text[1:]
    text = dedent(text)

    def desc_matchf(md):
        text = md.group('text')
        if md.group('tilde'):
            text = text.split('.')[-1]
        paren = md.group('paren')
        if paren:
            return f'{text} ({paren})'
        return text

    md = RE.match(text)

    if md:
        desc = md.group('desc')
        if desc:
            desc = ReferenceRE.sub(desc_matchf, desc)
        resp['desc'] = desc
        resp['parameters'] = []
        params = md.group('parameters')
        if params:
            for pmd in ParamRE.finditer(params):
                param_type = pmd.group('param_type')
                tp = ParamInfo(pmd.group('param_name').strip(),
                               param_type and param_type.strip(),
                               pmd.group('param_description').strip())
                resp['parameters'].append(tp)

    if not resp.get('desc') and not resp.get('parameters'):
        desc = text.strip()
        desc = ReferenceRE.sub(desc_matchf, desc)
        resp['desc'] = desc
    return resp

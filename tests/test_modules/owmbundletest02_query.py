from owmeta_core.bundle import Bundle
from owmeta_core.context import Context
from owmeta_core.dataobject import DataObject


with Bundle('person_bundle') as bnd:
    ctx = bnd(Context)().stored
    for x in ctx(DataObject)().load():
        assert type(x).__name__ == 'Person'

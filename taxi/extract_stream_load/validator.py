from pydantic import create_model
import json

type_map = {'string': str, 'int': int, 'float': float}
def create_pyd_dict(schema_dict):
    m = {}
    for field in schema_dict['fields']:
        m.update({field['name']: (type_map[field['type']], field.get("optional", False))})
    return schema_dict['name'], m

def create_pydantic_model(schema_file):
    with open(schema_file, 'r') as f:
        schema = json.load(f)
    
    name, dict_def = create_pyd_dict(schema)
    fields = {}
    for field_name,value in dict_def.items():
        if isinstance(value,tuple):
            fields[field_name]=value
        elif isinstance(value,dict):
            fields[field_name]=(dict_model(f'{name}_{field_name}',value),...)
        else:
            raise ValueError(f"Field {field_name}:{value} has invalid syntax")
    return create_model(name,**fields)


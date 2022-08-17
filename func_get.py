
import json
import os
from datetime import timedelta, datetime
import body

def get_tests():
    file_tests = os.path.join(f'{os.path.dirname(__file__)}/all_answers', 'all_tests.json')
    if not os.path.exists(file_tests) and os.path.getctime(file_tests) < datetime.now():
        body.get_all_tests()

    with open(file_tests) as f:
        all_tests = json.load(f)
        
    return all_tests

def get_info(name, date_from, date_before):
    answer = json.dumps(
        { 'metrix': 
    [
        {'name':'metrix_lol', 'test_group': 13, 'control_group': 4}, 
        {'name':'metrix_kek', 'test_group': 35, 'control_group': 66} 
    ]}
    )
    return answer
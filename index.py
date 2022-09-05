from cgi import test
from flask import Flask
from flask import render_template
from func_get import get_info, get_tests
from json import dumps
from flask import request, url_for
from flask_sqlalchemy import SQLAlchemy
import time
from datetime import datetime

#pipienv

app = Flask(__name__)

#get_info("all_numbers",date_begin, date_end)

@app.route('/')
def hello_world():
    tests = get_tests()
    url_for('static', filename='logic.js')
    url_for('static', filename='xls.js')
    url_for('static', filename='FileSaver.min.js')
    return render_template('hello.html', tests = tests) 

@app.route("/cmd", methods=['GET', 'POST'])
def cmd():

    if request.form['idAB'] == '':
        return dumps({'name': 'Выберите тест', 'error': 1})
    if request.form['date_from'] == '' or request.form['date_before'] == '':
        return dumps({'name': 'Выберите дату', 'error': 1})
    
    dt_start = time.strptime(request.form['date_from'], '%Y-%m-%d')
    dt_end = time.strptime(request.form['date_before'], '%Y-%m-%d')
    current_date = str(datetime.now().date())
    current_date = time.strptime(current_date, '%Y-%m-%d')

    if dt_start > dt_end:
        return dumps({'name': 'Начальная дата больше конечной', 'error': 1})
    if dt_end > current_date:
        return dumps({'name': 'Конечная дата не может быть из будущего', 'error': 1})
    
    print(dt_start)
    print(request.form['idAB'])
    print(request.form['date_from'])
    print(request.form['date_before'])

    dt_start = datetime.strptime(request.form['date_from'], '%Y-%m-%d')
    dt_end = datetime.strptime(request.form['date_before'], '%Y-%m-%d')

    answer = get_info(request.form['idAB'], dt_start, dt_end)
    return dumps({'name': answer, 'error': 0})

@app.route("/all_number", methods=['GET', 'POST'])
def all_number():
    dt_start = datetime.strptime(request.form['date_from'], '%Y-%m-%d')
    dt_end = datetime.strptime(request.form['date_before'], '%Y-%m-%d')

    answer = get_info("all_numbers",dt_start, dt_end)
    return dumps({'name': answer, 'error': 0})

if __name__ == '__main__':  
    app.debug = True
    app.run(host="0.0.0.0", port=9300)
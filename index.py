from cgi import test
from flask import Flask
from flask import render_template
from func_get import get_info, get_tests, get_top
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
    print('metrix_all')
    if request.form['idAB'] == '' and request.form['idAB'] == '':
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
    A_start = request.form['A_start']
    A_end = request.form['A_end']
    B_start = request.form['B_start']
    B_end = request.form['B_end']
    selectAB = request.form['idAB']


    if selectAB = '':
        answer = get_info("all_numbers",dt_start, dt_end, number_list=numbers_len, delta_konv=0.1, delta_avg=20)
    else:
        answer = get_info(idAB, dt_start, dt_end)


    #number_list = [A_start, A_end]
    numbers_len = [(A_start, A_end), (B_start, B_end)]

    #get_info('all_numbers', dt_start, dt_end, number_list=[('0','4'), ('5','9')])
    answer = get_info("all_numbers",dt_start, dt_end, number_list=numbers_len, delta_konv=0.1, delta_avg=20)

    print('answer')
    return dumps({'name': answer, 'error': 0})

@app.route("/all_number", methods=['GET', 'POST'])
def all_number():
    print('all_number')
    dt_start = datetime.strptime(request.form['date_from'], '%Y-%m-%d')
    dt_end = datetime.strptime(request.form['date_before'], '%Y-%m-%d')
    # A_start = request.form['A_start']
    # A_end = request.form['A_end']
    # B_start = request.form['B_start']
    # B_end = request.form['B_end']
    # numbers_len = [(A_start, A_end), (B_start, B_end)]

    # print('all_number_request')
    answer = get_info("all_numbers",dt_start, dt_end, week=True)

    #answer = get_info("4 adsasd",dt_start, dt_end)

    # answer = get_info("all_numbers",dt_start, dt_end)
    print('all_number_data_get')
    print(answer)

    return dumps({'name': answer, 'error': 0})

@app.route("/top_request", methods=['GET', 'POST'])
def top_request():
    print('top_request')
    dt_start = datetime.strptime(request.form['date_from'], '%Y-%m-%d')
    dt_end = datetime.strptime(request.form['date_before'], '%Y-%m-%d')
    A_start = request.form['A_start']
    A_end = request.form['A_end']
    B_start = request.form['B_start']
    B_end = request.form['B_end']

   

    #numbers_len = [A_start, A_end]
    numbers_len = [(A_start, A_end), (B_start, B_end)]
    print(numbers_len)

    print('top_request_request')
    answer = get_top(dt_start, dt_end, numbers_len = numbers_len,delta_plus=True)
    print('top_request_data_get')

    return dumps({'name': answer, 'error': 0})


if __name__ == '__main__':  
    app.debug = True
    app.run(host="0.0.0.0", port=9301)



     #   answer = [
  # {
  #   "date": 1661731200000,
  #   "metrics": "MS Выручка добавленных товаров из всех мест МП (поиск, каталог, подборки и др.)",
  #   "value": 0
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "MS Выручка товаров добавленных из поиска (не пустого)",
  #   "value": 0
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "MS Маржа добавленных товаров из всех мест МП (поиск, каталог, подборки и др.)",
  #   "value": 0
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "MS Маржа товаров добавленных из поиска (не пустого)",
  #   "value": 0
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество выдач 'Ничего не найдено'",
  #   "value": 21333
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество выдач пустого поиска (рекомендации в пустом поиске)",
  #   "value": 772423
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество выдач пустого поиска (рекомендации в пустом поиске) с добавлением",
  #   "value": 28794
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество выдач с интересом: любое действие: добавление, просмотр карточки товара, привозите больше, избранное, списки",
  #   "value": 813254
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество добавленных товаров из всех мест МП (поиск, каталог, подборки и др.)",
  #   "value": 11812
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество запросов 'Ничего не найдено'",
  #   "value": 21333
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество запросов со средней позицией добавления более 12",
  #   "value": 119048
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество запросов со средней позицией добавления более 40",
  #   "value": 11777
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество запросов со средней позицией добавления более 6",
  #   "value": 232129
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество клиентов, добавлявших товары из непустого поиска",
  #   "value": 142615
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество клиентов, заходивших в поиск",
  #   "value": 246564
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество клиентов, оформивших заказ",
  #   "value": 151251
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество клиентов, посещавших МП",
  #   "value": 571406
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество поисковых сессий (Уточнение запроса или исправления ошибки - единая поисковая сессия)",
  #   "value": 2144315
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество разных запросов",
  #   "value": 146770
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество сессий без добавления",
  #   "value": 503266
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество товаров добавленных из поиска (не пустого)",
  #   "value": 784460
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество товаров, добавленных из пустого поиска",
  #   "value": 28794
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество уникальных выдач (не пустых)",
  #   "value": 1269804
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество уникальных выдач с добавлением (не пустых)",
  #   "value": 487788
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество уникальных выдач с позицией добавления более 12",
  #   "value": 73821
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество уникальных выдач с позицией добавления более 40",
  #   "value": 7730
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество уникальных выдач с позицией добавления более 6",
  #   "value": 144710
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Количество уникальных корзин за период",
  #   "value": 680482
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Среднее время от запроса до добавления",
  #   "value": 8.05
  # },
  # {
  #   "date": 1661731200000,
  #   "metrics": "Средняя позиция добавления",
  #   "value": 6.44
  # },
  # {
  #   "date": 1661817600000,
  #   "metrics": "MS Выручка добавленных товаров из всех мест МП (поиск, каталог, подборки и др.)",
  #   "value": 0
  # },
  # {
  #   "date": 1661817600000,
  #   "metrics": "MS Маржа добавленных товаров из всех мест МП (поиск, каталог, подборки и др.)",
  #   "value": 0
  # },
  # {
  #   "date": 1661817600000,
  #   "metrics": "MS Маржа товаров добавленных из поиска (не пустого)",
  #   "value": 0
  # },
  # {
  #   "date": 1661817600000,
  #   "metrics": "Количество выдач 'Ничего не найдено'",
  #   "value": 15027
  # },
  # {
  #   "date": 1661817600000,
  #   "metrics": "Количество выдач пустого поиска (рекомендации в пустом поиске)",
  #   "value": 783408
  # },
  # {
  #   "date": 1661817600000,
  #   "metrics": "Количество выдач пустого поиска (рекомендации в пустом поиске) с добавлением",
  #   "value": 30581
  # },
  # {
  #   "date": 1661817600000,
  #   "metrics": "Количество выдач с интересом: любое действие: добавление, просмотр карточки товара, привозите больше, избранное, списки",
  #   "value": 825859
  # },
  # {
  #   "date": 1661817600000,
  #   "metrics": "Количество добавленных товаров из всех мест МП (поиск, каталог, подборки и др.)",
  #   "value": 12057
  # },
  # {
  #   "date": 1661817600000,
  #   "metrics": "Количество запросов 'Ничего не найдено'",
  #   "value": 15027
  # }]
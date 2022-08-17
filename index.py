from cgi import test
from flask import Flask
from flask import render_template
from func_get import get_info, get_tests
import os
from json import dumps
from flask import request, url_for
from flask_sqlalchemy import SQLAlchemy
#pipienv

app = Flask(__name__)



@app.route('/')
def hello_world():
    tests = get_tests()
    url_for('static', filename='logic.js')
    url_for('static', filename='xls.js')
    url_for('static', filename='FileSaver.min.js')
    return render_template('hello.html', tests = tests) 

@app.route("/cmd", methods=['GET', 'POST'])
def cmd():
    answer = get_info(request.form['idAB'], request.form['date_from'], request.form['date_before'])
    return dumps({'name': answer})


if __name__ == '__main__':  
    app.debug = True
    app.run(host="0.0.0.0", port=9300)
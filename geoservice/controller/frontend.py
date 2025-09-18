import os
from flask import redirect, url_for, render_template

from ..application import app
from ..buildinfo import version


@app.route("/")
def index():
    # return redirect(url_for('apidocs'))
    return render_template('index.html', title='Startpage')


@app.route('/apidocs')
def apidocs():
    return render_template("swagger_ui.html")

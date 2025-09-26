# Copyright 2025 Bundesdruckerei GmbH
# For the license, see the accompanying file LICENSE.md.

import os
from flask import redirect, url_for, render_template

from ..application import app
from ..buildinfo import version


@app.route("/")
def index():
    return render_template('index.html', title='Startpage')


@app.route('/apidocs')
def apidocs():
    return render_template("swagger_ui.html")

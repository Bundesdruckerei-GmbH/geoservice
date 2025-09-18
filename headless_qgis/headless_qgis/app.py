# remember: chmod  ugo+x qgis_process_xyz.sh
from pathlib import Path
import subprocess
from flask import Flask, request, make_response
from tempfile import TemporaryDirectory



app = Flask(__name__)


@app.route('/')
def hello_world():
    return  '<p>Hello, 🌐-less qgis!</p>'


@app.route('/buffer', methods=['POST'])
def buffer():
    distance = request.args.get('distance', default=10.0, type=float)

    with TemporaryDirectory() as tmpdir:
        in_filename = Path(tmpdir) / 'in.gpkg'
        request.files['file'].save(in_filename)
        proc = subprocess.run(
            ['qgis_process_buffer.sh', in_filename, f'{distance}'],
            check=True,
            stdout=subprocess.PIPE,
        )

    response = make_response(proc.stdout, 200)
    response.headers.set('Content-Disposition', 'attachment; filename="result.gpkg"')
    return response


@app.route('/print', methods=['POST'])
def print_qgis_layout():

    with TemporaryDirectory() as tmpdir:
        data_path = Path(tmpdir)
        script_filename = data_path / 'script.py'

        request.files['script'].save(script_filename)

        for file in request.files.getlist('data'):
            file.save(data_path / file.filename)
            file.close()

        subprocess.run(
            ['qgis_print_layout.sh', data_path, data_path / 'out'],
            check=True,
        )

        with (data_path / 'out').open('rb') as f:
            r = make_response(f.read(), 200)
            r.headers.set('Content-Disposition', 'attachment; filename="layout.png"')
            return r
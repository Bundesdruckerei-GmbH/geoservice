import shutil
import sys

from os import path
from pathlib import Path
from qgis.core import QgsApplication, QgsProject, QgsLayoutExporter,QgsVectorLayer


def print_map(data_dir, out_path):
    qgis_path = path.join("usr", "bin", "qgis")
    QgsApplication.setPrefixPath(qgis_path, False)  # QgsApplication.prefixPath()

    qapp = QgsApplication([], False)
    qapp.initQgis()

    project = QgsProject.instance()
    project.read(path.join(data_dir, 'layout.qgz'))

    QgsLayoutExporter(
        project.layoutManager().layoutByName('deu_example')
    ).exportToImage('result.png', QgsLayoutExporter.ImageExportSettings())
    
    shutil.move('result.png', out_path)


if __name__ == "__main__":
    print_map(sys.argv[1], sys.argv[2])

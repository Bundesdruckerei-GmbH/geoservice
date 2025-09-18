import sys
import os
import shutil

from os import path
from qgis.core import (
    QgsApplication,
    QgsProject,
    QgsVectorLayer,
    QgsPrintLayout,
    QgsFillSymbol,
    QgsColorRampShader,
    QgsRasterShader,
    QgsLayoutItem,
    QgsLayoutItemMap,
    QgsLayoutItemLabel,
    QgsLayoutExporter,
    QgsLayoutFrame,
    QgsUnitTypes,
    QgsLayoutPoint,
    QgsLayoutSize,
    QgsRectangle,
    QgsLayoutItemPage,
    QgsLegendStyle,
    QgsMarkerSymbol,
    QgsLayoutItemLegend,
    QgsSimpleMarkerSymbolLayer,
    QgsPalLayerSettings,
    QgsTextFormat,
    QgsTextBufferSettings,
    QgsVectorLayerSimpleLabeling,
    QgsRasterLayer,
    QgsRasterBandStats,
    QgsSingleBandPseudoColorRenderer,
    QgsCoordinateReferenceSystem,
)

from qgis.core import Qgis
from PyQt5.QtGui import QColor, QFont
from PyQt5.QtCore import QRectF


def create_print_layout(project, manager, layout_name):
    print_layout = QgsPrintLayout(project)
    print_layout.initializeDefaults()
    print_layout.setName(layout_name)  # + name of selected country
    for idx, item in enumerate(print_layout.items()):
        if isinstance(item, QgsLayoutItemPage):
            page = print_layout.items()[idx]
            page_size = QgsLayoutSize(80, 80, QgsUnitTypes.LayoutMillimeters)
            page.setPageSize(page_size)
    manager.addLayout(print_layout)

    return print_layout


def get_extent(layers, roi):
    geometries = None
    extent = None
    for layer in layers:
        if "adm" in str(layer):
            for feature in layer.getFeatures(country_query(roi)):
                if not geometries:
                    geometries = feature.geometry().buffer(distance=float(0.05), segments=5)
                    extent = geometries.boundingBox()
                else:
                    geometries = geometries.combine(feature.geometry().buffer(distance=float(0.05), segments=5))
                    extent = geometries.boundingBox()
    return extent


def add_title(print_layout, map_width, roi):
    # --- add title --- #
    title = QgsLayoutItemLabel(print_layout)
    print_layout.addLayoutItem(title)
    title_text = "Bevölkerung nach LandScan"
    title.setText(title_text)
    title.setFont(QFont("Arial", 8, QFont.Bold))
    title.setBackgroundEnabled(True)
    title.setBackgroundColor(QColor(255, 255, 255, int(255 * 0.75)))
    title.adjustSizeToText()
    title_scale = title.sizeForText()
    title.attemptResize(
        QgsLayoutSize(title_scale.width(), 7, QgsUnitTypes.LayoutMillimeters))  # line break after given window size
    title.attemptMove(
        QgsLayoutPoint((map_width / 2) - (title_scale.width() / 2), 1, QgsUnitTypes.LayoutMillimeters),
        useReferencePoint=False
    )

    # --- add subtitle --- #
    subtitle = QgsLayoutItemLabel(print_layout)
    print_layout.addLayoutItem(subtitle)
    subtitle_text = "Länder: " + ", ".join([f"{x}" for x in roi])
    subtitle.setText(subtitle_text)
    subtitle.setFont(QFont("Arial", 6, QFont.Bold))
    subtitle.setFontColor((QColor(87, 92, 97, 255)))
    subtitle.adjustSizeToText()
    subtitle.attemptResize(QgsLayoutSize(75, 1, QgsUnitTypes.LayoutMillimeters))  # line break after given window size
    subtitle_scale = subtitle.sizeForText()
    subtitle.attemptMove(
        QgsLayoutPoint((map_width / 2) - (subtitle_scale.width() / 2), 5, QgsUnitTypes.LayoutMillimeters),
        useReferencePoint=False
    )

    # --- add frame around title and subtitle --- #
    frame = QgsLayoutFrame.create(print_layout)
    print_layout.addLayoutItem(frame)
    frame.attemptSetSceneRect(QRectF(  # positionx, positiony, width,height
        (map_width / 2) - (title_scale.width() / 2), 1, title_scale.width(), 7))
    frame.setFrameEnabled(True)


def add_layer_labelling(layer, field_name):
    layer_settings = QgsPalLayerSettings()
    text_format = QgsTextFormat()

    text_format.setFont(QFont("Arial", 4))
    text_format.setSize(4)

    buffer_settings = QgsTextBufferSettings()
    buffer_settings.setEnabled(True)
    buffer_settings.setSize(0.5)
    buffer_settings.setColor(QColor("white"))

    text_format.setBuffer(buffer_settings)
    layer_settings.setFormat(text_format)

    layer_settings.fieldName = field_name
    layer_settings.placement = Qgis.LabelPlacement.OverPoint
    layer_settings.yOffset = 2.0
    layer_settings.enabled = True

    layer_settings = QgsVectorLayerSimpleLabeling(layer_settings)
    layer.setLabelsEnabled(True)
    layer.setLabeling(layer_settings)
    layer.triggerRepaint()


def add_legend(print_layout, n_classes):
    legend = QgsLayoutItemLegend(print_layout)
    print_layout.addLayoutItem(legend)
    legend.setAutoUpdateModel(False)
    legend.setBackgroundColor(QColor(255, 255, 255, int(255 * 0.5)))
    legend.setFrameEnabled(True)
    legend.setFrameStrokeColor(QColor(0, 0, 0, int(255 * 0.5)))
    legend.setSymbolWidth(2)
    legend.setSymbolHeight(1)
    legend_style = QgsLegendStyle()
    text_format = QgsTextFormat()
    item_font = QFont('Arial')
    text_format.setFont(item_font)
    text_format.setSize(4)
    legend_style.setTextFormat(text_format)
    legend.setStyle(QgsLegendStyle.SymbolLabel, legend_style)
    legend.setStyle(QgsLegendStyle.Title, legend_style)
    legend.setStyle(QgsLegendStyle.Subgroup, legend_style)
    legend.setStyleMargin(QgsLegendStyle.Symbol, QgsLegendStyle.Right, 1)
    legend.setStyleMargin(QgsLegendStyle.Symbol, QgsLegendStyle.Bottom, 0)
    legend.setStyleMargin(QgsLegendStyle.Symbol, QgsLegendStyle.Top, 0.5)

    # --- legend layer definition --- #
    for legend_layerTreeLayer in legend.model().rootGroup().children():
        if "landscan" in legend_layerTreeLayer.name():
            legend_layerTreeLayer.setCustomProperty(
                "legend/title-label",
                "\n Bevölkerungsdichte"
            )
            legend.model().refreshLayerLegend(legend_layerTreeLayer)

    legend.adjustBoxSize()
    if n_classes:
        legend.attemptMove(QgsLayoutPoint(57.5, 68.5 + (33 - (6.5 * n_classes)), QgsUnitTypes.LayoutMillimeters))
    else:
        legend.attemptMove(QgsLayoutPoint(61, 54.5, QgsUnitTypes.LayoutMillimeters))
    legend.attemptResize(QgsLayoutSize(30, 40, QgsUnitTypes.LayoutMillimeters))
    legend.refresh()


def raster_layer_classification(
        raster_layer: QgsRasterLayer,
):
    """
    :param raster_layer:
    :type raster_layer: QgsRasterLayer
    :return:
    """
    stats = raster_layer.dataProvider().bandStatistics(1, QgsRasterBandStats.All)
    minimum = stats.minimumValue
    maximum = stats.maximumValue
    shader_function = QgsColorRampShader()
    shader_function.setColorRampType(QgsColorRampShader.Discrete)
    classification = [  # data_info.txt classification used received by the downloaded Landscan data
        (QColor(255, 255, 255, 0), 1),
        (QColor(255, 255, 190, 255), 5),
        (QColor(255, 255, 115, 255), 25),
        (QColor(255, 255, 0, 255), 50),
        (QColor(255, 170, 0, 255), 100),
        (QColor(255, 102, 0, 255), 500),
        (QColor(255, 0, 0, 255), 2500),
        (QColor(204, 0, 0, 255), 5000),
        (QColor(115, 0, 0, 255), maximum),
    ]
    ramp_items = []
    previous = minimum
    for color, step in classification:
        list_item = QgsColorRampShader.ColorRampItem(step, color, lbl='{0:.2f} - {1:.2f}'.format(previous+1, step))
        previous = step
        ramp_items.append(list_item)
    shader_function.setColorRampItemList(ramp_items)

    raster_shader = QgsRasterShader()
    raster_shader.setRasterShaderFunction(shader_function)

    renderer = QgsSingleBandPseudoColorRenderer(
        input=raster_layer.dataProvider(), band=1, shader=raster_shader)

    raster_layer.setRenderer(renderer)
    raster_layer.renderer().setOpacity(0.5)
    raster_layer.triggerRepaint()

    return len(classification)


def arrange_print_layout(project, print_layout, roi, user_title, user_legend):
    layers = [project.mapLayer(id) for id in project.mapLayers()]

    # --- set layout and layout properties --- #
    print_layout_styled = QgsLayoutItemMap(print_layout)
    print_layout.addLayoutItem(print_layout_styled)
    print_layout_styled.setRect(1, 1, 1, 1)

    # --- set map extent --- #
    extent = get_extent(layers, roi)
    rect = QgsRectangle(extent).scaled(1.1)
    print_layout_styled.setExtent(rect)

    # --- set image size --- #
    print_layout_styled.attemptMove(QgsLayoutPoint(0, 0, QgsUnitTypes.LayoutMillimeters))
    print_layout_styled.attemptResize(QgsLayoutSize(80, 80, QgsUnitTypes.LayoutMillimeters))

    # --- layer layouts --- #
    layer_order = ["roi_capital", "capitals", "roi_highlight", "landscan", "hillshade", "ne_adm0"]

    print_layout_styled.setBackgroundColor(QColor(170, 211, 223, int(255 * 1)))

    # layout QgsSimpleFillSymbolLayer (symbol[0]) and QgsFillSymbol (symbol)
    layer = project.mapLayersByName("ne_adm0")[0]
    symbol = layer.renderer().symbol()
    symbol[0].setStrokeWidth(0.1)
    symbol.setColor(QColor(200, 200, 200, int(255 * 1)))
    layer.triggerRepaint()

    layer = project.mapLayersByName("roi_highlight")[0]
    symbol = layer.renderer().symbol()
    symbol[0].setStrokeWidth(0.3)
    symbol[0].setStrokeColor(QColor(100, 100, 100, int(255 * 1)))
    symbol.setColor(QColor(0, 0, 0, 1))
    layer.triggerRepaint()

    layer = project.mapLayersByName("roi_capital")[0]
    symbol_base = QgsMarkerSymbol.createSimple({'name': 'circle', 'color': QColor(255, 255, 255, int(255 * 1)), 'size': 1.5})
    marker_dot = QgsSimpleMarkerSymbolLayer.create({'shape': 'circle', 'size': 0.3, 'color': QColor(0, 0, 0, int(255 * 1))})
    layer.renderer().setSymbol(symbol_base)
    layer.renderer().symbol().appendSymbolLayer(marker_dot)
    layer.triggerRepaint()

    add_layer_labelling(layer, "NAME_DE")

    layer = project.mapLayersByName("capitals")[0]
    symbol_base = QgsMarkerSymbol.createSimple({'name': 'circle', 'color': QColor(255, 255, 255, int(255 * 1)), 'size': 1})
    marker_dot = QgsSimpleMarkerSymbolLayer.create({'shape': 'circle', 'size': 0.2, 'color': QColor(0, 0, 0, int(255 * 1))})
    layer.renderer().setSymbol(symbol_base)
    layer.renderer().symbol().appendSymbolLayer(marker_dot)
    layer.triggerRepaint()

    # when single band raster is given
    result = [layer for layer in project.instance().mapLayers().values() if "landscan" in layer.name()]
    n_classes = None
    opacity = 0.75
    if result:
        if result[0].bandCount() is 1:
            n_classes = raster_layer_classification(raster_layer=result[0])
        result[0].renderer().setOpacity(opacity)
        result[0].triggerRepaint()

    result = [layer for layer in project.instance().mapLayers().values() if "hillshade" in layer.name()]
    if result:
        result[0].dataProvider().setNoDataValue(1, 206)  # set no-data value for band, cell-value
        result[0].renderer().setOpacity(0.8)

    # --- set layer order --- #
    # layer on position 0 in tree root is displayed on top in map, last layer at the bottom
    root = QgsProject.instance().layerTreeRoot()
    for idx, layer_name in enumerate(layer_order):
        result = [child for child in root.children() if layer_name in child.name()]
        if result:
            child = result[0]
            _child = child.clone()
            root.insertChildNode(idx, _child)
            root.removeChildNode(child)

    # --- get map parameters --- #
    map_item = [i for i in print_layout.items() if isinstance(i, QgsLayoutItemMap)][0]
    map_width = map_item.sizeWithUnits().width()

    # --- set title --- #
    if user_title:
        add_title(print_layout, map_width, roi)

    # --- set legend --- #
    if user_legend:
        add_legend(print_layout, n_classes)


def country_query(roi):
    return " OR ".join([f"(\"adm0_code\" = '{x}')" for x in roi])


def roi_capital_query(roi, ne_featureclass):
    return (f"(\"FEATURECLA\" = '{ne_featureclass}')\n" +
            "AND " + "(" + " OR ".join([f"\"ADM0_A3\" = '{x}'" for x in roi]) + ")")


def other_capitals_query(roi, ne_featureclass):
    return (f"(\"FEATURECLA\" = '{ne_featureclass}')\n" +
            "AND NOT" + "(" + " OR ".join([f"\"ADM0_A3\" = '{x}'" for x in roi]) + ")")


def get_capital_layers(layer, roi, epsg):
    layers_to_create = ["roi_capital", "capitals"]
    layers = []
    for idx, layer_name in enumerate(layers_to_create):
        vector_layer = QgsVectorLayer(f"point", layer_name, "memory")  # point?crs=epsg:4326
        epsg_id = int(epsg.split(":")[1])
        vector_layer.crs().createFromId(epsg_id)
        attributes = layer.dataProvider().fields().toList()
        vector_layer.dataProvider().addAttributes(attributes)
        func = [roi_capital_query, other_capitals_query][idx]
        for feature in layer.getFeatures(func(roi, "Admin-0 capital")):
            vector_layer.startEditing()
            vector_layer.addFeature(feature)
            vector_layer.commitChanges()

        layers.append(vector_layer)
    return layers


def layout(data_dir, out_path, user_roi=None):
    """
    Creates the qgis print layout and returns it as PNG format
        1) region selection according to user ROI
        2) creation of QgsLayoutItemMap (print_layout)
        3) styling of print_layout (add layers (ne_adm0, populated places), text, legend, size, position, etc.)
        4) print of map
    :param data_dir:
    :param out_path:
    :param user_roi:
    :return:
    """
    os.environ["XDG_RUNTIME_DIR"] = "offscreen"

    # --- potential user inputs --- #
    user_layout_name = "landscan_usecase_"
    user_title = True
    user_legend = True
    epsg = "EPSG:4326"
    roi = [region for region in user_roi.split(",")] if user_roi else []

    # --- init --- #
    QgsApplication.setPrefixPath("/usr", True)

    qapp = QgsApplication([], False)
    qapp.initQgis()

    project = QgsProject.instance()
    manager = project.layoutManager()

    # --- change map crs --- #
    # todo: https://gis.stackexchange.com/questions/221528/changing-crs-of-layer-using-pyqgis-without-saving-file-and-iterating-every-featu#answer-447643
    # tansform = QgsCoordinateTansform(QgsCoordinateReferenceSystem(4326), QgsCoordinateReferenceSystem(3857), project)
    # geom.transform(tansform)

    files = os.listdir(data_dir)
    [project.read(path.join(data_dir, file)) for file in files if "qgz" in file]

    # --- set coordinate reference system --- #
    if not project.crs().isValid():
        crs = QgsCoordinateReferenceSystem(epsg)
        project.setCrs(crs)

    # --- add mapLayers to project --- #
    for file in files:
        if "places" in file:
            layer_name = file.split(".")[0]
            populated_places = QgsVectorLayer(path.join(data_dir, file), f"{layer_name}", "ogr")
            vector_layers = get_capital_layers(populated_places, roi, epsg)
            project.addMapLayers(vector_layers)
        if "landscan" in file:
            layer_name = file.split(".")[0]
            raster_layer = QgsRasterLayer(path.join(data_dir, file), f"{layer_name}", "gdal")
            project.addMapLayers([raster_layer])
        if "SR_LR" in file:
            raster_layer = QgsRasterLayer(path.join(data_dir, file), "hillshade", "gdal")
            project.addMapLayers([raster_layer])
        if "adm" in file:
            layer_name = file.split(".")[0]
            base_layer = QgsVectorLayer(path.join(data_dir, file), f"{layer_name}", "ogr")
            roi_layer = QgsVectorLayer(path.join(data_dir, file), f"roi_highlight", "ogr")
            project.addMapLayers([base_layer, roi_layer])

    # --- define layout --- #
    print_layout = create_print_layout(project, manager, user_layout_name)
    arrange_print_layout(project, print_layout, roi, user_title, user_legend)

    # --- export layout to image --- #
    QgsLayoutExporter(
        project.layoutManager().layoutByName(user_layout_name)
    ).exportToImage("landscan.png", QgsLayoutExporter.ImageExportSettings())

    shutil.move('landscan.png', out_path)


if __name__ == "__main__":
    layout(sys.argv[1], sys.argv[2], "DEU")

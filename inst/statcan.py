def convert_statcan_xml_to_csv(infile, outfile):
    import os
    if os.path.isdir(infile):
        data_dir = infile
    else:
        import tempfile
        import zipfile
        data_dir = tempfile.gettempdir() + "/data"
        zip_ref = zipfile.ZipFile(infile, 'r')
        zip_ref.extractall(data_dir)
        zip_ref.close()

    from os import listdir
    from os.path import isfile, join
    onlyfiles = [f for f in listdir(data_dir) if isfile(join(data_dir, f))]

    sf = list(filter(lambda x: 'Structure' in x, onlyfiles))[0]
    gf = list(filter(lambda x: 'Generic' in x, onlyfiles))[0]

    # import xml.etree.ElementTree as ET
    from lxml import etree
    tree = etree.parse(data_dir + "/" + sf)
    root = tree.getroot()

    ns = {'structure': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/structure',
          'ns': 'http://www.w3.org/XML/1998/namespace',
          'generic': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic'}

    all_concepts = [{elem.attrib['id']: elem.find('structure:Name[@ns:lang="en"]', ns).text} for elem in
                    root.iter('{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/structure}Concept')]
    concepts = list(
        set(map(lambda x: list(x.keys())[0], all_concepts)) - set(["GEO", "OBS_VALUE", "OBS_STATUS", "TIME"]))

    def get_description(elem):
        value = elem.attrib['value']
        desc = elem.find("{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/structure}Description").text.encode("utf8")
        return ([value, desc])

    def get_description_for_key(c):
        c = re.sub("0$", "", c)
        e = root.find(
            "{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message}CodeLists/{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/structure}CodeList[@id='CL_" + c + "']")
        if e == None:
            e = root.find(
                "{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message}CodeLists/{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/structure}CodeList[@id='CL_" + c.upper() + "']")
        cs = e.findall("{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/structure}Code")
        a = list(map(lambda e: get_description(e), cs))
        return ({key: value for (key, value) in a})

    import re
    concept_lookup = {}
    geo_lookup = get_description_for_key("GEO")
    for c in concepts:
        concept_lookup[c] = get_description_for_key(c)

    def extract(elem, concepts):
        geo = elem.find(
            "{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic}SeriesKey/{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic}Value[@concept='GEO']").attrib[
            'value']
        geo_name = geo_lookup[geo]
        if (len(geo) == 9): geo = geo[:7] + "." + geo[7:]  # for CTs
        cs = list(map(lambda c: elem.find(
            "{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic}SeriesKey/{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic}Value[@concept='" + c + "']").attrib[
            'value'], concepts))
        cs_names = list(map(lambda c: concept_lookup[concepts[c]][cs[c]], range(0, len(concepts))))
        time = elem.find(
            "{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic}Obs/{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic}Time").text
        value = elem.find(
            "{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic}Obs/{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic}ObsValue")
        if value != None:
            value = value.attrib['value']
        else:
            value = elem.find(
                "{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic}Obs/{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic}Attributes/{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic}Value[@concept='OBS_STATUS']")
            if value != None:
                value = value.attrib['value']
            # print("Found status "+str(value))
        return [geo, geo_name] + cs + cs_names + [time, value]

    import csv
    context = etree.iterparse(data_dir + "/" + gf, events=('end',),
                              tag='{http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic}Series')
    count = 0
    with open(outfile, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(["GeoUID", "Name"] + list(map(lambda x: x + " ID", concepts)) + concepts + ["Year", "Value"])
        for event, elem in context:
            row = extract(elem, concepts)
            csvwriter.writerow(row)
            count += 1
            elem.clear()
            if count % 100000 == 0:
                print("Done with row " + str(count / 1000) + "k")
                csvfile.flush()

    import shutil
    shutil.rmtree(data_dir, ignore_errors=True)


#convert_statcan_xml_to_csv('/Users/jens/stats canada/2001/95F0495XCB2001002', "test.csv")
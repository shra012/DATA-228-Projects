from zipfile import ZipFile, ZIP_DEFLATED
from datetime import datetime
import os
import xml.sax.saxutils as sax
from xml.etree.ElementTree import Element, SubElement, tostring

out_path = os.path.join(os.path.dirname(__file__), 'REPORT.docx')
os.makedirs(os.path.dirname(out_path), exist_ok=True)

sections = [
    ("Dataset Description", [
        "Source: Inside Airbnb open data — curated snapshots of Airbnb activity by city, published as CSV/CSV.GZ per date.",
        "Local sample: airbnb_sf_map_reduce/san_francisco_listings.csv:1 (single-city listings CSV for San Francisco; includes columns such as id, neighbourhood_cleansed, room_type, price, minimum_nights, review metrics, availability, etc.).",
        "Pipeline scope: Focuses on listings to compute average price by neighborhood × room type, identify affordable (budget) listings, and rank neighborhoods by budget supply.",
    ]),
    ("Setup", [
        "Hadoop streaming: Uses the Hadoop Streaming JAR to run Python mappers/reducers. Configure in airbnb_sf_map_reduce/bin/env.sh with HSTREAM_JAR=${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-*.jar.",
        "HDFS namespace: Defaults under /projects/DATA-228-Projects/hw1/airbnb_sf_map_reduce with subpaths for raw, clean, and outputs (avg_by_nb_rt, budget_supply, budget_supply_ranked).",
        "Make targets: make init → make fetch → make data_prep RAW=listings → make avg, make budget, make rank (or make all RAW=listings).",
        "Gotchas: Ensure Java version matches Hadoop; verify HSTREAM_JAR path expands; python3 available on nodes; HDFS permissions OK; .csv.gz read via TextInputFormat; for laptop, pseudo-distributed mode works; adjust YARN memory if container startup fails.",
    ]),
    ("Data Preprocessing", [
        "Input: InsideAirbnb listings CSV from raw/listings/ (or raw/ for full pipeline).",
        "Mapper: Parses CSV, normalizes neighborhood to lowercase, extracts room_type, parses price robustly, validates id/neighborhood/room_type with 0 < price < 5000 and non-negative minimum_nights.",
        "Emits TSV with 11 columns: id, nb, rt, price, min_nights, num_reviews, baths, bedrooms, beds, avail30, avail365.",
        "Reducer: Identity (pass-through). Output goes to clean/.",
    ]),
    ("MapReduce Jobs", [
        "Average price by neighborhood × room type: Mapper emits (nb, rt) → (price, 1); Reducer aggregates sum and count to print nb, rt, avg_price, count (output: avg_by_nb_rt/).",
        "Budget supply: Mapper filters price ≤ 150 and min_nights ≤ 7, emits nb → 1; Reducer counts per neighborhood (output: budget_supply/).",
        "Ranking budget supply: Mapper creates inverted numeric key for descending sort by count; Reducer drops sort key and prints nb, count (output: budget_supply_ranked/).",
    ]),
    ("Results and Insights", [
        "Cleaned schema: id, nb, rt, price, min_nights, num_reviews, baths, bedrooms, beds, avail30, avail365.",
        "Avg output: nb, rt, avg_price, listing_count (e.g., 'mission district, Entire home/apt, 187.35, 42').",
        "Budget counts: nb, budget_listing_count; Ranked: nb, budget_listing_count sorted desc.",
        "Insights: Entire homes priced higher than private/shared rooms; neighborhood variation is strong; minimum-night rules impact short-stay supply; ranked output highlights clusters of affordable options.",
    ]),
    ("Conclusion", [
        "Clean, modular Hadoop Streaming workflow from ingest → clean → aggregate → filter → rank.",
        "Robust parsing and simple outlier guards produce tidy TSVs for analytics.",
        "Answers common questions quickly: typical costs by neighborhood/room type and where budget-friendly, short-stay options concentrate.",
    ]),
    ("References", [
        "Inside Airbnb data: https://insideairbnb.com/get-the-data/",
        "Hadoop Streaming docs: https://hadoop.apache.org/docs/current/hadoop-streaming/HadoopStreaming.html",
        "Project files: README.md, SETUP.md, Makefile, bin/env.sh, bin/fetch_and_put.sh, bin/hdfs_init.sh, bin/run.sh, bin/run_all.sh, jobs/*.",
    ]),
]

code_blocks = [
    ("data_prep/mapper.py snippet", '''#!/usr/bin/env python3
import sys, csv, re
PRICE_RX = re.compile(r"[\\d.,]+")
# ... parse fields, validate, and emit TSV with 11 columns
print(f"{_id}\t{nb}\t{rt}\t{price:.2f}\t{mn}\t{nor}\t{baths}\t{bedr}\t{beds}\t{a30}\t{a365}")
'''),
    ("job_avg/mapper.py snippet", '''# Input TSV (11 cols) → emit (nb, rt) with (price, 1)
print(f"{nb}\t{rt}\t{p}\t1")
'''),
    ("job_avg/reducer.py snippet", '''# Aggregate sum(price), count -> avg
print(f"{nb}\t{rt}\t{(s/c):.2f}\t{c}")
'''),
    ("job_budget/mapper.py snippet", '''# Budget filter: price <= 150 and min_nights <= 7
if p <= 150.0 and mn_i <= 7:
    print(f"{nb}\t1")
'''),
    ("job_budget/reducer.py snippet", '''# Count per neighborhood
print(f"{k}\t{c}")
'''),
    ("job_rank/mapper.py snippet", '''# Invert count for descending sort
inv = MAX - c
print(f"{inv:010d}\t{c}\t{nb}")
'''),
    ("job_rank/reducer.py snippet", '''# Identity: drop sort key, print nb and count
print(f"{nb}\t{c}")
'''),
]

NS_W = 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'

def w(tag):
    return f'{{{NS_W}}}{tag}'

def para(text, bold=False):
    p = Element(w('p'))
    r = SubElement(p, w('r'))
    if bold:
        rPr = SubElement(r, w('rPr'))
        SubElement(rPr, w('b'))
    t = SubElement(r, w('t'))
    t.text = sax.escape(text)
    return p

body = Element(w('body'))
body.append(para('Airbnb SF MapReduce Report', bold=True))

for title, lines in sections:
    body.append(para(''))
    body.append(para(title, bold=True))
    for line in lines:
        body.append(para(line))

body.append(para(''))
body.append(para('MapReduce Jobs — Code Snippets', bold=True))
for label, code in code_blocks:
    body.append(para(label, bold=True))
    for line in code.rstrip('\n').splitlines():
        body.append(para(line))

sectPr = SubElement(body, w('sectPr'))

doc = Element(w('document'))
doc.set('xmlns:w', NS_W)
doc.append(body)

document_xml = '<?xml version="1.0" encoding="UTF-8"?>' + tostring(doc, encoding='unicode')

content_types = '''<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
  </Types>
'''

rels = '''<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="/docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="/docProps/app.xml"/>
  </Relationships>
'''

created = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
core = f'''<?xml version="1.0" encoding="UTF-8"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:title>Airbnb SF MapReduce Report</dc:title>
  <dc:creator>Codex CLI</dc:creator>
  <cp:lastModifiedBy>Codex CLI</cp:lastModifiedBy>
  <dcterms:created xsi:type="dcterms:W3CDTF">{created}</dcterms:created>
  <dcterms:modified xsi:type="dcterms:W3CDTF">{created}</dcterms:modified>
  </cp:coreProperties>
'''

app = '''<?xml version="1.0" encoding="UTF-8"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
  <Application>Python</Application>
  <DocSecurity>0</DocSecurity>
  <ScaleCrop>false</ScaleCrop>
  <HeadingPairs>
    <vt:vector size="2" baseType="variant">
      <vt:variant>
        <vt:lpstr>Title</vt:lpstr>
      </vt:variant>
      <vt:variant>
        <vt:i4>1</vt:i4>
      </vt:variant>
    </vt:vector>
  </HeadingPairs>
  <TitlesOfParts>
    <vt:vector size="1" baseType="lpstr">
      <vt:lpstr>Document</vt:lpstr>
    </vt:vector>
  </TitlesOfParts>
  </Properties>
'''

with ZipFile(out_path, 'w', ZIP_DEFLATED) as z:
    z.writestr('[Content_Types].xml', content_types)
    z.writestr('_rels/.rels', rels)
    z.writestr('docProps/core.xml', core)
    z.writestr('docProps/app.xml', app)
    z.writestr('word/document.xml', document_xml)

print(out_path)


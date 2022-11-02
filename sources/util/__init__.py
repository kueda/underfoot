"""
  Colleciton of methods and scripts for processing geologic unit data, mostly
  from the USGS.
"""

from subprocess import call, run
import json
import os
import re
import shutil
import time
import psycopg2
from glob import glob
import xml.etree.ElementTree as ET
import csv
from datetime import datetime as dt

WEB_MERCATOR_PROJ4 = "+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +over +no_defs"  # noqa: E501
NAD27_UTM10_PROJ4 = "+proj=utm +zone=10 +datum=NAD27 +units=m +no_defs"
NAD27_UTM11_PROJ4 = "+proj=utm +zone=11 +datum=NAD27 +units=m +no_defs"
GRS80_LONGLAT = "+proj=longlat +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +no_defs"
EPSG_4326_PROJ4 = "+proj=longlat +datum=WGS84 +no_defs"
SRS = EPSG_4326_PROJ4
METADATA_COLUMN_NAMES = [
  'code',
  'title',
  'description',
  'lithology',
  'rock_type',
  'formation',
  'grouping',
  'span',
  'min_age',
  'max_age',
  'est_age',
  'controlled_span'
]

# Note that you should try to list substrings *after* longer strings, e.g. mud
# after mudstone, otherwise "mudstone" in text will get matched to "mud"
LITHOLOGY_PATTERN = re.compile(
  # agglomerate|
  re.sub(r'\s+', '', r'''(
    alluvium|
    alluvial.fan|
    andesite|
    andesitic|
    ankaramite|
    aplite|
    arkose|
    basaltic andesite|
    basaltic|
    basalt|
    basanite|
    benmoreite|
    calcerenite|
    chert|
    claystone|
    clay|
    conglomerate|
    dacite|
    diabase|
    dolerite|
    dolomite|
    fanglomerate|
    gabbro|
    gneiss|
    gneissic|
    granite|
    granitic|
    granitoid|
    granodiorite|
    gravel|
    graywacke|
    greenstone|
    hawaiite|
    icelandite|
    keratophyre|
    limestone|
    listwanite|
    listvenite|
    listvanite|
    listwaenite|
    marble|
    m(e|é|é)lange|
    microdiorite|
    monzodiorite|
    monzogranite|
    moraine|
    mudstone|
    mugearite|
    mylonite|
    orthogneiss|
    paragneiss|
    pegmatite|
    pelit(e|ic)|
    peridotite|
    picrite|
    plutonic\srock|
    pyroxenite|
    quartz(\-lithic)?\sarenite|
    quartz\sdiorite|
    quartz\skeratophyre|
    quartz\slatite|
    quartz\smonzonite|
    quartzite|
    rhyodacite|
    rhyolite|
    rhyolitic|
    sandstone|
    schist|
    serpentine|
    serpentinite|
    shale|
    silica(\-|\s)carbonate|
    siltstone|
    surficial\sdeposit|
    syenite|
    talus|
    tephrite|
    till|
    tonalite|
    trachyte|
    tuff|
    volcanoclastic\sbreccia
  )''', flags=re.MULTILINE),
  flags=re.VERBOSE | re.I
)

# Words that should take lower precedence when trying to parse a lithology
# from text
LOW_PRIORITY_LITHOLOGY_PATTERN = re.compile(
  re.sub(r'\s+', '', r'''(
    arenaceous|
    artificial|
    carbonate rock|
    colluvium|
    landslide|
    levee|
    mud|
    sand|
    silt|
    unconsolidated\smaterial|
    water|
    metasedimentary|
    sedimentary|
    volcanic|
    (?# Very short so putting it at the end so others don't match)
    fill
  )''', flags=re.MULTILINE),
  flags=re.VERBOSE | re.I
)

LITHOLOGY_SYNONYMS = {
  'alluvial-fan': 'alluvial fan',
  'andesitic': 'andesite',
  'arenaceous': 'sand',
  'basaltic': 'basalt',
  'dolostone': 'dolomite',
  'dolerite': 'diabase',
  'dolostone (dolomite)': 'dolomite',
  'fanglomerate': 'alluvial fan',
  'fill': 'artificial',
  'gneissic': 'gneiss',
  'granitic': 'granite',
  'listwanite': 'silica-carbonate',
  'listvenite': 'silica-carbonate',
  'listvanite': 'silica-carbonate',
  'listwaenite': 'silica-carbonate',
  'metasedimentary': 'metasedimentary rock',
  'mélange': 'melange',
  'mélange': 'melange',
  'orthogneiss': 'gneiss',
  'paragneiss': 'gneiss',
  'pelitic': 'pelite',
  'picrobasalt': 'picrite',
  'quartz-lithic arenite': 'quartz arenite',
  'rhyolitic': 'rhyolite',
  'sedimentary': 'sedimentary rock',
  'serpentine': 'serpentinite',
  'silica carbonate': 'silica-carbonate',
  'surficial deposit': 'surficial deposits',
  'volcanic': 'volcanic rock',
}

IGNEOUS_ROCKS = [
  'agglomerate',
  'andesite',
  'aplite',
  'basalt',
  'basaltic andesite',
  'basanite',
  'benmoreite'
  'breccia',
  'dacite',
  'diabase',
  'dolerite',
  'gabbro',
  'granite',
  'granitoid',
  'granodiorite',
  'hawaiite',
  'icelandite',
  'keratophyre',
  'microdiorite',
  'monzodiorite',
  'monzogranite',
  'mugearite',
  'pegmatite',
  'peridotite',
  'picrite',
  'plutonic rock',
  'pyroxenite',
  'quartz diorite',
  'quartz keratophyre',
  'quartz latite',
  'quartz monzonite',
  'rhyodacite',
  'rhyolite',
  'syenite',
  'tephrite',
  'tonalite',
  'trachyte',
  'tuff',
  'volcanic rock',
  'volcanoclastic breccia'
]

METAMORPHIC_ROCKS = [
  'gneiss',
  'greenstone',
  'marble',
  'mylonite',
  'quartzite',
  'schist',
  'serpentinite',
  'silica-carbonate',
  'metasedimentary rock'
]

SEDIMENTARY_ROCKS = [
  'arkose',
  'carbonate rock',
  'calcerenite',
  'chert',
  'clay',
  'claystone',
  'conglomerate',
  'dolomite',
  'graywacke',
  'limestone',
  'mudstone',
  'pelite',
  'quartz arenite',
  'sandstone',
  'schist',
  'shale',
  'siltstone',
  'sedimentary rock'
]

NON_ROCKS = [
  'alluvium',
  'alluvial fan',
  'colluvium',
  'landslide',
  'melange',
  'moraine',
  'sand',
  'talus',
  'till'
]

GROUPING_PATTERN = re.compile(
  r'([Ff]ranciscan [Cc]omplex|[Gg]reat [Vv]alley [Ss]equence)'
)
FORMATION_PATTERN = re.compile(r'([A-Z]\w+ )+\s?([Ff]ormation|[Tt]errane)')

# Adapted from https://en.wikipedia.org/w/index.php?title=Template:Period_start&action=edit  # noqa: E501
WIKI_SPANS = {
  "precambrian": [4600, 541.0],
    "hadean": [4600, 4000],  # noqa: E131
    "archaean": [4000, 2500],
      "eoarchean": [4000, 3600],  # noqa: E131
      "isuan": [4000, 3600],
      "paleoarchean": [3600, 3200],
      "mesoarchean": [3200, 2800],
      "neoarchean": [2800, 2500],
    "proterozoic": [2500, 541.0],
      "early proterozoic": [2500, 1600],
      "paleoproterozoic": [2500, 1600],
        "siderian": [2500, 2300],  # noqa: E131
        "rhyacian": [2300, 2050],
        "orosirian": [2050, 1800],
        "statherian": [1800, 1600],
      "middle proterozoic": [1600, 1000],
      "mesoproterozoic": [1600, 1000],
        "calymmian": [1600, 1400],
        "ectasian": [1400, 1200],
        "riphean": [1400, 1200],
        "stenian": [1200, 1000],
          "mayanian": [1100, 1050],  # noqa: E131
          "sinian": [1050, 1000],
          "sturtian": [1050, 1000],
      "late proterozoic": [1000, 541.0],
      "neoproterozoic": [1000, 541.0],
        "tonian": [1000, 850],
        "baikalian": [850, 720],
        "cryogenian": [720, 635],
        "ediacaran": [635, 541.0],
        "vendian": [635, 541.0],
  "phanerozoic": [541,0],  # noqa: E231
    "paleozoic": [541, 251.902],
      "cambrian": [541, 485.4],
        "lower cambrian": [541, 509],
          "terreneuvian": [541, 521],
          "lowest cambrian": [541, 521],
          "earliest cambrian": [541, 521],
            "fortunian": [541, 529],  # noqa: E131
              "manykaian": [541, 530],  # noqa: E131
              "nemakit daldynian": [541, 530],
              "caerfai": [530, 529],
              "tommotian": [530, 529],
            "cambrian stage 2": [529, 521],
          "cambrian series 2": [522, 509],
            "cambrian stage 3": [522, 514],
            "middle lower cambrian": [522, 514],
              "botomian": [522, 521],
              "atdabanian": [521, 516],
              "toyonian": [516, 514],
              "upper lower cambrian": [516, 514],
            "cambrian stage 4": [514, 509],
          "miaolingian": [509, 497],
          "cambrian series 3": [509, 497],
          "middle cambrian": [509, 497],
            "wuliuan": [509, 504.5],
            "cambrian stage 5": [509, 504.5],
            "lower middle cambrian": [509, 504.5],
            "st davids": [509, 504.5],
            "drumian": [504.5, 500.5],
            "guzhangian": [500.5, 497],
            "nganasanian": [500.5, 497],
            "mindyallan": [500.5, 497],
          "furongian": [497, 485.4],
          "upper cambrian": [497, 485.4],
          "merioneth": [497, 485.4],
            "paibian": [497, 494],
            "franconian": [497, 494],
            "jiangshanian": [494, 485.4],
            "cambrian stage 10": [489.5, 485.4],
      "ordovician": [485.4, 443.8],
        "lower ordovician": [485.4, 470.0],
          "tremadocian": [485.4, 477.7],
            "upper lower ordovician": [479, 477.7],
          "floian": [477.7, 470.0],
          "arenig": [477.7, 470.0],
        "middle ordovician": [470.0, 458.4],
          "dapingian": [470.0, 458.4],
          "ordovician iii": [470.0, 458.4],
          "lower middle ordovician": [470.0, 458.4],
          "darriwilian": [467.3, 458.4],
        "upper ordovician": [458.4, 443.8],
          "sandbian": [458.4, 453.0],
          "ordovician v": [458.4, 453.0],
          "lower upper ordovician": [458.4, 453.0],
            "middle upper ordovician": [455, 453.0],
          "katian": [453.0, 445.2],
          "ordovician vi": [453.0, 445.2],
          "hirnantian": [445.2, 443.8],
      "silurian": [443.8, 419.2],
        "llandovery": [443.8, 433.4],
        "lower silurian": [443.8, 433.4],
          "rhuddanian": [443.8, 440.8],
          "aeronian": [440.8, 438.5],
          "telychian": [438.5, 433.4],
        "wenlock": [433.4, 427.4],
          "sheinwoodian": [433.4, 430.5],
          "homerian": [430.5, 427],
        "ludlow": [427.4, 423.0],
        "upper silurian": [427.4, 423.0],
          "gorstian": [427.4, 425.6],
          "ludfordian": [425.6, 423.0],
        "pridoli": [423.0, 419.2],
          "unnamed pridoli stage": [423.0, 419.2],
      "devonian": [419.2, 358.9],
        "lower devonian": [419.2, 393.3],
          "lochkovian": [419.2, 410.8],
          "downtonian": [419.2, 410.8],
          "pragian": [410.8, 407.6],
          "praghian": [410.8, 407.6],
          "emsian": [407.6, 393.3],
        "middle devonian": [393.3, 382.7],
          "eifelian": [393.3, 387.7],
          "givetian": [387.7, 382.7],
        "upper devonian": [382.7, 358.9],
          "frasnian": [382.7, 372.2],
          "famennian": [372.2, 358.9],
      "carboniferous": [358.9, 298.9],
        "mississippian": [358.9, 323.2],
        "lower carboniferous": [358.9, 323.2],
            "lower mississippian": [358.9, 346.7],
            "tournaisian": [358.9, 346.7],
          "middle mississippian": [346.7, 330.9],
            "visean": [346.7, 330.9],
          "upper mississippian": [330.9, 323.2],
            "serpukhovian": [330.9, 323.2],
              "namurian": [326, 323.2],
        "pennsylvanian": [323.2, 298.9],
        "upper carboniferous": [323.2, 298.9],
          "lower pennsylvanian": [323.2, 315.2],
            "bashkirian": [323.2, 315.2],
          "middle pennsylvanian": [315.2, 307.0],
            "moscovian": [315.2, 307.0],
          "westphalian": [313, 304],
          "upper pennsylvanian": [307.0, 298.9],
            "kasimovian": [307.0, 303.7],
              "stephanian": [304, 303.7],
            "gzhelian": [303.7, 298.9],
      "permian": [298.9, 251.902],
        "cisuralian": [298.9, 272.95],
        "lower permian": [298.9, 272.95],
          "asselian": [298.9, 295.0],
          "sakmarian": [295.0, 290.1],
          "artinskian": [290.1, 283.5],
          "kungurian": [283.5, 272.95],
        "guadalupian": [272.95, 259.1],
        "middle permian": [272.95, 259.1],
          "roadian": [272.95, 268.8],
          "ufimian": [272.95, 268.8],
          "wordian": [268.8, 265.1],
          "capitanian": [265.1, 259.1],
        "lopingian": [259.1, 251.902],
        "upper permian": [259.1, 251.902],
          "wuchiapingian": [259.1, 254.14],
          "longtanian": [259.1, 254.14],
          "changhsingian": [254.14, 251.902],
    "mesozoic": [251.902, 66.0],
      "triassic": [251.902, 201.3],
        "lower triassic": [251.902, 247.2],
          "induan": [251.902, 251.2],
          "olenekian": [251.2, 247.2],
          "spathian": [251.2, 247.2],
        "middle triassic": [247.2, 237],
          "anisian": [247.2, 242],
          "ladinian": [242, 237],
        "upper triassic": [237, 201.3],
          "carnian": [237, 227],
          "norian": [227, 208.5],
          "rhaetian": [208.5, 201.3],
      "jurassic": [201.3, 145.0],
        "lower jurassic": [201.3, 174.1],
          "hettangian": [201.3, 199.3],
          "sinemurian": [199.3, 190.8],
          "pliensbachian": [190.8, 182.7],
          "toarcian": [182.7, 174.1],
        "middle jurassic": [174.1, 163.5],
          "aalenian": [174.1, 170.3],
          "bajocian": [170.3, 168.3],
          "bathonian": [168.3, 166.1],
          "callovian": [166.1, 163.5],
        "upper jurassic": [163.5, 145.0],
          "oxfordian": [163.5, 157.3],
          "kimmeridgian": [157.3, 152.1],
          "tithonian": [152.1, 145.0],
      "cretaceous": [145.0, 66.0],
        "lower cretaceous": [145.0, 100.5],
          "berriasian": [145.0, 139.8],
          "neocomian": [145.0, 139.8],
          "valanginian": [139.8, 132.9],
          "hauterivian": [132.9, 129.4],
          "barremian": [129.4, 125.0],
          "gallic": [129.4, 125.0],
          "aptian": [125.0, 100.5],
          "albian": [113.0, 100.5],
        "upper cretaceous": [100.5, 66.0],
          "cenomanian": [100.5, 93.9],
          "turonian": [93.9, 89.8],
          "coniacian": [89.8, 86.3],
          "senonian": [89.8, 86.3],
          "santonian": [86.3, 83.6],
          "campanian": [83.6, 72.1],
          "maastrichtian": [72.1, 66.0],
    "cenozoic": [66.0, 0],
      "tertiary": [66.0, 2.58],
        "paleogene": [66.0, 56.0],
          "paleocene": [66.0, 56.0],
            "danian": [66.0, 61.6],
            "lower paleocene": [66.0, 61.6],
              "puercan": [65, 63.3],
              "torrejonian": [63.3, 61.6],
            "selandian": [61.6, 59.2],
            "middle paleocene": [61.6, 59.2],
              "tiffanian": [60.2, 59.2],
            "thanetian": [59.2, 56.0],
            "upper paleocene": [59.2, 56.0],
              "clarkforkian": [56.8, 56.0],
          "eocene": [56.0, 33.9],
            "ypresian": [56.0, 47.8],
            "lower eocene": [56.0, 47.8],
            "mp 10": [56.0, 47.8],
              "wasatchian": [55.4, 50.3],
              "bridgerian": [50.3, 47.8],
            "middle eocene": [47.8, 37.8],
              "lutetian": [47.8, 41.2],
              "mp 11": [47.8, 41.2],
                "uintan": [46.2, 42],  # noqa: E131
                "duchesnean": [42, 41.2],
              "bartonian": [41.2, 37.8],
                "chadronian": [38, 37.8],
            "priabonian": [37.8, 33.9],
            "upper eocene": [37.8, 33.9],
          "oligocene": [33.9, 23.03],
            "rupelian": [33.9, 28.1],
            "lower oligocene": [33.9, 28.1],
              "orellan": [33.9, 33.3],
              "whitneyan": [33.3, 30.6],
              "arikeean": [30.6, 28.1],
            "chattian": [28.1, 23.03],
            "upper oligocene": [28.1, 23.03],
        "neogene": [23.03, 2.58],
          "miocene": [23.03, 5.333],
            "lower miocene": [23.03, 15.97],
              "aquitanian": [23.03, 20.44],
                "hemingfordian": [20.6, 20.44],
              "burdigalian": [20.44, 15.97],
                "barstovian": [16.3, 15.97],
            "middle miocene": [15.97, 11.63],
              "langhian": [15.97, 13.82],
              "serravallian": [13.82, 11.63],
                "clarendonian": [13.6, 11.63],
            "upper miocene": [11.63, 5.333],
              "tortonian": [11.63, 7.246],
                "hemphillian": [10.3, 7.246],
              "messinian": [7.246, 5.333],
          "pliocene": [5.333, 2.58],
            "zanclean": [5.333, 3.600],
            "lower pliocene": [5.333, 3.600],
              "blancan": [4.75, 3.600],
            "piacenzian": [3.600, 2.58],
            "upper pliocene": [3.600, 2.58],
      "quaternary": [2.58, 0],
        "pleistocene": [2.58, 0.0117],
          "lower pleistocene": [2.58, 0.781],
            "gelasian": [2.58, 1.80],
            "calabrian": [1.80, 0.781],
              "irvingtonian": [1.8, 0.781],
          "middle pleistocene": [0.781, 0.126],
            "rancholabrean": [0.24, 0.126],
          "upper pleistocene": [0.126, 0.0117],
        "holocene": [0.0117, 0],
        "greenlandian": [0.0117, 0],
          "northgrippian": [0.0082, 0.0042],
          "meghalayan": [0.0042, 0],
        "now": [0, 0],
        "recent": [0, 0],
        "present": [0, 0],
        "current": [0, 0]
}
SPANS = {}
for span, dates in WIKI_SPANS.items():
    SPANS[span] = [d * 1000000 for d in dates]
    third = ((SPANS[span][0] - SPANS[span][1]) / 3.0)
    SPANS['late {}'.format(span).lower()] = [
       SPANS[span][1] + third,
       SPANS[span][1]
    ]
    upper_key = 'upper {}'.format(span).lower()
    if upper_key not in WIKI_SPANS.keys():
        SPANS[upper_key] = [
           SPANS[span][1] + third,
           SPANS[span][1]
        ]
    early_key = 'early {}'.format(span).lower()
    if early_key not in WIKI_SPANS.keys():
        SPANS[early_key] = [
           SPANS[span][0],
           SPANS[span][0] - third,
        ]
    lower_key = 'lower {}'.format(span).lower()
    if lower_key not in WIKI_SPANS.keys():
        SPANS[lower_key] = [
           SPANS[span][0],
           SPANS[span][0] - third,
        ]
    middle_key = 'middle {}'.format(span).lower()
    if middle_key not in WIKI_SPANS.keys():
        SPANS[middle_key] = [
           SPANS[span][0] - third,
           SPANS[span][1] + third
        ]
SPAN_PATTERN = re.compile(r'('+('|').join(SPANS.keys())+')', re.I)


def log(msg="", **kwargs):
    print("[{}] {}".format(dt.now().isoformat(), msg), **kwargs)


def extless_basename(path):
    if os.path.isfile(path):
        return os.path.splitext(os.path.basename(path))[0]
    return os.path.split(path)[-1]


def call_cmd(*args, **kwargs):
    if 'check' not in kwargs or kwargs['check'] is None:
        kwargs['check'] = True
    if isinstance(args[0], str) and len(args) == 1:
        print(f"Calling `{args[0]}` with kwargs: {kwargs}")
        return run(args[0], **kwargs)
    print("Calling `{}` with kwargs: {}".format(" ".join(args[0]), kwargs))
    return run(*args, **kwargs)


def run_sql(sql, dbname="underfoot", quiet=False, interpolations=None):
    con = psycopg2.connect("dbname={}".format(dbname))
    cur = con.cursor()
    if interpolations:
        if not quiet:
            log(f"Running {sql} ({interpolations})")
        cur.execute(sql, interpolations)
    else:
        if not quiet:
            log(f"Running {sql}")
        cur.execute(sql)
    results = None
    try:
        results = cur.fetchall()
    except psycopg2.ProgrammingError:
        results = None
    con.commit()
    cur.close()
    con.close()
    return results


def run_sql_with_retries(
    sql,
    max_retries=3,
    retry=1,
    dbname="underfoot",
    quiet=False
):
    try:
        return run_sql(sql, dbname=dbname, quiet=quiet)
    except psycopg2.OperationalError as e:
        if retry > max_retries:
            raise e
        else:
            sleep = retry ** 3
            print(
                "Failed to execute `{}`: {}. Trying again in {}s".format(
                    sql, e, sleep
                )
            )
            time.sleep(sleep)
            return run_sql_with_retries(sql, retry + 1)


def basename_for_path(path):
    basename = extless_basename(path)
    if basename == "__init__":
        dirpath = os.path.dirname(os.path.realpath(path))
        basename = extless_basename(dirpath)
    return basename


def make_work_dir(path):
    dirpath = os.path.dirname(os.path.realpath(__file__))
    basename = basename_for_path(path)
    work_path = os.path.join(dirpath, "..", "work-{}".format(basename))
    if not os.path.isdir(work_path):
        os.makedirs(work_path)
    return work_path


def underscore(s):
    return "_".join([piece.lower() for piece in re.split(r'\s', s)])


def extract_e00(path):
    dirpath = os.path.join(
        os.path.realpath(os.path.dirname(path)),
        "{}-shapefiles".format(extless_basename(path))
    )
    if os.path.isfile(os.path.join(dirpath, "PAL.shp")):
        return dirpath
    shutil.rmtree(dirpath, ignore_errors=True)
    os.makedirs(dirpath)
    call(["ogr2ogr", "-f", "ESRI Shapefile", dirpath, path])
    return dirpath


def polygonize_arcs(
    shapefiles_path,
    polygon_pattern=".+-ID?$",
    force=False,
    outfile_path=None
):
    """Convert shapefile arcs from an extracted ArcINFO coverage and convert
    them to polygons.

    More often than not ArcINFO coverages seem to include arcs but not polygons
    when converted to shapefiles using ogr. A PAL.shp file gets created, but it
    only has polygon IDs, not geometries. This method will walk through all the
    arcs and combine them into their relevant polygons.

    Why is it in its own python script and not in this library? For reasons as
    mysterious as they are maddening, when you import fiona into this module,
    it causes subproccess calls to ogr2ogr to ignore the "+nadgrids=@null" proj
    flag, which results in datum shifts when attempting to project to web
    mercator. Yes, seriously. I have no idea why or how it does this, but that
    was the only explanation I could find for the datum shift.
    """
    if not outfile_path:
        outfile_path = os.path.join(shapefiles_path, "polygons.shp")
    if force:
        shutil.rmtree(outfile_path, ignore_errors=True)
    elif os.path.isfile(outfile_path):
        return outfile_path
    pal_path = os.path.join(shapefiles_path, "PAL.shp")
    arc_path = os.path.join(shapefiles_path, "ARC.shp")
    polygonize_arcs_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "polygonize_arcs.py"
    )
    call_cmd([
        "python",
        polygonize_arcs_path,
        outfile_path,
        pal_path,
        arc_path,
        "--polygon-column-pattern",
        polygon_pattern
    ])
    return outfile_path


def met2xml(path):
    output_path = os.path.join(
        os.path.realpath(os.path.dirname(path)),
        "{}.xml".format(extless_basename(path))
    )
    met2xml_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "met2xml.py"
    )
    call_cmd(["python", met2xml_path, path, output_path])
    return output_path


def lithology_from_text(text):
    """Extract normalized lithology from free text"""
    if not text:
        return
    lithology_matches = LITHOLOGY_PATTERN.search(text)
    if not lithology_matches:
        lithology_matches = LOW_PRIORITY_LITHOLOGY_PATTERN.search(text)
    lithology = (lithology_matches[0] if lithology_matches else '').lower()
    if lithology in LITHOLOGY_SYNONYMS:
        return LITHOLOGY_SYNONYMS[lithology]
    return lithology


def formation_from_text(text):
    """Extract normalized formation from free text"""
    if not text:
        return
    # basically any proper nouns
    formation_matches = FORMATION_PATTERN.search(text)
    if formation_matches:
        return formation_matches.group(0).title()


def grouping_from_text(text):
    """Extract normalized grouping from free text"""
    if not text:
        return
    grouping_matches = GROUPING_PATTERN.search(text)
    if grouping_matches:
        return grouping_matches.group(0).title()


def rock_type_from_lithology(lithology):
    """Extract rock type from normalized lithology"""
    rock_type = ''
    if lithology in IGNEOUS_ROCKS:
        rock_type = 'igneous'
    elif lithology in METAMORPHIC_ROCKS:
        rock_type = 'metamorphic'
    elif lithology in SEDIMENTARY_ROCKS:
        rock_type = 'sedimentary'
    return rock_type


def span_from_text(text):
    """Extract normalized geologic time span from free text"""
    if not text:
        return
    span_matches = SPAN_PATTERN.search(text.lower())
    if span_matches:
        return span_matches.group(1).lower()


def span_from_lithology(lithology):
    """Extract normalized geologic time span from normalized lithology"""
    if lithology in ("artificial", "water"):
        return "present"


def span_from_code(code):
    """Extract normalized geologic time span from map unit / code"""
    if code and len(code) <= 4:
        if code[0] == "K":
            return "cretaceous"
        if code[0] == "T":
            return "tertiary"
        if code[0] == "Q":
            return "quaternary"
        if code[0] == "J":
            return "jurassic"
        return "present"


def controlled_span_from_span(text):
    """
    Extract controlled geologic time span from free text.

    Unlike span_from_text, this attempts to make reasonable guesses about the
    meaning of terms like "early" or "upper"
    """
    if not text:
        return
    key = re.sub(r'\(.+?\)', "", text)
    key = re.sub(r'undivided', "", key)
    key = re.sub(r'\s+', " ", key)
    key = key.lower().strip()
    synonyms = {
      'present': 'holocene'
    }
    if key in synonyms and synonyms[key] in WIKI_SPANS:
        return synonyms[key]
    if key in WIKI_SPANS:
        return key
    key_sans_x_to_y = re.sub(
        r'(early|middle|late) to (early|middle|late)', "",
        key
    ).strip()
    if key_sans_x_to_y in WIKI_SPANS:
        return key_sans_x_to_y
    key_sans_lower = re.sub(r'early\s+', "lower ", key).strip()
    if key_sans_lower in WIKI_SPANS:
        return key_sans_lower
    key_sans_upper = re.sub(r'late\s+', "upper ", key).strip()
    if key_sans_upper in WIKI_SPANS:
        return key_sans_upper
    key_sans_subspan = re.sub(
        r'(upper|middle|lower|early|late)\s+', "",
        key
    ).strip()
    if key_sans_subspan in WIKI_SPANS:
        return key_sans_subspan
    # Split on (to|\-)
    matches = re.findall(r'([\s\w]+)\s+(to|\-|or|and\/or)\s+([\s\w]+)', key)
    if len(matches) > 0:
        # get controlled_span for each half
        start_span = controlled_span_from_span(matches[0][0])
        end_span = controlled_span_from_span(matches[0][2])
        if start_span and end_span:
            start_start_age = SPANS[start_span][0]
            end_end_age = SPANS[end_span][1]
            # sort wiki spans by start_date asc
            sorted_spans = sorted(
                [(k, SPANS[k][0], SPANS[k][1]) for k in SPANS],
                key=lambda s: s[1]
            )
            # find first span where start_age >= 1st half start age and end_age
            # <= 2nd half end age
            container_span = next(
                (
                    s for s in sorted_spans
                    if s[1] >= start_start_age and s[2] <= end_end_age
                ),
                None
            )
            if container_span:
                return container_span[0]


def ages_from_span(span):
    """
    Parses a text description of a time span using the names of geologic
    periods, epochs, and other time spans into a list of minimum, maximum, and
    estimated age in years BCE

    >>> ages_from_span("Cretaceous")
    (66000000.0, 145000000.0, 105500000)
    """
    if ";" in span:
        for piece in span.split(";"):
            ages = ages_from_span(piece)
            if ages and None not in ages:
                return ages
    min_age = None
    max_age = None
    est_age = None
    span_to_span_pattern = r'(.+)\s+?(to|\-|and)\s+?(.+)'
    part_to_part_span_pattern = r'(?P<part1>\w+)\s+?(to|\-)\s+?(?P<part2>\w+)\s+(?P<span>\w+)'  # noqa: E501
    if span is None:
        return (min_age, max_age, est_age)
    span = span.lower()
    span = span.replace('undivided', '')
    span = re.sub(r'\(.+\)', '', span).strip()
    span = span.replace('?-', ' -')
    span = span.replace('?', '')
    span = re.sub(r"\-\s*$", "", span)
    span = re.sub(r"^\s*-", "", span)
    span = span.strip()
    if len(span) == 0:
        return (min_age, max_age, est_age)
    ages = SPANS.get(span)
    if ages:
        max_age = ages[0]
        min_age = ages[1]
    else:
        min_ages = None
        max_ages = None
        if match := re.match(span_to_span_pattern, span):
            min_ages = SPANS.get(match[1])
            max_ages = SPANS.get(match[3])
        if not min_ages or not max_ages:
            if match := re.match(part_to_part_span_pattern, span):
                part1 = f"{match.group('part1')} {match.group('span')}".lower()
                part2 = f"{match.group('part2')} {match.group('span')}".lower()
                min_ages = SPANS.get(part1)
                max_ages = SPANS.get(part2)
        if min_ages:
            min_age = min_ages[1]
        if max_ages:
            max_age = max_ages[0]
    if min_age is not None and max_age is not None:
        est_age = int((min_age + max_age) / 2.0)
    return (min_age, max_age, est_age)


def span_from_usgs_code(code):
    """
    Derive a span from a USGS geologic map unit code

    USGS geologic data has a convention of using the first letter (or two) to
    indiciate the age of the unit, roughly corresponding to the symbols at
    https://ngmdb.usgs.gov/fgdc_gds/geolsymstd/fgdc-geolsym-sec32.pdf
    """
    mapping = {
        "Cz": "cenozoic",
        "Q": "quaternary",
        "T": "tertiary",
        "N": "neogene",
        "Pe": "paleogene",
        "Mz": "mesozoic",
        "K": "cretaceous",
        "J": "jurassic",
        "Tr": "triassic",
        "Pz": "paleozoic",
        "P": "permian",
        "C": "carboniferous",
        # "P": "pennsylvanian", # not sure how to render this symbol
        "M": "mississippian",
        "D": "devonian",
        "S": "silurian",
        "O": "ordovician",
        # "C": "Cambrian"
        "pC": "precambrian",
        # "P": "proterozoic",
        "Z": "late proterozoic",
        "Y": "middle proterozoic",
        "Y3": "late middle proterozoic",
        "Y2": "middle middle proterozoic"
    }
    for prefix, span in mapping.items():
        if re.match(rf"^{prefix}", code):
            return span

def metadata_from_usgs_met(path):
    """Parse metadata from a USGS met file

    USGS geologic databases seem to come with a somewhat machine-readable
    document with a .met extension that contains information about the
    geological units in the database. This method tries to convert that
    document into an array of arrays suitable for use in underfoot. It's not
    going to do a perfect job for every such document, but it will get you part
    of the way there.
    """
    data = [[col for col in METADATA_COLUMN_NAMES]]
    xml_path = met2xml(path)
    tree = ET.parse(xml_path)
    for ed in tree.iterfind('.//Attribute[Attribute_Label="PTYPE"]//Enumerated_Domain'):  # noqa: E501
        row = dict([[col, None] for col in METADATA_COLUMN_NAMES])
        edv = ed.find('Enumerated_Domain_Value')
        edvd = ed.find('Enumerated_Domain_Value_Definition')
        if edv is None or edvd is None:
            continue
        row['code'] = edv.text
        row['title'] = edvd.text
        if row['code'] is None or row['title'] is None:
            continue
        row['title'] = re.sub(r"\n", " ", row['title'])
        row['lithology'] = lithology_from_text(row['title'])
        row['formation'] = formation_from_text(row['title'])
        row['rock_type'] = rock_type_from_lithology(row['lithology'])
        row['span'] = span_from_text(row['title'])
        if not row['span']:
            row['span'] = span_from_lithology(row['lithology'])
        if not row['span']:
            row['span'] = span_from_code(row['code'])
        row['min_age'], row['max_age'], row['est_age'] = ages_from_span(
            row['span']
        )
        row['controlled_span'] = controlled_span_from_span(row['span'])
        if row['controlled_span'] and not row['min_age']:
            row['min_age'], row['max_age'], row['est_age'] = ages_from_span(
                row['controlled_span']
            )
        data.append([row[col] for col in METADATA_COLUMN_NAMES])
    return data


def join_polygons_and_metadata(
    polygons_path,
    metadata_path,
    output_path="units.geojson",
    polygons_join_col="PTYPE",
    polygons_table_name=None,
    metadata_join_col="code",
    output_format="GeoJSON"
):
    # print("polygons_path: {}".format(polygons_path))
    polygons_table_name = polygons_table_name or extless_basename(
        polygons_path)
    column_names = [col for col in METADATA_COLUMN_NAMES]
    # column_names[column_names.index('code')] = "{} AS code".format(join_col)
    metadata_layer_name = extless_basename(metadata_path)
    sql = """
      SELECT
        {}
      FROM {}
        LEFT JOIN '{}'.{} ON {}.{} = {}.{}
    """.format(
      ", ".join(column_names),
      polygons_table_name,
      metadata_path,
      metadata_layer_name,
      polygons_table_name,
      polygons_join_col,
      metadata_layer_name,
      metadata_join_col
    )
    # print("sql: {}".format(sql))
    call_cmd(["rm", "-f", output_path])
    call_cmd([
      "ogr2ogr",
      "-sql", sql.replace("\n", " "),
      "-f", output_format,
      output_path,
      polygons_path
    ])
    return output_path


def infer_metadata_from_csv_row(row):
    """Infer metadata from a row in a metadata file"""
    if not row.get('lithology') or len(row['lithology']) == 0:
        row['lithology'] = lithology_from_text(row['title'])
    if not row.get('lithology') or len(row['lithology']) == 0:
        row['lithology'] = lithology_from_text(row['description'])
    row['span'] = span_from_text(row['title'])
    if not row['span'] and row['lithology']:
        row['span'] = span_from_lithology(row['lithology'])
    if not row.get('span') and row.get('code'):
        row['span'] = span_from_code(row['code'])
    row['controlled_span'] = controlled_span_from_span(row['span'])
    row['formation'] = formation_from_text(row['title'])
    if row['lithology']:
        row['rock_type'] = rock_type_from_lithology(
            row['lithology']
        )
    if row['span']:
        min_age, max_age, est_age = ages_from_span(row['span'])
        row['min_age'] = min_age
        row['max_age'] = max_age
        row['est_age'] = est_age
    return row


def infer_metadata_from_csv(infile_path):
    """Fill in missing metadata columns in a CSV

    The metadata file *can* have every column, but in general it just has the
    info that can't be inferred and we infer the rest from the title and
    description.
    """
    outfile_path = "data.csv"
    with open(infile_path, encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        with open(outfile_path, 'w', encoding="utf-8") as outfile:
            writer = csv.DictWriter(
                outfile,
                fieldnames=METADATA_COLUMN_NAMES,
                extrasaction='ignore'
            )
            writer.writeheader()
            for row in reader:
                row = infer_metadata_from_csv_row(row)
                writer.writerow(row)
                uncertain_row = row.copy()
                uncertain_row['code'] = "{}?".format(row['code'])
                uncertain_row['title'] = "[?] {}".format(row['title'])
                uncertain_row['description'] = "[UNCERTAIN] {}".format(
                    row['description']
                )
                writer.writerow(uncertain_row)
    return outfile_path


def process_usgs_source(
    base_path,
    url,
    e00_path,
    polygon_pattern=None,
    srs=NAD27_UTM10_PROJ4,
    metadata_csv_path=None,
    polygons_join_col="PTYPE",
    skip_polygonize_arcs=False,
    uncompress_e00=False,
    # As opposed to gunzip
    use_unzip=False
):
    """Process units from a USGS Arc Info archive given a couple configurations.

    Most USGS map databases seem to be in the form of a gzipped tarbal
    containing Arc Info coverages, so this method just wraps up some of the
    code I keep repeating.

    Args:
      base_path: path to the source module's __init__.py
      url: URL of the gzipped tarball
      e00_path: Relative path to the e00 to extract
      polygon_pattern (optional): Pattern to use when finding the polygon ID
        column in the e00 arcs
      srs (optional): Proj4 coordinate reference string for the geodata.
        Default is NAD27 UTM Zone 10
      metadata_csv_path: Full path to a CSV containing transcribed metadata for
        the map units. Default behavior is to try and import unit titles
      polygons_join_col: Name of the column to dissolve polygons by and to join
        them to the units column in the metadata
      skip_polygonize_arcs: If ogr2ogr successfully creates a useable polygon
        shapefile in PAL.shp, then we don't need to polygonize arcs, but this
        usually isn't the case. Default value is false.
      uncompress_e00: If the e00 is itself compressed, uncompress it with
        e00conv. Default is false.
    """
    work_path = make_work_dir(base_path)
    os.chdir(work_path)
    download_path = os.path.basename(url)
    # download the file if necessary
    if not os.path.isfile(download_path):
        print(f"DOWNLOADING {url}")
        call_cmd(["curl", "-OL", url])

    # extract the archive if necessary
    if len(glob(e00_path)) == 0:
        print("EXTRACTING ARCHIVE...")
        if (
            ".tar.gz" in download_path
            or ".tgz" in download_path
            or ".tar.Z" in download_path
        ):
            call_cmd(["tar", "xzvf", download_path])
        elif use_unzip:
            call_cmd(["unzip", download_path])
        else:
            copy_path = download_path + "-copy"
            call_cmd(["cp", download_path, copy_path])
            call_cmd(["gunzip", download_path])
            call_cmd(["cp", copy_path, download_path])
            call_cmd(["rm", copy_path])

    # convert the Arc Info coverages to shapefiles
    polygons_path = "e00_polygons.shp"
    if not os.path.isfile(polygons_path):
        print("CONVERTING E00 TO SHAPEFILES...")
        polygon_paths = []
        for path in glob(e00_path):
            if uncompress_e00:
                uncompressed_e00_path = "uncompressed.e00"
                if not os.path.isfile(uncompressed_e00_path):
                    print("\tUncompressing e00")
                    call_cmd([
                        "../../bin/e00compr/e00conv",
                        path,
                        uncompressed_e00_path
                    ])
                    path = uncompressed_e00_path
            print("\tExtracting e00...")
            shapefiles_path = extract_e00(path)
            print(f"\tshapefiles_path: {shapefiles_path}")
            print("\tPolygonizing arcs...")
            if skip_polygonize_arcs:
                polygon_paths.append(os.path.join(shapefiles_path, "PAL.shp"))
            elif polygon_pattern:
                polygon_paths.append(polygonize_arcs(
                    shapefiles_path,
                    polygon_pattern=polygon_pattern
                ))
            else:
                polygon_paths.append(polygonize_arcs(shapefiles_path))
        print("MERGING SHAPEFILES...")
        call_cmd(["ogr2ogr", "-overwrite", polygons_path, polygon_paths.pop()])
        for path in polygon_paths:
            call_cmd(["ogr2ogr", "-update", "-append", polygons_path, path])

    # dissolve all the shapes by polygons_join_col and project them into Google Mercator
    print("DISSOLVING SHAPES AND REPROJECTING...")
    final_polygons_path = "polygons.shp"
    call_cmd([
        "ogr2ogr",
        "-s_srs", srs,
        "-t_srs", SRS,
        final_polygons_path, polygons_path,
        "-overwrite",
        "-dialect", "sqlite",
        "-sql",
        # pylint: disable=line-too-long
        f"SELECT {polygons_join_col},ST_Union(geometry) as geometry FROM 'e00_polygons' GROUP BY {polygons_join_col}"
        # pylint: enable=line-too-long
    ])

    print("EXTRACTING METADATA...")
    metadata_path = "data.csv"
    globs = glob(os.path.join(os.path.dirname(e00_path), "*.met"))
    met_path = globs[0] if globs else None
    if metadata_csv_path:
        metadata_path = infer_metadata_from_csv(metadata_csv_path)
        if met_path:
            fill_in_custom_metadata_from_met(met_path, metadata_path)
    elif met_path:
        data = metadata_from_usgs_met(met_path)
        with open(metadata_path, 'w') as metadata_file:
            csv.writer(metadata_file).writerows(data)
    else:
        # write an empty metadata csv file
        data = [METADATA_COLUMN_NAMES]
        with open(metadata_path, 'w') as metadata_file:
            csv.writer(metadata_file).writerows(data)

    print("JOINING METADATA...")
    join_polygons_and_metadata(final_polygons_path, metadata_path,
      polygons_join_col=polygons_join_col)

    print("COPYING CITATION")
    call_cmd([
      "cp",
      os.path.join(os.path.dirname(base_path), "citation.json"),
      os.path.join(work_path, "citation.json")
    ])


def fill_in_custom_metadata_from_met(met_path, metadata_path):
    met_data = metadata_from_usgs_met(met_path)
    hashed_met_data = {}
    for row in met_data:
        hashed_met_data[row[0]] = dict(zip(METADATA_COLUMN_NAMES, row))
    with open(metadata_path, 'r') as infile:
        reader = csv.DictReader(infile)
        outfile_path = "temp.csv"
        with open(outfile_path, 'w') as outfile:
            writer = csv.DictWriter(
                outfile,
                fieldnames=METADATA_COLUMN_NAMES,
                extrasaction='ignore'
            )
            writer.writeheader()
            for row in reader:
                met_row = hashed_met_data.get(row['code'], None)
                for col in METADATA_COLUMN_NAMES:
                    if (
                        (not row[col] or len(row[col]) == 0)
                        and met_row
                        and met_row[col]
                    ):
                        row[col] = met_row[col]
                    writer.writerow(row)
        os.rename(outfile_path, metadata_path)


def initialize_masks_table(mask_table_name, source_table_name, buff=0.01):
    """
      Creates the first row in a table intended to hold a single MULTIPOLYGON
      geometry that acts as a mask. This assumes that both tables have
      MULTIPOLYGON geom columns themselves.
    """
    run_sql(f"""
      INSERT INTO {mask_table_name} (geom)
      SELECT
        ST_Multi(
          ST_Buffer(
            ST_Buffer(
              ST_MakeValid(
                ST_Union(geom)
              ),
              {buff},
              'join=mitre'
            ),
            -{buff},
            'join=mitre'
          )
        )
      FROM {source_table_name}
    """)


def update_masks_table(mask_table_name, source_table_name, buff=0.01):
    """
      Updates an existing masks table with geometries from another table.
      Again, the assumption is that all tables have geom columns. This also
      assumes no holes: it just takes the exterior ring from all the geoms in
      the source.
    """
    run_sql(f"""
        UPDATE {mask_table_name} m SET geom = ST_Multi(
          ST_Union(
            ST_MakeValid(m.geom),
            ST_MakeValid(
              ST_Multi(
                (
                  SELECT
                    ST_Buffer(
                      ST_Buffer(
                        ST_MakeValid(
                          ST_Union(ST_MakeValid(s.geom))
                        ),
                        {buff},
                        'join=mitre'
                      ),
                      -{buff},
                      'join=mitre'
                    )
                  FROM {source_table_name} s
                )
              )
            )
          )
        )
    """)


def process_omca_creeks_source(url, dir_name, waterways_shp_path,
                               watersheds_shp_path, waterways_name_col,
                               watersheds_name_col, srs):
    # Download the data
    download_path = os.path.basename(url)
    if not os.path.isfile(download_path):
        print("DOWNLOADING {}".format(url))
        call_cmd(["curl", "-OL", url])

    # Unpack
    dir_path = dir_name
    if not os.path.isdir(dir_path):
        print("EXTRACTING ARCHIVE...")
        call_cmd(["unzip", download_path])

    # Project into EPSG 4326 along with name, type, and natural attributes
    waterways_gpkg_path = "waterways.gpkg"
    if os.path.isfile(waterways_gpkg_path):
        log("{waterways_gpkg_path} exists, skipping...")
    else:
        lyr_name = extless_basename(waterways_shp_path)
        sql = f"""
            SELECT
                null as waterway_id,
                {waterways_name_col} as name,
                LTYPE as type,
                (LTYPE = 'Creek') AS natural,
                geometry AS geom
            FROM '{lyr_name}'
        """
        sql = re.sub(r'\s+', " ", sql)
        cmd = f"""
            ogr2ogr \
              -s_srs '{srs}' \
              -t_srs '{SRS}' \
              -overwrite \
              {waterways_gpkg_path} \
              {waterways_shp_path} \
              -dialect sqlite \
              -nln waterways \
              -nlt MULTILINESTRING \
              -sql "{sql}"
        """
        call_cmd(cmd, shell=True, check=True)
    watersheds_gpkg_path = "watersheds.gpkg"
    if os.path.isfile(watersheds_gpkg_path):
        log("{watersheds_gpkg_path} exists, skipping...")
    else:
        lyr_name = extless_basename(watersheds_shp_path)
        sql = f"""
            SELECT
                OID AS source_id,
                'OID' AS source_id_attr,
                {watersheds_name_col} as name,
                geometry AS geom
            FROM '{lyr_name}'
            WHERE
                {watersheds_name_col} IS NOT NULL
                AND {watersheds_name_col} != ''
        """
        sql = re.sub(r'\s+', " ", sql)
        cmd = f"""
        ogr2ogr \
          -s_srs '{srs}' \
          -t_srs '{SRS}' \
          -overwrite \
          {watersheds_gpkg_path} \
          {watersheds_shp_path} \
          -dialect sqlite \
          -nln watersheds \
          -nlt MULTIPOLYGON \
          -sql "{sql}"
        """
        call_cmd(cmd, shell=True, check=True)


def process_nhdplus_hr_source_waterways(gdb_path, srs):
    # Project into EPSG 4326 along with name, type, and natural attributes
    waterways_gpkg_path = "waterways.gpkg"
    if os.path.isfile(waterways_gpkg_path):
        log(f"{waterways_gpkg_path} exists, skipping...")
        return
    sql = """
        SELECT
          GNIS_ID AS waterway_id,
          GNIS_Name AS name,
          NHDPlusID AS source_id,
          'NHDPlusID' AS source_id_attr,
          CASE
          WHEN NHDFlowline.FCode = 42813 THEN 'siphon'
          WHEN NHDFlowline.FCode IN (33601, 42801, 42804) THEN 'aqueduct'
          WHEN NHDFlowline.FTYPE = 336 THEN 'canal/ditch'
          WHEN NHDFlowline.FTYPE = 558 THEN 'artificial'
          WHEN NHDFlowline.FTYPE = 334 THEN 'connector'
          ELSE
            'stream'
          END AS type,
          (
            -- LAKE/POND
            (NHDFlowline.FCode BETWEEN 39000 AND 39012)
            OR (NHDFlowline.FCode IN (
              -- BAY/INLET
              31200,
              -- PLAYA
              36100,
              -- FORESHORE
              36400,
              -- RAPIDS
              43100,
              -- REEF
              43400,
              -- ROCK
              44100,
              -- ROCK
              44101,
              -- ROCK
              44102,
              -- SEA/OCEAN
              44500,
              -- WASH
              48400,
              -- WATERFALL
              48700,
              -- ESTUARY
              49300,
              -- AREA OF COMPLEX CHANNELS
              53700,
              -- COASTLINE
              56600,
              -- ???
              56700
            ))
            -- SPRING/SEEP
            -- STREAM/RIVER
            -- SUBMERGED STREAM
            -- SWAMP/MARSH
            OR (NHDFlowline.FCode BETWEEN 45800 AND 46602)
          ) AS is_natural,
          LOWER(
            COALESCE(
              NULLIF(NHDFCode.RelationshipToSurface, ' '),
              'surface'
            )
          ) AS surface,
          LOWER(
            COALESCE(
              NULLIF(NHDFCode.HydrographicCategory, ' '),
              'perennial'
            )
          ) AS permanence,
          Shape AS geom
        FROM
          NHDFlowline
            JOIN NHDFcode ON NHDFcode.FCode = NHDFlowline.FCode
        WHERE
          NHDFlowLine.FCode NOT IN (56600, 56700)
    """
    # remove comments, ogr doesn't like them
    sql = re.sub(r'--.+', "", sql)
    sql = re.sub(r'\s+', " ", sql)
    cmd = f"""
        ogr2ogr \
          -s_srs '{srs}' \
          -t_srs '{SRS}' \
          -overwrite \
          {waterways_gpkg_path} \
          {gdb_path} \
          -dialect sqlite \
          -nln waterways \
          -nlt MULTILINESTRING \
          -sql "{sql}"
    """
    call_cmd(cmd, shell=True, check=True)


def process_nhdplus_hr_source_waterbodies(gdb_path, srs):
    waterbodies_gpkg_path = "waterbodies.gpkg"
    if os.path.isfile(waterbodies_gpkg_path):
        log(f"{waterbodies_gpkg_path} exists, skipping...")
        return
    sql = """
        SELECT
          GNIS_ID AS waterbody_id,
          GNIS_ID AS source_id,
          'GNIS_ID' AS source_id_attr,
          GNIS_Name AS name,
          CASE
          WHEN NHDWaterbody.FCode = 43624 THEN 'treatment'
          WHEN NHDWaterbody.FCode = 43613 THEN 'storage'
          WHEN NHDWaterbody.FCode = 43607 THEN 'evaporator'
          WHEN NHDWaterbody.FCode = 46600 THEN 'swamp/marsh'
          WHEN NHDWaterbody.FType = 436 THEN 'reservoir'
          ELSE 'lake/pond'
          END AS type,
          NOT (
            GNIS_Name LIKE '%reservoir%'
            OR NHDFcode.Description LIKE '%reservoir%'
            OR NHDWaterbody.FType = 436
            OR NHDWaterbody.FCode = 43607
            OR NHDWaterbody.FCode = 43613
            OR NHDWaterbody.FCode = 43624
          ) AS is_natural,
          LOWER(
            COALESCE(
              NULLIF(NHDFCode.HydrographicCategory, ' '),
              'perennial'
            )
          ) AS permanence,
          Shape AS geom
        FROM
          NHDWaterbody
            JOIN NHDFcode ON NHDFcode.FCode = NHDWaterbody.FCode
        WHERE
          NHDWaterbody.FCode NOT IN (56600, 56700)
    """
    sql = re.sub(r'\s+', " ", sql)
    cmd = f"""
        ogr2ogr \
          -s_srs '{srs}' \
          -t_srs '{SRS}' \
          -overwrite \
          {waterbodies_gpkg_path} \
          {gdb_path} \
          -dialect sqlite \
          -nln waterbodies \
          -nlt MULTIPOLYGON \
          -sql "{sql}"
    """
    call_cmd(cmd, shell=True, check=True)


def process_nhdplus_hr_source_watersheds(gdb_path, srs):
    watersheds_gpkg_path = "watersheds.gpkg"
    if os.path.isfile(watersheds_gpkg_path):
        log(f"{watersheds_gpkg_path} exists, skipping...")
        return
    sql = """
        SELECT
            Name AS name,
            HUC10 AS source_id,
            'HUC10' AS source_id_attr,
            Shape AS geom
        FROM WBDHU10
    """
    sql = re.sub(r'\s+', " ", sql)
    cmd = f"""
        ogr2ogr \
          -s_srs '{srs}' \
          -t_srs '{SRS}' \
          -overwrite \
          {watersheds_gpkg_path} \
          {gdb_path} \
          -dialect sqlite \
          -nln watersheds \
          -nlt MULTIPOLYGON \
          -sql "{sql}"
    """
    call_cmd(cmd, shell=True, check=True)


def process_nhdplus_hr_source_waterways_network(gdb_path):
    sqlite_path = "waterways-network.sqlite"
    if not os.path.isfile(sqlite_path):
        # Extract network data from the GDB to a sqlite database that we can
        # index for efficient queries
        call_cmd(
            f"ogr2ogr {sqlite_path} {gdb_path} NHDPlusFlowlineVAA",
            shell=True
        )
        # Make those indexes
        call_cmd(
            f"sqlite3 {sqlite_path} 'CREATE INDEX vaa_tonode ON "
            "NHDPlusFlowlineVAA (ToNode)'",
            shell=True
        )
        call_cmd(
            f"sqlite3 {sqlite_path} 'CREATE INDEX vaa_fromnode ON "
            "NHDPlusFlowlineVAA (FromNode)'",
            shell=True
        )
    csv_path = "waterways-network.csv"
    if not os.path.isfile(csv_path):
        # Dump the network from sqlite to CSV. Each segment has an NHDPlusID,
        # and we're storing the NHDPlusID of the segment upstream
        # (from_source_id) and downstream (to_source_id). You don't technically
        # need both to construct the graph, but they save a lot of calculation
        sql = """
            SELECT
                CAST(a.NHDPlusID AS INTEGER) AS source_id,
                CAST(t.NHDPlusID AS INTEGER) AS to_source_id,
                CAST(f.NHDPlusID AS INTEGER) AS from_source_id
            FROM
                NHDPlusFlowlineVAA a
                    LEFT JOIN NHDPlusFlowlineVAA t ON t.FromNode = a.ToNode
                    LEFT JOIN NHDPlusFlowlineVAA f ON f.ToNode = a.FromNode
        """
        sql = re.sub(r'\s+', " ", sql)
        call_cmd(
            f"""
                sqlite3 {sqlite_path} -csv -header "{sql}" > {csv_path}
            """,
            shell=True
        )


def process_nhdplus_hr_source_citation(url):
    citation_json_path = "citation.json"
    if os.path.isfile(citation_json_path):
        return
    globs = glob("*_GDB.xml")
    if len(globs) == 0:
        log("No metadata XML found, skipping citation generation...")
        return
    tree = ET.parse(globs[0])
    citeinfo = tree.find("./idinfo/citation/citeinfo")
    pubdate = citeinfo.find("pubdate").text
    pubyear = pubdate[0:4]
    pubmonth = pubdate[4:6]
    pubday = pubdate[6:8]
    title = citeinfo.find("title").text
    citation_json = [{
        "id": url,
        "title": title,
        "container-title": "USGS National Hydrography Dataset Plus High Resolution",  # noqa: E501
        "URL": url,
        "author": [
            {"family": "U.S. Geological Survey"}
        ],
        "issued": {
            "date-parts": [
                [
                    pubyear,
                    pubmonth,
                    pubday
                ]
            ]
        }
    }]
    with open("citation.json", 'w') as outfile:
        json.dump(citation_json, outfile)


def process_nhdplus_hr_source(
        base_path,
        url,
        dir_name,
        gdb_name,
        srs=GRS80_LONGLAT):
    work_path = make_work_dir(os.path.realpath(base_path))
    os.chdir(work_path)
    # Download the data
    download_path = os.path.basename(url)
    if os.path.isfile(download_path):
        log(f"Download exists at {download_path}, skipping...")
    else:
        log(f"DOWNLOADING {url}")
        call_cmd(["curl", "-OL", url])
    # Unpack the waterways data
    gdb_path = gdb_name
    if os.path.isdir(gdb_path):
        log(f"Archive already extracted at {gdb_path}, skipping...")
    else:
        log("EXTRACTING ARCHIVE...")
        call_cmd(["unzip", "-o", download_path])
    process_nhdplus_hr_source_waterways(gdb_path, srs)
    process_nhdplus_hr_source_waterbodies(gdb_path, srs)
    process_nhdplus_hr_source_watersheds(gdb_path, srs)
    process_nhdplus_hr_source_waterways_network(gdb_path)
    process_nhdplus_hr_source_citation(url)


def add_table_from_query_to_mbtiles(
        table_name, dbname, query, mbtiles_path, index_columns=[]):
    csv_path = f"{os.path.basename(mbtiles_path)}.csv"
    call_cmd(
        f"psql {dbname} -c \"COPY ({query}) TO STDOUT WITH CSV HEADER\" > {csv_path}",  # noqa: E501
        shell=True, check=True)
    shutil.rmtree(csv_path, ignore_errors=True)
    call_cmd([
        "sqlite3",
        "-csv",
        mbtiles_path,
        f".import {csv_path} {table_name}"
    ])
    for index_column in index_columns:
        sql = f"""
            CREATE INDEX {table_name}_{index_column}
            ON {table_name}({index_column})
        """
        call_cmd([
            "sqlite3",
            mbtiles_path,
            sql
        ], check=True)

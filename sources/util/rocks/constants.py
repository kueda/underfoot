"""Constants for processing rock sources"""

import re

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
  re.sub(r'\s+', '', r'''(
    agglomerate|
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
    sedimentary\sbreccia|
    serpentine|
    serpentinite|
    shale|
    silica(\-|\s)carbonate|
    siltstone|
    surficial\sdeposit|
    syenite|
    talus|
    tectonic\sbreccia|
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
    breccia|
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
    SPANS[f"late {span}".lower()] = [
       SPANS[span][1] + third,
       SPANS[span][1]
    ]
    upper_key = f"upper {span}".lower()
    if upper_key not in WIKI_SPANS.keys():
        SPANS[upper_key] = [
           SPANS[span][1] + third,
           SPANS[span][1]
        ]
    early_key = f"early {span}".lower()
    if early_key not in WIKI_SPANS.keys():
        SPANS[early_key] = [
           SPANS[span][0],
           SPANS[span][0] - third,
        ]
    lower_key = f"lower {span}".lower()
    if lower_key not in WIKI_SPANS.keys():
        SPANS[lower_key] = [
           SPANS[span][0],
           SPANS[span][0] - third,
        ]
    middle_key = f"middle {span}".lower()
    if middle_key not in WIKI_SPANS.keys():
        SPANS[middle_key] = [
           SPANS[span][0] - third,
           SPANS[span][1] + third
        ]
SPAN_PATTERN = re.compile(r'('+('|').join(SPANS.keys())+')', re.I)

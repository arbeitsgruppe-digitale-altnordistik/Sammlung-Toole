# CRAWLER_PATH_COLLECTIONS = 'data/collections.csv'
# CRAWLER_PATH_POTENTIAL_XMLS = 'data/pot_ms_urls.csv'
# CRAWLER_PATH_URLS = 'data/ms_urls.csv'
# CRAWLER_PATH_CONTENT_PICKLE = 'data/contents.pickle'
# CRAWLER_PICKLE_PATH = "data/cache.pickle"
# CRAWLER_PATH_IDS = 'data/ms_ids.csv'
# CRAWLER_PATH_404S = 'data/404urls.txt'

XML_BASE_PATH = 'data/handrit/Manuscripts'
PREFIX_BACKUPS = 'data/backups/'
PREFIX_XML_URL = 'https://handrit.is/en/manuscript/xml/'
PREFIX_PERSON_XML_URL = 'https://handrit.is/en/biography/xml/'
PERSON_DATA_PATH = 'data/handrit/Authority Files/names.xml'

HANDLER_BACKUP_PATH_MSS = "data/backups/mss.csv"
HANDLER_BACKUP_PATH_TXT_MATRIX = "data/backups/text-matrix.parquet"
HANDLER_BACKUP_PATH_PERS_MATRIX = "data/backups/person-matrix.csv"
HANDLER_BACKUP_PATH_PERS_DICT = "data/backups/person-name-dict.json"
HANDLER_BACKUP_PATH_PERS_DICT_INV = "data/backups/person-name-inverse-dict.json"
HANDLER_PATH_PICKLE = "data/handler_cache.pickle"
DATABASE_PATH = "data/allthedata.db"

GROUPS_PATH_PICKLE = "data/groups.pickle"

# The following list should contain all locations where handler data is cached and/or backed up. Used in tamer.wipe_cache()
PURGELIST = [HANDLER_BACKUP_PATH_MSS, HANDLER_BACKUP_PATH_TXT_MATRIX, HANDLER_BACKUP_PATH_PERS_MATRIX,
             HANDLER_BACKUP_PATH_PERS_DICT, HANDLER_BACKUP_PATH_PERS_DICT_INV, HANDLER_PATH_PICKLE,
             GROUPS_PATH_PICKLE, DATABASE_PATH]


IMAGE_HOME = 'data/title.png'

DOC_CITAVI = 'docs/CITAVI-README.md'

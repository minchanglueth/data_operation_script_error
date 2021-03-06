import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict

from core.models.base_class import BaseMix, TimestampMixin


class V4CrawlingTaskActionMaster(BaseMix, TimestampMixin):
    __tablename__ = "CrawlingTaskActionMaster"
    action_id = sa.Column("ActionId", sa.String(32), primary_key=True)
    name = sa.Column("Name", sa.String(128), nullable=False)
    ext = sa.Column("Ext", MutableDict.as_mutable(sa.JSON))

    # Itunes Crawler
    ITUNES_ALBUM = "9C8473C36E57472281A1C7936108FC06"
    ITUNES_TRACK = "0001892134894FCF83ABCFC1F39E0DE5"
    ITUNES_ARTIST_ALBUMS = "3FFA9CB0E221416288ACFE96B5810BD2"

    # Auto Crawler
    REPORT_SINGLES = "988CE4D571B4455EA4B9B1904BA92916"
    REPORT_CLASSIC = "E4B85D0A993146EEB84426C2246EFCA0"
    REPORT_ALBUMS = "90ECECF350D94F8C8A16B209CADF9B9E"
    REPORT_RELEASES = "KEFBWAFZNHR00KB9GVMC8VRE3752KAS3"
    JP_TOP_SINGLES = "IYGRBCUV0QYA4STUQP0BTW65OEH8HMV2"
    JP_TOP_ALBUMS = "VQUS3GNK0ESOAUW57AXAIJALZQ6SGYS5"
    JP_ARTIST_ALBUMS = "XQKEDDYWE2VFRH0BJKDFGGUBSM0TKBZ0"
    ACTION_ID_AUTO_CRAWLER_THEME = "1632BBBB2A984ABCA8081C7E821DBB24"
    REPORT_VALIDATOR = "ZL1MORSOVFESAQQNYFM7OHEHPI4XTGL5"

    # youtube crawler
    APP_USER_VIDEO = "9776C9A529D845CE8DF37447C7DE8A83"
    DOWNLOAD_VIDEO_YOUTUBE = "F91244676ACD47BD9A9048CF2BA3FFC1"
    APP2WEB_VIDEO_CONVERTER = "DPMXY1970RJNR9QSQJRVHSP7GOPLJGV3"
    ACTION_ID_CRAWL_MP4_FROM_YOUTUBE = "AE7D97FCEC73492281D6D6506A39902B"
    CONTRIBUTION_TRACK_VIDEO = "1BB6B994C60F4216998282F92D27EDD9"
    CONTRIBUTION_YOUTUBE_COLLECTOR = "A3AWQBJ8S5KAUSFX527JWMAHGKWZX2P0"

    ACTION_ID_USER_CONTRIBUTION_ARTIST_CONTENT = "F30C8B6348C245BF8C910997A5B24427"
    CONTRIBUTION_ALBUM = "348637F531454DD6B1CC108A77C94DB2"
    CONTRIBUTION_TRACK = "6190234708294349B515067FB8E16991"
    CONTRIBUTION_ARTIST = "B06AEE0F622D47F8B6FEF07DC2EABEAE"

    TRACK_LYRIC = "E45ECF4F5F704AEFA7F46F430DFFF832"
    ACTION_ID_CRAWL_SINGLE_IMAGE = "496A12427F0B4A66BB804475D9257060"
    TRACK_NEWS = "9A451D4BC3A74FA0AAEC7A6418FF0DD1"
    ARTIST_NEWS = "8DDFBEDE10BB43D2840B170C4109000E"
    RAW_ARTIST_INFO = "721I15B0NGBY0NB51E3SGUWWCB1QH348"
    ARTIST_ALBUM_IMAGE = "OA9CPKSUT6PBGI1ZHPLQUPQCGVYQ71S9"
    ACTION_ID_CRAWL_ARTIST_IMAGE = "EEF7D2864FBA40178762171284284F95"
    ACTION_ID_CRAWL_ALBUM_IMAGE = "0EB78EA89D2543F5A63B65C12559897B"

    ACTION_ID_CRAWL_YOUTUBE_INFO = "79CFAF49FEFF4425B229ED23A4B37018"
    ACTION_ID_EXTRACT_MP3_FROM_YOUTUBE = "39F7CF4F5EA846EC85666B59FCC5ADD3"

    # Tasks of Spider
    WIKI_URL = "A6KZIKBHIVHYVFKXO8AON16993YOTG15"
    WIKI_ARTIST = "881A2A193A2D41538C71A4998876BC85"
    WIKI_ALBUM = "45E6082757124096AA699AE8015135DE"
    WIKI_TRACK = "98E4D7CA6C494C488A1F960EA96D8CF8"

    # Weekly task
    POPULARITY_TRACK = "PTVHZHY56CWQKYODVID8QYCRBY0NYKJ0"
    POPULARITY_ALBUM = "PTVHZHY56CWQKYODVID8QYCRBY0NYKJ1"
    POPULARITY_ARTIST = "PTVHZHY56CWQKYODVID8QYCRBY0NYKJ2"
    ALBUM_TRACK_PRIORITY = "SL60NQ15ONNQ3RCGUD0PYFBXZBCMGZZ3"

    REPORT_ACTION_IDS = (REPORT_ALBUMS, REPORT_CLASSIC, REPORT_RELEASES, REPORT_SINGLES)
    API_ACTION_IDS = (
        # Itunes Crawler
        ITUNES_ALBUM,
        ITUNES_TRACK,
        ITUNES_ARTIST_ALBUMS,
        # Auto Crawler
        REPORT_SINGLES,
        REPORT_CLASSIC,
        REPORT_ALBUMS,
        REPORT_RELEASES,
        TRACK_NEWS,
        ARTIST_NEWS,
        RAW_ARTIST_INFO,
        CONTRIBUTION_ALBUM,
        CONTRIBUTION_ARTIST,
        CONTRIBUTION_TRACK,
        ARTIST_ALBUM_IMAGE,
        REPORT_VALIDATOR,
        APP2WEB_VIDEO_CONVERTER,
    )
    WEB_ACTION_IDS = (
        WIKI_URL,
        WIKI_ARTIST,
        WIKI_ALBUM,
        WIKI_TRACK,
        TRACK_LYRIC,
        JP_TOP_SINGLES,
        JP_TOP_ALBUMS,
        JP_ARTIST_ALBUMS,
    )
    VIDEO_ACTION_IDS = [
        DOWNLOAD_VIDEO_YOUTUBE,
        CONTRIBUTION_TRACK_VIDEO,
        APP_USER_VIDEO,
        CONTRIBUTION_YOUTUBE_COLLECTOR,
    ]

    NORMAL_TASK_ACTION_IDS = API_ACTION_IDS + WEB_ACTION_IDS

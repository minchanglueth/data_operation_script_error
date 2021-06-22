from core.models.data_source_format_master import DataSourceFormatMaster

class crawlingtaskactionid:
    # Itunes Crawler
    ITUNES_ALBUM = "9C8473C36E57472281A1C7936108FC06"
    ITUNES_TRACK = "0001892134894FCF83ABCFC1F39E0DE5"
    ITUNES_ARTIST_ALBUMS = "3FFA9CB0E221416288ACFE96B5810BD2"
    RAW_ARTIST_INFO = "721I15B0NGBY0NB51E3SGUWWCB1QH348"

    # Crawl youtube:
    DOWNLOAD_VIDEO_YOUTUBE = "F91244676ACD47BD9A9048CF2BA3FFC1"
    CONTRIBUTION_TRACK_VIDEO = "1BB6B994C60F4216998282F92D27EDD9"
    APP_USER_VIDEO = "9776C9A529D845CE8DF37447C7DE8A83"
    CONTRIBUTION_YOUTUBE_COLLECTOR = "A3AWQBJ8S5KAUSFX527JWMAHGKWZX2P0"

    # App to web converter:
    APP2WEB_VIDEO_CONVERTER = "DPMXY1970RJNR9QSQJRVHSP7GOPLJGV3"

    # Crawl image:
    ARTIST_ALBUM_IMAGE = "OA9CPKSUT6PBGI1ZHPLQUPQCGVYQ71S9"
    ACTION_ID_CRAWL_ARTIST_IMAGE = "EEF7D2864FBA40178762171284284F95"
    ACTION_ID_CRAWL_ALBUM_IMAGE = "0EB78EA89D2543F5A63B65C12559897B"

    # Tasks of Spider
    WIKI_URL = "A6KZIKBHIVHYVFKXO8AON16993YOTG15"
    WIKI_ARTIST = "881A2A193A2D41538C71A4998876BC85"
    WIKI_ALBUM = "45E6082757124096AA699AE8015135DE"
    WIKI_TRACK = "98E4D7CA6C494C488A1F960EA96D8CF8"
    TRACK_LYRIC = "E45ECF4F5F704AEFA7F46F430DFFF832"
    TRACK_NEWS = "9A451D4BC3A74FA0AAEC7A6418FF0DD1"
    ARTIST_NEWS = "8DDFBEDE10BB43D2840B170C4109000E"


class when_exist:
    SKIP = "skip"
    REPLACE = "replace"
    KEEP_BOTH = "keep both"


def generate_task(action_id: str, object_id: str, task_detail: dict) -> str:
    jdjfd


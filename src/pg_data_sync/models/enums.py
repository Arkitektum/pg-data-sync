from enum import Enum


class AreaType(str, Enum):
    LANDSDEKKENDE = 'landsdekkende'
    FYLKE = 'fylke'
    KOMMUNE = 'kommune'


class Format(str, Enum):
    FGDB = 'FGDB'
    POST_GIS = 'PostGIS'

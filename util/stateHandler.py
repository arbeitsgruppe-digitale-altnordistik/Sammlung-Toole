from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, List

import pandas as pd

from util.datahandler import DataHandler
from util.utils import SearchOptions


class Step:
    class Handrit_URL(Enum):
        Preprocessing = auto()
        Processing = auto()
        Postprocessing = auto()

    class MS_by_Pers(Enum):
        Search_person = 1
        Store_Results = 2

    class Pers_by_Ms(Enum):
        Search_Ms = 1
        Store_Results = 2

    class MS_by_Txt(Enum):
        Search_Txt = 1
        Store_Results = 2

    class Txt_by_Ms(Enum):
        Search_Ms = 1
        Store_Results = 2

    class Browse_Groups(Enum):
        Browse = 1
        Combine_MSS = 2
        Combine_TXT = 3
        Combine_PPL = 4

    def reset(self) -> None:
        self.browseGroups = Step.Browse_Groups.Browse
        self.search_mss_by_persons = Step.MS_by_Pers.Search_person
        self.search_ppl_by_mss = Step.Pers_by_Ms.Search_Ms
        self.search_mss_by_txt = Step.MS_by_Txt.Search_Txt

    browseGroups: Browse_Groups = Browse_Groups.Browse
    search_mss_by_persons: MS_by_Pers = MS_by_Pers.Search_person
    search_ppl_by_mss: Pers_by_Ms = Pers_by_Ms.Search_Ms
    search_mss_by_txt: MS_by_Txt = MS_by_Txt.Search_Txt
    search_txt_by_mss: Txt_by_Ms = Txt_by_Ms.Search_Ms


class SearchState:
    @dataclass
    class MS_by_Pers:
        mss: List[str] = field(default_factory=list)
        ppl: List[str] = field(default_factory=list)
        mode: SearchOptions = SearchOptions.CONTAINS_ALL

    @dataclass
    class Pers_by_MS:
        mss: List[str] = field(default_factory=list)
        ppl: List[str] = field(default_factory=list)
        mode: SearchOptions = SearchOptions.CONTAINS_ALL

    @dataclass
    class MS_by_Txt:
        mss: pd.DataFrame = field(default_factory=pd.DataFrame)
        txt: List[str] = field(default_factory=list)
        mode: SearchOptions = SearchOptions.CONTAINS_ALL

    @dataclass
    class Txt_by_MS:
        mss: List[str] = field(default_factory=list)
        txt: List[str] = field(default_factory=list)
        mode: SearchOptions = SearchOptions.CONTAINS_ALL

    ms_by_pers = MS_by_Pers()
    pers_by_ms = Pers_by_MS()
    ms_by_txt = MS_by_Txt()
    txt_by_ms = Txt_by_MS()


class StateHandler:

    def __init__(self) -> None:
        self.currentData = pd.DataFrame()
        self.currentURLs_str: str = ''
        self.currentURL_list: List[str] = []
        self.joinMode = 'All'
        self.didRun = 'dnr'
        self.CitaviSelect: Any = []
        self.handrit_step: Step.Handrit_URL = Step.Handrit_URL.Preprocessing
        self.postStep = ''
        self.currentCitaviData = pd.DataFrame()
        self.searchState = SearchState()
        self.steps: Step = Step()
        self.data_handler: DataHandler = None  # type: ignore

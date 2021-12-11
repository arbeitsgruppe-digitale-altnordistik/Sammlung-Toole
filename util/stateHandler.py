from typing import Any, List, Optional
import pandas as pd
from util.datahandler import DataHandler
from enum import Enum, auto
from dataclasses import dataclass, field

from util.utils import SearchOptions

# LATER: this could be divided with nested classes for more order


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

    class Browse_Groups(Enum):
        Browse = "browse"
        Combine_MSS = "combine_manuscripts"

    def reset(self) -> None:
        self.browseGroups = Step.Browse_Groups.Browse
        self.search_mss_by_persons = Step.MS_by_Pers.Search_person
        self.search_ppl_by_mss = Step.Pers_by_Ms.Search_Ms

    browseGroups: Browse_Groups = Browse_Groups.Browse
    search_mss_by_persons: MS_by_Pers = MS_by_Pers.Search_person
    search_ppl_by_mss: Pers_by_Ms = Pers_by_Ms.Search_Ms


class SearchState:
    @dataclass
    class MS_by_Pers:
        mss: List[str]
        ppl: List[str]
        mode: SearchOptions


class StateHandler:
    def __init__(self) -> None:
        self.currentData = pd.DataFrame()
        self.resultMode = ''
        self.currentURLs_str: str = ''
        self.currentURL_list: List[str] = []
        self.currentSURL: str = ''
        self.currentBURL = ''  # TODO: check what's still used
        self.URLType = ''
        # self.multiSearch = 'False'
        self.multiBrowse = 'False'
        self.joinMode = 'All'
        self.didRun = 'dnr'
        self.CitaviSelect: Any = []
        self.handrit_step: Step.Handrit_URL = Step.Handrit_URL.Preprocessing
        # self.ms_by_pers_step: Step.MS_by_Pers = Step.MS_by_Pers.Search_person
        self.postStep = ''
        self.currentCitaviData = pd.DataFrame()
        self.data_handler: DataHandler = None  # type: ignore
        self.search_ms_by_person_result_mss: List[str] = []
        self.search_ms_by_person_result_ppl: List[str] = []
        self.search_ms_by_person_result_mode: SearchOptions = SearchOptions.CONTAINS_ALL
        self.search_person_by_ms_result_mss: List[str] = []
        self.search_person_by_ms_result_ppl: List[str] = []
        self.search_person_by_ms_result_mode: SearchOptions = SearchOptions.CONTAINS_ALL
        self.steps: Step = Step()

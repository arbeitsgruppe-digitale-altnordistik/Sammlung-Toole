from dataclasses import dataclass, field
from enum import Enum

from src.lib.utils import SearchOptions


class Step:

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
        Meta_MSS = 5

    browseGroups: Browse_Groups = Browse_Groups.Browse
    search_mss_by_persons: MS_by_Pers = MS_by_Pers.Search_person
    search_ppl_by_mss: Pers_by_Ms = Pers_by_Ms.Search_Ms
    search_mss_by_txt: MS_by_Txt = MS_by_Txt.Search_Txt
    search_txt_by_mss: Txt_by_Ms = Txt_by_Ms.Search_Ms


class SearchState:
    @dataclass
    class MS_by_Pers:
        mss: list[str] = field(default_factory=list)
        ppl: list[str] = field(default_factory=list)
        mode: SearchOptions = SearchOptions.CONTAINS_ALL

    @dataclass
    class Pers_by_MS:
        mss: list[str] = field(default_factory=list)
        ppl: list[str] = field(default_factory=list)
        mode: SearchOptions = SearchOptions.CONTAINS_ALL

    @dataclass
    class MS_by_Txt:
        mss: list[str] = field(default_factory=list)
        txt: list[str] = field(default_factory=list)
        mode: SearchOptions = SearchOptions.CONTAINS_ALL

    @dataclass
    class Txt_by_MS:
        mss: list[str] = field(default_factory=list)
        txt: list[str] = field(default_factory=list)
        mode: SearchOptions = SearchOptions.CONTAINS_ALL

    ms_by_pers = MS_by_Pers()
    pers_by_ms = Pers_by_MS()
    ms_by_txt = MS_by_Txt()
    txt_by_ms = Txt_by_MS()


class StateHandler:

    def __init__(self) -> None:
        self.searchState = SearchState()
        self.steps: Step = Step()

    def store_ms_by_person_search_state(self, mss: list[str], ppl: list[str], mode: SearchOptions) -> None:
        """Update the state after searching manuscripts by people"""
        self.searchState.ms_by_pers.mss = mss
        self.searchState.ms_by_pers.ppl = ppl
        self.searchState.ms_by_pers.mode = mode
        self.steps.search_mss_by_persons = Step.MS_by_Pers.Store_Results

    def store_ppl_by_ms_search_state(self, ppl: list[str], mss: list[str], mode: SearchOptions) -> None:
        """Update the state after searching people by manuscripts"""
        self.searchState.pers_by_ms.ppl = ppl
        self.searchState.pers_by_ms.mss = mss
        self.searchState.pers_by_ms.mode = mode
        self.steps.search_ppl_by_mss = Step.Pers_by_Ms.Store_Results

    def store_ms_by_txt_search_state(self, mss: list[str], txt: list[str], mode: SearchOptions) -> None:
        """Update the state after searching manuscripts by texts"""
        self.searchState.ms_by_txt.mss = mss
        self.searchState.ms_by_txt.txt = txt
        self.searchState.ms_by_txt.mode = mode
        self.steps.search_mss_by_txt = Step.MS_by_Txt.Store_Results

    def store_txt_by_ms_search_state(self, txt: list[str], mss: list[str], mode: SearchOptions) -> None:
        """Update the state after searching texts by manuscripts"""
        self.searchState.txt_by_ms.txt = txt
        self.searchState.txt_by_ms.mss = mss
        self.searchState.txt_by_ms.mode = mode
        self.steps.search_txt_by_mss = Step.Txt_by_Ms.Store_Results

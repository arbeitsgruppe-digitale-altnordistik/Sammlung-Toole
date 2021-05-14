from typing import Any, List, Optional
import pandas as pd
from datahandler import DataHandler


class StateHandler:
    def __init__(self) -> None:
        self.currentData = pd.DataFrame()
        self.resultMode = ''
        self.currentURLs_str: str = ''
        self.currentURL_list: List[str] = []
        self.currentSURL: str = ''
        self.currentBURL = ''
        self.URLType = ''
        # self.multiSearch = 'False'
        self.multiBrowse = 'False'
        self.joinMode = 'All'
        self.didRun = 'dnr'
        self.CitaviSelect: Any = []
        self.CurrentStep = 'Preprocessing'
        self.postStep = ''
        self.currentCitaviData = pd.DataFrame()
        self.data_handler: DataHandler = None  # type: ignore

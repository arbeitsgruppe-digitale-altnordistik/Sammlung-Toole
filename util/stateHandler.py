from typing import Any, Optional
import pandas as pd
from datahandler import DataHandler


class StateHandler:
    data_handler: DataHandler

    def __init__(self) -> None:
        self.currentData = pd.DataFrame()
        self.resultMode = ''
        self.currentSURL = ''
        self.currentBURL = ''
        self.URLType = ''
        self.multiSearch = 'False'
        self.multiBrowse = 'False'
        self.joinMode = 'All'
        self.didRun = 'dnr'
        self.CitaviSelect: Any = []
        self.CurrentStep = 'Preprocessing'
        self.postStep = ''
        self.currentCitaviData = pd.DataFrame()

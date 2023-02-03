import pytest
from lib import stateHandler
from lib.utils import SearchOptions


@pytest.fixture
def state() -> stateHandler.StateHandler:
    return stateHandler.StateHandler()


def test_default(state: stateHandler.StateHandler) -> None:
    assert state.searchState.ms_by_pers == stateHandler.SearchState.MS_by_Pers()
    assert state.searchState.pers_by_ms == stateHandler.SearchState.Pers_by_MS()
    assert state.searchState.ms_by_txt == stateHandler.SearchState.MS_by_Txt()
    assert state.searchState.txt_by_ms == stateHandler.SearchState.Txt_by_MS()
    assert state.steps.browseGroups == stateHandler.Step.Browse_Groups.Browse
    assert state.steps.search_mss_by_persons == stateHandler.Step.MS_by_Pers.Search_person
    assert state.steps.search_ppl_by_mss == stateHandler.Step.Pers_by_Ms.Search_Ms
    assert state.steps.search_mss_by_txt == stateHandler.Step.MS_by_Txt.Search_Txt
    assert state.steps.search_txt_by_mss == stateHandler.Step.Txt_by_Ms.Search_Ms


def test_store_ms_by_person_search_state(state: stateHandler.StateHandler) -> None:
    # check defaults
    assert state.searchState.ms_by_pers.mss == []
    assert state.searchState.ms_by_pers.ppl == []
    assert state.searchState.ms_by_pers.mode == SearchOptions.CONTAINS_ALL
    assert state.steps.search_mss_by_persons == stateHandler.Step.MS_by_Pers.Search_person
    # update values
    new_mss = ["a", "b", "c"]
    new_ppl = ["x", "y", "z"]
    new_mode = SearchOptions.CONTAINS_ONE
    state.store_ms_by_person_search_state(mss=new_mss, ppl=new_ppl, mode=new_mode)
    # check updated values
    assert state.searchState.ms_by_pers.mss == new_mss
    assert state.searchState.ms_by_pers.ppl == new_ppl
    assert state.searchState.ms_by_pers.mode == new_mode
    assert state.steps.search_mss_by_persons == stateHandler.Step.MS_by_Pers.Store_Results


def test_store_ppl_by_ms_search_state(state: stateHandler.StateHandler) -> None:
    # check defaults
    assert state.searchState.pers_by_ms.mss == []
    assert state.searchState.pers_by_ms.ppl == []
    assert state.searchState.pers_by_ms.mode == SearchOptions.CONTAINS_ALL
    assert state.steps.search_ppl_by_mss == stateHandler.Step.Pers_by_Ms.Search_Ms
    # update values
    new_mss = ["a", "b", "c"]
    new_ppl = ["x", "y", "z"]
    new_mode = SearchOptions.CONTAINS_ONE
    state.store_ppl_by_ms_search_state(mss=new_mss, ppl=new_ppl, mode=new_mode)
    # check updated values
    assert state.searchState.pers_by_ms.mss == new_mss
    assert state.searchState.pers_by_ms.ppl == new_ppl
    assert state.searchState.pers_by_ms.mode == new_mode
    assert state.steps.search_ppl_by_mss == stateHandler.Step.Pers_by_Ms.Store_Results


def test_store_ms_by_txt_search_state(state: stateHandler.StateHandler) -> None:
    # check defaults
    assert state.searchState.ms_by_txt.mss == []
    assert state.searchState.ms_by_txt.txt == []
    assert state.searchState.ms_by_txt.mode == SearchOptions.CONTAINS_ALL
    assert state.steps.search_mss_by_txt == stateHandler.Step.MS_by_Txt.Search_Txt
    # update values
    new_mss = ["a", "b", "c"]
    new_txt = ["x", "y", "z"]
    new_mode = SearchOptions.CONTAINS_ONE
    state.store_ms_by_txt_search_state(mss=new_mss, txt=new_txt, mode=new_mode)
    # check updated values
    assert state.searchState.ms_by_txt.mss == new_mss
    assert state.searchState.ms_by_txt.txt == new_txt
    assert state.searchState.ms_by_txt.mode == new_mode
    assert state.steps.search_mss_by_txt == stateHandler.Step.MS_by_Txt.Store_Results


def test_store_txt_by_ms_search_state(state: stateHandler.StateHandler) -> None:
    # check defaults
    assert state.searchState.txt_by_ms.mss == []
    assert state.searchState.txt_by_ms.txt == []
    assert state.searchState.txt_by_ms.mode == SearchOptions.CONTAINS_ALL
    assert state.steps.search_txt_by_mss == stateHandler.Step.Txt_by_Ms.Search_Ms
    # update values
    new_mss = ["a", "b", "c"]
    new_txt = ["x", "y", "z"]
    new_mode = SearchOptions.CONTAINS_ONE
    state.store_txt_by_ms_search_state(mss=new_mss, txt=new_txt, mode=new_mode)
    # check updated values
    assert state.searchState.txt_by_ms.mss == new_mss
    assert state.searchState.txt_by_ms.txt == new_txt
    assert state.searchState.txt_by_ms.mode == new_mode
    assert state.steps.search_txt_by_mss == stateHandler.Step.Txt_by_Ms.Store_Results

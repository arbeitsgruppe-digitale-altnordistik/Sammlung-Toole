from typing import Optional
import streamlit as st


class Progress:
    def __init__(self, start_msg: str = None, end_msg: str = None, parent_task=None):
        self.__container = st.beta_container()
        self.__steps = 0
        self.__state = 0
        self.__parent = parent_task
        self.__end_msg = end_msg
        with self.__container:
            if start_msg:
                if self.__parent:
                    st.text(start_msg)
                else:
                    st.subheader(start_msg)
            self.__progressbar = st.progress(0)

    def set_steps(self, steps: int):
        self.__steps = steps

    def increment(self, msg: Optional[str] = None):
        self.__state += 1
        percentage = int(100 * self.__state / self.__steps)
        self.__progressbar.progress(percentage)
        if msg:
            with self.__container:
                st.text(msg)
        if self.__state >= self.__steps:
            self.done()

    def next_subtask(self, start_msg=None, end_msg=None):
        with self.__container:
            return Progress(start_msg=start_msg, end_msg=end_msg, parent_task=self)

    def done(self):
        self.__progressbar.progress(100)
        if self.__end_msg:
            with self.__container:
                if self.__parent:
                    st.text(self.__end_msg)
                else:
                    st.markdown(f'**{self.__end_msg}**')
        if self.__parent:
            self.__parent.increment()

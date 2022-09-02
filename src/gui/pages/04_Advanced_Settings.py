import streamlit as st
from src.lib import tamer
from streamlit.components.v1 import html


def nav_page(page_name: str, timeout_secs: int = 3) -> None:
    # Hack from https://github.com/streamlit/streamlit/issues/4832#issuecomment-1201938174
    nav_script = """
        <script type="text/javascript">
            function attempt_nav_page(page_name, start_time, timeout_secs) {
                var links = window.parent.document.getElementsByTagName("a");
                for (var i = 0; i < links.length; i++) {
                    if (links[i].href.toLowerCase().endsWith("/" + page_name.toLowerCase())) {
                        links[i].click();
                        return;
                    }
                }
                var elasped = new Date() - start_time;
                if (elasped < timeout_secs * 1000) {
                    setTimeout(attempt_nav_page, 100, page_name, start_time, timeout_secs);
                } else {
                    alert("Unable to navigate to page '" + page_name + "' after " + timeout_secs + " second(s).");
                }
            }
            window.addEventListener("load", function() {
                attempt_nav_page("%s", new Date(), %d);
            });
        </script>
    """ % (page_name, timeout_secs)
    html(nav_script)


# LATER: At some point we should consider changing crawling into a background task
st.title("Advanced Options Menu")
st.write("Careful! Some of these options can take a long time to complete! Like, a loooong time!")
st.warning("There will be no confirmation on any of these! Clicking any of the option without thinking first is baaad juju!")

if st.button("Wipe cache"):
    tamer._wipe_cache()
    st.success("Cache is wiped entirely. Please reload the data handler.")
if st.button("Reload Data Handler"):
    st.experimental_singleton.clear()  # type: ignore
    nav_page("")

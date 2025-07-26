import streamlit as st
from PIL import Image
from pathlib import Path
from datetime import datetime
import json

# === Page Config ===
favicon_path = "assets/icons/favicon.png"
favicon = Image.open(favicon_path) if Path(favicon_path).is_file() else "🤖"

st.set_page_config(
    page_title="BrokeTechBro",
    page_icon=favicon,
    layout="centered"
)

# === Hero Section ===
logo_path = "assets/icons/logo.png"
if Path(logo_path).is_file():
    st.image(logo_path, width=250)
else:
    st.markdown("🤖")



# === Initialize Session ===

def initialize_session_state():
    defaults = {
        "selected_category": "FAQs" #open services by default
        }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


# === Load Knowledge Base JSON ===
def render_kb():
    with open("assets/docs/kb.json", "r") as f:
        kb = json.load(f)

    # === Render Category Buttons ===
    # st.markdown("### 📚 Knowledge Base")
    cols = st.columns(len(kb["knowledge_base"]))
    for idx, section in enumerate(kb["knowledge_base"]):
        with cols[idx]:
            if st.button(section["category"]):
                st.session_state.selected_category = section["category"]

    # === Display Selected Section ===
    selected = st.session_state.selected_category
    if selected:
        section_data = next(
            (sec for sec in kb["knowledge_base"] if sec["category"] == selected), None
        )
        if section_data:
            st.markdown(f"##### {selected}")
            for entry in section_data["entries"]:
                with st.expander(entry["title"]):
                    st.markdown(entry["description"])



def footer():
    # === Footer Navigation Row ===
    st.divider()
    with st.container():
        #st.markdown("---")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.page_link("pages/1_About.py", label="👤 About", icon=None)

        with col2:
            st.page_link("pages/2_Contact.py", label="✉️ Contact", icon=None)

        with col3:
            st.page_link("pages/3_Projects.py", label="📂 Projects", icon=None)

        with col4:
            st.markdown(
                '<a href="https://medium.com/@brokeTechBro" target="_blank">📝 Blog</a>',
                unsafe_allow_html=True
            )

    # # === Footer ===
    # st.divider()
    # st.markdown(
    #     "<p style='text-align: center; color: #6E2FD6;'>Break fast, build forward - I bet you've never heard that one before</p>",
    #     unsafe_allow_html=True
    # )

initialize_session_state()
render_kb()
footer()


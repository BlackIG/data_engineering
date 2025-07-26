import streamlit as st
import json
from PIL import Image
from pathlib import Path

# === Load Projects from JSON ===
with open("assets/docs/projects.json") as f:
    projects = json.load(f)

# === Page Header ===
st.title("📂 Projects")
st.markdown("---")
# === Display Projects in Shelves ===
for project in projects:
    with st.container():
        col1, col2 = st.columns([1, 2])

        # Left: Optional image
        if project["img"] and Path(project["img"]).is_file():
            img = Image.open(project["img"])
            col1.image(img, width=200)
        else:
            col1.markdown("📷 *No image*")

        # Right: Details
        col2.markdown(f"### [{project['title']}]({project['url']})")
        col2.markdown(project["text"])
        
        st.markdown("---")

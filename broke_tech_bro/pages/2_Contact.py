import streamlit as st
from PIL import Image

# === Layout: Two Columns for About ===
st.title("✉️ Contact")

col1, col2 = st.columns([1.2, 2])

# === Left: Profile Image ===
with col1:
    profile_path = "assets/photos/about_photo/photo_working.jpeg"
    profile_img = Image.open(profile_path)
    st.image(profile_img, width=250, caption="Ikechukwu Chilaka")

# === Right: Contact Details ===
with col2:
    st.markdown("""
        <div style='font-size:16px; line-height:1.8'>
            <p><strong>LinkedIn:</strong> <a href='https://www.linkedin.com/in/chilakaig/' target='_blank'>chilakaig</a></p>
            <p><strong>Email:</strong> <a href='mailto:chilaka.ig@gmail.com'>chilaka.ig@gmail.com</a></p>
        </div>
    """, unsafe_allow_html=True)

import streamlit as st
from PIL import Image
import os
import time


# === Paths ===
event_path = "assets/photos/event_photo"

# === About Section ===
st.title("👤 About")

st.markdown("""
<div style='font-size:14px; line-height:1.6'>
Hi, I’m Ikechukwu Chilaka, a data engineer and technology strategist with more than seven years of professional experience in customer experience, data analytics, and data engineering.
<br><br>My career has been centered around solving real-world business problems through scalable data systems, automation, and analytics that inform impactful decisions. Over the years, I have worked with prominent financial institutions such as United Bank for Africa (UBA) and Carbon Microfinance Bank, where I led and contributed to key projects that improved operational efficiency, enhanced customer outcomes, and drove data-informed strategies across teams.<br><br>

In addition to my roles in the corporate sector, I am the Chief Data Strategist and Co-founder of contactOZ, a startup dedicated to enabling organizations across Nigeria to build truly customer-centric workforces.
I also hold membership with the British Computer Society (BCS), the Chartered Institute for IT, where I continue to engage in global conversations around professionalism, ethics, and emerging best practices in data and technology.

At the core of everything I do is a desire to bridge the gap between technology and people. Whether I’m building automated systems, designing scalable data workflows, or mentoring others in tech, I’m driven by the belief that people should always come first.
</div>
""", unsafe_allow_html=True)

# === Divider ===
#st.markdown("---")

# === Load Valid Images ===
valid_extensions = [".jpg", ".jpeg", ".png", ".webp"]
event_images = sorted([
    img for img in os.listdir(event_path)
    if os.path.splitext(img)[1].lower() in valid_extensions
])
img_paths = [os.path.join(event_path, img) for img in event_images]
total_images = len(img_paths)

# === Index Control ===
if "slideshow_index" not in st.session_state:
    st.session_state.slideshow_index = 0

# Compute left and right image indexes
idx_left = st.session_state.slideshow_index % total_images
idx_right = (st.session_state.slideshow_index + 1) % total_images

# === Display Side-by-Side ===
col1, col2 = st.columns(2)

with col1:
    img_left = Image.open(img_paths[idx_left])
    st.image(img_left, width=350)

with col2:
    img_right = Image.open(img_paths[idx_right])
    st.image(img_right, width=350)

# === Loop ===
time.sleep(3)
st.session_state.slideshow_index = (st.session_state.slideshow_index + 1) % total_images
st.rerun()



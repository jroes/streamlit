import streamlit as st
name = st.text_input("name", key="name")
st.write("hello", name)
